# telemetryd som base for en multi-tenant SaaS

Undersøgelse, oktober 2026. Kigget på `main` (0.73.0, ClickHouse som store).
Målet: en Nightwatch-lignende hostet løsning, hvor kunder køber en kvote (fx X spans
om måneden), og hvor telemetry-ui og et API sidder ovenpå.

## TLDR

Det kan godt lade sig gøre, og fundamentet er godt: OTLP-ingest, ClickHouse-backend der
skalerer horisontalt, Loki/Tempo/Prometheus-API'er som telemetry-ui allerede taler, og
et identitetsmønster (relay mode) der næsten er det vi skal bruge. Men telemetryd er
**single-tenant med vilje i dag**. `app` er et query-namespace, ikke en
sikkerhedsgrænse, og multi-tenant isolation står eksplicit som "deliberately out of
scope" i `docs/security/_index.md`.

Det der skal til, groft sagt:

1. **Tenant som førsteklasses begreb i telemetryd**, sat ud fra credential og aldrig
   fra payload, og påtvunget på *alle* læsninger. Det er det største og vigtigste stykke.
2. **Kun ClickHouse-backend til SaaS.** Tenant-kolonne først i alle sorteringsnøgler.
   Parquet-backenden forbliver single-tenant.
3. **Entitlements pr. projekt**: målinger i ClickHouse, en policy pr. projekt fra control plane, og
   per-tenant limits, så én kunde ikke kan sulte de andre.
4. **Et separat control plane** (Laravel-app) til konti, API-nøgler, planer, Cbox
   Billing, forbrug og det offentlige API. telemetryd skal ikke kende til priser.
5. **telemetry-ui i tenant-mode**, med læsetoken pr. tenant i stedet for at stole på
   en `X-Scope-OrgID`-header.

*Besluttet:* tenancy er **lukket**. SaaS'en er et nyt, lukket produkt, hvor data gemmes
i vores cloud. Det åbne telemetryd (MIT) forbliver single-tenant og self-hosted, og kan
som tilvalg sende data videre til cloud'en (se "Lukket kode oven på det åbne
telemetryd" og "Export fra self-hosted til cloud" nedenfor).

## De tre måder at gøre det på

| | Hvordan | Godt til | Problem |
|---|---|---|---|
| A. Instans pr. kunde | En telemetryd (+ evt. egen ClickHouse-database) pr. kunde | Få store kunder, enterprise, "dedicated" | Dyrt og tungt at drive for mange små kunder. Virker i dag uden kodeændringer |
| B. Database pr. kunde i delt ClickHouse | Én ClickHouse, mange databaser, telemetryd vælger database ud fra token | Middelvejen | ClickHouse kan ikke lide tusindvis af tabeller (parts, merges, MV'er ganget op) |
| **C. Delt cluster, tenant-kolonne** | Alle tenants i samme tabeller, `TenantId` først i `ORDER BY` | Nightwatch-modellen: mange små og mellemstore kunder | Isolation skal håndhæves i koden, og det skal testes hårdt |

**Anbefaling: C som produkt, A som "dedicated"-tilvalg** til kunder der kræver det
(og det virker allerede).

## Hvad der skal laves i telemetryd

### 1. Identitet: hvem er tenant?

*Besluttet:* en tenant i telemetryd er et **projekt**. En organisation kan have mange
projekter, men det begreb lever kun i control plane. telemetryd kender kun projekter.
`app` (laravel-telemetrys `service.name`) bliver ved med at være et namespace inden i
projektet.

I dag er credentials statiske fra config og byttes på `SIGHUP`
(`crates/server/src/state.rs:36`). Der findes tre token-sæt (ingest, query, admin),
og så relay-klienter, der binder et token til en `app`
(`crates/server/src/auth.rs`, `ClientIdentity`).

Det skal ændres:

- `ClientIdentity` får en `tenant` ud over `app`.
- **Ingest-nøgler** (det kunden lægger i `.env`, som Nightwatch-tokenet) er opaque
  API-nøgler, hashed, der slås op mod en nøglebutik. Den bliver synket fra control
  plane med cache (pull hvert N sekund, plus push-invalidering ved revoke). Det kan ikke
  være config-filer og `SIGHUP` med tusindvis af kunder.
- **Læsetokens** (UI og API) er kortlivede JWT'er med et `tenant`-claim, udstedt af
  Cbox ID eller control plane. `crates/server/src/oidc.rs` validerer allerede JWT'er
  og læser `client_id`, så der skal kun et claim mere til.
- Admin-overfladen (`/status`, `/metrics`, `/debug`) er kun for os, aldrig for tenants.

### 2. Ingest: stemple tenant, aldrig stole på payload

`stamp()` i `crates/server/src/ingest.rs:122` gør allerede præcis det her for `app` i
relay mode: overskriver labelen ud fra credential og tæller forsøg på at snyde. Den skal
generaliseres, så tenant altid stemples i tenant-mode, og en `tenant`-label i payload
smides væk. Alle ingest-ruter (OTLP HTTP, OTLP gRPC, `remote_write`, Loki push) går
gennem samme router og guard (`crates/server/src/grpc.rs` sender gRPC videre som HTTP),
så det er ét sted.

### 3. Storage: tenant først i ClickHouse-skemaet

`crates/store/src/clickhouse/schema.rs` sorterer i dag på `ServiceName` først
(`otel_logs`, `otel_traces`, metrics, rollups, `otel_trace_keys`). Det bliver:

- `TenantId LowCardinality(String)` (eller `UInt64`) på alle tabeller og materialized
  views, og **først** i `ORDER BY`. Så prunes der på tenant før alt andet, og én
  kundes forespørgsel læser ikke andres data fra disk.
- Trace-id-tabellen skal være `ORDER BY (TenantId, TraceId)`. Ellers er et trace-id
  nok til at hente en andens trace (se næste punkt).
- Migrering via den eksisterende `migrate.rs`. For en ny SaaS-installation er det
  nemmest bare at oprette skemaet med tenant fra start.
- Defense in depth: ClickHouse **row policies** (`USING TenantId = getSetting(...)`)
  med tenant sat pr. forespørgsel. Så lækker en glemt `WHERE` ikke data.

### 4. Læsninger: alle forespørgsler scopes (det kritiske)

Query-motorerne går allerede gennem tre traits: `LogStore` og `SpanStore`
(`crates/store/src/backend.rs:70`, `:249`) og `MetricStore`
(`crates/store/src/metric_backend.rs:21`), ca. 20 metoder i alt. Det er det rigtige
choke point: en wrapper der lægger `tenant="…"` på alle matchers.

Men flere metoder tager **ingen matchers**, og dem skal signaturen ændres på:

- `SpanStore::trace(trace_id)` og `of_traces(...)`: uden tenant kan man hente en anden
  kundes trace, hvis man kender eller gætter id'et. Det er den mest oplagte lækage.
- `label_names` / `label_values` på alle tre: uden tenant afslører de andre kunders
  app-navne, label-værdier og metric-navne.
- `oldest_sample_nanos` og andre "hvad har storen"-kald.

Derudover skal disse overflader scopes eller lukkes:

- `/loki/api/v1/tail` (live tail poller ClickHouse hvert sekund)
- `/api/v1/export`
- Tempo search (`/api/search`, tags), Prometheus- og Loki-labels/series
- Interne caches, folds og label-discovery skal keyes på tenant
- `/debug`, `/status` og `/metrics` kun på admin-overfladen

`X-Scope-OrgID` ignoreres i dag (`docs/cookbook/connect-the-ui.md:126`). I tenant-mode
må den højst bruges som et hint, der skal matche tokenets tenant. Ellers bliver den
afvist. Den må aldrig selv vælge tenant.

Tests: en kontrakttest-suite der skriver data for tenant A og B og asserter, at hver
eneste route kun returnerer A's data med A's token. Den skal køres mod ClickHouse i CI.

### 5. Limits pr. tenant (noisy neighbour)

Alle limits er i dag globale eller pr. `app`, og pr. instans: `max_series`,
`max_series_per_app`, `ingest_queue_depth`, query-concurrency og `max_query_samples`.
Bag en load balancer tæller series-limits pr. instans
(`docs/cookbook/scale-out-on-clickhouse.md`, "What is per instance").

Det skal til:

- Køandel pr. tenant. Relay mode har allerede `max_queue_share`
  (`docs/cookbook/relay-mode.md`), og det mønster kan genbruges direkte.
- Query-slots og sample-loft pr. tenant, så én tung dashboard-forespørgsel ikke tager
  alle slots.
- Series-loft pr. tenant, fra planen. Enten route efter tenant på load balanceren
  (consistent hashing), så tællingen er præcis, eller accepter "op til N gange" og
  størrelsesjuster.

### 6. Retention: mindst 90 dage, custom pr. kunde/projekt/plan

*Besluttet:* mindst 90 dage og frit sat pr. plan, kunde eller projekt. Det gælder alle
signaler. Metrics har i dag 30 dage som default.

TTL er i dag pr. tabel og sat fra config (`schema.rs:270`). En hel tabel deler altså én
retention, og det holder ikke længere. Forslag:

- En `RetentionDays`-kolonne på hver række, sat ved ingest ud fra projektets aktuelle
  retention (fra control plane, se punkt 7).
- `PARTITION BY (RetentionDays, toMonday(Timestamp))`, og en baggrundsopgave i
  telemetryd dropper partitioner, hvor `uge + RetentionDays` er passeret. Udløb forbliver
  dermed at droppe hele partitioner, som er billigt. Row-level TTL på blandede
  partitioner betyder merges og omskrivning, som er dyrt i den skala.
- Ugepartitioner i stedet for dagpartitioner, fordi custom retention ganger antallet
  af partitioner op: 20 forskellige værdier × 400 dage i dagpartitioner er 8.000
  partitioner pr. tabel, og det kan ClickHouse ikke lide. Med uger er det ca. 1.100.
  Prisen er, at data lever op til 7 dage ekstra. Det er fint, når løftet er
  "mindst N dage".
- Control plane bør runde custom-værdier til hele uger eller måneder. Det holder antallet
  af værdier nede og ændrer ikke noget for kunden.

**Når retention ændres** (en kunde opgraderer fra 90 til 365 dage): nye data får den nye
værdi med det samme. Eksisterende data beholder den gamle, medmindre vi kører en
`ALTER TABLE … UPDATE RetentionDays` for projektet. Det er en mutation, så den er dyr,
men den er sjælden og kan køres som et job. Ved nedgradering er det rimeligt kun at lade
den gælde nye data.

Desuden: sletning af et helt projekt ved opsigelse eller GDPR
(`ALTER TABLE … DELETE WHERE TenantId = …` på alle tabeller), som en admin-kommando.

### 7. Entitlements, måling og håndhævelse

*Besluttet:* alt med en grænse er et entitlement. Overforbrug kan tillades og faktureres,
eller der er et soft limit på X % over kvoten, eller nye data afvises. Det afhænger af
entitlementet.

**Rollefordeling.** telemetryd skal hverken kende planer, priser eller organisationer.
Den måler, rapporterer og håndhæver en *policy pr. projekt*, som control plane har
regnet ud. Control plane ejer entitlements, pooling på tværs af en organisations
projekter og fakturering. Så kan en kvote, der deles af en organisations ti projekter,
ændre sig uden kodeændringer i telemetryd.

**Hvad telemetryd måler** (pr. projekt, pr. time, pr. signal), i en materialized view
`usage_hourly` (`SummingMergeTree`, nøgle `TenantId, Hour, Signal`):

| Måler | Kilde |
|---|---|
| spans, loglinjer, metric-samples | rækker ved insert |
| bytes ingested | rå størrelse ved insert |
| aktive metric-serier | den eksisterende serietælling, pr. projekt |
| lagret volumen | `system.parts` pr. projekt og partition (bruges allerede til `telemetryd_app_bytes_estimate`) |
| forespørgsler / læste rækker | tælles i query-laget, flushes til ClickHouse |

Det virker på tværs af alle instanser, overlever genstarter og kan læses direkte af
control plane. `telemetryd_ingest_accepted_total` har i dag kun en `signal`-label, og
tenant må ikke blive en Prometheus-label (kardinalitet).

Entitlements, der ikke er volumen, håndhæves de steder, de hører til:

| Entitlement | Håndhæves af |
|---|---|
| spans/logs/samples pr. periode | telemetryd ved ingest, efter policy |
| aktive serier | telemetryd, som `max_series_per_app` i dag |
| retention | telemetryd via `RetentionDays` |
| query-concurrency, rate | telemetryd |
| projekter pr. organisation, brugere, API-nøgler | control plane |
| features (live tail, export, alerts …) | telemetryd pr. route, ud fra en feature-liste i policyen |

**Policyen pr. projekt.** Det er det, telemetryd henter fra control plane: cachet,
pull hvert ~30 sekund plus push ved ændringer, og eventual consistent. Pr. måler står der
en tilstand:

- `accept`: under kvoten, eller overforbrug faktureres uden loft
- `accept_until(n)`: soft limit, så modtag op til `n` mere i perioden og afvis derefter.
  telemetryd tæller lokalt fra det tal, så et minuts forsinkelse ikke bliver til
  ubegrænset overforbrug.
- `reject`: hard limit nået, eller projektet er suspenderet

Med flere instanser tæller hver for sig. `accept_until(n)` kan derfor overskrides med op
til (instanser × det, der kom ind før næste sync). Det er acceptabelt for et *soft*
limit. Skal det være skarpt, så route pr. projekt på load balanceren.

Uden fast loft er overforbrug fakturering og ikke håndhævelse: control plane læser
`usage_hourly`, trækker kvoten fra og sender resten til Cbox Billing som forbrug.

**Svaret til senderen, når der afvises**: *ikke* `429`/`503`. OTLP-exportere prøver dem
igen, og så får vi en retry-storm fra alle kundens servere. Brug et ikke-retrybart svar
(`200` med `partialSuccess` og en besked, der siger "quota exceeded for project X"),
og tæl det, så kunden kan se på forbrugssiden, hvad der er droppet. Advarsler ved 80 og
100 % og ved start på overforbrug kommer fra control plane.

### 8. Sampling (nice to have, men vigtig for "X spans")

Når kunderne betaler pr. span, vil de styre hvad der sendes. laravel-telemetry bør
kunne sample (head sampling pr. route, eller "kun fejl og langsomme"), så kunden ikke
brænder kvoten af på healthchecks. Det hører i SDK'et og ikke i telemetryd.

## Uden for telemetryd

### Control plane (ny Laravel-app)

Det er her "SaaS'en" bor, og det er sandsynligvis mere arbejde end telemetryd-delen:

- Organisationer → projekter. Et projekt er en tenant i telemetryd. Brugere og roller
  via Cbox ID, på organisations- og projektniveau.
- API-nøgler pr. projekt: oprette, rotere, tilbagekalde, synket til telemetryd
- Entitlement-model: planer giver entitlements, og de kan overstyres pr. organisation
  eller projekt. Pr. entitlement: kvote, pulje (organisation eller projekt) og
  overforbrugsregel (faktureret, soft limit på X %, hard). Ud af den regnes policyen
  pr. projekt, som sendes til telemetryd.
- Cbox Billing: abonnement pr. organisation, forbrug (overforbrug) afregnet månedligt
- Forbrugsside pr. organisation og projekt (læser `usage_hourly`) og mails ved 80/100 %
- **Det offentlige API**: nøgler, forbrug og projekter, og evt. en proxy til
  query-API'erne med kundens token
- Onboarding: "composer require laravel-telemetry, sæt nøglen i `.env`, færdig"
- Admin: tenant-oversigt, suspendering, sletning

### telemetry-ui

UI'et taler allerede Loki/Tempo/Prometheus mod én base-URL og sender `X-Scope-OrgID`,
når en tenant er konfigureret. Jeg har ikke adgang til det repo fra denne opgave, så det
skal tjekkes, men det skal kunne:

- Hente tenant og læsetoken fra den indloggede bruger pr. request i stedet for fra
  statisk config
- Køre embedded i control plane (samme login) eller som selvstændig app mod den
  hostede telemetryd
- Skjule ting, der ikke giver mening for en tenant (instans-status, admin)

### Drift og sikkerhed

- Threat model og `docs/security` skal skrives om for tenant-mode. Lige nu lover de,
  at det ikke er en sikkerhedsgrænse.
- Pentest af isolationen inden lancering
- Kryptering at rest er out of scope i telemetryd, så det klares med krypterede diske hos
  hostingudbyderen
- EU-hosting og databehandleraftale er nok et salgsargument mod Nightwatch
- Backup pr. tenant er svært i delte tabeller. Backup af hele clusteret og "restore ind i
  en midlertidig database og kopier én tenant ud" er nok rigeligt.

## Rækkefølge

1. **Tenancy-kerne**: seams i det åbne telemetryd, og i den lukkede binary identitet,
   stempling, skema med `TenantId`, scoping-wrapper og isolationstests. Kun med
   ClickHouse. *Størst og vigtigst.*
2. **Nøglebutik og sync** fra en minimal control plane (bare tenants og nøgler).
3. **Måling og entitlements**: `usage_hourly`, policy-sync pr. projekt (accept, soft limit, reject), ikke-retrybart afslag.
4. **Per-tenant limits** (køandel, query-slots, serier) og **custom retention**.
5. **Control plane til produkt**: Billing, forbrugsside, offentligt API, onboarding.
6. **telemetry-ui i tenant-mode**.
7. Sikkerhedsdocs, pentest, lancering. Dedicated-instanser (model A) kan sælges fra dag
   ét parallelt med det her.

Punkt 1 til 4 er Rust-arbejde, mest i den lukkede repo, med små generiske seams i det
åbne telemetryd (se nedenfor). Punkt 5 og 6 er Laravel. Første milepæl, der er værd at
sigte efter, er punkt 1 og 2 med to testkunder på et ClickHouse-cluster.

## Lukket kode oven på det åbne telemetryd

telemetryd er bygget som en binary, men workspace'et er allerede delt i publicerbare
crates (`telemetryd-core`, `-store`, `-ingest`, `-query`, `-server`). Tre måder at holde
tenancy lukket:

| | Hvordan | Problem |
|---|---|---|
| Lukket fork | Kopi af repo'et med tenancy flettet ind | Hver upstream-release skal merges ind, for altid. Det bliver dyrere for hver release |
| **Lukket binary oven på de åbne crates** | En privat repo med sin egen `main`, der bruger de åbne crates som biblioteker og lægger tenancy ovenpå | Det åbne telemetryd skal have nogle få generiske seams |
| Plugin | Dynamisk loading | Strider mod "no plugins, a single static binary" i `docs/extension-points` |

**Anbefaling: lukket binary oven på de åbne crates.** De seams, det åbne telemetryd skal
have, er generiske og afslører intet om SaaS'en:

- **Identitet som trait**: en `CredentialResolver`, der går fra request til
  `ClientIdentity`. Den åbne implementation er config og OIDC som i dag, og den lukkede
  slår nøgler op i nøglebutikken og giver en tenant med.
- **Store-wrapper**: de tre store-traits skal kunne wrappes udefra, og
  `trace`/`label_*` skal tage et scope-argument (punkt 4). Signaturændringen er også en
  forbedring for det åbne produkt, fordi `app`-scoping så kan håndhæves samme sted.
- **Skema-udvidelse**: ClickHouse-skemaet skal kunne få ekstra kolonner og en anden
  `ORDER BY`/`PARTITION BY`, eller også ejer den lukkede repo sit eget skema og sin egen
  migrering. Det sidste er nok enklest, fordi `TenantId` og `RetentionDays` alligevel
  ændrer hele nøglen.
- **Ingest-hook** før insert: stempling, entitlement-tjek og tælling (punkt 2 og 7).
- **Router, der kan udvides**: den lukkede binary monterer sine egne admin- og
  policy-routes.

Seams lander i det åbne repo som almindelige PR'er, testet med de åbne
implementationer. Isolationstests, nøglebutik, entitlements, retention pr. række og
`usage_hourly` ligger i den lukkede repo. Den lukkede binary pinner en telemetryd-version
og følger med, som en almindelig afhængighed.

Licensen skal stå ærligt: den lukkede binary er proprietær og bundter MIT-crates, så
deres licens- og copyright-tekst skal med (SBOM). Det åbne telemetryd ændrer ikke
licens.

## Export fra self-hosted til cloud

*Besluttet:* et tilvalg til at sende data videre til cloud'en. Der er to veje, og de kan
begge findes:

- **Fra laravel-telemetry**: exporteren får en ekstra, valgfri OTLP-destination, så
  appen sender til både den lokale telemetryd og cloud'en med projektets ingest-nøgle.
  Det er ren config i SDK'et og kræver intet i telemetryd.
- **Fra telemetryd**: relay mode (`crates/server/src/relay.rs`) forwarder allerede
  forseglede segmenter til en `upstream` med at-least-once-levering og en cursor, der
  overlever offline-perioder. Peg `upstream` på cloud'en og `token` på projektets
  ingest-nøgle. Cloud'en stempler tenant ud fra nøglen, så den lokale instans ikke kan
  skrive til et andet projekt. Relay mode gemmer lokalt og kan stadig forespørges
  ("The relay is still queryable" i `docs/cookbook/relay-mode.md`), men docs'ene taler om
  "the last few hours", altså en buffer og ikke et fuldt lokalt lager. Det skal tjekkes,
  om normal retention gælder i relay mode. Men relay mode afviser i dag en
  almindelig `auth.ingest_token`, medmindre `trust_client_identity = true`, og den
  kræver Parquet (ikke `[clickhouse]`). En self-hosted installation, der bare vil
  sende en kopi, har derfor brug for en lettere variant, fx `[export.cloud]` uden
  relay-reglerne om identitet. Det er et lille, generisk tilvalg i det åbne
  telemetryd.

Begge veje bruger kundens kvote i cloud'en, og at-least-once betyder, at dubletter
tæller med. Det skal stå på forbrugssiden og i vilkårene, eller også skal cloud'en
dedupe på segment-id.

Markedsføringen løser sig med det: self-hosted telemetryd lover stadig "your telemetry
never leaves your infrastructure", fordi export er slået fra som default og kun sender,
når kunden selv slår det til. SaaS'en er et andet produkt, hvor data ligger hos os (EU).

## Besluttet

1. **Entitlements**: alt med en grænse er et entitlement, ikke kun spans.
2. **Tenant = projekt**. En organisation har mange projekter, og det håndteres i control
   plane.
3. **Overforbrug** afhænger af entitlementet: faktureres, soft limit på X %, eller
   afvisning af nye data.
4. **Retention** er mindst 90 dage, custom pr. kunde, projekt eller plan.

5. **Kvoter** kan både deles af en organisations projekter og være pr. projekt. Det
   vælges pr. entitlement i planen eller kontrakten.
6. **Afregningsperiode** er kalendermåned som standard. Enterprise kan få tilpassede
   kontrakter (anden periode, årlig kvote osv.). Periode og nulstilling er derfor et
   felt på kontrakten i control plane, og telemetryd kender kun til policyen.
7. **Tenancy er lukket**. Det åbne telemetryd forbliver MIT og single-tenant.
8. **SaaS'en gemmer i cloud'en**. Self-hosted kan som tilvalg exportere dertil.

## Stadig åbne

1. **Hvilken export-vej først**: laravel-telemetry med to destinationer, eller
   telemetryd relay mod cloud'en? SDK-vejen er mindst arbejde. Relay-vejen dækker også
   ikke-Laravel-kilder.
2. **Dubletter ved export**: tæller de med i kvoten, eller dedupes de i cloud'en?
