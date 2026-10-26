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
3. **Kvoter og målinger pr. tenant**: tæller i ClickHouse, kvotestatus i telemetryd og
   per-tenant limits, så én kunde ikke kan sulte de andre.
4. **Et separat control plane** (Laravel-app) til konti, API-nøgler, planer, Cbox
   Billing, forbrug og det offentlige API. telemetryd skal ikke kende til priser.
5. **telemetry-ui i tenant-mode**, med læsetoken pr. tenant i stedet for at stole på
   en `X-Scope-OrgID`-header.

Min anbefaling er at bygge tenancy ind i telemetryd som en opt-in mode
(`[tenancy] enabled = true`, kræver ClickHouse). Default, altså én node uden noget,
skal forblive som i dag. Så sælger self-hosted og SaaS det samme program, og det passer
med README'ens "local and production are the same program".

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

### 6. Retention pr. plan

TTL er i dag pr. tabel og sat fra config (`schema.rs:270`). Kunder på forskellige planer
skal have forskellig retention. Forslag: et lille antal retention-trin (fx 7/14/30/90
dage), en `RetentionDays`-kolonne sat ved ingest ud fra planen, og
`PARTITION BY (RetentionDays, toDate(Timestamp))`. Så er udløb stadig at droppe hele
partitioner, som er billigt, i stedet for row-level TTL-merges, som er dyrt.

Desuden: sletning af en hel tenant ved opsigelse eller GDPR
(`ALTER TABLE … DELETE WHERE TenantId = …` på alle tabeller), som et admin-kommando.

### 7. Måling og kvoter ("X spans")

**Måling.** Kilden til sandheden bør være ClickHouse: en materialized view
`usage_hourly` (`SummingMergeTree`, nøgle `TenantId, Hour, Signal`) med rækker og
bytes. Det virker på tværs af alle instanser, overlever genstarter og kan læses direkte
af control plane til faktura og forbrugsgraf. `telemetryd_ingest_accepted_total` har i
dag kun en `signal`-label, og tenant må ikke blive en Prometheus-label (kardinalitet).

**Håndhævelse.** telemetryd henter kvotestatus pr. tenant fra control plane (eller fra
en lille tabel i ClickHouse som control plane skriver), cachet og eventual consistent.
Det gør ikke noget, at en kunde ryger lidt over i et minut.

**Svaret til senderen, når kvoten er brugt**: *ikke* `429`/`503`. OTLP-exportere prøver
dem igen, og så får vi en retry-storm fra alle kundens servere. Brug et ikke-retrybart
svar (`200` med `partialSuccess`, eller `400` med en klar besked) og tæl det. Planen kan
have en "soft overage"-margin, og der skal være advarsler ved 80/100 %.

**Hvad sælger vi?** Det skal besluttes (se åbne spørgsmål): spans alene, "events"
(spans + loglinjer) som Nightwatch, eller GB. Metrics passer dårligt til "antal", og
prissættes bedre pr. aktiv serie.

### 8. Sampling (nice to have, men vigtig for "X spans")

Når kunderne betaler pr. span, vil de styre hvad der sendes. laravel-telemetry bør
kunne sample (head sampling pr. route, eller "kun fejl og langsomme"), så kunden ikke
brænder kvoten af på healthchecks. Det hører i SDK'et og ikke i telemetryd.

## Uden for telemetryd

### Control plane (ny Laravel-app)

Det er her "SaaS'en" bor, og det er sandsynligvis mere arbejde end telemetryd-delen:

- Organisationer, projekter (= tenants eller apps), brugere og roller via Cbox ID
- API-nøgler: oprette, rotere, tilbagekalde, synket til telemetryd
- Planer, kvoter, retention-trin, Cbox Billing-abonnementer, overforbrug
- Forbrugsside (læser `usage_hourly`) og mails ved 80/100 %
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

1. **Tenancy-kerne i telemetryd**: identitet, stempling, skema med `TenantId`, scoping
   wrapper, signaturændringer på `trace`/`label_*`, isolationstests. Det hele bag
   `[tenancy]` og kun med ClickHouse. *Størst og vigtigst.*
2. **Nøglebutik og sync** fra en minimal control plane (bare tenants og nøgler).
3. **Måling og kvoter**: `usage_hourly`, kvotestatus, ikke-retrybart afslag.
4. **Per-tenant limits** (køandel, query-slots, serier) og **retention-trin**.
5. **Control plane til produkt**: Billing, forbrugsside, offentligt API, onboarding.
6. **telemetry-ui i tenant-mode**.
7. Sikkerhedsdocs, pentest, lancering. Dedicated-instanser (model A) kan sælges fra dag
   ét parallelt med det her.

Punkt 1 til 4 er Rust-arbejde i det her repo. Punkt 5 og 6 er Laravel. Første
milepæl, der er værd at sigte efter, er punkt 1 og 2 med to testkunder på et
ClickHouse-cluster.

## Åbne spørgsmål til dig

1. **Enhed**: sælger vi spans, events (spans + logs) eller GB? Og hvordan prissættes
   metrics?
2. **Tenant = organisation eller projekt?** Nightwatch har én app pr. abonnement. Hvis en
   kunde har flere apps, er tenant så kunden, med `app` indenfor, eller hver app for sig?
   Jeg ville vælge organisation = tenant og `app` som i dag.
3. **Over kvoten**: smid væk, sample ned, eller fortsæt og fakturer overforbrug?
4. **Retention-trin**: hvilke, og hænger de på planen?
5. **Skal tenancy ind i det åbne telemetryd (MIT)**, eller i en lukket fork eller et
   plugin? Det er MIT i dag, så alle kan bygge det samme. Det taler for at holde control
   plane lukket og telemetryd åben.
6. **Self-hosted vs. SaaS i markedsføringen**: README'en sælger "your telemetry never
   leaves your infrastructure". SaaS'en er et andet tilbud ved siden af, og det skal
   formuleres, så de ikke modsiger hinanden.
