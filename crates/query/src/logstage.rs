//! The pieces of LogQL's pipeline that are small languages of their own: the Go
//! templates of `line_format` and `label_format`, and the `pattern` parser's patterns.
//!
//! Each supports what LogQL queries are written with in practice and refuses the rest
//! by name at parse time. A template construct half-understood would render the wrong
//! line and look right doing it.

use telemetryd_core::{Error, Labels, Result};

/// A `line_format` or `label_format` template: text with Go template actions —
/// `{{.label}}`, `{{ToUpper .path}}`, `{{.path | trunc 3}}`, `{{default "none" .x}}`.
#[derive(Debug, Clone)]
pub struct Template {
    parts: Vec<Part>,
}

#[derive(Debug, Clone)]
enum Part {
    Text(String),
    /// A pipeline: each command's value is the last argument of the next, as in Go.
    Action(Vec<Command>),
}

#[derive(Debug, Clone)]
struct Command {
    /// `None` for a bare operand, which only a pipeline's first command may be.
    function: Option<Function>,
    args: Vec<Operand>,
}

#[derive(Debug, Clone)]
enum Operand {
    Label(String),
    Line,
    Literal(String),
}

#[derive(Debug, Clone, Copy)]
enum Function {
    Upper,
    Lower,
    Title,
    Trim,
    Default,
    Replace,
    Truncate,
    TrimPrefix,
    TrimSuffix,
}

impl Function {
    fn named(name: &str) -> Option<Self> {
        Some(match name {
            "ToUpper" | "upper" => Self::Upper,
            "ToLower" | "lower" => Self::Lower,
            "Title" | "title" => Self::Title,
            "TrimSpace" | "trim" => Self::Trim,
            "default" => Self::Default,
            "replace" => Self::Replace,
            "trunc" => Self::Truncate,
            "trimPrefix" | "TrimPrefix" => Self::TrimPrefix,
            "trimSuffix" | "TrimSuffix" => Self::TrimSuffix,
            _ => return None,
        })
    }

    /// How many arguments it takes, the piped value included.
    fn arity(self) -> usize {
        match self {
            Self::Upper | Self::Lower | Self::Title | Self::Trim => 1,
            Self::Default | Self::Truncate | Self::TrimPrefix | Self::TrimSuffix => 2,
            Self::Replace => 3,
        }
    }

    /// Apply to its arguments, in Go's order: the value is the last.
    fn apply(self, args: &[String]) -> String {
        let value = args.last().cloned().unwrap_or_default();
        match self {
            Self::Upper => value.to_uppercase(),
            Self::Lower => value.to_lowercase(),
            Self::Title => value
                .split(' ')
                .map(|word| {
                    let mut chars = word.chars();
                    chars.next().map_or_else(String::new, |first| {
                        first.to_uppercase().chain(chars).collect()
                    })
                })
                .collect::<Vec<_>>()
                .join(" "),
            Self::Trim => value.trim().to_owned(),
            Self::Default => {
                if value.is_empty() {
                    args[0].clone()
                } else {
                    value
                }
            }
            // An empty `old` would insert `new` between every character; refused at parse.
            Self::Replace if args[0].is_empty() => value,
            Self::Replace => value.replace(args[0].as_str(), &args[1]),
            Self::Truncate => {
                let length = args[0].trim().parse::<i64>().unwrap_or(0);
                let count = value.chars().count();
                // Sprig's `trunc`: a negative length keeps the end.
                if length >= 0 {
                    value
                        .chars()
                        .take(usize::try_from(length).unwrap_or(0))
                        .collect()
                } else {
                    let keep = usize::try_from(-length).unwrap_or(0).min(count);
                    value.chars().skip(count - keep).collect()
                }
            }
            Self::TrimPrefix => value
                .strip_prefix(args[0].as_str())
                .unwrap_or(&value)
                .to_owned(),
            Self::TrimSuffix => value
                .strip_suffix(args[0].as_str())
                .unwrap_or(&value)
                .to_owned(),
        }
    }
}

/// The most a template renders, as Loki's default `max_line_size`. Past it the output
/// stops. Stages chain and `{{__line__}}{{__line__}}` doubles a line, so without a cap
/// ten stages asked for gigabytes a line — and an allocation that fails ends the process.
pub const MAX_RENDERED_BYTES: usize = 256 * 1024;

impl Template {
    /// Parse a template.
    ///
    /// # Errors
    /// An action outside `.label`, `__line__`, quoted text and numbers, composed with the
    /// supported functions — `if`, `range`, variables and the rest are named.
    pub fn parse(text: &str) -> Result<Self> {
        let mut parts = Vec::new();
        let mut rest = text;
        while let Some(open) = rest.find("{{") {
            if open > 0 {
                parts.push(Part::Text(rest[..open].to_owned()));
            }
            let after = &rest[open + 2..];
            let close = after.find("}}").ok_or_else(|| {
                Error::BadRequest(format!("an unclosed `{{{{` in the template {text:?}"))
            })?;
            parts.push(parse_action(
                after[..close].trim_matches(|c: char| c == '-' || c.is_whitespace()),
            )?);
            rest = &after[close + 2..];
        }
        if !rest.is_empty() {
            parts.push(Part::Text(rest.to_owned()));
        }
        Ok(Self { parts })
    }

    /// Render against a line's labels and the line itself.
    #[must_use]
    pub fn render(&self, labels: &Labels, line: &str) -> String {
        let operand = |operand: &Operand| match operand {
            Operand::Label(name) => labels.get(name).unwrap_or("").to_owned(),
            Operand::Line => line.to_owned(),
            Operand::Literal(text) => text.clone(),
        };
        let mut out = String::new();
        for part in &self.parts {
            match part {
                Part::Text(text) => out.push_str(text),
                Part::Action(commands) => {
                    let mut piped: Option<String> = None;
                    for command in commands {
                        let mut args: Vec<String> = command.args.iter().map(operand).collect();
                        args.extend(piped.take());
                        let mut value = match command.function {
                            Some(function) => function.apply(&args),
                            None => args.pop().unwrap_or_default(),
                        };
                        truncate(&mut value, MAX_RENDERED_BYTES);
                        piped = Some(value);
                    }
                    out.push_str(&piped.unwrap_or_default());
                }
            }
            if out.len() > MAX_RENDERED_BYTES {
                truncate(&mut out, MAX_RENDERED_BYTES);
                break;
            }
        }
        out
    }
}

fn parse_action(action: &str) -> Result<Part> {
    let mut commands = Vec::new();
    for (index, command) in split_pipeline(action).iter().enumerate() {
        let words = split_words(command);
        let Some(first) = words.first() else {
            return Err(unsupported_action(action));
        };
        let command = if let Some(function) = Function::named(first) {
            let args = words[1..]
                .iter()
                .map(|word| parse_operand(word).ok_or_else(|| unsupported_action(action)))
                .collect::<Result<Vec<_>>>()?;
            let piped = usize::from(index > 0);
            if matches!(function, Function::Replace)
                && matches!(args.first(), Some(Operand::Literal(old)) if old.is_empty())
            {
                return Err(Error::BadRequest(format!(
                    "`replace` needs something to replace; \"\" in `{{{{{action}}}}}` \
                     would insert between every character"
                )));
            }
            if args.len() + piped != function.arity() {
                return Err(Error::BadRequest(format!(
                    "`{first}` takes {} argument(s) in the template action `{{{{{action}}}}}`",
                    function.arity()
                )));
            }
            Command {
                function: Some(function),
                args,
            }
        } else {
            let operand = parse_operand(first).filter(|_| words.len() == 1 && index == 0);
            let Some(operand) = operand else {
                return Err(if first.chars().all(|c| c.is_ascii_alphabetic()) {
                    Error::unsupported_with_hint(
                        format!("the template function `{first}`"),
                        "supported: ToUpper, ToLower, Title, TrimSpace, default, replace, \
                         trunc, trimPrefix, trimSuffix",
                    )
                } else {
                    unsupported_action(action)
                });
            };
            Command {
                function: None,
                args: vec![operand],
            }
        };
        commands.push(command);
    }
    Ok(Part::Action(commands))
}

/// Cut `text` to at most `max` bytes, at a character boundary.
fn truncate(text: &mut String, max: usize) {
    if text.len() > max {
        let mut end = max;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
}

fn parse_operand(word: &str) -> Option<Operand> {
    if word == "__line__" {
        return Some(Operand::Line);
    }
    if let Some(name) = word.strip_prefix('.') {
        return (!name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
            .then(|| Operand::Label(name.to_owned()));
    }
    if let Some(text) = quoted(word) {
        return Some(Operand::Literal(text));
    }
    word.parse::<f64>()
        .is_ok()
        .then(|| Operand::Literal(word.to_owned()))
}

/// The commands of a pipeline, split on `|` outside quotes.
fn split_pipeline(action: &str) -> Vec<String> {
    let mut commands = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for c in action.chars() {
        match quote {
            Some('"') if escaped => escaped = false,
            Some('"') if c == '\\' => escaped = true,
            Some(q) if c == q => quote = None,
            None if c == '"' || c == '`' => quote = Some(c),
            None if c == '|' => {
                commands.push(std::mem::take(&mut current).trim().to_owned());
                continue;
            }
            Some(_) | None => {}
        }
        current.push(c);
    }
    commands.push(current.trim().to_owned());
    commands
}

fn unsupported_action(action: &str) -> Error {
    Error::unsupported_with_hint(
        format!("the template action `{{{{{action}}}}}`"),
        "supported: `{{.label}}`, `{{__line__}}`, quoted text and numbers, with ToUpper, \
         ToLower, Title, TrimSpace, default, replace, trunc, trimPrefix and trimSuffix — \
         called, like `{{ToUpper .path}}`, or piped, like `{{.path | ToUpper}}`",
    )
}

/// Words of a template call, keeping quoted arguments whole.
fn split_words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for c in text.chars() {
        match quote {
            Some('"') if escaped => {
                current.push(c);
                escaped = false;
            }
            Some('"') if c == '\\' => {
                current.push(c);
                escaped = true;
            }
            Some(q) if c == q => {
                current.push(c);
                quote = None;
            }
            None if c == '"' || c == '`' => {
                current.push(c);
                quote = Some(c);
            }
            None if c.is_whitespace() => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
            }
            Some(_) | None => current.push(c),
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

fn quoted(text: &str) -> Option<String> {
    let inner = text
        .strip_prefix('"')
        .and_then(|t| t.strip_suffix('"'))
        .or_else(|| text.strip_prefix('`').and_then(|t| t.strip_suffix('`')))?;
    Some(
        inner
            .replace("\\\"", "\"")
            .replace("\\n", "\n")
            .replace("\\t", "\t"),
    )
}

/// A `pattern` parser's pattern: literals, `<name>` captures and `<_>` placeholders.
#[derive(Debug, Clone)]
pub struct Pattern {
    parts: Vec<PatternPart>,
}

#[derive(Debug, Clone)]
enum PatternPart {
    Literal(String),
    Capture(Option<String>),
}

impl Pattern {
    /// Parse a pattern.
    ///
    /// # Errors
    /// No named capture, or two captures with no text between them — which Loki refuses
    /// too, since nothing would say where one ends.
    pub fn parse(pattern: &str) -> Result<Self> {
        let mut parts = Vec::new();
        let mut rest = pattern;
        while !rest.is_empty() {
            if let Some(inner) = rest.strip_prefix('<')
                && let Some(end) = inner.find('>')
                && inner[..end]
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
                && end > 0
            {
                let name = &inner[..end];
                if matches!(parts.last(), Some(PatternPart::Capture(_))) {
                    return Err(Error::BadRequest(format!(
                        "the pattern {pattern:?} has two captures with nothing between them"
                    )));
                }
                parts.push(PatternPart::Capture((name != "_").then(|| name.to_owned())));
                rest = &inner[end + 1..];
                continue;
            }
            // Past the first character, whatever its width: slicing one byte in split a
            // multi-byte character and panicked on `<a>é<b>`.
            let skip = rest.chars().next().map_or(0, char::len_utf8);
            let next = rest[skip..].find('<').map_or(rest.len(), |i| i + skip);
            match parts.last_mut() {
                Some(PatternPart::Literal(text)) => text.push_str(&rest[..next]),
                _ => parts.push(PatternPart::Literal(rest[..next].to_owned())),
            }
            rest = &rest[next..];
        }
        if !parts
            .iter()
            .any(|part| matches!(part, PatternPart::Capture(Some(_))))
        {
            return Err(Error::BadRequest(format!(
                "the pattern {pattern:?} names no capture, like `<status>`"
            )));
        }
        Ok(Self { parts })
    }

    /// The captures of `line`, or nothing when it does not match.
    #[must_use]
    pub fn captures(&self, line: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let mut position = 0;
        let mut pending: Option<&Option<String>> = None;
        for part in &self.parts {
            match part {
                PatternPart::Capture(name) => pending = Some(name),
                PatternPart::Literal(literal) => {
                    let Some(found) = line[position..].find(literal.as_str()) else {
                        return Vec::new();
                    };
                    match pending.take() {
                        Some(Some(name)) => {
                            out.push((name.clone(), line[position..position + found].to_owned()));
                        }
                        // A leading literal has to be where the line starts.
                        None if found != 0 => return Vec::new(),
                        Some(None) | None => {}
                    }
                    position += found + literal.len();
                }
            }
        }
        if let Some(Some(name)) = pending {
            out.push((name.clone(), line[position..].to_owned()));
        }
        out
    }
}

/// A line with its ANSI colour escapes taken out, as `| decolorize`.
#[must_use]
pub fn decolorize(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// A label value read as a Go duration, in seconds: `250ms`, `1.5s`, `1h2m3s`, `-2s`
/// or a bare `0` — how Loki reads a label compared with a duration.
#[must_use]
pub fn parse_duration(text: &str) -> Option<f64> {
    let text = text.trim();
    let (negative, mut rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    if rest == "0" {
        return Some(0.0);
    }
    if rest.is_empty() {
        return None;
    }
    let mut total = 0.0;
    while !rest.is_empty() {
        let digits = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(rest.len());
        let value: f64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let unit = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let scale = match &rest[..unit] {
            "ns" => 1e-9,
            "us" | "µs" | "μs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return None,
        };
        rest = &rest[unit..];
        total += value * scale;
    }
    Some(if negative { -total } else { total })
}

/// A label value read as a byte size: `1024`, `20MB`, `1.5 KiB`, `3k` — how Loki reads
/// a label compared with a size. Decimal units are powers of 1000, `i` units of 1024.
#[must_use]
pub fn parse_bytes(text: &str) -> Option<f64> {
    let text = text.trim();
    let digits = text
        .find(|c: char| !c.is_ascii_digit() && c != '.' && c != ',')
        .unwrap_or(text.len());
    let value: f64 = text[..digits].replace(',', "").parse().ok()?;
    let unit = text[digits..].trim().to_ascii_lowercase();
    let scale = match unit.as_str() {
        "" => 1.0,
        // `k` and `ki` are `kb` and `kib` without the `b`.
        "k" | "m" | "g" | "t" | "p" | "e" | "ki" | "mi" | "gi" | "ti" | "pi" | "ei" => {
            crate::lexer::byte_unit(&format!("{unit}b"))?
        }
        other => crate::lexer::byte_unit(other)?,
    };
    Some(value * scale)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_template_cannot_render_without_bound() {
        let labels = Labels::new();
        let mut out = "x".repeat(1000);
        let doubling = Template::parse("{{__line__}}{{__line__}}").unwrap();
        for _ in 0..20 {
            out = doubling.render(&labels, &out);
        }
        assert_eq!(out.len(), MAX_RENDERED_BYTES);
        assert!(Template::parse(r#"{{ replace "" "x" .a }}"#).is_err());
    }

    #[test]
    fn a_pattern_with_wide_characters_parses() {
        let pattern = Pattern::parse("<a>é<b>").unwrap();
        assert_eq!(
            pattern.captures("1é2"),
            vec![
                ("a".to_owned(), "1".to_owned()),
                ("b".to_owned(), "2".to_owned())
            ]
        );
        assert!(Pattern::parse("é<a>").is_ok());
    }

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn templates_render_labels_and_functions() {
        let set = labels(&[("method", "get"), ("status", "500"), ("path", " /a ")]);
        let template = Template::parse(r#"{{.method | ToUpper}} {{ .path | TrimSpace }} -> {{.status}} {{.missing | default "none"}}"#).unwrap();
        assert_eq!(template.render(&set, "raw"), "GET /a -> 500 none");
        assert_eq!(
            Template::parse("[{{__line__}}]")
                .unwrap()
                .render(&set, "raw"),
            "[raw]"
        );
        assert_eq!(
            Template::parse(r#"{{ .method | replace "g" "G" }}"#)
                .unwrap()
                .render(&set, ""),
            "Get"
        );
        for refused in [
            "{{ if .a }}x{{ end }}",
            "{{ $x := 1 }}",
            "{{ .a | sha256 }}",
        ] {
            assert!(Template::parse(refused).is_err(), "{refused}");
        }
    }

    #[test]
    fn patterns_capture_between_literals() {
        let pattern =
            Pattern::parse(r#"<ip> - - [<_>] "<method> <path> <_>" <status> <size>"#).unwrap();
        let line = r#"10.0.0.1 - - [01/Jan/2024:00:00:00 +0000] "GET /index HTTP/1.1" 200 512"#;
        assert_eq!(
            pattern.captures(line),
            vec![
                ("ip".to_owned(), "10.0.0.1".to_owned()),
                ("method".to_owned(), "GET".to_owned()),
                ("path".to_owned(), "/index".to_owned()),
                ("status".to_owned(), "200".to_owned()),
                ("size".to_owned(), "512".to_owned()),
            ]
        );
        assert!(pattern.captures("not a log line").is_empty());
        assert!(Pattern::parse("<a><b>").is_err());
        assert!(Pattern::parse("no captures").is_err());
    }

    #[test]
    fn colour_codes_come_out() {
        assert_eq!(decolorize("\u{1b}[31merror\u{1b}[0m done"), "error done");
    }
}
