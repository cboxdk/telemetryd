//! The pieces of LogQL's pipeline that are small languages of their own: the Go
//! templates of `line_format` and `label_format`, and the `pattern` parser's patterns.
//!
//! Each supports what LogQL queries are written with in practice and refuses the rest
//! by name at parse time. A template construct half-understood would render the wrong
//! line and look right doing it.

use telemetryd_core::{Error, Labels, Result};

/// A `line_format` or `label_format` template: text with `{{ .label | fn … }}` actions.
#[derive(Debug, Clone)]
pub struct Template {
    parts: Vec<Part>,
}

#[derive(Debug, Clone)]
enum Part {
    Text(String),
    Action {
        source: Source,
        functions: Vec<Call>,
    },
}

#[derive(Debug, Clone)]
enum Source {
    Label(String),
    Line,
    Literal(String),
}

#[derive(Debug, Clone)]
enum Call {
    Upper,
    Lower,
    Title,
    Trim,
    Default(String),
    Replace(String, String),
    Truncate(usize),
}

impl Template {
    /// Parse a template.
    ///
    /// # Errors
    /// An action that is not `.label`, `__line__` or a quoted string, piped through the
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
        let mut out = String::new();
        for part in &self.parts {
            match part {
                Part::Text(text) => out.push_str(text),
                Part::Action { source, functions } => {
                    let mut value = match source {
                        Source::Label(name) => labels.get(name).unwrap_or("").to_owned(),
                        Source::Line => line.to_owned(),
                        Source::Literal(text) => text.clone(),
                    };
                    for function in functions {
                        value = function.apply(value);
                    }
                    out.push_str(&value);
                }
            }
        }
        out
    }
}

fn parse_action(action: &str) -> Result<Part> {
    let mut stages = action.split('|').map(str::trim);
    let head = stages.next().unwrap_or_default();
    let source = if head == "__line__" {
        Source::Line
    } else if let Some(name) = head.strip_prefix('.') {
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(unsupported_action(action));
        }
        Source::Label(name.to_owned())
    } else if let Some(text) = quoted(head) {
        Source::Literal(text)
    } else {
        return Err(unsupported_action(action));
    };
    let functions = stages.map(parse_call).collect::<Result<Vec<_>>>()?;
    Ok(Part::Action { source, functions })
}

fn parse_call(call: &str) -> Result<Call> {
    let mut words = split_words(call).into_iter();
    let name = words.next().unwrap_or_default();
    let args: Vec<String> = words.collect();
    let arg = |i: usize| args.get(i).and_then(|a| quoted(a));
    Ok(match (name.as_str(), args.len()) {
        ("ToUpper" | "upper", 0) => Call::Upper,
        ("ToLower" | "lower", 0) => Call::Lower,
        ("Title" | "title", 0) => Call::Title,
        ("TrimSpace" | "trim", 0) => Call::Trim,
        ("default", 1) => Call::Default(arg(0).ok_or_else(|| unsupported_action(call))?),
        ("replace", 2) => Call::Replace(
            arg(0).ok_or_else(|| unsupported_action(call))?,
            arg(1).ok_or_else(|| unsupported_action(call))?,
        ),
        ("trunc", 1) => Call::Truncate(args[0].parse().map_err(|_| unsupported_action(call))?),
        _ => {
            return Err(Error::unsupported_with_hint(
                format!("the template function `{name}`"),
                "supported: ToUpper, ToLower, Title, TrimSpace, default, replace, trunc",
            ));
        }
    })
}

impl Call {
    fn apply(&self, value: String) -> String {
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
            Self::Default(fallback) => {
                if value.is_empty() {
                    fallback.clone()
                } else {
                    value
                }
            }
            // Go's `replace` in Loki's templates: the value is the last argument.
            Self::Replace(from, to) => value.replace(from.as_str(), to),
            Self::Truncate(length) => value.chars().take(*length).collect(),
        }
    }
}

fn unsupported_action(action: &str) -> Error {
    Error::unsupported_with_hint(
        format!("the template action `{{{{{action}}}}}`"),
        "supported: `{{.label}}`, `{{__line__}}` and quoted text, piped through ToUpper, \
         ToLower, Title, TrimSpace, default, replace or trunc",
    )
}

/// Words of a template call, keeping quoted arguments whole.
fn split_words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    for c in text.chars() {
        match quote {
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
            let next = rest[1..].find('<').map_or(rest.len(), |i| i + 1);
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
