//! The arithmetic of PromQL's functions, ported from Prometheus's own.
//!
//! Each follows the Prometheus source rather than the textbook, because a dashboard
//! compares our numbers with theirs: sums are Kahan-compensated the way theirs are, the
//! regression behind `deriv` and `predict_linear` is theirs, quantiles interpolate the
//! way `quantile` does, and a counter reset in `irate` is read the way they read it. The
//! conformance suite holds every one of them to Prometheus's answers.

use telemetryd_core::Labels;

use crate::promql::Function;

const NANOS_PER_SECOND: f64 = 1_000_000_000.0;

#[allow(clippy::cast_precision_loss)]
fn seconds(nanos: u64) -> f64 {
    nanos as f64 / NANOS_PER_SECOND
}

/// The `sum` aggregation the way Prometheus adds a group up: from its first element,
/// each further one added with compensation, the compensation added at the end.
pub(crate) fn group_sum(values: &[f64]) -> f64 {
    let Some((first, rest)) = values.split_first() else {
        return 0.0;
    };
    let (sum, compensation) = rest
        .iter()
        .fold((*first, 0.0), |(sum, c), value| kahan_add(*value, sum, c));
    sum + compensation
}

/// Kahan–Neumaier summation, one term at a time, as Prometheus's `kahanSumInc`.
#[must_use]
pub fn kahan_add(increment: f64, sum: f64, compensation: f64) -> (f64, f64) {
    let total = sum + increment;
    let compensation = if total.is_infinite() {
        0.0
    } else if sum.abs() >= increment.abs() {
        compensation + ((sum - total) + increment)
    } else {
        compensation + ((increment - total) + sum)
    };
    (total, compensation)
}

/// A range function other than `rate`/`increase` over one series' samples in its
/// window, staleness markers already left out. `None` when it has no answer there.
///
/// `at_nanos` is the evaluation time, which `predict_linear` projects from.
#[must_use]
pub fn over_range(
    function: Function,
    samples: &[(u64, f64)],
    window: (u64, u64),
    at_nanos: u64,
    parameter: f64,
) -> Option<f64> {
    let (first, last) = (samples.first()?, samples.last()?);
    Some(match function {
        Function::Irate | Function::Idelta => {
            let [.., previous, latest] = samples else {
                return None;
            };
            let elapsed = latest.0.checked_sub(previous.0).filter(|e| *e > 0)?;
            if function == Function::Irate {
                let change = if latest.1 < previous.1 {
                    latest.1
                } else {
                    latest.1 - previous.1
                };
                change / seconds(elapsed)
            } else {
                latest.1 - previous.1
            }
        }
        Function::Delta => delta(samples, window)?,
        Function::Deriv => {
            if samples.len() < 2 {
                return None;
            }
            linear_regression(samples, first.0).0
        }
        Function::PredictLinear => {
            if samples.len() < 2 {
                return None;
            }
            let (slope, intercept) = linear_regression(samples, at_nanos);
            intercept + slope * parameter
        }
        // Exact equality: `changes` counts any change at all, as Prometheus's does.
        #[allow(clippy::float_cmp)]
        Function::Changes => count_pairs(samples, |a, b| !(a == b || (a.is_nan() && b.is_nan()))),
        Function::Resets => count_pairs(samples, |a, b| b < a),
        Function::AvgOverTime => average(samples),
        Function::MinOverTime => {
            samples.iter().skip(1).fold(
                first.1,
                |min, (_, v)| if *v < min || min.is_nan() { *v } else { min },
            )
        }
        Function::MaxOverTime => {
            samples.iter().skip(1).fold(
                first.1,
                |max, (_, v)| if *v > max || max.is_nan() { *v } else { max },
            )
        }
        Function::SumOverTime => {
            let (sum, compensation) = samples
                .iter()
                .fold((0.0, 0.0), |(sum, c), (_, v)| kahan_add(*v, sum, c));
            if sum.is_infinite() {
                sum
            } else {
                sum + compensation
            }
        }
        #[allow(clippy::cast_precision_loss)]
        Function::CountOverTime => samples.len() as f64,
        Function::LastOverTime => last.1,
        Function::PresentOverTime => 1.0,
        Function::StddevOverTime => variance(samples.iter().map(|(_, v)| *v)).sqrt(),
        Function::StdvarOverTime => variance(samples.iter().map(|(_, v)| *v)),
        Function::QuantileOverTime => {
            let mut values: Vec<f64> = samples.iter().map(|(_, v)| *v).collect();
            quantile(parameter, &mut values)
        }
        _ => return None,
    })
}

/// `delta`: the change across the window, extrapolated to its edges as Prometheus
/// extrapolates a gauge — no counter resets, no clamp at zero.
#[allow(clippy::similar_names)] // `sampled` and `samples`, as Prometheus names them
fn delta(samples: &[(u64, f64)], (floor, at): (u64, u64)) -> Option<f64> {
    let (first, last) = (samples.first()?, samples.last()?);
    if samples.len() < 2 {
        return None;
    }
    let sampled = seconds(last.0.saturating_sub(first.0));
    if sampled <= 0.0 {
        return None;
    }
    #[allow(clippy::cast_precision_loss)]
    let average = sampled / (samples.len() - 1) as f64;
    let threshold = average * 1.1;
    let mut to_start = seconds(first.0.saturating_sub(floor));
    if to_start >= threshold {
        to_start = average / 2.0;
    }
    let mut to_end = seconds(at.saturating_sub(last.0));
    if to_end >= threshold {
        to_end = average / 2.0;
    }
    Some((last.1 - first.1) * (sampled + to_start + to_end) / sampled)
}

#[allow(clippy::cast_precision_loss)]
fn count_pairs(samples: &[(u64, f64)], counts: impl Fn(f64, f64) -> bool) -> f64 {
    samples
        .windows(2)
        .filter(|pair| counts(pair[0].1, pair[1].1))
        .count() as f64
}

/// `avg_over_time`: a compensated sum divided at the end, switching to an incremental
/// mean only if the sum would overflow — Prometheus's own approach.
fn average(samples: &[(u64, f64)]) -> f64 {
    mean(samples.iter().map(|(_, value)| *value))
}

/// The mean the way Prometheus takes it, for `avg_over_time` and the `avg` aggregation
/// alike: a compensated sum divided at the end, switching to an incremental mean only if
/// the sum would overflow. `NaN` for no values.
pub(crate) fn mean(mut values: impl Iterator<Item = f64>) -> f64 {
    let Some(first) = values.next() else {
        return f64::NAN;
    };
    let (mut sum, mut count, mut mean, mut compensation) = (first, 1.0f64, 0.0, 0.0);
    let mut incremental = false;
    for value in values {
        let value = &value;
        count += 1.0;
        if !incremental {
            let (new_sum, new_c) = kahan_add(*value, sum, compensation);
            if !new_sum.is_infinite() {
                sum = new_sum;
                compensation = new_c;
                continue;
            }
            incremental = true;
            mean = sum / (count - 1.0);
            compensation /= count - 1.0;
        }
        let q = (count - 1.0) / count;
        (mean, compensation) = kahan_add(value / count, q * mean, q * compensation);
    }
    if incremental {
        mean + compensation
    } else {
        sum / count + compensation / count
    }
}

/// Population variance by Welford's method, compensated, as `stdvar_over_time`.
fn variance(values: impl Iterator<Item = f64>) -> f64 {
    let (mut count, mut mean, mut c_mean, mut aux, mut c_aux) = (0.0f64, 0.0, 0.0, 0.0, 0.0);
    for value in values {
        count += 1.0;
        let delta = value - (mean + c_mean);
        (mean, c_mean) = kahan_add(delta / count, mean, c_mean);
        (aux, c_aux) = kahan_add(delta * (value - (mean + c_mean)), aux, c_aux);
    }
    (aux + c_aux) / count
}

/// The least-squares line through the samples, as Prometheus's `linearRegression`:
/// slope per second, and the value at `intercept_nanos`.
#[allow(clippy::similar_names)] // Prometheus's names, so the port can be read against it
fn linear_regression(samples: &[(u64, f64)], intercept_nanos: u64) -> (f64, f64) {
    let initial = samples[0].1;
    #[allow(clippy::float_cmp)] // an exact test for a flat line, as theirs is
    let constant = samples.iter().all(|(_, v)| *v == initial);
    if constant {
        return if initial.is_infinite() {
            (f64::NAN, f64::NAN)
        } else {
            (0.0, initial)
        };
    }
    let (mut n, mut sum_x, mut c_x, mut sum_y, mut c_y) = (0.0f64, 0.0, 0.0, 0.0, 0.0);
    let (mut sum_xy, mut c_xy, mut sum_x2, mut c_x2) = (0.0, 0.0, 0.0, 0.0);
    for (timestamp, value) in samples {
        n += 1.0;
        #[allow(clippy::cast_precision_loss, clippy::cast_possible_wrap)]
        let x = (*timestamp as i64 - intercept_nanos as i64) as f64 / NANOS_PER_SECOND;
        (sum_x, c_x) = kahan_add(x, sum_x, c_x);
        (sum_y, c_y) = kahan_add(*value, sum_y, c_y);
        (sum_xy, c_xy) = kahan_add(x * value, sum_xy, c_xy);
        (sum_x2, c_x2) = kahan_add(x * x, sum_x2, c_x2);
    }
    let (sum_x, sum_y, sum_xy, sum_x2) = (sum_x + c_x, sum_y + c_y, sum_xy + c_xy, sum_x2 + c_x2);
    let covariance = sum_xy - sum_x * sum_y / n;
    let spread = sum_x2 - sum_x * sum_x / n;
    let slope = covariance / spread;
    (slope, sum_y / n - slope * sum_x / n)
}

/// The φ-quantile of `values`, interpolating between neighbours as Prometheus's
/// `quantile` does. Sorts `values`.
#[must_use]
pub fn quantile(phi: f64, values: &mut [f64]) -> f64 {
    if values.is_empty() || phi.is_nan() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }
    values.sort_by(f64::total_cmp);
    #[allow(clippy::cast_precision_loss)]
    let n = values.len() as f64;
    let rank = phi * (n - 1.0);
    let lower = rank.floor().max(0.0);
    let upper = (lower + 1.0).min(n - 1.0);
    let weight = rank - rank.floor();
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let (lower, upper) = (lower as usize, upper as usize);
    values[lower] * (1.0 - weight) + values[upper] * weight
}

/// Population standard deviation or variance of a group, as `stddev`/`stdvar`.
#[must_use]
pub fn group_variance(values: &[f64]) -> f64 {
    variance(values.iter().copied())
}

/// A function of one number, for the math functions.
#[must_use]
pub fn of_value(function: Function, value: f64, parameter: f64) -> f64 {
    match function {
        Function::Abs => value.abs(),
        Function::Ceil => value.ceil(),
        Function::Floor => value.floor(),
        Function::Round => {
            // `round(v, to_nearest)`: halves round up, as Go's `math.Floor(v*x + 0.5)`.
            let inverse = 1.0 / parameter;
            (value * inverse + 0.5).floor() / inverse
        }
        Function::Sqrt => value.sqrt(),
        Function::Exp => value.exp(),
        Function::Ln => value.ln(),
        Function::Log2 => value.log2(),
        Function::Log10 => value.log10(),
        Function::Sgn => {
            if value > 0.0 {
                1.0
            } else if value < 0.0 {
                -1.0
            } else {
                value
            }
        }
        Function::Acos => value.acos(),
        Function::Asin => value.asin(),
        Function::Atan => value.atan(),
        Function::Cos => value.cos(),
        Function::Sin => value.sin(),
        Function::Tan => value.tan(),
        Function::Acosh => value.acosh(),
        Function::Asinh => value.asinh(),
        Function::Atanh => value.atanh(),
        Function::Cosh => value.cosh(),
        Function::Sinh => value.sinh(),
        Function::Tanh => value.tanh(),
        Function::Deg => value.to_degrees(),
        Function::Rad => value.to_radians(),
        Function::DayOfMonth
        | Function::DayOfWeek
        | Function::DayOfYear
        | Function::DaysInMonth
        | Function::Hour
        | Function::Minute
        | Function::Month
        | Function::Year => calendar(function, value),
        _ => value,
    }
}

/// A calendar field of a Unix time in seconds, in UTC.
fn calendar(function: Function, unix_seconds: f64) -> f64 {
    if !unix_seconds.is_finite() {
        return f64::NAN;
    }
    #[allow(clippy::cast_possible_truncation)]
    let whole = unix_seconds.floor() as i64;
    let days = whole.div_euclid(86_400);
    let second_of_day = whole.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let month_lengths = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let month_index = (month - 1) as usize;
    #[allow(clippy::cast_precision_loss)]
    let field = match function {
        Function::DayOfMonth => day,
        Function::DayOfWeek => (days + 4).rem_euclid(7),
        Function::DayOfYear => month_lengths[..month_index].iter().sum::<i64>() + day,
        Function::DaysInMonth => month_lengths[month_index],
        Function::Hour => second_of_day / 3600,
        Function::Minute => second_of_day % 3600 / 60,
        Function::Month => month,
        _ => year,
    } as f64;
    field
}

/// Year, month and day of a count of days since 1970-01-01, by Howard Hinnant's method.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// `label_replace`: set `destination` from `replacement` when `source` matches the
/// fully anchored `regex`, expanding `$1` and `${name}`; leave the set alone otherwise.
/// An empty result removes the label.
#[must_use]
pub fn label_replace(
    labels: &Labels,
    destination: &str,
    replacement: &str,
    source: &str,
    regex: &regex::Regex,
) -> Labels {
    let value = labels.get(source).unwrap_or("");
    let Some(captures) = regex.captures(value) else {
        return labels.clone();
    };
    let mut expanded = String::new();
    captures.expand(replacement, &mut expanded);
    let mut out = labels.clone();
    if expanded.is_empty() {
        out.remove(destination);
    } else {
        out.insert(destination, expanded);
    }
    out
}

/// `label_join`: set `destination` to the named labels' values joined by `separator`.
#[must_use]
pub fn label_join(
    labels: &Labels,
    destination: &str,
    separator: &str,
    sources: &[String],
) -> Labels {
    let joined = sources
        .iter()
        .map(|name| labels.get(name).unwrap_or(""))
        .collect::<Vec<_>>()
        .join(separator);
    let mut out = labels.clone();
    if joined.is_empty() {
        out.remove(destination);
    } else {
        out.insert(destination, joined);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_fields_match_a_known_date() {
        // 2024-02-29T13:45:00Z, a Thursday, the 60th day of a leap year.
        let at = 1_709_214_300.0;
        assert!((calendar(Function::Year, at) - 2024.0).abs() < f64::EPSILON);
        assert!((calendar(Function::Month, at) - 2.0).abs() < f64::EPSILON);
        assert!((calendar(Function::DayOfMonth, at) - 29.0).abs() < f64::EPSILON);
        assert!((calendar(Function::DaysInMonth, at) - 29.0).abs() < f64::EPSILON);
        assert!((calendar(Function::DayOfYear, at) - 60.0).abs() < f64::EPSILON);
        assert!((calendar(Function::DayOfWeek, at) - 4.0).abs() < f64::EPSILON);
        assert!((calendar(Function::Hour, at) - 13.0).abs() < f64::EPSILON);
        assert!((calendar(Function::Minute, at) - 45.0).abs() < f64::EPSILON);
        // And before the epoch.
        assert!((calendar(Function::Year, -1.0) - 1969.0).abs() < f64::EPSILON);
    }
}
