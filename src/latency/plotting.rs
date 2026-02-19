use anyhow::Result;
use charming::{
    component::{
        Axis, DataZoom, DataZoomType, Feature, Grid, Legend, Restore, SaveAsImage, Title, Toolbox,
        ToolboxDataZoom,
    },
    element::{
        AreaStyle, AxisLabel, AxisLine, AxisPointer, AxisPointerType, AxisTick, AxisType,
        ItemStyle, JsFunction, Label, LineStyle, LineStyleType, MarkArea, MarkAreaData, MarkLine,
        MarkLineData, MarkLineVariant, NameLocation, Orient, SplitLine, Symbol, TextStyle, Tooltip,
        Trigger,
    },
    series::Line,
    Chart,
};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub avg: Duration,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub min: Duration,
    pub max: Duration,
}

fn build_density_chart(latencies: &[Duration], stats: &LatencyStats) -> Chart {
    let latencies: Vec<f64> = latencies.iter().map(|d| d.as_millis() as f64).collect();

    // For bimodal distributions, reduce bandwidth to better show the peaks
    let bandwidth = silverman_bandwidth(&latencies) * 0.7;
    let (kde_x, kde_y) = kernel_density_estimation(&latencies, bandwidth, 500);

    let n = latencies.len();
    let subtitle = format!("N = {n}   Bandwidth = {bandwidth:.2}");

    // Set fixed y-axis max for consistent display
    let data_max = kde_y.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let y_axis_max = (data_max * 1.1 * 10000.0).ceil() / 10000.0; // Round up to nice value

    Chart::new()
        .title(
            Title::new()
                .text("Latency Distribution")
                .left("center")
                .top("5%")
                .text_style(TextStyle::new().font_size(18).color("#e4e6eb")),
        )
        .title(
            Title::new()
                .text(&subtitle)
                .left("center")
                .bottom("8%")
                .text_style(TextStyle::new().font_size(12).color("#64748b")),
        )
        .legend(
            Legend::new()
                .data(vec!["Density", "P50", "P95", "P99"])
                .top("5%")
                .right("5%")
                .orient(Orient::Vertical)
                .text_style(TextStyle::new().color("#64748b"))
                .item_width(25)
                .item_height(14),
        )
        .tooltip(
            Tooltip::new()
                .trigger(Trigger::Axis)
                .axis_pointer(AxisPointer::new().type_(AxisPointerType::Line))
                .formatter(JsFunction::new_with_args(
                    "params",
                    "const latency = params[0].value[0].toFixed(0);
                     const density = params[0].value[1].toFixed(4);
                     return 'Latency: ' + latency + ' ms<br/>Density: ' + density;",
                )),
        )
        .toolbox(
            Toolbox::new().feature(
                Feature::new()
                    .restore(Restore::new())
                    .save_as_image(SaveAsImage::new()),
            ),
        )
        .grid(Grid::new().left("8%").right("12%").top("8%").bottom("15%"))
        .x_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name("Latency (ms)")
                .name_location(NameLocation::Middle)
                .name_gap(30)
                .name_text_style(TextStyle::new().color("#64748b"))
                .split_line(SplitLine::new().show(false))
                .axis_label(AxisLabel::new().color("#64748b").formatter("{value} ms")),
        )
        .y_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name("Density")
                .name_location(NameLocation::Middle)
                .name_gap(40)
                .name_text_style(TextStyle::new().color("#64748b"))
                .split_line(
                    SplitLine::new()
                        .show(true)
                        .line_style(LineStyle::new().color("#2a3451")),
                )
                .axis_label(AxisLabel::new().color("#64748b"))
                .axis_line(AxisLine::new().show(false))
                .axis_tick(AxisTick::new().show(false))
                .max(y_axis_max),
        )
        .series(
            Line::new()
                .name("Density")
                .data(
                    kde_x
                        .into_iter()
                        .zip(kde_y)
                        .map(|(x, y)| vec![x, y])
                        .collect::<Vec<_>>(),
                )
                .symbol(Symbol::None)
                .smooth(0.3)
                .line_style(LineStyle::new().width(3).color("#00d4ff"))
                .area_style(AreaStyle::new().color("rgba(0, 212, 255, 0.2)")),
        )
        .series(
            Line::new()
                .name("P50")
                .data(vec![
                    vec![stats.p50.as_millis() as f64, 0.0],
                    vec![stats.p50.as_millis() as f64, y_axis_max],
                ])
                .symbol(Symbol::None)
                .line_style(
                    LineStyle::new()
                        .width(2)
                        .color("#ffd700")
                        .type_(LineStyleType::Dashed),
                ),
        )
        .series(
            Line::new()
                .name("P95")
                .data(vec![
                    vec![stats.p95.as_millis() as f64, 0.0],
                    vec![stats.p95.as_millis() as f64, y_axis_max],
                ])
                .symbol(Symbol::None)
                .line_style(
                    LineStyle::new()
                        .width(2)
                        .color("#ff6b6b")
                        .type_(LineStyleType::Dashed),
                ),
        )
        .series(
            Line::new()
                .name("P99")
                .data(vec![
                    vec![stats.p99.as_millis() as f64, 0.0],
                    vec![stats.p99.as_millis() as f64, y_axis_max],
                ])
                .symbol(Symbol::None)
                .line_style(
                    LineStyle::new()
                        .width(2)
                        .color("#e74c3c")
                        .type_(LineStyleType::Dashed),
                ),
        )
}

fn silverman_bandwidth(data: &[f64]) -> f64 {
    if data.is_empty() {
        return 1.0;
    }

    let n = data.len() as f64;
    let mean = data.iter().sum::<f64>() / data.len() as f64;
    let variance = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / data.len() as f64;
    let std_dev = variance.sqrt();
    let iqr = calculate_iqr(data);

    let h = 0.9 * (std_dev.min(iqr / 1.34)) * n.powf(-0.2);
    h.max(1.0)
}

fn calculate_iqr(data: &[f64]) -> f64 {
    let mut sorted = data.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let q1_idx = sorted.len() / 4;
    let q3_idx = (sorted.len() * 3) / 4;

    sorted[q3_idx] - sorted[q1_idx]
}

fn kernel_density_estimation(
    data: &[f64],
    bandwidth: f64,
    n_points: usize,
) -> (Vec<f64>, Vec<f64>) {
    if data.is_empty() {
        return (vec![], vec![]);
    }

    let min = data.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = data.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    let padding = bandwidth * 3.0;
    let x_min = min - padding;
    let x_max = max + padding;

    let x: Vec<f64> = (0..n_points)
        .map(|i| x_min + (x_max - x_min) * (i as f64) / (n_points - 1) as f64)
        .collect();

    let mut y = vec![0.0; n_points];
    let sqrt_2pi = (2.0 * std::f64::consts::PI).sqrt();
    let n = data.len() as f64;

    for (i, &x) in x.iter().enumerate() {
        for &xi in data {
            let u = (x - xi) / bandwidth;
            let kernel = (-0.5 * u * u).exp() / sqrt_2pi;
            y[i] += kernel / (n * bandwidth);
        }
    }

    (x, y)
}

fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    let idx = (sorted_data.len() as f64 - 1.0) * p / 100.0;
    let lower = idx.floor() as usize;
    let upper = idx.ceil() as usize;
    let weight = idx - lower as f64;

    sorted_data[lower] * (1.0 - weight) + sorted_data[upper] * weight
}

#[derive(Debug, Clone)]
pub struct TimestampedLatency {
    pub timestamp: SystemTime,
    pub latency: Duration,
}

fn calculate_optimal_window_seconds(data: &[TimestampedLatency]) -> u64 {
    if data.is_empty() {
        return 60; // default 1 minute
    }

    // Find time span
    let timestamps: Vec<_> = data.iter().map(|d| d.timestamp).collect();
    let first = timestamps.iter().min().unwrap();
    let last = timestamps.iter().max().unwrap();
    let span_secs = last.duration_since(*first).unwrap().as_secs();

    // Target 30-50 buckets for good granularity
    let target_buckets = 40;
    let window_secs = (span_secs / target_buckets).max(1);

    // Round to nice intervals
    match window_secs {
        0..=2 => 1,
        3..=7 => 5,
        8..=20 => 10,
        21..=45 => 30,
        46..=90 => 60,
        91..=180 => 120,
        181..=450 => 300,
        451..=900 => 600,
        _ => window_secs.div_ceil(300) * 300, // round to 5-minute intervals
    }
}

fn bucket_by_time_window(
    data: &[TimestampedLatency],
    window_seconds: u64,
) -> HashMap<i64, Vec<f64>> {
    let mut buckets: HashMap<i64, Vec<f64>> = HashMap::new();

    for item in data {
        let timestamp = item.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs();
        let bucket = (timestamp / window_seconds) * window_seconds;
        buckets
            .entry(bucket as i64)
            .or_default()
            .push(item.latency.as_millis() as f64);
    }

    buckets
}

fn calculate_percentiles_by_bucket(
    buckets: &HashMap<i64, Vec<f64>>,
    percentiles: &[f64],
) -> HashMap<i64, Vec<f64>> {
    let mut result = HashMap::new();

    for (bucket, values) in buckets {
        let mut sorted = values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let percentiles: Vec<f64> = percentiles
            .iter()
            .map(|&p| percentile(&sorted, p))
            .collect();

        result.insert(*bucket, percentiles);
    }

    result
}

fn mark_sla_violations(
    data: &[(i64, f64)],
    sla_threshold: f64,
) -> Vec<(MarkAreaData, MarkAreaData)> {
    let mut violations = Vec::new();
    let mut in_violation = false;
    let mut start_time = 0i64;

    for (timestamp, value) in data {
        if *value > sla_threshold && !in_violation {
            in_violation = true;
            start_time = *timestamp;
        } else if *value <= sla_threshold && in_violation {
            in_violation = false;
            let start_dt = DateTime::<Utc>::from_timestamp(start_time, 0).unwrap();
            let end_dt = DateTime::<Utc>::from_timestamp(*timestamp, 0).unwrap();

            violations.push((
                MarkAreaData::new()
                    .name("SLA Violation")
                    .x_axis(start_dt.format("%Y-%m-%d %H:%M").to_string()),
                MarkAreaData::new().x_axis(end_dt.format("%Y-%m-%d %H:%M").to_string()),
            ));
        }
    }

    violations
}

fn build_time_series_confidence_chart(
    data: &[TimestampedLatency],
    window_minutes: Option<u64>,
    sla_threshold: Option<f64>,
) -> Chart {
    // Calculate optimal window if not provided
    let window_seconds = match window_minutes {
        Some(minutes) => minutes * 60,
        None => calculate_optimal_window_seconds(data),
    };

    let buckets = bucket_by_time_window(data, window_seconds);
    let percentile_data = calculate_percentiles_by_bucket(&buckets, &[10.0, 50.0, 90.0]);

    let mut sorted_buckets: Vec<_> = buckets.keys().collect();
    sorted_buckets.sort();

    let mut timestamps = Vec::new();
    let mut p10 = Vec::new();
    let mut p50 = Vec::new();
    let mut p90_minus_p10 = Vec::new();
    let mut p50_line = Vec::new();

    for &bucket in &sorted_buckets {
        let dt = DateTime::<Utc>::from_timestamp(*bucket, 0).unwrap();
        // Format based on window size
        let format_str = match window_seconds {
            1..=59 => "%H:%M:%S",  // Show seconds for windows < 1 minute
            60..=3599 => "%H:%M",  // Show hours:minutes for windows < 1 hour
            _ => "%Y-%m-%d %H:%M", // Show date for larger windows
        };
        timestamps.push(dt.format(format_str).to_string());

        if let Some(percentiles) = percentile_data.get(bucket) {
            p10.push(percentiles[0]);
            p50.push(percentiles[1]);
            p90_minus_p10.push(percentiles[2] - percentiles[0]);
            p50_line.push((*bucket, percentiles[1]));
        }
    }

    let mut chart = Chart::new()
        .title(
            Title::new()
                .text("Latency Over Time with Confidence Bands")
                .left("center")
                .top("2%")
                .text_style(TextStyle::new().color("#e4e6eb").font_size(18)),
        )
        .tooltip(
            Tooltip::new()
                .trigger(Trigger::Axis)
                .axis_pointer(AxisPointer::new().type_(AxisPointerType::Cross))
                .formatter(JsFunction::new_with_args(
                    "params",
                    "let time = params[0].axisValue;
                     let p10 = params[0].value;
                     let p90 = p10 + params[1].value;
                     let p50 = params[2].value;
                     return time + '<br/>' +
                            'P10: ' + p10.toFixed(0) + ' ms<br/>' +
                            'P50: ' + p50.toFixed(0) + ' ms<br/>' +
                            'P90: ' + p90.toFixed(0) + ' ms';",
                )),
        )
        .legend(
            Legend::new()
                .data(vec!["P10-P90 Band", "P50 (Median)"])
                .top("8%")
                .text_style(TextStyle::new().color("#64748b"))
                .item_width(30)
                .item_height(14),
        )
        .toolbox(
            Toolbox::new().feature(
                Feature::new()
                    .data_zoom(ToolboxDataZoom::new().y_axis_index("none"))
                    .restore(Restore::new())
                    .save_as_image(SaveAsImage::new()),
            ),
        )
        .grid(
            Grid::new()
                .left("5%")
                .right("3%")
                .bottom("12%")
                .top("12%")
                .contain_label(true),
        )
        .x_axis(
            Axis::new()
                .type_(AxisType::Category)
                .data(timestamps)
                .boundary_gap(false)
                .axis_label(AxisLabel::new().rotate(45).color("#64748b").formatter(
                    JsFunction::new_with_args("value", "return value.split(' ')[1] || value;"),
                )),
        )
        .y_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name("Latency (ms)")
                .name_location(NameLocation::Middle)
                .name_gap(40)
                .name_text_style(TextStyle::new().color("#64748b"))
                .axis_label(AxisLabel::new().color("#64748b").formatter("{value}"))
                .split_line(SplitLine::new().line_style(LineStyle::new().color("#2a3451")))
                .min(0),
        )
        .data_zoom(
            DataZoom::new()
                .type_(DataZoomType::Inside)
                .start(0)
                .end(100),
        )
        .data_zoom(DataZoom::new().start(0).end(100))
        .series(
            Line::new()
                .name("P10")
                .data(p10)
                .line_style(LineStyle::new().opacity(0))
                .stack("confidence-band")
                .symbol(Symbol::None),
        )
        .series(
            Line::new()
                .name("P10-P90 Band")
                .data(p90_minus_p10)
                .line_style(LineStyle::new().opacity(0))
                .area_style(AreaStyle::new().color("rgba(0, 212, 255, 0.3)"))
                .stack("confidence-band")
                .symbol(Symbol::None),
        )
        .series(
            Line::new()
                .name("P50 (Median)")
                .data(p50)
                .line_style(LineStyle::new().width(3).color("#40e0d0"))
                .symbol(Symbol::Circle)
                .symbol_size(6)
                .item_style(ItemStyle::new().color("#40e0d0"))
                .smooth(0.3),
        );

    if let Some(threshold) = sla_threshold {
        let violations = mark_sla_violations(&p50_line, threshold);
        if !violations.is_empty() {
            chart = chart.series(
                Line::new()
                    .name("SLA Threshold")
                    .mark_line(
                        MarkLine::new()
                            .data(vec![MarkLineVariant::Simple(
                                MarkLineData::new()
                                    .y_axis(threshold)
                                    .name("SLA Threshold")
                                    .label(Label::new().formatter("SLA Threshold")),
                            )])
                            .line_style(
                                LineStyle::new()
                                    .color("#ff006e")
                                    .width(2)
                                    .type_(LineStyleType::Dashed),
                            ),
                    )
                    .mark_area(
                        MarkArea::new().data(violations).item_style(
                            ItemStyle::new()
                                .color("rgba(255, 0, 110, 0.15)")
                                .border_color("#ff006e")
                                .border_width(2),
                        ),
                    )
                    .data(Vec::<f64>::new()),
            );
        }
    }

    chart
}

fn build_interactive_time_series_chart(data: &[TimestampedLatency]) -> Chart {
    let mut sorted_data = data.to_vec();
    sorted_data.sort_by_key(|item| item.timestamp);

    let mut timestamps = Vec::new();
    let mut values = Vec::new();

    for item in &sorted_data {
        let dt = DateTime::<Utc>::from(item.timestamp);
        timestamps.push(dt.format("%Y-%m-%d %H:%M:%S").to_string());
        values.push(item.latency.as_millis() as f64);
    }

    let chart = Chart::new()
        .title(
            Title::new()
                .text("Interactive Latency Time Series")
                .subtext(format!("{} data points", values.len()))
                .left("center")
                .text_style(TextStyle::new().color("#e4e6eb").font_size(18))
                .subtext_style(TextStyle::new().color("#64748b")),
        )
        .tooltip(
            Tooltip::new()
                .trigger(Trigger::Axis)
                .axis_pointer(AxisPointer::new().type_(AxisPointerType::Cross))
                .formatter(JsFunction::new_with_args("params",
                    "const time = params[0].axisValue;
                     const value = params[0].value;
                     return time + '<br/>Latency: ' + value.toFixed(0) + ' ms';"
                )),
        )
        .toolbox(
            Toolbox::new()
                .feature(Feature::new()
                    .data_zoom(ToolboxDataZoom::new().y_axis_index("none"))
                    .restore(Restore::new())
                    .save_as_image(SaveAsImage::new())),
        )
        .grid(
            Grid::new()
                .left("5%")
                .right("3%")
                .bottom("15%")
                .top("10%")
                .contain_label(true),
        )
        .x_axis(
            Axis::new()
                .type_(AxisType::Category)
                .data(timestamps)
                .boundary_gap(false)
                .axis_label(AxisLabel::new()
                    .color("#64748b")
                    .formatter(JsFunction::new_with_args("value",
                        "return value.split(' ')[1] || value;"
                    )))
,
        )
        .y_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name("Latency (ms)")
                .name_location(NameLocation::Middle)
                .name_gap(40)
                .name_text_style(TextStyle::new().color("#64748b"))
                .axis_label(AxisLabel::new().color("#64748b"))
                .split_line(SplitLine::new().line_style(LineStyle::new().color("#2a3451"))),
        )
        .data_zoom(
            DataZoom::new()
                .type_(DataZoomType::Inside)
                .start(90)
                .end(100),
        )
        .data_zoom(
            DataZoom::new()
                .show(true)
                .start(90)
                .end(100)
                .handle_icon("path://M306.1,413c0,2.2-1.8,4-4,4h-59.8c-2.2,0-4-1.8-4-4V200.8c0-2.2,1.8-4,4-4h59.8c2.2,0,4,1.8,4,4V413z"),
        )
        .series(
            Line::new()
                .name("Latency")
                .data(values)
                .symbol(Symbol::None)
                .line_style(LineStyle::new().width(2).color("#1e90ff"))
                .area_style(AreaStyle::new()
                    .color("rgba(30, 144, 255, 0.15)")),
        );

    chart
}

pub fn generate_combined_plots(
    latencies: &[Duration],
    timestamped_data: &[TimestampedLatency],
    output_path: PathBuf,
    stats: &LatencyStats,
    window_minutes: Option<u64>,
    sla_threshold: Option<f64>,
) -> Result<()> {
    let density_chart = build_density_chart(latencies, stats);
    let timeseries_chart =
        build_time_series_confidence_chart(timestamped_data, window_minutes, sla_threshold);
    let interactive_chart = build_interactive_time_series_chart(timestamped_data);

    let density_option = density_chart.to_string();
    let timeseries_option = timeseries_chart.to_string();
    let interactive_option = interactive_chart.to_string();

    crate::latency::combined_plot::generate_combined_html(
        &density_option,
        &timeseries_option,
        &interactive_option,
        stats,
        &output_path,
    )?;

    Ok(())
}
