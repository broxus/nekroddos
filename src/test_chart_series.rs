use charming::{series::Line, Chart};

fn test_multiple_series() {
    let chart = Chart::new()
        .series(
            Line::new()
                .name("Series 1")
                .data(vec![vec![1.0, 2.0], vec![2.0, 3.0]]),
        )
        .series(
            Line::new()
                .name("Series 2")
                .data(vec![vec![1.0, 3.0], vec![2.0, 4.0]]),
        )
        .series(
            Line::new()
                .name("Series 3")
                .data(vec![vec![1.0, 4.0], vec![2.0, 5.0]]),
        );

    let json = chart.to_string();
    println!("{json}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chart_serialization() {
        test_multiple_series();
    }
}
