use crate::latency::plotting::LatencyStats;
use anyhow::Result;
use std::fs;
use std::path::Path;

pub fn generate_combined_html(
    density_chart_option: &str,
    timeseries_chart_option: &str,
    interactive_chart_option: &str,
    stats: &LatencyStats,
    output_path: &Path,
) -> Result<()> {
    let html_content = format!(
        r##"<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Latency Analysis</title>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.1/dist/echarts.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/echarts-gl@2.0.9/dist/echarts-gl.min.js"></script>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap" rel="stylesheet">
    <style>
      * {{
        box-sizing: border-box;
      }}
      
      body {{
        background: #0a0e27;
        color: #e4e6eb;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        margin: 0;
        padding: 0;
        height: 100vh;
        position: relative;
        overflow: hidden;
        display: flex;
        flex-direction: column;
      }}
      
      body::before {{
        content: '';
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: radial-gradient(circle at 20% 50%, rgba(0, 212, 255, 0.1) 0%, transparent 50%),
                    radial-gradient(circle at 80% 80%, rgba(255, 0, 110, 0.1) 0%, transparent 50%);
        pointer-events: none;
        z-index: 1;
      }}
      
      .container {{ 
        margin: 0 auto;
        width: 100%;
        max-width: 100%;
        position: relative;
        z-index: 2;
        padding: 10px;
        flex: 1;
        display: flex;
        flex-direction: column;
        overflow: hidden;
      }}
      
      .stats-panel {{
        background: #1a1f3a;
        border-radius: 8px;
        padding: 12px;
        margin-bottom: 12px;
        box-shadow: 0 3px 10px rgba(0, 0, 0, 0.5);
        border: 1px solid #2a3451;
        animation: fadeInDown 0.4s ease-out;
      }}
      
      .stats-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        gap: 20px;
      }}
      
      .stat-item {{
        text-align: center;
        padding: 8px;
        background: rgba(37, 43, 72, 0.5);
        border-radius: 8px;
        border: 1px solid rgba(42, 52, 81, 0.5);
        transition: all 0.2s ease;
      }}
      
      .stat-item:hover {{
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
        border-color: #00d4ff;
      }}
      
      .stat-label {{
        font-size: 12px;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 8px;
      }}
      
      .stat-value {{
        font-size: 20px;
        font-weight: 600;
        color: #00d4ff;
      }}
      
      .charts-grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 12px;
        height: 100%;
        flex: 1;
      }}
      
      .full-width {{
        grid-column: 1 / -1;
        height: 100%;
      }}
      
      h1 {{
        font-weight: 300;
        letter-spacing: -0.02em;
        font-size: 1.5rem;
        background: linear-gradient(135deg, #00d4ff 0%, #ff006e 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin: 0.5rem 0;
        animation: fadeInDown 0.4s ease-out;
      }}
      
      @keyframes fadeInDown {{
        from {{
          opacity: 0;
          transform: translateY(-20px);
        }}
        to {{
          opacity: 1;
          transform: translateY(0);
        }}
      }}
      
      .nav-tabs {{
        border: none;
        gap: 8px;
        margin-bottom: 12px;
        animation: fadeIn 0.5s ease-out 0.15s both;
      }}
      
      @keyframes fadeIn {{
        from {{
          opacity: 0;
        }}
        to {{
          opacity: 1;
        }}
      }}
      
      .nav-link {{
        background: #1a1f3a;
        border: 1px solid #2a3451;
        color: #64748b;
        padding: 12px 28px;
        border-radius: 12px 12px 0 0;
        transition: all 0.2s ease;
        font-weight: 500;
        position: relative;
        overflow: hidden;
      }}
      
      .nav-link::before {{
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(0, 212, 255, 0.1), transparent);
        transition: left 0.3s ease;
      }}
      
      .nav-link:hover {{
        background: #252b48;
        color: #e4e6eb;
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
      }}
      
      .nav-link:hover::before {{
        left: 100%;
      }}
      
      .nav-link.active {{
        background: #252b48;
        border-color: #00d4ff;
        border-bottom: 2px solid #00d4ff;
        color: #00d4ff;
        box-shadow: 0 4px 20px rgba(0, 212, 255, 0.2);
      }}
      
      .chart-container {{
        background: #1a1f3a;
        border-radius: 8px;
        padding: 12px;
        box-shadow: 0 3px 10px rgba(0, 0, 0, 0.5);
        border: 1px solid #2a3451;
        width: 100%;
        height: 100%;
        position: relative;
        animation: fadeInUp 0.4s ease-out;
      }}
      
      .chart-container-small {{
        height: 100%;
      }}
      
      @keyframes fadeInUp {{
        from {{
          opacity: 0;
          transform: translateY(20px);
        }}
        to {{
          opacity: 1;
          transform: translateY(0);
        }}
      }}
      
      .tab-content {{
        padding-top: 0;
        flex: 1;
        display: flex;
        overflow: hidden;
      }}
      
      .tab-pane {{
        animation: fadeIn 0.3s ease-out;
        width: 100%;
        height: 100%;
        display: flex;
        flex-direction: column;
      }}
      
      /* Override Bootstrap dark theme conflicts */
      .nav-tabs .nav-link:focus,
      .nav-tabs .nav-link:hover {{
        border-color: #2a3451;
      }}
      
      .nav-tabs .nav-link.active:focus,
      .nav-tabs .nav-link.active:hover {{
        border-color: #00d4ff #00d4ff #00d4ff;
      }}
      
      /* Loading spinner */
      .loading {{
        display: inline-block;
        width: 20px;
        height: 20px;
        border: 2px solid rgba(0, 212, 255, 0.3);
        border-radius: 50%;
        border-top-color: #00d4ff;
        animation: spin 1s ease-in-out infinite;
        margin: 0 auto;
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
      }}
      
      @keyframes spin {{
        to {{ transform: translate(-50%, -50%) rotate(360deg); }}
      }}
      
      /* Subtle background pattern */
      .chart-container::after {{
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background-image: radial-gradient(circle at 1px 1px, rgba(0, 212, 255, 0.03) 1px, transparent 1px);
        background-size: 40px 40px;
        pointer-events: none;
        border-radius: 16px;
      }}
    </style>
  </head>
  <body>
    <div class="container">
      <h1 class="text-center mb-4">Latency Analysis Dashboard</h1>
      
      <div class="stats-panel">
        <div class="stats-grid">
          <div class="stat-item">
            <div class="stat-label">Average</div>
            <div class="stat-value">{avg_ms} ms</div>
          </div>
          <div class="stat-item">
            <div class="stat-label">Median (P50)</div>
            <div class="stat-value">{p50_ms} ms</div>
          </div>
          <div class="stat-item">
            <div class="stat-label">P95</div>
            <div class="stat-value">{p95_ms} ms</div>
          </div>
          <div class="stat-item">
            <div class="stat-label">P99</div>
            <div class="stat-value">{p99_ms} ms</div>
          </div>
          <div class="stat-item">
            <div class="stat-label">Min</div>
            <div class="stat-value">{min_ms} ms</div>
          </div>
          <div class="stat-item">
            <div class="stat-label">Max</div>
            <div class="stat-value">{max_ms} ms</div>
          </div>
        </div>
      </div>
      
      <ul class="nav nav-tabs" id="plotTabs" role="tablist">
        <li class="nav-item" role="presentation">
          <button class="nav-link active" id="overview-tab" data-bs-toggle="tab" data-bs-target="#overview" type="button" role="tab" aria-controls="overview" aria-selected="true">
            Overview
          </button>
        </li>
        <li class="nav-item" role="presentation">
          <button class="nav-link" id="detailed-tab" data-bs-toggle="tab" data-bs-target="#detailed" type="button" role="tab" aria-controls="detailed" aria-selected="false">
            Detailed Analysis
          </button>
        </li>
      </ul>
      
      <div class="tab-content" id="plotTabContent">
        <div class="tab-pane fade show active" id="overview" role="tabpanel" aria-labelledby="overview-tab">
          <div class="charts-grid">
            <div id="densityChart" class="chart-container"></div>
            <div id="timeseriesChart" class="chart-container"></div>
          </div>
        </div>
        <div class="tab-pane fade" id="detailed" role="tabpanel" aria-labelledby="detailed-tab">
          <div id="interactiveChart" class="chart-container full-width"></div>
        </div>
      </div>
    </div>
    
    <script type="text/javascript">
      var densityChart = echarts.init(document.getElementById('densityChart'), 'dark');
      var timeseriesChart = echarts.init(document.getElementById('timeseriesChart'), 'dark');
      var interactiveChart = echarts.init(document.getElementById('interactiveChart'), 'dark');
      
      var densityOption = {density_option};
      var timeseriesOption = {timeseries_option};
      var interactiveOption = {interactive_option};
      
      densityChart.setOption(densityOption);
      timeseriesChart.setOption(timeseriesOption);
      
      document.getElementById('detailed-tab').addEventListener('shown.bs.tab', function (e) {{
        interactiveChart.setOption(interactiveOption);
        interactiveChart.resize();
      }});
      
      window.addEventListener('resize', function() {{
        densityChart.resize();
        timeseriesChart.resize();
        interactiveChart.resize();
      }});
      
      // Initial resize to fill containers
      setTimeout(() => {{
        densityChart.resize();
        timeseriesChart.resize();
        interactiveChart.resize();
      }}, 100);
    </script>
  </body>
</html>"##,
        density_option = density_chart_option,
        timeseries_option = timeseries_chart_option,
        interactive_option = interactive_chart_option,
        avg_ms = stats.avg.as_millis(),
        p50_ms = stats.p50.as_millis(),
        p95_ms = stats.p95.as_millis(),
        p99_ms = stats.p99.as_millis(),
        min_ms = stats.min.as_millis(),
        max_ms = stats.max.as_millis()
    );

    fs::write(output_path, html_content)?;
    Ok(())
}
