CORPORATE_COMPLIANCE_HTML = r'''
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Reporte SIPAE</title>
  <style>
    :root {
      --bg: #f4f6f8;
      --panel: #ffffff;
      --panel-soft: #fbfcfd;
      --text: #111827;
      --muted: #6b7280;
      --muted-2: #9ca3af;
      --line: #e5e7eb;
      --line-strong: #d1d5db;
      --soft: #f9fafb;
      --green: #15803d;
      --green-soft: #dcfce7;
      --yellow: #b45309;
      --yellow-soft: #fef3c7;
      --red: #b91c1c;
      --red-soft: #fee2e2;
      --gray: #64748b;
      --gray-soft: #f1f5f9;
      --ink: #0f172a;
      --radius: 16px;
      --shadow: 0 14px 36px rgba(15, 23, 42, .06);
      --font: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    html { background: var(--bg); }
    body { margin: 0; color: var(--text); background: var(--bg); font-family: var(--font); }
    button, input, select { font: inherit; }
    button { cursor: pointer; }
    .topbar { position: sticky; top: 0; z-index: 10; border-bottom: 1px solid var(--line); background: rgba(255,255,255,.94); backdrop-filter: blur(16px); }
    .topbar-inner { max-width: 1480px; margin: 0 auto; padding: 14px 22px; display: grid; grid-template-columns: minmax(260px, 1fr) auto; gap: 18px; align-items: center; }
    .eyebrow { color: var(--muted); font-size: 11px; font-weight: 800; letter-spacing: .13em; text-transform: uppercase; }
    .title { margin-top: 2px; font-size: 18px; font-weight: 850; letter-spacing: -.02em; }
    .filters { display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 8px; align-items: center; }
    .chip-row { display: inline-flex; gap: 4px; padding: 4px; border: 1px solid var(--line); background: var(--soft); border-radius: 999px; max-width: 100%; overflow-x: auto; }
    .chip { border: 0; border-radius: 999px; padding: 7px 11px; background: transparent; color: #374151; font-size: 12px; font-weight: 750; white-space: nowrap; }
    .chip.active { background: var(--ink); color: #fff; }
    .date-input { display: none; border: 1px solid var(--line); border-radius: 999px; padding: 7px 10px; color: #374151; background: #fff; font-size: 12px; font-weight: 700; }
    .date-input.visible { display: inline-block; }
    .refresh { border: 0; border-radius: 999px; padding: 8px 14px; background: var(--ink); color: #fff; font-size: 12px; font-weight: 800; }
    .page { max-width: 1480px; margin: 0 auto; padding: 22px 22px 64px; }
    .hidden { display: none !important; }
    .state { padding: 18px 20px; border: 1px solid var(--line); border-radius: var(--radius); background: var(--panel); box-shadow: var(--shadow); color: var(--muted); font-weight: 750; }
    .state.error { border-color: rgba(185,28,28,.35); background: #fff7f7; color: var(--red); }
    .hero { display: grid; grid-template-columns: minmax(0, 1.1fr) repeat(3, minmax(180px, .3fr)); gap: 12px; margin-bottom: 12px; }
    .panel { background: var(--panel); border: 1px solid rgba(15,23,42,.08); border-radius: var(--radius); box-shadow: var(--shadow); }
    .intro { padding: 22px; }
    h1 { margin: 0; font-size: clamp(26px, 3.2vw, 42px); line-height: 1.02; letter-spacing: -.045em; font-weight: 900; }
    .subtitle { margin-top: 10px; max-width: 760px; color: #4b5563; font-size: 14px; line-height: 1.55; }
    .stamp-row { display: flex; flex-wrap: wrap; gap: 7px; margin-top: 16px; }
    .stamp { display: inline-flex; align-items: center; gap: 7px; padding: 6px 9px; border: 1px solid var(--line); border-radius: 999px; background: #fff; color: #374151; font-size: 12px; font-weight: 750; }
    .kpi { padding: 17px; display: flex; min-height: 144px; flex-direction: column; justify-content: space-between; }
    .kpi-label { color: var(--muted); font-size: 11px; font-weight: 850; letter-spacing: .1em; text-transform: uppercase; }
    .kpi-value { margin-top: 12px; font-size: 37px; line-height: .95; font-weight: 900; letter-spacing: -.055em; }
    .kpi-name { font-size: 15px; font-weight: 820; }
    .kpi-detail { margin-top: 5px; color: var(--muted); font-size: 12px; font-weight: 650; }
    .score.green { color: var(--green); } .score.yellow { color: var(--yellow); } .score.red { color: var(--red); } .score.gray { color: var(--gray); }
    .dot { width: 9px; height: 9px; display: inline-block; border-radius: 99px; margin-right: 7px; vertical-align: middle; background: var(--gray); }
    .dot.green { background: var(--green); } .dot.yellow { background: var(--yellow); } .dot.red { background: var(--red); } .dot.gray { background: var(--gray); }
    .main-grid { display: grid; gap: 12px; }
    .section-head { padding: 15px 17px 13px; border-bottom: 1px solid var(--line); display: flex; align-items: center; justify-content: space-between; gap: 14px; }
    .section-label { color: var(--muted); font-size: 11px; font-weight: 850; letter-spacing: .11em; text-transform: uppercase; }
    .section-title { margin-top: 3px; font-size: 19px; font-weight: 880; letter-spacing: -.025em; }
    .section-body { padding: 17px; }
    .matrix-wrap { overflow-x: auto; }
    table { border-collapse: collapse; width: 100%; }
    .matrix { min-width: 1460px; border: 1px solid var(--line); border-radius: 14px; overflow: hidden; border-collapse: separate; border-spacing: 0; }
    .matrix th, .matrix td { padding: 12px; border-bottom: 1px solid var(--line); border-right: 1px solid var(--line); text-align: left; }
    .matrix tr:last-child td { border-bottom: 0; }
    .matrix th:last-child, .matrix td:last-child { border-right: 0; }
    .matrix th { background: var(--panel-soft); color: #475569; font-size: 11px; letter-spacing: .08em; text-transform: uppercase; font-weight: 850; }
    .plantel-code { font-weight: 900; }
    .plantel-name { margin-top: 2px; color: var(--muted); font-size: 12px; line-height: 1.35; }
    .heat { min-width: 124px; cursor: pointer; transition: box-shadow .15s ease, transform .15s ease; }
    .heat:hover { box-shadow: inset 0 0 0 1px rgba(15,23,42,.08); transform: translateY(-1px); }
    .heat:hover { box-shadow: inset 0 0 0 999px rgba(17,24,39,.035); }
    .heat.green { background: var(--green-soft); } .heat.yellow { background: var(--yellow-soft); } .heat.red { background: var(--red-soft); } .heat.gray { background: var(--gray-soft); }
    .cell-score { font-size: 19px; font-weight: 900; letter-spacing: -.035em; }
    .cell-label { margin-top: 3px; color: #4b5563; font-size: 11px; line-height: 1.35; }
    .charts { display: grid; grid-template-columns: minmax(0, 1fr) minmax(310px, .44fr); gap: 12px; }
    .chart-card { min-height: 335px; border: 1px solid var(--line); border-radius: 14px; background: #fff; padding: 14px; }
    .level-access { display: grid; gap: 10px; }
    .level-row { display: grid; grid-template-columns: 120px minmax(160px,1fr) 110px 90px; gap: 12px; align-items: center; padding: 10px 0; border-bottom: 1px solid var(--line); }
    .level-row:last-child { border-bottom: 0; }
    .level-name { font-weight: 900; color: #1f2937; }
    .level-meta { color: var(--muted); font-size: 12px; margin-top: 2px; }
    .level-number { text-align: right; font-size: 19px; font-weight: 900; color: #111827; letter-spacing: -.035em; }
    .level-label { text-align: right; color: var(--muted); font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing: .06em; }
    .chart-title { margin-bottom: 12px; font-size: 13px; color: #374151; font-weight: 850; }
    .bar-list { display: grid; gap: 11px; }
    .bar-row { display: grid; grid-template-columns: 90px minmax(120px,1fr) 56px; gap: 10px; align-items: center; }
    .bar-name { font-size: 12px; font-weight: 800; color: #374151; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .track { height: 12px; border-radius: 999px; background: #eef2f7; overflow: hidden; }
    .fill { height: 100%; width: 0; border-radius: 999px; background: var(--gray); }
    .fill.green { background: var(--green); } .fill.yellow { background: var(--yellow); } .fill.red { background: var(--red); } .fill.gray { background: var(--gray); }
    .bar-value { text-align: right; font-size: 12px; font-weight: 850; }
    .trend-grid { display: grid; grid-template-columns: minmax(0, 1fr) 260px; gap: 12px; }
    .select { border: 1px solid var(--line); border-radius: 999px; background: #fff; color: #374151; padding: 7px 11px; font-size: 12px; font-weight: 800; }
    .line-chart { width: 100%; height: 350px; border: 1px solid var(--line); border-radius: 14px; background: #fff; display: block; }
    .trend-side { border: 1px solid var(--line); border-radius: 14px; background: #fff; padding: 15px; }
    .trend-big { margin-top: 14px; font-size: 32px; font-weight: 900; letter-spacing: -.04em; }
    .trend-small { margin-top: 8px; color: var(--muted); font-size: 13px; line-height: 1.45; }
    .legend { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
    .legend-item { display: inline-flex; align-items: center; gap: 6px; color: #4b5563; font-size: 12px; font-weight: 700; }
    .legend-line { width: 18px; height: 3px; border-radius: 99px; background: var(--gray); }
    .detail-grid { display: grid; grid-template-columns: 270px minmax(0,1fr); gap: 12px; }
    .plantel-list-detail { display: grid; gap: 8px; }
    .plantel-option { border: 1px solid var(--line); background: #fff; border-radius: 12px; padding: 11px 12px; display: flex; justify-content: space-between; gap: 12px; align-items: center; text-align: left; }
    .plantel-option.active { border-color: var(--ink); box-shadow: inset 4px 0 0 var(--ink); }
    .detail-table { border: 1px solid var(--line); border-radius: 14px; overflow: hidden; border-collapse: separate; border-spacing: 0; }
    .detail-table th, .detail-table td { padding: 12px 13px; border-bottom: 1px solid var(--line); text-align: left; }
    .detail-table tr:last-child td { border-bottom: 0; }
    .detail-table th { background: #fafafa; color: #4b5563; font-size: 11px; letter-spacing: .08em; text-transform: uppercase; font-weight: 850; }

    .diagnostic-box { display: grid; gap: 10px; }
    .diag-actions { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .copy-btn { border: 1px solid var(--line); background: #fff; border-radius: 999px; padding: 8px 12px; font-size: 12px; font-weight: 850; color: #374151; cursor: pointer; }
    .diag-help { color: var(--muted); font-size: 12px; }
    .diag-json { margin: 0; min-height: 150px; max-height: 260px; overflow: auto; border: 1px solid var(--line); border-radius: 14px; background: #111827; color: #e5e7eb; padding: 12px; font-size: 11px; line-height: 1.5; white-space: pre-wrap; word-break: break-word; }

    @media (max-width: 1100px) { .topbar-inner, .hero, .charts, .trend-grid, .detail-grid { grid-template-columns: 1fr; } .filters { justify-content: flex-start; } }
    @media (max-width: 720px) { .page { padding: 14px 12px 48px; } .topbar-inner { padding: 12px; } .kpi-value { font-size: 33px; } .chart-card, .line-chart { min-height: 300px; height: 300px; } }
  </style>
</head>
<body>
  <header class="topbar">
    <div class="topbar-inner">
      <div>
        <div class="eyebrow">Reporte ejecutivo</div>
        <div class="title">Reporte SIPAE</div>
      </div>
      <div class="filters">
        <div class="chip-row" id="scopeFilters">
          <button class="chip active" data-scope="month">Mes</button>
          <button class="chip" data-scope="today">Hoy</button>
          <button class="chip" data-scope="ciclo_escolar">Ciclo</button>
          <button class="chip" data-scope="range">Rango</button>
        </div>
        <input id="startDate" class="date-input" type="date" />
        <input id="endDate" class="date-input" type="date" />
        <div class="chip-row" id="plantelFilters"></div>
        <button class="refresh" id="refreshBtn">Actualizar</button>
      </div>
    </div>
  </header>

  <main class="page">
    <div id="loading" class="state">Cargando reporte...</div>
    <div id="error" class="state error hidden"></div>

    <div id="report" class="hidden">
      <section class="hero">
        <div class="panel intro">
          <h1>Reporte SIPAE</h1>
          <div class="subtitle">Vista ejecutiva del periodo evaluado con métricas de 1 a 100 por plantel. Los indicadores operativos se calculan como valor positivo: 100 es sano, menor valor indica mayor atención requerida.</div>
          <div class="stamp-row">
            <span class="stamp" id="periodStamp">Periodo —</span><span class="stamp" id="businessDaysStamp">Días hábiles —</span>
            <span class="stamp" id="updatedStamp">Actualizado —</span>
            <span class="stamp"><span class="dot green"></span>85–100</span>
            <span class="stamp"><span class="dot yellow"></span>70–84</span>
            <span class="stamp"><span class="dot red"></span>1–69</span>
          </div>
        </div>
        <div class="panel kpi"><div><div class="kpi-label">General</div><div class="kpi-value score gray" id="generalScore">—</div></div><div><div class="kpi-name" id="generalTitle">Sin datos</div><div class="kpi-detail" id="generalDetail">—</div></div></div>
        <div class="panel kpi"><div><div class="kpi-label">Mejor plantel</div><div class="kpi-value score gray" id="bestScore">—</div></div><div><div class="kpi-name" id="bestTitle">—</div><div class="kpi-detail" id="bestDetail">—</div></div></div>
        <div class="panel kpi"><div><div class="kpi-label">Menor resultado</div><div class="kpi-value score gray" id="worstScore">—</div></div><div><div class="kpi-name" id="worstTitle">—</div><div class="kpi-detail" id="worstDetail">—</div></div></div>
      </section>

      <div class="main-grid">
        <section class="panel">
          <div class="section-head"><div><div class="section-label">Resumen</div><div class="section-title">Mapa de cumplimiento</div></div></div>
          <div class="section-body"><div class="matrix-wrap"><table class="matrix" id="matrix"></table></div></div>
        </section>

        <section class="panel">
          <div class="section-head"><div><div class="section-label">Comparativo</div><div class="section-title">Planteles y áreas</div></div></div>
          <div class="section-body charts">
            <div class="chart-card"><div class="chart-title">Cumplimiento general por plantel</div><div id="plantelBars" class="bar-list"></div></div>
            <div class="chart-card"><div class="chart-title">Promedio por métrica</div><div id="metricBars" class="bar-list"></div></div>
          </div>
        </section>

        <section class="panel">
          <div class="section-head"><div><div class="section-label">Accesos</div><div class="section-title">Hora promedio de entrada por nivel</div></div></div>
          <div class="section-body">
            <div class="level-access" id="levelAccess"></div>
          </div>
        </section>

        <section class="panel">
          <div class="section-head">
            <div><div class="section-label">Tendencia</div><div class="section-title">Evolución del periodo</div></div>
            <select id="trendMetric" class="select">
              <option value="general">General</option>
              <option value="roll_call">Pase de lista</option>
              <option value="student_attendance">Asistencia alumnos</option>
              <option value="scans">Escaneos</option>
              <option value="scan_balance">Balance accesos</option>
              <option value="student_punctuality">Puntualidad alumnos</option>
            </select>
          </div>
          <div class="section-body trend-grid">
            <div>
              <svg id="trendSvg" class="line-chart" viewBox="0 0 900 350" preserveAspectRatio="none" role="img" aria-label="Tendencia"></svg>
              <div id="trendLegend" class="legend"></div>
            </div>
            <div class="trend-side"><div class="section-label" id="trendName">Métrica</div><div class="trend-big score gray" id="trendAverage">—</div><div class="trend-small" id="trendText">Promedio del periodo seleccionado.</div></div>
          </div>
        </section>

        <section class="panel">
          <div class="section-head"><div><div class="section-label">Detalle</div><div class="section-title">Plantel seleccionado</div></div></div>
          <div class="section-body detail-grid">
            <div class="plantel-list-detail" id="plantelSelector"></div>
            <table class="detail-table" id="detailTable"></table>
          </div>
        </section>

        <section class="panel">
          <div class="section-head"><div><div class="section-label">Diagnóstico</div><div class="section-title">JSON compacto para validar datos</div></div></div>
          <div class="section-body diagnostic-box">
            <div class="diag-actions">
              <button class="copy-btn" id="copyDiagnosticBtn">Copiar diagnóstico</button>
              <span class="diag-help">Pega este bloque en el chat para ajustar la lógica con datos reales.</span>
            </div>
            <pre class="diag-json" id="diagnosticJson">{}</pre>
          </div>
        </section>
      </div>
    </div>
  </main>

  <script>
    var PLANTELES = ["PT", "PM", "ST", "SM", "PREET", "PREEM"];
    var METRIC_ORDER = ["general", "roll_call", "student_attendance", "scans", "scan_balance", "student_punctuality", "planning", "observations", "observation_coverage", "sapf"];
    var METRIC_LABELS = { general: "General", roll_call: "Pase de lista", student_attendance: "Asistencia alumnos", scans: "Escaneos", scan_balance: "Balance accesos", student_punctuality: "Puntualidad alumnos", planning: "Planeaciones", observations: "Observaciones", observation_coverage: "Cobertura obs.", sapf: "Seguimientos" };
    var LINE_COLORS = ["#111827", "#15803d", "#b91c1c", "#b45309", "#2563eb", "#7c3aed"];
    var state = { scope: "month", planteles: {}, data: null, selectedPlantel: null };
    for (var p0 = 0; p0 < PLANTELES.length; p0 += 1) state.planteles[PLANTELES[p0]] = true;

    function byId(id) { return document.getElementById(id); }
    function hasOwn(obj, key) { return Object.prototype.hasOwnProperty.call(obj || {}, key); }
    function get(obj, path, fallback) {
      var current = obj;
      for (var i = 0; i < path.length; i += 1) {
        if (current === null || current === undefined || !hasOwn(current, path[i])) return fallback;
        current = current[path[i]];
      }
      return current === undefined ? fallback : current;
    }
    function esc(value) {
      return String(value === null || value === undefined ? "" : value).replace(/[&<>"']/g, function (c) {
        return { "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" }[c];
      });
    }
    function num(value) {
      if (value === null || value === undefined || value === "") return null;
      var parsed = Number(value);
      if (!isFinite(parsed)) return null;
      return parsed;
    }
    function pct(value) {
      var n = num(value);
      return n === null ? "—" : n.toFixed(1) + "%";
    }
    function colorFor(score) {
      var n = num(score);
      if (n === null) return "gray";
      if (n >= 85) return "green";
      if (n >= 70) return "yellow";
      return "red";
    }
    function mix(a, b, t) { return a + (b - a) * t; }
    function heatBackground(score) {
      var n = num(score);
      if (n === null) return "linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%)";
      var red = [185, 28, 28], yellow = [180, 83, 9], green = [21, 128, 61];
      var from = n < 70 ? red : (n < 85 ? yellow : green);
      var alpha = 0.12 + (Math.max(1, Math.min(100, n)) / 100) * 0.18;
      var top = 'rgba(' + from[0] + ',' + from[1] + ',' + from[2] + ',' + alpha.toFixed(3) + ')';
      var bottom = 'rgba(' + from[0] + ',' + from[1] + ',' + from[2] + ',' + (alpha * 0.55).toFixed(3) + ')';
      return 'linear-gradient(180deg, ' + top + ' 0%, ' + bottom + ' 100%)';
    }
    function heatStyle(metric) { return ' style="background:' + heatBackground(metric ? metric.score : null) + '"'; }
    function metricColor(metric) { return metric && metric.color ? metric.color : colorFor(metric ? metric.score : null); }
    function dot(metric) { return '<span class="dot ' + metricColor(metric) + '"></span>'; }
    function selectedPlanteles() {
      var out = [];
      for (var i = 0; i < PLANTELES.length; i += 1) if (state.planteles[PLANTELES[i]]) out.push(PLANTELES[i]);
      return out;
    }

    function initControls() {
      var plantelHtml = "";
      for (var i = 0; i < PLANTELES.length; i += 1) {
        plantelHtml += '<button class="chip active" data-plantel="' + esc(PLANTELES[i]) + '">' + esc(PLANTELES[i]) + '</button>';
      }
      byId("plantelFilters").innerHTML = plantelHtml;

      var scopeBtns = document.querySelectorAll("#scopeFilters .chip");
      for (var s = 0; s < scopeBtns.length; s += 1) {
        scopeBtns[s].addEventListener("click", function () {
          for (var j = 0; j < scopeBtns.length; j += 1) scopeBtns[j].classList.remove("active");
          this.classList.add("active");
          state.scope = this.getAttribute("data-scope") || "month";
          toggleRangeInputs();
          loadReport(false);
        });
      }
      var plantelBtns = document.querySelectorAll("#plantelFilters .chip");
      for (var b = 0; b < plantelBtns.length; b += 1) {
        plantelBtns[b].addEventListener("click", function () {
          var code = this.getAttribute("data-plantel");
          if (!code) return;
          if (state.planteles[code] && selectedPlanteles().length > 1) state.planteles[code] = false;
          else state.planteles[code] = true;
          this.classList.toggle("active", !!state.planteles[code]);
          loadReport(false);
        });
      }
      byId("refreshBtn").addEventListener("click", function () { loadReport(true); });
      byId("copyDiagnosticBtn").addEventListener("click", copyDiagnostic);
      byId("startDate").addEventListener("change", function () { if (state.scope === "range") loadReport(false); });
      byId("endDate").addEventListener("change", function () { if (state.scope === "range") loadReport(false); });
      byId("trendMetric").addEventListener("change", renderTrend);
    }
    function toggleRangeInputs() {
      var visible = state.scope === "range";
      byId("startDate").classList.toggle("visible", visible);
      byId("endDate").classList.toggle("visible", visible);
    }
    function buildUrl(force) {
      var params = new URLSearchParams();
      params.set("scope", state.scope);
      params.set("planteles", selectedPlanteles().join(","));
      if (state.scope === "range") {
        if (byId("startDate").value) params.set("start_date", byId("startDate").value);
        if (byId("endDate").value) params.set("end_date", byId("endDate").value);
      }
      if (force) params.set("force_refresh", "true");
      return "/api/v1/corporate-compliance-risk-index?" + params.toString();
    }
    async function loadReport(force) {
      byId("loading").classList.remove("hidden");
      byId("error").classList.add("hidden");
      byId("report").classList.add("hidden");
      try {
        var response = await fetch(buildUrl(force), { cache: "no-store" });
        if (!response.ok) throw new Error("HTTP " + response.status);
        state.data = await response.json();
        var planteles = get(state.data, ["planteles"], []);
        var found = false;
        for (var i = 0; i < planteles.length; i += 1) if (planteles[i].plantel === state.selectedPlantel) found = true;
        if (!found) state.selectedPlantel = planteles.length ? planteles[0].plantel : null;
        renderReport();
        byId("report").classList.remove("hidden");
      } catch (err) {
        byId("error").textContent = "No se pudo cargar el reporte: " + (err && err.message ? err.message : String(err));
        byId("error").classList.remove("hidden");
      } finally {
        byId("loading").classList.add("hidden");
      }
    }
    function renderReport() {
      renderHero();
      renderMatrix();
      renderBars();
      renderLevelAccess();
      renderTrend();
      renderDetail();
      renderDiagnostic();
    }
    function renderHero() {
      var aggregate = get(state.data, ["aggregate"], {});
      var general = get(aggregate, ["corporate_index"], {});
      var win = get(aggregate, ["window"], {});
      byId("periodStamp").textContent = "Periodo evaluado: " + (win.start || "—") + " → " + (win.end || "—");
      byId("businessDaysStamp").textContent = (win.business_days || 0) + " días hábiles";
      var generated = get(state.data, ["generated_at"], null);
      byId("updatedStamp").textContent = generated ? "Actualizado " + new Date(generated).toLocaleString("es-MX") : "Actualizado —";
      byId("generalScore").className = "kpi-value score " + metricColor(general);
      byId("generalScore").textContent = pct(general.score);
      byId("generalTitle").innerHTML = dot(general) + esc(general.traffic_label || "Sin datos");
      byId("generalDetail").textContent = general.detail || "—";
      renderKpi("best", aggregate.best_plantel);
      renderKpi("worst", aggregate.worst_plantel);
    }
    function renderKpi(prefix, item) {
      var metric = item || {};
      byId(prefix + "Score").className = "kpi-value score " + metricColor(metric);
      byId(prefix + "Score").textContent = pct(metric.score);
      byId(prefix + "Title").innerHTML = item ? dot(metric) + esc(item.plantel) : "—";
      byId(prefix + "Detail").textContent = item && item.resolved_name ? item.resolved_name : "—";
    }
    function renderMatrix() {
      var rows = get(state.data, ["matrix"], []);
      var html = "<tr><th>Plantel</th>";
      for (var h = 0; h < METRIC_ORDER.length; h += 1) html += "<th>" + esc(METRIC_LABELS[METRIC_ORDER[h]]) + "</th>";
      html += "</tr>";
      for (var r = 0; r < rows.length; r += 1) {
        var row = rows[r];
        html += "<tr><td><div class=\"plantel-code\">" + esc(row.plantel) + "</div><div class=\"plantel-name\">" + esc(row.name) + "</div></td>";
        for (var c = 0; c < METRIC_ORDER.length; c += 1) {
          var key = METRIC_ORDER[c];
          var metric = get(row, ["cells", key], {});
          html += '<td class="heat ' + metricColor(metric) + '" data-plantel="' + esc(row.plantel) + '"' + heatStyle(metric) + '><div class="cell-score score ' + metricColor(metric) + '">' + pct(metric.score) + '</div><div class="cell-label">' + esc(metric.label || "Sin datos") + '</div></td>';
        }
        html += "</tr>";
      }
      byId("matrix").innerHTML = html;
      var cells = document.querySelectorAll(".heat[data-plantel]");
      for (var i = 0; i < cells.length; i += 1) cells[i].addEventListener("click", function () { state.selectedPlantel = this.getAttribute("data-plantel"); renderDetail(); });
    }
    function renderBars() {
      renderBarList("plantelBars", get(state.data, ["rankings", "planteles"], []), "plantel");
      renderBarList("metricBars", get(state.data, ["rankings", "metrics"], []), "label");
    }
    function renderBarList(targetId, rows, labelKey) {
      var html = "";
      for (var i = 0; i < rows.length; i += 1) {
        var row = rows[i];
        var score = num(row.score);
        var width = score === null ? 0 : Math.max(0, Math.min(100, score));
        var color = row.color || colorFor(score);
        var label = row[labelKey] || row.plantel || row.key || "—";
        html += '<div class="bar-row"><div class="bar-name">' + esc(label) + '</div><div class="track"><div class="fill ' + color + '" style="width:' + width + '%"></div></div><div class="bar-value score ' + color + '">' + pct(score) + '</div></div>';
      }
      byId(targetId).innerHTML = html || '<div class="state">Sin datos</div>';
    }
    function renderLevelAccess() {
      var rows = get(state.data, ["access_by_level", "rows"], []);
      var html = "";
      for (var r = 0; r < rows.length; r += 1) {
        var row = rows[r];
        var time = row.avg_entry_time || "—";
        var samples = Number(row.sample_count || 0);
        var days = Number(row.days_with_samples || 0);
        var meta = (row.planteles || []).join(", ");
        var detail = samples > 0 ? (samples.toLocaleString("es-MX") + " entradas · " + days + " días con muestra") : "Sin muestra";
        html += '<div class="level-row"><div><div class="level-name">' + esc(row.nivel || "—") + '</div><div class="level-meta">' + esc(meta) + '</div></div><div class="level-number">' + esc(time) + '</div><div class="level-label">hora promedio</div><div class="level-meta">' + esc(detail) + '</div></div>';
      }
      byId("levelAccess").innerHTML = html || '<div class="state">Sin datos de entradas</div>';
    }

    function average(values) {
      var sum = 0;
      var count = 0;
      for (var i = 0; i < values.length; i += 1) {
        var n = num(values[i]);
        if (n !== null) { sum += n; count += 1; }
      }
      return count ? sum / count : null;
    }
    function renderTrend() {
      var key = byId("trendMetric").value;
      var trend = get(state.data, ["trend", "metrics", key], {});
      var labels = get(state.data, ["trend", "labels"], []);
      var series = trend.series || [];
      drawLineChart(labels, series);
      var allValues = [];
      for (var i = 0; i < series.length; i += 1) allValues = allValues.concat(series[i].values || []);
      var avg = average(allValues);
      byId("trendName").textContent = trend.label || METRIC_LABELS[key] || "Métrica";
      byId("trendAverage").className = "trend-big score " + colorFor(avg);
      byId("trendAverage").textContent = pct(avg);
      byId("trendText").textContent = "Promedio del periodo evaluado.";
      var legend = "";
      for (var l = 0; l < series.length; l += 1) {
        legend += '<span class="legend-item"><span class="legend-line" style="background:' + LINE_COLORS[l % LINE_COLORS.length] + '"></span>' + esc(series[l].plantel || series[l].name || "—") + '</span>';
      }
      byId("trendLegend").innerHTML = legend;
    }
    function drawLineChart(labels, series) {
      var width = 900, height = 350, padL = 42, padR = 18, padT = 18, padB = 42;
      var plotW = width - padL - padR;
      var plotH = height - padT - padB;
      var html = '';
      for (var y = 0; y <= 100; y += 25) {
        var py = padT + plotH - (y / 100) * plotH;
        html += '<line x1="' + padL + '" y1="' + py + '" x2="' + (width - padR) + '" y2="' + py + '" stroke="#e5e7eb" stroke-width="1" />';
        html += '<text x="10" y="' + (py + 4) + '" font-size="11" fill="#6b7280">' + y + '</text>';
      }
      var labelStep = Math.max(1, Math.ceil(labels.length / 8));
      for (var lx = 0; lx < labels.length; lx += labelStep) {
        var x = labels.length <= 1 ? padL : padL + (lx / (labels.length - 1)) * plotW;
        html += '<text x="' + x + '" y="330" text-anchor="middle" font-size="11" fill="#6b7280">' + esc(labels[lx]) + '</text>';
      }
      for (var s = 0; s < series.length; s += 1) {
        var values = series[s].values || [];
        var path = '';
        var started = false;
        for (var i = 0; i < labels.length; i += 1) {
          var n = num(values[i]);
          if (n === null) { started = false; continue; }
          var px = labels.length <= 1 ? padL : padL + (i / (labels.length - 1)) * plotW;
          var py2 = padT + plotH - (Math.max(0, Math.min(100, n)) / 100) * plotH;
          path += (started ? ' L ' : ' M ') + px.toFixed(1) + ' ' + py2.toFixed(1);
          started = true;
        }
        if (path) html += '<path d="' + path + '" fill="none" stroke="' + LINE_COLORS[s % LINE_COLORS.length] + '" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />';
      }
      byId("trendSvg").innerHTML = html;
    }
    function renderDetail() {
      var planteles = get(state.data, ["planteles"], []);
      if (!planteles.length) return;
      var selected = planteles[0];
      for (var i = 0; i < planteles.length; i += 1) if (planteles[i].plantel === state.selectedPlantel) selected = planteles[i];
      state.selectedPlantel = selected.plantel;
      var html = "";
      for (var p = 0; p < planteles.length; p += 1) {
        var row = planteles[p];
        var general = get(row, ["metrics", "general"], {});
        var active = row.plantel === state.selectedPlantel ? " active" : "";
        html += '<button class="plantel-option' + active + '" data-plantel="' + esc(row.plantel) + '"><span><strong>' + esc(row.plantel) + '</strong><div class="plantel-name">' + esc(row.resolved_name) + '</div></span><span class="score ' + metricColor(general) + '">' + pct(general.score) + '</span></button>';
      }
      byId("plantelSelector").innerHTML = html;
      var buttons = document.querySelectorAll(".plantel-option[data-plantel]");
      for (var b = 0; b < buttons.length; b += 1) buttons[b].addEventListener("click", function () { state.selectedPlantel = this.getAttribute("data-plantel"); renderDetail(); });
      var table = '<thead><tr><th>Área</th><th>Valor</th><th>Resumen</th><th>Detalle</th></tr></thead><tbody>';
      for (var m = 0; m < METRIC_ORDER.length; m += 1) {
        var key = METRIC_ORDER[m];
        var metric = get(selected, ["metrics", key], {});
        table += '<tr><td>' + dot(metric) + '<strong>' + esc(METRIC_LABELS[key]) + '</strong></td><td class="score ' + metricColor(metric) + '">' + pct(metric.score) + '</td><td>' + esc(metric.label || "Sin datos") + '</td><td>' + esc(metric.detail || "—") + '</td></tr>';
      }
      table += '</tbody>';
      byId("detailTable").innerHTML = table;
    }


    function renderDiagnostic() {
      var diagnostic = get(state.data, ["diagnostic"], {});
      byId("diagnosticJson").textContent = JSON.stringify(diagnostic);
    }
    async function copyDiagnostic() {
      var text = byId("diagnosticJson").textContent || "{}";
      try {
        if (navigator.clipboard && navigator.clipboard.writeText) await navigator.clipboard.writeText(text);
        else {
          var area = document.createElement("textarea");
          area.value = text;
          document.body.appendChild(area);
          area.select();
          document.execCommand("copy");
          document.body.removeChild(area);
        }
        byId("copyDiagnosticBtn").textContent = "Copiado";
        setTimeout(function () { byId("copyDiagnosticBtn").textContent = "Copiar diagnóstico"; }, 1400);
      } catch (err) {
        byId("copyDiagnosticBtn").textContent = "No se pudo copiar";
        setTimeout(function () { byId("copyDiagnosticBtn").textContent = "Copiar diagnóstico"; }, 1800);
      }
    }

    initControls();
    loadReport(false);
  </script>
</body>
</html>
'''
