CORPORATE_COMPLIANCE_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Índice Corporativo de Cumplimiento</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.8/dist/chart.umd.min.js"></script>
  <style>
    :root {
      --bg: #f4f6f8;
      --panel: #ffffff;
      --text: #172033;
      --muted: #64748b;
      --line: #dde4ee;
      --soft: #f8fafc;
      --green: #16a34a;
      --green-bg: #dcfce7;
      --yellow: #d97706;
      --yellow-bg: #fef3c7;
      --red: #dc2626;
      --red-bg: #fee2e2;
      --gray: #94a3b8;
      --gray-bg: #f1f5f9;
      --shadow: 0 16px 38px rgba(15, 23, 42, .07);
      --radius: 18px;
      --font: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--text); font-family: var(--font); }
    button, input, select { font: inherit; }
    .topbar { position: sticky; top: 0; z-index: 20; background: rgba(255,255,255,.96); border-bottom: 1px solid var(--line); backdrop-filter: blur(14px); }
    .topbar-inner { max-width: 1560px; margin: 0 auto; padding: 14px 22px; display: grid; grid-template-columns: minmax(300px,1fr) auto; gap: 16px; align-items: center; }
    .brand-kicker { color: var(--muted); font-size: 11px; font-weight: 900; letter-spacing: .14em; text-transform: uppercase; }
    .brand-title { margin-top: 2px; font-weight: 900; font-size: 19px; letter-spacing: -.025em; }
    .filters { display: flex; align-items: center; justify-content: flex-end; flex-wrap: wrap; gap: 10px; }
    .segmented, .plantel-list { display: inline-flex; align-items: center; gap: 5px; padding: 5px; border: 1px solid var(--line); background: var(--soft); border-radius: 999px; }
    .scope-btn, .plantel-btn, .refresh-btn { border: 0; border-radius: 999px; padding: 8px 12px; color: #334155; background: transparent; cursor: pointer; font-size: 12px; font-weight: 850; white-space: nowrap; }
    .scope-btn.active, .plantel-btn.active { background: #111827; color: #fff; }
    .refresh-btn { background: #111827; color: #fff; padding-inline: 16px; }
    .date-input { display: none; border: 1px solid var(--line); border-radius: 999px; padding: 8px 11px; background: #fff; color: var(--text); font-size: 12px; font-weight: 800; }
    .date-input.visible { display: inline-block; }
    .page { max-width: 1560px; margin: 0 auto; padding: 22px 22px 64px; }
    .hidden { display: none !important; }
    .state-box { padding: 22px; border: 1px solid var(--line); background: var(--panel); border-radius: var(--radius); box-shadow: var(--shadow); color: var(--muted); font-weight: 800; }
    .state-box.error { color: #991b1b; background: #fff7f7; border-color: rgba(220,38,38,.35); }
    .hero { display: grid; grid-template-columns: minmax(0, 1.15fr) repeat(3, minmax(210px,.28fr)); gap: 14px; margin-bottom: 14px; }
    .card, .section { background: var(--panel); border: 1px solid var(--line); border-radius: var(--radius); box-shadow: var(--shadow); }
    .intro { padding: 24px; }
    h1 { margin: 0; font-size: clamp(30px, 4vw, 50px); line-height: .98; letter-spacing: -.055em; }
    .subtitle { margin-top: 12px; color: #475569; font-size: 14px; font-weight: 620; max-width: 900px; }
    .stamp-row { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 18px; }
    .stamp { display: inline-flex; align-items: center; gap: 7px; padding: 7px 10px; border: 1px solid var(--line); border-radius: 999px; color: #334155; background: #fff; font-size: 12px; font-weight: 800; }
    .kpi { padding: 18px; min-height: 154px; display: flex; flex-direction: column; justify-content: space-between; }
    .kpi-label { font-size: 11px; text-transform: uppercase; letter-spacing: .1em; color: var(--muted); font-weight: 900; }
    .kpi-value { margin-top: 12px; font-size: 40px; font-weight: 950; letter-spacing: -.06em; }
    .kpi-title { margin-top: 4px; font-size: 15px; font-weight: 850; }
    .kpi-detail { margin-top: 6px; color: var(--muted); font-size: 12px; font-weight: 650; }
    .dot { display: inline-block; width: 10px; height: 10px; border-radius: 999px; margin-right: 6px; vertical-align: middle; background: var(--gray); }
    .dot.green { background: var(--green); } .dot.yellow { background: var(--yellow); } .dot.red { background: var(--red); } .dot.gray { background: var(--gray); }
    .score.green { color: var(--green); } .score.yellow { color: var(--yellow); } .score.red { color: var(--red); } .score.gray { color: var(--gray); }
    .layout { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; align-items: start; }
    .section.wide { grid-column: 1 / -1; }
    .section-head { padding: 16px 18px; border-bottom: 1px solid var(--line); display: flex; align-items: center; justify-content: space-between; gap: 14px; }
    .section-kicker { color: var(--muted); font-size: 11px; font-weight: 900; letter-spacing: .12em; text-transform: uppercase; }
    .section-title { margin-top: 3px; font-size: 20px; font-weight: 930; letter-spacing: -.03em; }
    .section-body { padding: 18px; }
    .matrix-wrap { overflow-x: auto; }
    .matrix { width: 100%; border-collapse: separate; border-spacing: 0; min-width: 980px; }
    .matrix th, .matrix td { border-bottom: 1px solid var(--line); border-right: 1px solid var(--line); padding: 12px; text-align: left; }
    .matrix th:first-child, .matrix td:first-child { border-left: 1px solid var(--line); }
    .matrix tr:first-child th { border-top: 1px solid var(--line); }
    .matrix th { background: #f8fafc; color: #475569; font-size: 11px; font-weight: 950; letter-spacing: .09em; text-transform: uppercase; }
    .matrix tr:first-child th:first-child { border-top-left-radius: 14px; }
    .matrix tr:first-child th:last-child { border-top-right-radius: 14px; }
    .matrix tr:last-child td:first-child { border-bottom-left-radius: 14px; }
    .matrix tr:last-child td:last-child { border-bottom-right-radius: 14px; }
    .plantel-name { font-size: 13px; color: var(--muted); margin-top: 2px; }
    .heat-cell { cursor: pointer; transition: transform .12s ease, box-shadow .12s ease; }
    .heat-cell:hover { transform: translateY(-1px); box-shadow: inset 0 0 0 999px rgba(15,23,42,.025); }
    .heat-green { background: var(--green-bg); }
    .heat-yellow { background: var(--yellow-bg); }
    .heat-red { background: var(--red-bg); }
    .heat-gray { background: var(--gray-bg); color: #64748b; }
    .cell-score { font-size: 20px; font-weight: 950; letter-spacing: -.035em; }
    .cell-detail { margin-top: 3px; color: #475569; font-size: 11px; font-weight: 680; }
    .chart-grid { display: grid; grid-template-columns: minmax(0, 1fr) minmax(310px,.42fr); gap: 16px; align-items: stretch; }
    .chart-box { min-height: 370px; border: 1px solid var(--line); border-radius: 14px; padding: 14px; background: #fff; position: relative; }
    .chart-box.small { min-height: 330px; }
    .chart-box.tall { min-height: 430px; }
    .side-card { border: 1px solid var(--line); border-radius: 14px; background: #f8fafc; padding: 16px; }
    .side-title { font-size: 12px; text-transform: uppercase; letter-spacing: .09em; font-weight: 950; color: #475569; }
    .side-big { margin-top: 14px; font-size: 34px; font-weight: 950; letter-spacing: -.05em; }
    .side-line { margin-top: 10px; font-size: 14px; color: #334155; font-weight: 700; }
    .selector { border: 1px solid var(--line); border-radius: 999px; padding: 8px 12px; background: #fff; color: #334155; font-size: 12px; font-weight: 850; }
    .detail-grid { display: grid; grid-template-columns: 280px 1fr; gap: 16px; }
    .plantel-select { display: flex; flex-direction: column; gap: 8px; }
    .plantel-row { border: 1px solid var(--line); background: #fff; border-radius: 12px; padding: 12px; cursor: pointer; display: flex; justify-content: space-between; align-items: center; gap: 12px; }
    .plantel-row.active { border-color: #111827; box-shadow: inset 4px 0 0 #111827; }
    .detail-table { width: 100%; border-collapse: separate; border-spacing: 0; border: 1px solid var(--line); border-radius: 14px; overflow: hidden; }
    .detail-table th, .detail-table td { padding: 13px 14px; border-bottom: 1px solid var(--line); text-align: left; }
    .detail-table tr:last-child td { border-bottom: 0; }
    .detail-table th { background: #f8fafc; color: #475569; font-size: 11px; font-weight: 950; letter-spacing: .09em; text-transform: uppercase; }
    @media (max-width: 1180px) { .topbar-inner, .hero, .layout, .chart-grid, .detail-grid { grid-template-columns: 1fr; } .filters { justify-content: flex-start; } }
    @media (max-width: 720px) { .page { padding: 14px 12px 42px; } .topbar-inner { padding: 12px; } .plantel-list { max-width: 100%; overflow-x: auto; } .kpi-value { font-size: 34px; } .chart-box { min-height: 310px; } }
  </style>
</head>
<body>
  <header class="topbar">
    <div class="topbar-inner">
      <div>
        <div class="brand-kicker">Planteles</div>
        <div class="brand-title">Índice Corporativo de Cumplimiento</div>
      </div>
      <div class="filters">
        <div class="segmented">
          <button class="scope-btn active" data-scope="month">Mes</button>
          <button class="scope-btn" data-scope="today">Hoy</button>
          <button class="scope-btn" data-scope="ciclo_escolar">Ciclo</button>
          <button class="scope-btn" data-scope="range">Rango</button>
        </div>
        <input id="startDate" class="date-input" type="date" />
        <input id="endDate" class="date-input" type="date" />
        <div id="plantelFilters" class="plantel-list"></div>
        <button id="refreshBtn" class="refresh-btn">Actualizar</button>
      </div>
    </div>
  </header>

  <main class="page">
    <div id="loading" class="state-box">Cargando reporte...</div>
    <div id="error" class="state-box error hidden"></div>
    <div id="report" class="hidden">
      <section class="hero">
        <div class="card intro">
          <h1>Índice Corporativo de Cumplimiento</h1>
          <div class="subtitle">Reporte ejecutivo por plantel. Cada valor se calcula de 0 a 100 y se muestra con semáforo.</div>
          <div class="stamp-row">
            <span id="periodStamp" class="stamp">Periodo</span>
            <span id="updatedStamp" class="stamp">Actualizado</span>
            <span class="stamp"><span class="dot green"></span>85–100</span>
            <span class="stamp"><span class="dot yellow"></span>70–84</span>
            <span class="stamp"><span class="dot red"></span>0–69</span>
          </div>
        </div>
        <div class="card kpi"><div><div class="kpi-label">Cumplimiento general</div><div id="generalScore" class="kpi-value score gray">—</div></div><div><div id="generalTitle" class="kpi-title">Sin datos</div><div id="generalDetail" class="kpi-detail">—</div></div></div>
        <div class="card kpi"><div><div class="kpi-label">Mejor plantel</div><div id="bestScore" class="kpi-value score gray">—</div></div><div><div id="bestTitle" class="kpi-title">—</div><div id="bestDetail" class="kpi-detail">—</div></div></div>
        <div class="card kpi"><div><div class="kpi-label">Peor plantel</div><div id="worstScore" class="kpi-value score gray">—</div></div><div><div id="worstTitle" class="kpi-title">—</div><div id="worstDetail" class="kpi-detail">—</div></div></div>
      </section>

      <div class="layout">
        <section class="section wide">
          <div class="section-head"><div><div class="section-kicker">Resumen</div><div class="section-title">Mapa de cumplimiento</div></div></div>
          <div class="section-body"><div class="matrix-wrap"><table id="matrix" class="matrix"></table></div></div>
        </section>

        <section class="section wide">
          <div class="section-head"><div><div class="section-kicker">Gráficas</div><div class="section-title">Cumplimiento por plantel y por área</div></div></div>
          <div class="section-body chart-grid">
            <div class="chart-box"><canvas id="plantelBarChart"></canvas></div>
            <div class="chart-box small"><canvas id="metricBarChart"></canvas></div>
          </div>
        </section>

        <section class="section wide">
          <div class="section-head">
            <div><div class="section-kicker">Tendencia</div><div class="section-title">Evolución del periodo</div></div>
            <select id="trendMetric" class="selector">
              <option value="general">General</option>
              <option value="attendance">Asistencia</option>
              <option value="lists">Listas</option>
              <option value="tardies">Retardos</option>
            </select>
          </div>
          <div class="section-body chart-grid">
            <div class="chart-box tall"><canvas id="trendChart"></canvas></div>
            <div id="trendSide" class="side-card"><div class="side-title">Métrica seleccionada</div><div class="side-big">—</div><div class="side-line">—</div></div>
          </div>
        </section>

        <section class="section wide">
          <div class="section-head"><div><div class="section-kicker">Detalle</div><div class="section-title">Plantel seleccionado</div></div></div>
          <div class="section-body detail-grid">
            <div id="plantelSelector" class="plantel-select"></div>
            <div><table id="detailTable" class="detail-table"></table></div>
          </div>
        </section>
      </div>
    </div>
  </main>

  <script>
    const PLANTELES = ["PT", "PM", "ST", "SM", "PREET", "PREEM"];
    const METRIC_LABELS = {
      general: "General",
      attendance: "Asistencia",
      lists: "Listas",
      tardies: "Retardos",
      academic: "Académico",
      sapf: "SAPF"
    };
    const METRIC_ORDER = ["general", "attendance", "lists", "tardies", "academic", "sapf"];
    const COLORS = {
      green: "#16a34a",
      yellow: "#d97706",
      red: "#dc2626",
      gray: "#94a3b8",
      blue: "#2563eb",
      purple: "#7c3aed",
      teal: "#0f766e"
    };
    const LINE_COLORS = ["#2563eb", "#16a34a", "#dc2626", "#d97706", "#7c3aed", "#0f766e"];

    let state = { scope: "month", planteles: new Set(PLANTELES), data: null, selectedPlantel: null };
    let charts = {};

    const $ = (id) => document.getElementById(id);
    const pct = (v) => v === null || v === undefined || Number.isNaN(Number(v)) ? "—" : `${Number(v).toFixed(1)}%`;
    const scoreClass = (metric) => `score ${metric?.color || "gray"}`;
    const heatClass = (metric) => `heat-cell heat-${metric?.color || "gray"}`;
    const dot = (metric) => `<span class="dot ${metric?.color || "gray"}"></span>`;
    const escapeHtml = (value) => String(value ?? "").replace(/[&<>'"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;","\"":"&quot;"}[c]));

    function initControls() {
      $("plantelFilters").innerHTML = PLANTELES.map(code => `<button class="plantel-btn active" data-plantel="${code}">${code}</button>`).join("");
      document.querySelectorAll(".scope-btn").forEach(btn => {
        btn.addEventListener("click", () => {
          document.querySelectorAll(".scope-btn").forEach(b => b.classList.remove("active"));
          btn.classList.add("active");
          state.scope = btn.dataset.scope;
          const range = state.scope === "range";
          $("startDate").classList.toggle("visible", range);
          $("endDate").classList.toggle("visible", range);
          loadReport();
        });
      });
      document.querySelectorAll(".plantel-btn").forEach(btn => {
        btn.addEventListener("click", () => {
          const code = btn.dataset.plantel;
          if (state.planteles.has(code) && state.planteles.size > 1) state.planteles.delete(code);
          else state.planteles.add(code);
          btn.classList.toggle("active", state.planteles.has(code));
          loadReport();
        });
      });
      $("refreshBtn").addEventListener("click", () => loadReport(true));
      $("startDate").addEventListener("change", () => state.scope === "range" && loadReport());
      $("endDate").addEventListener("change", () => state.scope === "range" && loadReport());
      $("trendMetric").addEventListener("change", renderTrend);
    }

    function buildUrl(force=false) {
      const params = new URLSearchParams();
      params.set("scope", state.scope);
      params.set("planteles", Array.from(state.planteles).join(","));
      if (state.scope === "range") {
        if ($("startDate").value) params.set("start_date", $("startDate").value);
        if ($("endDate").value) params.set("end_date", $("endDate").value);
      }
      if (force) params.set("force_refresh", "true");
      return `/api/v1/corporate-compliance-risk-index?${params.toString()}`;
    }

    async function loadReport(force=false) {
      $("loading").classList.remove("hidden");
      $("error").classList.add("hidden");
      $("report").classList.add("hidden");
      try {
        const res = await fetch(buildUrl(force), { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        state.data = await res.json();
        if (!state.selectedPlantel || !(state.data.planteles || []).some(p => p.plantel === state.selectedPlantel)) {
          state.selectedPlantel = state.data.planteles?.[0]?.plantel || null;
        }
        renderReport();
        $("report").classList.remove("hidden");
      } catch (err) {
        $("error").textContent = `No se pudo cargar el reporte: ${err.message || err}`;
        $("error").classList.remove("hidden");
      } finally {
        $("loading").classList.add("hidden");
      }
    }

    function renderReport() {
      renderHero();
      renderMatrix();
      renderBars();
      renderTrend();
      renderDetail();
    }

    function renderHero() {
      const data = state.data || {};
      const agg = data.aggregate || {};
      const general = agg.corporate_index || {};
      const window = agg.window || data.meta || {};
      $("periodStamp").textContent = `${window.start || "—"} → ${window.end || "—"}`;
      $("updatedStamp").textContent = data.generated_at ? `Actualizado ${new Date(data.generated_at).toLocaleString("es-MX")}` : "Actualizado —";

      $("generalScore").className = scoreClass(general);
      $("generalScore").textContent = pct(general.score);
      $("generalTitle").innerHTML = `${dot(general)}${escapeHtml(general.traffic_label || "Sin datos")}`;
      $("generalDetail").textContent = general.detail || "—";

      renderPlantelKpi("best", agg.best_plantel);
      renderPlantelKpi("worst", agg.worst_plantel);
    }

    function renderPlantelKpi(prefix, item) {
      const color = item?.color || "gray";
      $(`${prefix}Score`).className = `kpi-value score ${color}`;
      $(`${prefix}Score`).textContent = pct(item?.score);
      $(`${prefix}Title`).innerHTML = item ? `${dot(item)}${escapeHtml(item.plantel)}` : "—";
      $(`${prefix}Detail`).textContent = item?.resolved_name || "—";
    }

    function renderMatrix() {
      const rows = state.data?.matrix || [];
      const header = `<tr><th>Plantel</th>${METRIC_ORDER.map(k => `<th>${METRIC_LABELS[k]}</th>`).join("")}</tr>`;
      const body = rows.map(row => {
        const cells = METRIC_ORDER.map(key => {
          const m = row.cells?.[key] || {};
          return `<td class="${heatClass(m)}" data-plantel="${escapeHtml(row.plantel)}"><div class="cell-score">${pct(m.score)}</div><div class="cell-detail">${escapeHtml(m.label || "Sin datos")}</div></td>`;
        }).join("");
        return `<tr><td><strong>${escapeHtml(row.plantel)}</strong><div class="plantel-name">${escapeHtml(row.name)}</div></td>${cells}</tr>`;
      }).join("");
      $("matrix").innerHTML = header + body;
      document.querySelectorAll(".heat-cell").forEach(cell => {
        cell.addEventListener("click", () => {
          state.selectedPlantel = cell.dataset.plantel;
          renderDetail();
        });
      });
    }

    function metricColor(value) {
      if (value === null || value === undefined) return COLORS.gray;
      if (value >= 85) return COLORS.green;
      if (value >= 70) return COLORS.yellow;
      return COLORS.red;
    }

    function chartDataValues(rows) {
      return rows.map(r => r.score === null || r.score === undefined ? null : Number(r.score));
    }

    function destroyChart(key) {
      if (charts[key]) { charts[key].destroy(); charts[key] = null; }
    }

    function renderBars() {
      const plantelRows = state.data?.rankings?.planteles || [];
      const metricRows = state.data?.rankings?.metrics || [];
      destroyChart("plantelBar");
      destroyChart("metricBar");

      charts.plantelBar = new Chart($("plantelBarChart"), {
        type: "bar",
        data: {
          labels: plantelRows.map(r => r.plantel),
          datasets: [{
            label: "General",
            data: chartDataValues(plantelRows),
            backgroundColor: plantelRows.map(r => metricColor(r.score)),
            borderWidth: 0,
            borderRadius: 10
          }]
        },
        options: barOptions("%")
      });

      charts.metricBar = new Chart($("metricBarChart"), {
        type: "bar",
        data: {
          labels: metricRows.map(r => r.label),
          datasets: [{
            label: "Promedio",
            data: chartDataValues(metricRows),
            backgroundColor: metricRows.map(r => metricColor(r.score)),
            borderWidth: 0,
            borderRadius: 10
          }]
        },
        options: barOptions("%")
      });
    }

    function barOptions(suffix) {
      return {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => `${ctx.raw ?? "—"}${suffix}` } } },
        scales: { y: { min: 0, max: 100, grid: { color: "#edf2f7" } }, x: { grid: { display: false } } }
      };
    }

    function renderTrend() {
      const key = $("trendMetric").value;
      const trend = state.data?.trend?.metrics?.[key] || {};
      const labels = state.data?.trend?.labels || [];
      destroyChart("trend");
      charts.trend = new Chart($("trendChart"), {
        type: "line",
        data: {
          labels,
          datasets: (trend.series || []).map((serie, idx) => ({
            label: serie.plantel,
            data: serie.values,
            borderColor: LINE_COLORS[idx % LINE_COLORS.length],
            backgroundColor: LINE_COLORS[idx % LINE_COLORS.length],
            tension: .28,
            borderWidth: 3,
            pointRadius: 3,
            spanGaps: true
          }))
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { position: "bottom" }, tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${ctx.raw === null ? "—" : Number(ctx.raw).toFixed(1) + "%"}` } } },
          scales: { y: { min: 0, max: 100, grid: { color: "#edf2f7" } }, x: { grid: { display: false } } }
        }
      });
      const side = $("trendSide");
      const avg = average((trend.series || []).flatMap(s => s.values || []));
      side.innerHTML = `<div class="side-title">${escapeHtml(trend.label || METRIC_LABELS[key])}</div><div class="side-big score ${avgColor(avg)}">${pct(avg)}</div><div class="side-line">Promedio del periodo seleccionado</div>`;
    }

    function average(values) {
      const nums = values.filter(v => v !== null && v !== undefined && Number.isFinite(Number(v))).map(Number);
      if (!nums.length) return null;
      return nums.reduce((a,b) => a+b, 0) / nums.length;
    }
    function avgColor(value) {
      if (value === null || value === undefined) return "gray";
      if (value >= 85) return "green";
      if (value >= 70) return "yellow";
      return "red";
    }

    function renderDetail() {
      const planteles = state.data?.planteles || [];
      const selected = planteles.find(p => p.plantel === state.selectedPlantel) || planteles[0];
      if (!selected) return;
      state.selectedPlantel = selected.plantel;
      $("plantelSelector").innerHTML = planteles.map(p => {
        const m = p.metrics?.general || {};
        return `<button class="plantel-row ${p.plantel === state.selectedPlantel ? "active" : ""}" data-plantel="${escapeHtml(p.plantel)}"><span><strong>${escapeHtml(p.plantel)}</strong><div class="plantel-name">${escapeHtml(p.resolved_name)}</div></span><span class="score ${m.color || "gray"}">${pct(m.score)}</span></button>`;
      }).join("");
      document.querySelectorAll(".plantel-row").forEach(btn => btn.addEventListener("click", () => { state.selectedPlantel = btn.dataset.plantel; renderDetail(); }));

      const rows = METRIC_ORDER.map(key => {
        const m = selected.metrics?.[key] || {};
        return `<tr><td>${dot(m)}<strong>${METRIC_LABELS[key]}</strong></td><td class="score ${m.color || "gray"}">${pct(m.score)}</td><td>${escapeHtml(m.label || "Sin datos")}</td><td>${escapeHtml(m.detail || "—")}</td></tr>`;
      }).join("");
      $("detailTable").innerHTML = `<thead><tr><th>Área</th><th>Valor</th><th>Resumen</th><th>Detalle</th></tr></thead><tbody>${rows}</tbody>`;
    }

    initControls();
    loadReport();
  </script>
</body>
</html>
"""
