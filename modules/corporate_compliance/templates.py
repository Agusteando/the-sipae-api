CORPORATE_COMPLIANCE_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SIPAE Corporate Compliance & Risk Index</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.8/dist/chart.umd.min.js"></script>
  <style>
    :root {
      --bg: #f8fafc;
      --panel: #ffffff;
      --text: #0f172a;
      --muted: #64748b;
      --line: #e2e8f0;
      --soft: #f1f5f9;
      --green: #22c55e;
      --yellow: #f59e0b;
      --red: #ef4444;
      --ink: #111827;
      --shadow: 0 18px 40px rgba(15, 23, 42, 0.08);
      --radius: 18px;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      --sans: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: var(--sans);
      line-height: 1.35;
    }

    .topbar {
      position: sticky;
      top: 0;
      z-index: 50;
      background: rgba(255, 255, 255, 0.96);
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(16px);
      box-shadow: 0 10px 30px rgba(15, 23, 42, 0.04);
    }

    .topbar-inner {
      max-width: 1680px;
      margin: 0 auto;
      padding: 14px 22px;
      display: grid;
      grid-template-columns: minmax(280px, 1fr) auto;
      gap: 18px;
      align-items: center;
    }

    .brand-kicker {
      font-size: 11px;
      font-weight: 800;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.18em;
    }

    .brand-title {
      margin-top: 2px;
      font-size: 20px;
      font-weight: 900;
      letter-spacing: -0.035em;
    }

    .filters {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      justify-content: flex-end;
    }

    .filter-group {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 6px;
      background: var(--soft);
      border: 1px solid var(--line);
      border-radius: 999px;
    }

    .scope-btn, .plantel-pill, .refresh-btn {
      border: 1px solid transparent;
      background: transparent;
      color: #334155;
      padding: 9px 12px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
      transition: 150ms ease;
      white-space: nowrap;
    }

    .scope-btn:hover, .plantel-pill:hover { background: #ffffff; border-color: var(--line); }
    .scope-btn.active, .plantel-pill.active {
      background: var(--ink);
      border-color: var(--ink);
      color: #ffffff;
      box-shadow: 0 8px 18px rgba(17, 24, 39, 0.14);
    }

    .date-input {
      appearance: none;
      border: 1px solid var(--line);
      background: #ffffff;
      border-radius: 999px;
      padding: 9px 12px;
      color: var(--text);
      font-size: 12px;
      font-weight: 800;
    }

    .refresh-btn {
      background: var(--text);
      color: #ffffff;
      border-color: var(--text);
      padding-inline: 16px;
    }

    .page {
      max-width: 1680px;
      margin: 0 auto;
      padding: 24px 22px 70px;
    }

    .hero {
      background: linear-gradient(135deg, #ffffff, #f8fafc 62%, #eef2ff);
      border: 1px solid var(--line);
      border-radius: 28px;
      box-shadow: var(--shadow);
      padding: 28px;
      display: grid;
      grid-template-columns: 1.3fr 0.7fr;
      gap: 26px;
      align-items: stretch;
    }

    .hero h1 {
      margin: 0;
      font-size: clamp(31px, 4vw, 56px);
      line-height: 0.94;
      letter-spacing: -0.06em;
    }

    .hero-subtitle {
      margin: 14px 0 0;
      max-width: 920px;
      font-size: 17px;
      color: #334155;
    }

    .stamp-row {
      margin-top: 22px;
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    .stamp {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid var(--line);
      background: #ffffff;
      border-radius: 999px;
      padding: 9px 12px;
      font-size: 12px;
      font-weight: 800;
      color: #334155;
    }

    .index-card {
      border: 1px solid var(--line);
      background: #ffffff;
      border-radius: 24px;
      padding: 22px;
      display: grid;
      place-items: center;
      min-height: 250px;
      text-align: center;
      position: relative;
      overflow: hidden;
    }

    .index-card::before {
      content: "";
      position: absolute;
      inset: 0;
      background: radial-gradient(circle at 50% 18%, rgba(34, 197, 94, 0.12), transparent 38%);
      pointer-events: none;
    }

    .index-number {
      position: relative;
      font-size: clamp(74px, 8vw, 124px);
      font-weight: 950;
      letter-spacing: -0.08em;
      line-height: 0.9;
    }

    .index-label {
      position: relative;
      margin-top: 10px;
      font-size: 13px;
      font-weight: 900;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .grid-kpis {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 14px;
      margin: 18px 0 22px;
    }

    .kpi {
      background: #ffffff;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 16px;
      box-shadow: 0 8px 20px rgba(15, 23, 42, 0.04);
      min-height: 136px;
    }

    .kpi-title {
      font-size: 11px;
      font-weight: 900;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }

    .kpi-value {
      margin-top: 10px;
      font-size: 34px;
      font-weight: 950;
      letter-spacing: -0.05em;
    }

    .kpi-note {
      margin-top: 6px;
      font-size: 12px;
      color: var(--muted);
      font-weight: 700;
    }

    .section {
      margin-top: 18px;
      background: #ffffff;
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }

    .section-head {
      padding: 20px 22px;
      border-bottom: 1px solid var(--line);
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: flex-start;
    }

    .section-eyebrow {
      font-size: 11px;
      font-weight: 950;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.16em;
    }

    .section-title {
      margin-top: 4px;
      font-size: 22px;
      font-weight: 950;
      letter-spacing: -0.035em;
    }

    .section-body {
      padding: 20px 22px 22px;
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(300px, 0.75fr);
      gap: 20px;
      align-items: stretch;
    }

    .chart-box {
      min-height: 340px;
      height: 380px;
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      background: #ffffff;
    }

    .chart-box.tall { height: 460px; }

    .interpretation {
      border: 1px solid var(--line);
      border-radius: 18px;
      background: #f8fafc;
      padding: 18px;
      min-height: 160px;
    }

    .interpretation-title {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 12px;
      font-weight: 950;
      color: var(--text);
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }

    .interpretation p {
      margin: 12px 0 0;
      color: #334155;
      font-size: 15px;
      font-weight: 650;
    }

    .callout-critical { border-left: 6px solid var(--red); }
    .callout-warning { border-left: 6px solid var(--yellow); }
    .callout-fulfilled { border-left: 6px solid var(--green); }

    .two-col {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
      margin-top: 18px;
    }

    .table-panel {
      background: #ffffff;
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }

    .table-title {
      padding: 16px 18px;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
      font-weight: 950;
      letter-spacing: -0.02em;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }

    th, td {
      padding: 12px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }

    th {
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      background: #f8fafc;
    }

    td strong { font-weight: 900; }
    .num { font-variant-numeric: tabular-nums; font-family: var(--mono); font-weight: 800; }

    .badge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 5px 8px;
      font-size: 11px;
      font-weight: 950;
      color: #ffffff;
      white-space: nowrap;
    }

    .badge.fulfilled { background: var(--green); }
    .badge.warning { background: var(--yellow); }
    .badge.critical { background: var(--red); }
    .badge.unavailable { background: #94a3b8; }

    .heatmap-cell {
      color: #ffffff;
      font-weight: 950;
      text-align: center;
      border-radius: 10px;
      padding: 8px 6px;
      min-width: 74px;
    }

    .risk-list {
      display: grid;
      gap: 10px;
    }

    .risk-item {
      border: 1px solid var(--line);
      border-left-width: 6px;
      border-radius: 16px;
      padding: 13px 14px;
      background: #ffffff;
    }

    .risk-item.critical { border-left-color: var(--red); }
    .risk-item.warning { border-left-color: var(--yellow); }
    .risk-item.fulfilled { border-left-color: var(--green); }

    .risk-item-title {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      font-size: 14px;
      font-weight: 950;
    }

    .risk-item-body {
      margin-top: 5px;
      color: #475569;
      font-size: 13px;
      font-weight: 650;
    }

    .status-line {
      height: 8px;
      border-radius: 999px;
      background: var(--line);
      overflow: hidden;
      margin-top: 12px;
    }

    .status-fill { height: 100%; border-radius: 999px; }

    .loading, .error-box {
      border: 1px solid var(--line);
      background: #ffffff;
      border-radius: 22px;
      padding: 24px;
      color: var(--muted);
      font-weight: 800;
      box-shadow: var(--shadow);
    }

    .error-box { border-color: rgba(239, 68, 68, 0.35); color: #991b1b; background: #fff7f7; }

    .hidden { display: none !important; }

    @media (max-width: 1180px) {
      .topbar-inner, .hero, .section-body, .two-col { grid-template-columns: 1fr; }
      .filters { justify-content: flex-start; }
      .grid-kpis { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }

    @media (max-width: 760px) {
      .page { padding: 16px 12px 42px; }
      .topbar-inner { padding: 12px; }
      .grid-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .section-head { display: block; }
      .chart-box { height: 330px; }
    }
  </style>
</head>
<body>
  <div class="topbar">
    <div class="topbar-inner">
      <div>
        <div class="brand-kicker">SIPAE · Owner/CEO View</div>
        <div class="brand-title">Índice Corporativo de Cumplimiento SIPAE</div>
      </div>
      <div class="filters">
        <div class="filter-group" aria-label="Alcance de fechas">
          <button class="scope-btn active" data-scope="today">Hoy</button>
          <button class="scope-btn" data-scope="month">Mes</button>
          <button class="scope-btn" data-scope="ciclo_escolar">Ciclo</button>
          <button class="scope-btn" data-scope="range">Rango</button>
        </div>
        <input id="startDate" class="date-input hidden" type="date" />
        <input id="endDate" class="date-input hidden" type="date" />
        <div id="plantelFilters" class="filter-group" aria-label="Planteles"></div>
        <button id="refreshBtn" class="refresh-btn">Actualizar</button>
      </div>
    </div>
  </div>

  <main class="page">
    <div id="loadState" class="loading">Cargando tablero ejecutivo...</div>
    <div id="errorState" class="error-box hidden"></div>
    <div id="dashboard" class="hidden">
      <section class="hero">
        <div>
          <h1>SIPAE Corporate Compliance & Risk Index</h1>
          <p class="hero-subtitle">Lectura ejecutiva de accountability, continuidad legal, seguridad, supervisión académica y fuga de capital humano en los seis planteles.</p>
          <div class="stamp-row">
            <span id="dateStamp" class="stamp">Periodo</span>
            <span id="generatedStamp" class="stamp">Generado</span>
            <span class="stamp">Orden fijo: PT · PM · ST · SM · PREET · PREEM</span>
          </div>
        </div>
        <div id="heroIndex" class="index-card">
          <div>
            <div id="indexNumber" class="index-number">--</div>
            <div id="indexLabel" class="index-label">Sin lectura</div>
            <div class="status-line"><div id="indexFill" class="status-fill" style="width:0%"></div></div>
          </div>
        </div>
      </section>

      <section class="grid-kpis">
        <div class="kpi"><div class="kpi-title">Grupos sin lista</div><div id="kpiMissingGroups" class="kpi-value">--</div><div class="kpi-note">Continuidad legal rota</div></div>
        <div class="kpi"><div class="kpi-title">Alumnos sin traza</div><div id="kpiStudentsTrace" class="kpi-value">--</div><div class="kpi-note">Expediente operativo incompleto</div></div>
        <div class="kpi"><div class="kpi-title">Incidencias personal</div><div id="kpiEmployee" class="kpi-value">--</div><div class="kpi-note">Faltas + retardos Kardex</div></div>
        <div class="kpi"><div class="kpi-title">Brecha seguridad</div><div id="kpiSecurityGap" class="kpi-value">--</div><div class="kpi-note">Entradas no respaldadas por scan</div></div>
        <div class="kpi"><div class="kpi-title">Backlog académico</div><div id="kpiAcademic" class="kpi-value">--</div><div class="kpi-note">Planeaciones + observaciones</div></div>
        <div class="kpi"><div class="kpi-title">SAPF documentado</div><div id="kpiSapf" class="kpi-value">--</div><div class="kpi-note">Atenciones y seguimientos</div></div>
      </section>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Comparativo fijo por plantel</div><div class="section-title">Índice de riesgo corporativo</div></div>
          <span class="stamp">Verde ≥ 85 · Amarillo 70-84 · Rojo &lt; 70 o bandera crítica</span>
        </div>
        <div class="section-body">
          <div class="chart-box"><canvas id="riskIndexChart"></canvas></div>
          <div id="riskIndexInterpretation" class="interpretation"></div>
        </div>
      </section>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Salud corporativa</div><div class="section-title">Radar de cumplimiento por dimensión</div></div>
        </div>
        <div class="section-body">
          <div class="chart-box"><canvas id="radarChart"></canvas></div>
          <div id="radarInterpretation" class="interpretation"></div>
        </div>
      </section>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Riesgo legal y operativo</div><div class="section-title">Asistencia: grupos sin pase de lista</div></div>
        </div>
        <div class="section-body">
          <div class="chart-box"><canvas id="attendanceChart"></canvas></div>
          <div id="attendanceInterpretation" class="interpretation"></div>
        </div>
      </section>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Cadena de custodia</div><div class="section-title">Husky Pass: tasa de escaneo y vulnerabilidad de acceso</div></div>
        </div>
        <div class="section-body">
          <div class="chart-box"><canvas id="huskyChart"></canvas></div>
          <div id="huskyInterpretation" class="interpretation"></div>
        </div>
      </section>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Fuga de capital humano</div><div class="section-title">Kardex: ausencias, retardos y minutos descontables</div></div>
        </div>
        <div class="section-body">
          <div class="chart-box"><canvas id="employeeChart"></canvas></div>
          <div id="employeeInterpretation" class="interpretation"></div>
        </div>
      </section>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Negligencia de supervisión</div><div class="section-title">Académico: planeaciones sin revisión y docentes sin observación</div></div>
        </div>
        <div class="section-body">
          <div class="chart-box"><canvas id="academicChart"></canvas></div>
          <div id="academicInterpretation" class="interpretation"></div>
        </div>
      </section>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Trazabilidad con familias</div><div class="section-title">SAPF: presión operativa documentada por plantel</div></div>
        </div>
        <div class="section-body">
          <div class="chart-box"><canvas id="sapfChart"></canvas></div>
          <div id="sapfInterpretation" class="interpretation"></div>
        </div>
      </section>

      <div class="two-col">
        <section class="table-panel">
          <div class="table-title">Mapa de calor ejecutivo por plantel</div>
          <div style="overflow:auto"><table id="heatmapTable"></table></div>
        </section>
        <section class="table-panel">
          <div class="table-title">Lista de responsabilidad directa</div>
          <div id="watchlist" class="risk-list" style="padding:14px"></div>
        </section>
      </div>

      <div class="two-col">
        <section class="table-panel">
          <div class="table-title">Grupos sin pase de lista</div>
          <div style="overflow:auto"><table id="missingGroupsTable"></table></div>
        </section>
        <section class="table-panel">
          <div class="table-title">Motivos SAPF dominantes</div>
          <div style="overflow:auto"><table id="sapfMotivesTable"></table></div>
        </section>
      </div>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Base histórica</div><div class="section-title">Comparación contra comportamiento histórico disponible</div></div>
        </div>
        <div class="section-body">
          <div class="chart-box"><canvas id="baselineChart"></canvas></div>
          <div id="baselineInterpretation" class="interpretation"></div>
        </div>
      </section>
    </div>
  </main>

  <script>
    const PLANTEL_ORDER = ["PT", "PM", "ST", "SM", "PREET", "PREEM"];
    const GREEN = "#22c55e";
    const YELLOW = "#f59e0b";
    const RED = "#ef4444";
    const GRAY = "#94a3b8";
    const INK = "#0f172a";
    const charts = {};
    const state = { scope: "today", selected: new Set(PLANTEL_ORDER), data: null };

    function fmt(value, digits = 0) {
      const num = Number(value || 0);
      return num.toLocaleString("es-MX", { maximumFractionDigits: digits, minimumFractionDigits: digits });
    }

    function pct(value) { return `${fmt(value, 1)}%`; }
    function statusColor(status) { return status === "critical" ? RED : status === "warning" ? YELLOW : status === "fulfilled" ? GREEN : GRAY; }
    function scoreColor(score) { return score < 70 ? RED : score < 85 ? YELLOW : GREEN; }
    function fixedPlanteles() { return PLANTEL_ORDER.filter(code => state.selected.has(code)); }
    function plantelRows() { return (state.data?.planteles || []).slice().sort((a, b) => PLANTEL_ORDER.indexOf(a.plantel) - PLANTEL_ORDER.indexOf(b.plantel)); }

    function setInterpretation(id, html, status = "warning") {
      const el = document.getElementById(id);
      el.className = `interpretation callout-${status || "warning"}`;
      el.innerHTML = `<div class="interpretation-title">Interpretación Ejecutiva</div><p>${html}</p>`;
    }

    function destroyChart(id) {
      if (charts[id]) {
        charts[id].destroy();
        delete charts[id];
      }
    }

    function renderChart(id, config) {
      destroyChart(id);
      const ctx = document.getElementById(id);
      charts[id] = new Chart(ctx, config);
    }

    function commonOptions(extra = {}) {
      return {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 260 },
        plugins: {
          legend: { labels: { color: INK, font: { weight: "bold" } } },
          tooltip: { backgroundColor: "#0f172a", titleFont: { weight: "bold" }, bodyFont: { weight: "bold" } }
        },
        scales: {
          x: { grid: { color: "#e2e8f0" }, ticks: { color: "#334155", font: { weight: "bold" } } },
          y: { grid: { color: "#e2e8f0" }, ticks: { color: "#334155", font: { weight: "bold" } } }
        },
        ...extra
      };
    }

    function initFilters() {
      const today = new Date();
      const iso = today.toISOString().slice(0, 10);
      document.getElementById("startDate").value = iso;
      document.getElementById("endDate").value = iso;

      document.querySelectorAll(".scope-btn").forEach(btn => {
        btn.addEventListener("click", () => {
          document.querySelectorAll(".scope-btn").forEach(b => b.classList.remove("active"));
          btn.classList.add("active");
          state.scope = btn.dataset.scope;
          const range = state.scope === "range";
          document.getElementById("startDate").classList.toggle("hidden", !range);
          document.getElementById("endDate").classList.toggle("hidden", !range);
          loadDashboard();
        });
      });

      const plantelWrap = document.getElementById("plantelFilters");
      PLANTEL_ORDER.forEach(code => {
        const btn = document.createElement("button");
        btn.className = "plantel-pill active";
        btn.textContent = code;
        btn.type = "button";
        btn.addEventListener("click", () => {
          if (state.selected.has(code) && state.selected.size > 1) {
            state.selected.delete(code);
            btn.classList.remove("active");
          } else {
            state.selected.add(code);
            btn.classList.add("active");
          }
          loadDashboard();
        });
        plantelWrap.appendChild(btn);
      });

      document.getElementById("refreshBtn").addEventListener("click", loadDashboard);
      document.getElementById("startDate").addEventListener("change", loadDashboard);
      document.getElementById("endDate").addEventListener("change", loadDashboard);
    }

    async function loadDashboard() {
      document.getElementById("loadState").classList.remove("hidden");
      document.getElementById("errorState").classList.add("hidden");
      document.getElementById("dashboard").classList.add("hidden");
      try {
        const params = new URLSearchParams();
        params.set("scope", state.scope);
        params.set("planteles", fixedPlanteles().join(","));
        params.set("include_baselines", "true");
        if (state.scope === "range") {
          params.set("start_date", document.getElementById("startDate").value);
          params.set("end_date", document.getElementById("endDate").value);
        }
        const response = await fetch(`/api/v1/corporate-compliance-risk-index?${params.toString()}`, { cache: "no-store" });
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${await response.text()}`);
        state.data = await response.json();
        renderDashboard();
        document.getElementById("dashboard").classList.remove("hidden");
      } catch (error) {
        const box = document.getElementById("errorState");
        box.textContent = `No se pudo cargar el tablero ejecutivo: ${error.message}`;
        box.classList.remove("hidden");
      } finally {
        document.getElementById("loadState").classList.add("hidden");
      }
    }

    function renderDashboard() {
      renderHero();
      renderKpis();
      renderRiskIndex();
      renderRadar();
      renderAttendance();
      renderHusky();
      renderEmployee();
      renderAcademic();
      renderSapf();
      renderHeatmap();
      renderWatchlist();
      renderMissingGroupsTable();
      renderSapfMotivesTable();
      renderBaseline();
    }

    function renderHero() {
      const agg = state.data.aggregate;
      const index = agg.corporate_index.score || 0;
      document.getElementById("indexNumber").textContent = fmt(index, 1);
      document.getElementById("indexLabel").textContent = agg.corporate_index.label;
      document.getElementById("indexNumber").style.color = scoreColor(index);
      document.getElementById("indexFill").style.width = `${Math.max(0, Math.min(100, index))}%`;
      document.getElementById("indexFill").style.background = scoreColor(index);
      document.getElementById("dateStamp").textContent = `Periodo: ${agg.window.start} → ${agg.window.end} · ${agg.window.business_days} días hábiles`;
      document.getElementById("generatedStamp").textContent = `Generado: ${new Date(state.data.generated_at).toLocaleString("es-MX")}`;
    }

    function renderKpis() {
      const t = state.data.aggregate.totals;
      document.getElementById("kpiMissingGroups").textContent = fmt(t.missing_groups);
      document.getElementById("kpiStudentsTrace").textContent = fmt(t.students_without_legal_attendance_trace);
      document.getElementById("kpiEmployee").textContent = fmt(t.employee_incidents);
      document.getElementById("kpiSecurityGap").textContent = fmt(t.security_scan_gap);
      document.getElementById("kpiAcademic").textContent = fmt(t.academic_backlog);
      document.getElementById("kpiSapf").textContent = fmt(t.sapf_parent_interactions);
    }

    function renderRiskIndex() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const values = rows.map(p => p.index.score || 0);
      renderChart("riskIndexChart", {
        type: "bar",
        data: { labels, datasets: [{ label: "Índice de cumplimiento", data: values, backgroundColor: values.map(scoreColor), borderRadius: 12 }] },
        options: commonOptions({ indexAxis: "y", scales: { x: { min: 0, max: 100, grid: { color: "#e2e8f0" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#0f172a", font: { weight: "bold" } } } } })
      });
      const worst = rows.reduce((a, b) => (Number(a.index.score || 0) < Number(b.index.score || 0) ? a : b), rows[0]);
      const critical = rows.filter(p => p.index.status === "critical").length;
      const status = critical ? "critical" : (worst?.index?.status || "warning");
      setInterpretation("riskIndexInterpretation", `${worst.plantel} es el punto más débil del sistema con ${fmt(worst.index.score, 1)} puntos. Hay ${critical} plantel(es) en rojo. Esta vista no ordena por desempeño: conserva PT, PM, ST, SM, PREET, PREEM para que la comparación sea visualmente consistente y la desviación salte de inmediato.`, status);
    }

    function renderRadar() {
      const rows = plantelRows();
      const dimensions = ["attendance", "husky", "employee", "academic", "sapf"];
      const labels = ["Asistencia", "Husky", "Kardex", "Académico", "SAPF"];
      const palette = ["#0f172a", "#2563eb", "#7c3aed", "#db2777", "#0891b2", "#65a30d"];
      renderChart("radarChart", {
        type: "radar",
        data: {
          labels,
          datasets: rows.map((p, idx) => ({
            label: p.plantel,
            data: dimensions.map(d => p.domain_scores[d]?.compliance_score || 0),
            borderColor: palette[idx % palette.length],
            backgroundColor: `${palette[idx % palette.length]}22`,
            pointBackgroundColor: palette[idx % palette.length],
            borderWidth: 2
          }))
        },
        options: commonOptions({ scales: { r: { min: 0, max: 100, grid: { color: "#e2e8f0" }, pointLabels: { color: "#0f172a", font: { weight: "bold" } }, ticks: { backdropColor: "transparent", color: "#64748b" } } } })
      });
      const allScores = rows.flatMap(p => dimensions.map(d => ({ plantel: p.plantel, dim: d, score: p.domain_scores[d]?.compliance_score || 0 })));
      const worst = allScores.reduce((a, b) => a.score < b.score ? a : b, allScores[0]);
      const names = { attendance: "asistencia", husky: "cadena de custodia", employee: "capital humano", academic: "supervisión académica", sapf: "trazabilidad SAPF" };
      setInterpretation("radarInterpretation", `La dimensión más vulnerable es ${names[worst.dim]} en ${worst.plantel}, con ${fmt(worst.score, 1)} puntos. El radar convierte datos operativos dispersos en una lectura de responsabilidad por dirección: quien se contrae hacia el centro está fallando en disciplina de gestión.`, worst.score < 70 ? "critical" : worst.score < 85 ? "warning" : "fulfilled");
    }

    function renderAttendance() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const completion = rows.map(p => p.domains.attendance.completion_percent || 0);
      const missing = rows.map(p => p.domains.attendance.missing_groups_count || 0);
      renderChart("attendanceChart", {
        type: "bar",
        data: {
          labels,
          datasets: [
            { label: "% grupos con lista", data: completion, backgroundColor: completion.map(v => v >= 98 ? GREEN : v >= 90 ? YELLOW : RED), borderRadius: 10 },
            { label: "Grupos sin lista", data: missing, backgroundColor: RED, borderRadius: 10, yAxisID: "y1" }
          ]
        },
        options: commonOptions({
          scales: {
            y: { min: 0, max: 100, grid: { color: "#e2e8f0" }, ticks: { color: "#334155", font: { weight: "bold" } } },
            y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { color: "#991b1b", font: { weight: "bold" } } },
            x: { grid: { display: false }, ticks: { color: "#0f172a", font: { weight: "bold" } } }
          }
        })
      });
      const worst = rows.reduce((a, b) => (Number(a.domains.attendance.missing_groups_count || 0) > Number(b.domains.attendance.missing_groups_count || 0) ? a : b), rows[0]);
      const total = state.data.aggregate.totals.missing_groups;
      const affected = state.data.aggregate.totals.students_without_legal_attendance_trace;
      const status = total > 0 ? "critical" : "fulfilled";
      const conclusion = total > 0
        ? `${worst.plantel} concentra el foco rojo de asistencia. Hay ${fmt(total)} grupos sin pase de lista y ${fmt(affected)} alumnos estimados sin continuidad documental. Esto no es un error de sistema: son grupos sin pase de lista, rompiendo la continuidad del expediente legal y operativo del alumno, generando riesgo de compliance.`
        : `No hay grupos sin pase de lista en el periodo consultado. La continuidad del expediente legal y operativo del alumno está completa en los planteles seleccionados.`;
      setInterpretation("attendanceInterpretation", conclusion, status);
    }

    function renderHusky() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const rates = rows.map(p => p.domains.husky.scan_rate_percent || 0);
      const gaps = rows.map(p => p.domains.husky.scan_gap || 0);
      renderChart("huskyChart", {
        type: "bar",
        data: {
          labels,
          datasets: [
            { label: "% entradas escaneadas", data: rates, backgroundColor: rates.map(v => v >= 90 ? GREEN : v >= 70 ? YELLOW : RED), borderRadius: 10 },
            { label: "Brecha de scans", data: gaps, backgroundColor: RED, borderRadius: 10, yAxisID: "y1" }
          ]
        },
        options: commonOptions({
          scales: {
            y: { min: 0, max: 100, grid: { color: "#e2e8f0" }, ticks: { color: "#334155", font: { weight: "bold" } } },
            y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { color: "#991b1b", font: { weight: "bold" } } },
            x: { grid: { display: false }, ticks: { color: "#0f172a", font: { weight: "bold" } } }
          }
        })
      });
      const worst = rows.reduce((a, b) => (Number(a.domains.husky.scan_rate_percent || 0) < Number(b.domains.husky.scan_rate_percent || 0) ? a : b), rows[0]);
      const gap = state.data.aggregate.totals.security_scan_gap;
      const status = gap > 0 || (worst.domains.husky.scan_rate_percent || 0) < 70 ? "critical" : (worst.domains.husky.scan_rate_percent || 0) < 90 ? "warning" : "fulfilled";
      setInterpretation("huskyInterpretation", `${worst.plantel} muestra la cadena de custodia más débil con ${pct(worst.domains.husky.scan_rate_percent)} de entradas respaldadas. La brecha corporativa es de ${fmt(gap)} accesos esperados sin soporte de escaneo. Lectura ejecutiva: vulnerabilidad de seguridad y cadena de custodia en accesos.`, status);
    }

    function renderEmployee() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const absences = rows.map(p => p.domains.employee.employee_absences || 0);
      const tardies = rows.map(p => p.domains.employee.employee_tardies || 0);
      const minutes = rows.map(p => p.domains.employee.payroll_waste_minutes || 0);
      renderChart("employeeChart", {
        type: "bar",
        data: {
          labels,
          datasets: [
            { label: "Ausencias", data: absences, backgroundColor: RED, borderRadius: 10, stack: "incidents" },
            { label: "Retardos", data: tardies, backgroundColor: YELLOW, borderRadius: 10, stack: "incidents" },
            { label: "Minutos descontables", data: minutes, backgroundColor: INK, borderRadius: 10, yAxisID: "y1" }
          ]
        },
        options: commonOptions({
          scales: {
            x: { stacked: true, grid: { display: false }, ticks: { color: "#0f172a", font: { weight: "bold" } } },
            y: { stacked: true, beginAtZero: true, grid: { color: "#e2e8f0" }, ticks: { color: "#334155", font: { weight: "bold" } } },
            y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { color: "#0f172a", font: { weight: "bold" } } }
          }
        })
      });
      const worst = rows.reduce((a, b) => (Number(a.domains.employee.employee_incidents || 0) > Number(b.domains.employee.employee_incidents || 0) ? a : b), rows[0]);
      const t = state.data.aggregate.totals;
      const status = t.employee_absences > 0 ? "critical" : t.employee_incidents > 0 ? "warning" : "fulfilled";
      setInterpretation("employeeInterpretation", `${worst.plantel} acumula la mayor carga de incidencias de personal. Total corporativo: ${fmt(t.employee_absences)} ausencias, ${fmt(t.employee_tardies)} retardos y ${fmt(t.payroll_waste_minutes)} minutos descontables registrados. Lectura ejecutiva: fuga de capital humano y horas no laboradas.`, status);
    }

    function renderAcademic() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const pendingPlans = rows.map(p => p.domains.academic.planeaciones_pendientes || 0);
      const noObs = rows.map(p => p.domains.academic.docentes_sin_observacion_30_dias || 0);
      const neverObs = rows.map(p => p.domains.academic.docentes_nunca_observados_ciclo || 0);
      renderChart("academicChart", {
        type: "bar",
        data: {
          labels,
          datasets: [
            { label: "Planeaciones sin revisión", data: pendingPlans, backgroundColor: RED, borderRadius: 10, stack: "academic" },
            { label: "Docentes sin observación 30 días", data: noObs, backgroundColor: YELLOW, borderRadius: 10, stack: "academic" },
            { label: "Nunca observados en ciclo", data: neverObs, backgroundColor: INK, borderRadius: 10, stack: "academic" }
          ]
        },
        options: commonOptions({ scales: { x: { stacked: true, grid: { display: false }, ticks: { color: "#0f172a", font: { weight: "bold" } } }, y: { stacked: true, beginAtZero: true, grid: { color: "#e2e8f0" }, ticks: { color: "#334155", font: { weight: "bold" } } } } })
      });
      const worst = rows.reduce((a, b) => (Number(a.domains.academic.supervision_backlog || 0) > Number(b.domains.academic.supervision_backlog || 0) ? a : b), rows[0]);
      const total = state.data.aggregate.totals.academic_backlog;
      const status = total > 0 ? "critical" : "fulfilled";
      setInterpretation("academicInterpretation", `${worst.plantel} exhibe el mayor rezago académico con ${fmt(worst.domains.academic.supervision_backlog)} pendientes de supervisión. Total corporativo: ${fmt(total)} señales de backlog. Lectura ejecutiva: negligencia de supervisión académica por parte de Dirección/Coordinación.`, status);
    }

    function renderSapf() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const values = rows.map(p => p.domains.sapf.parent_interactions || 0);
      renderChart("sapfChart", {
        type: "bar",
        data: { labels, datasets: [{ label: "Atenciones SAPF documentadas", data: values, backgroundColor: values.map(v => v > 0 ? GREEN : YELLOW), borderRadius: 10 }] },
        options: commonOptions({ indexAxis: "y", scales: { x: { beginAtZero: true, grid: { color: "#e2e8f0" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#0f172a", font: { weight: "bold" } } } } })
      });
      const zeroes = rows.filter(p => (p.domains.sapf.parent_interactions || 0) === 0).map(p => p.plantel);
      const highest = rows.reduce((a, b) => (Number(a.domains.sapf.parent_interactions || 0) > Number(b.domains.sapf.parent_interactions || 0) ? a : b), rows[0]);
      const status = zeroes.length ? "warning" : "fulfilled";
      const msg = zeroes.length
        ? `${zeroes.join(", ")} no tiene registros SAPF en el periodo. Para una dueña, cero interacción no significa necesariamente tranquilidad; puede significar falta de trazabilidad de atención a padres.`
        : `${highest.plantel} concentra la mayor presión documentada con ${fmt(highest.domains.sapf.parent_interactions)} atenciones. SAPF sirve como lectura de trazabilidad con familias y temperatura operativa del plantel.`;
      setInterpretation("sapfInterpretation", msg, status);
    }

    function renderHeatmap() {
      const rows = plantelRows();
      const domains = [
        ["attendance", "Asistencia"],
        ["husky", "Husky"],
        ["employee", "Kardex"],
        ["academic", "Académico"],
        ["sapf", "SAPF"]
      ];
      const table = document.getElementById("heatmapTable");
      table.innerHTML = `<thead><tr><th>Plantel</th><th>Índice</th>${domains.map(d => `<th>${d[1]}</th>`).join("")}</tr></thead><tbody>${rows.map(p => {
        const index = Number(p.index.score || 0);
        return `<tr><td><strong>${p.plantel}</strong><br><span style="color:#64748b">${p.resolved_name}</span></td><td><div class="heatmap-cell" style="background:${scoreColor(index)}">${fmt(index, 1)}</div></td>${domains.map(([key]) => {
          const score = Number(p.domain_scores[key]?.compliance_score || 0);
          return `<td><div class="heatmap-cell" style="background:${scoreColor(score)}">${fmt(score, 1)}</div></td>`;
        }).join("")}</tr>`;
      }).join("")}</tbody>`;
    }

    function renderWatchlist() {
      const rows = plantelRows();
      const items = [];
      rows.forEach(p => {
        const d = p.domains;
        if ((d.attendance.missing_groups_count || 0) > 0) items.push({ status: "critical", plantel: p.plantel, title: "Asistencia incompleta", body: `${fmt(d.attendance.missing_groups_count)} grupos sin pase de lista; ${fmt(d.attendance.missing_expected_students)} alumnos estimados sin continuidad de expediente.` });
        if ((d.academic.supervision_backlog || 0) > 0) items.push({ status: "critical", plantel: p.plantel, title: "Supervisión académica rezagada", body: `${fmt(d.academic.planeaciones_pendientes)} planeaciones sin revisión y ${fmt(d.academic.docentes_sin_observacion_30_dias)} docentes sin observación reciente.` });
        if ((d.employee.employee_absences || 0) > 0) items.push({ status: "critical", plantel: p.plantel, title: "Ausencias de personal", body: `${fmt(d.employee.employee_absences)} ausencias Kardex; riesgo financiero por horas no laboradas.` });
        if ((d.husky.scan_rate_percent || 0) < 70) items.push({ status: "critical", plantel: p.plantel, title: "Cadena de custodia débil", body: `${pct(d.husky.scan_rate_percent)} de entradas escaneadas; brecha de ${fmt(d.husky.scan_gap)} scans.` });
        if ((d.sapf.parent_interactions || 0) === 0) items.push({ status: "warning", plantel: p.plantel, title: "SAPF sin trazabilidad", body: "No hay atenciones documentadas en el periodo consultado." });
      });
      const wrap = document.getElementById("watchlist");
      const sorted = items.sort((a, b) => (a.status === "critical" ? -1 : 1) - (b.status === "critical" ? -1 : 1)).slice(0, 18);
      wrap.innerHTML = sorted.length ? sorted.map(item => `<div class="risk-item ${item.status}"><div class="risk-item-title"><span>${item.plantel} · ${item.title}</span><span class="badge ${item.status}">${item.status === "critical" ? "Rojo" : "Amarillo"}</span></div><div class="risk-item-body">${item.body}</div></div>`).join("") : `<div class="risk-item fulfilled"><div class="risk-item-title"><span>Sin focos rojos accionables</span><span class="badge fulfilled">Verde</span></div><div class="risk-item-body">Los planteles seleccionados no presentan banderas críticas en el periodo.</div></div>`;
    }

    function renderMissingGroupsTable() {
      const rows = plantelRows().flatMap(p => (p.domains.attendance.missing_groups || []).map(g => ({ plantel: p.plantel, ...g })));
      const table = document.getElementById("missingGroupsTable");
      table.innerHTML = `<thead><tr><th>Plantel</th><th>Fecha</th><th>Grupo</th><th>Alumnos esperados</th></tr></thead><tbody>${rows.length ? rows.map(g => `<tr><td><strong>${g.plantel}</strong></td><td>${g.date}</td><td>${g.grado} ${g.grupo}</td><td class="num">${fmt(g.expected_students)}</td></tr>`).join("") : `<tr><td colspan="4">Sin grupos faltantes.</td></tr>`}</tbody>`;
    }

    function renderSapfMotivesTable() {
      const rows = plantelRows().flatMap(p => (p.domains.sapf.top_motives || []).slice(0, 4).map(m => ({ plantel: p.plantel, ...m })));
      const table = document.getElementById("sapfMotivesTable");
      table.innerHTML = `<thead><tr><th>Plantel</th><th>Área</th><th>Motivo</th><th>Conteo</th></tr></thead><tbody>${rows.length ? rows.map(m => `<tr><td><strong>${m.plantel}</strong></td><td>${m.area || "Sin área"}</td><td>${m.motivo || "Sin motivo"}</td><td class="num">${fmt(m.conteo)}</td></tr>`).join("") : `<tr><td colspan="4">Sin motivos SAPF en el periodo.</td></tr>`}</tbody>`;
    }

    function renderBaseline() {
      const rows = plantelRows();
      const withBaseline = rows.filter(p => p.baseline && p.baseline.score !== null && p.baseline.score !== undefined);
      const labels = rows.map(p => p.plantel);
      const current = rows.map(p => p.index.score || 0);
      const baseline = rows.map(p => p.baseline?.score || 0);
      renderChart("baselineChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "Índice ejecutivo actual", data: current, backgroundColor: current.map(scoreColor), borderRadius: 10 },
          { label: "Baseline histórico disponible", data: baseline, backgroundColor: "#94a3b8", borderRadius: 10 }
        ] },
        options: commonOptions({ scales: { y: { min: 0, max: 100, grid: { color: "#e2e8f0" }, ticks: { color: "#334155", font: { weight: "bold" } } }, x: { grid: { display: false }, ticks: { color: "#0f172a", font: { weight: "bold" } } } } })
      });
      if (!withBaseline.length) {
        setInterpretation("baselineInterpretation", "No hay baseline histórico suficiente para contrastar esta lectura. El índice ejecutivo actual sigue siendo válido porque se calcula con reglas de compliance directas, no por comparación estadística.", "warning");
        return;
      }
      const deltas = withBaseline.map(p => ({ plantel: p.plantel, delta: Number(p.index.score || 0) - Number(p.baseline.score || 0) }));
      const worst = deltas.reduce((a, b) => a.delta < b.delta ? a : b, deltas[0]);
      const status = worst.delta < -10 ? "critical" : worst.delta < 0 ? "warning" : "fulfilled";
      setInterpretation("baselineInterpretation", `${worst.plantel} muestra la mayor caída frente a su baseline histórico: ${fmt(worst.delta, 1)} puntos. Una caída sostenida indica que la dirección no sólo está fallando contra el estándar ejecutivo, sino contra su propio comportamiento normal.`, status);
    }

    initFilters();
    loadDashboard();
  </script>
</body>
</html>
"""
