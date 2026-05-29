CORPORATE_COMPLIANCE_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SIPAE · Cumplimiento operativo</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.8/dist/chart.umd.min.js"></script>
  <style>
    :root {
      --bg: #f8fafc;
      --panel: #ffffff;
      --text: #111827;
      --muted: #64748b;
      --line: #e5e7eb;
      --soft: #f1f5f9;
      --green: #22c55e;
      --yellow: #f59e0b;
      --red: #ef4444;
      --gray: #94a3b8;
      --ink: #1f2937;
      --shadow: 0 14px 32px rgba(15, 23, 42, 0.06);
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
      backdrop-filter: blur(14px);
    }

    .topbar-inner {
      max-width: 1680px;
      margin: 0 auto;
      padding: 14px 22px;
      display: grid;
      grid-template-columns: minmax(250px, 1fr) auto;
      gap: 16px;
      align-items: center;
    }

    .brand-kicker {
      font-size: 11px;
      font-weight: 800;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.14em;
    }

    .brand-title {
      margin-top: 2px;
      font-size: 19px;
      font-weight: 850;
      letter-spacing: -0.025em;
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

    .summary {
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(280px, 0.75fr);
      gap: 18px;
      align-items: stretch;
    }

    .intro, .score-card, .kpi, .section, .loading, .error-box {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }

    .intro {
      padding: 24px;
    }

    h1 {
      margin: 0;
      font-size: clamp(30px, 4vw, 52px);
      line-height: 0.98;
      letter-spacing: -0.055em;
    }

    .intro p {
      margin: 12px 0 0;
      max-width: 820px;
      font-size: 16px;
      color: #334155;
      font-weight: 560;
    }

    .stamp-row {
      margin-top: 20px;
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

    .score-card {
      padding: 22px;
      display: grid;
      place-items: center;
      min-height: 240px;
      text-align: center;
    }

    .score-number {
      font-size: clamp(76px, 8vw, 122px);
      font-weight: 920;
      letter-spacing: -0.08em;
      line-height: 0.9;
    }

    .score-label {
      margin-top: 10px;
      font-size: 13px;
      font-weight: 850;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #334155;
    }

    .status-line {
      height: 8px;
      border-radius: 999px;
      background: var(--line);
      overflow: hidden;
      margin-top: 14px;
    }

    .status-fill { height: 100%; border-radius: 999px; }

    .grid-kpis {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 14px;
      margin: 18px 0;
    }

    .kpi {
      padding: 16px;
      min-height: 126px;
    }

    .kpi-title {
      font-size: 11px;
      font-weight: 850;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.11em;
    }

    .kpi-value {
      margin-top: 10px;
      font-size: 34px;
      font-weight: 900;
      letter-spacing: -0.045em;
    }

    .kpi-note {
      margin-top: 6px;
      font-size: 12px;
      color: var(--muted);
      font-weight: 650;
    }

    .section {
      margin-top: 18px;
      overflow: hidden;
    }

    .section-head {
      padding: 18px 20px;
      border-bottom: 1px solid var(--line);
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: flex-start;
    }

    .section-eyebrow {
      font-size: 11px;
      font-weight: 850;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.14em;
    }

    .section-title {
      margin-top: 4px;
      font-size: 22px;
      font-weight: 880;
      letter-spacing: -0.03em;
    }

    .section-body {
      padding: 18px 20px 20px;
      display: grid;
      grid-template-columns: minmax(0, 1.28fr) minmax(280px, 0.72fr);
      gap: 18px;
      align-items: stretch;
    }

    .chart-box {
      min-height: 340px;
      height: 380px;
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      background: #ffffff;
    }

    .chart-box.tall { height: 460px; }

    .note-box {
      border: 1px solid var(--line);
      border-radius: 16px;
      background: #f8fafc;
      padding: 16px;
      min-height: 150px;
    }

    .note-title {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 12px;
      font-weight: 850;
      color: var(--text);
      text-transform: uppercase;
      letter-spacing: 0.11em;
    }

    .note-box p {
      margin: 12px 0 0;
      color: #334155;
      font-size: 15px;
      font-weight: 580;
    }

    .callout-critical { border-left: 6px solid var(--red); }
    .callout-warning { border-left: 6px solid var(--yellow); }
    .callout-fulfilled { border-left: 6px solid var(--green); }
    .callout-unavailable { border-left: 6px solid var(--gray); }

    .two-col {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
      margin-top: 18px;
    }

    .loading, .error-box {
      padding: 24px;
      color: var(--muted);
      font-weight: 750;
    }

    .error-box { border-color: rgba(239, 68, 68, 0.35); color: #991b1b; background: #fff7f7; }
    .hidden { display: none !important; }

    @media (max-width: 1180px) {
      .topbar-inner, .summary, .section-body, .two-col { grid-template-columns: 1fr; }
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
        <div class="brand-kicker">SIPAE</div>
        <div class="brand-title">Cumplimiento operativo por plantel</div>
      </div>
      <div class="filters">
        <div class="filter-group" aria-label="Periodo">
          <button class="scope-btn active" data-scope="month">Mes</button>
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
    <div id="loadState" class="loading">Cargando tablero...</div>
    <div id="errorState" class="error-box hidden"></div>
    <div id="dashboard" class="hidden">
      <section class="summary">
        <div class="intro">
          <h1>Cumplimiento operativo</h1>
          <p>Lectura mensual de asistencia, puntualidad, personal, académico, accesos y SAPF. El orden de planteles se mantiene fijo para facilitar comparación.</p>
          <div class="stamp-row">
            <span id="scopeStamp" class="stamp">Mes</span>
            <span id="dateStamp" class="stamp">Periodo</span>
            <span id="generatedStamp" class="stamp">Actualizado</span>
            <span class="stamp">PT · PM · ST · SM · PREET · PREEM</span>
          </div>
        </div>
        <div class="score-card">
          <div>
            <div id="indexNumber" class="score-number">--</div>
            <div id="indexLabel" class="score-label">Sin lectura</div>
            <div class="status-line"><div id="indexFill" class="status-fill" style="width:0%"></div></div>
          </div>
        </div>
      </section>

      <section class="grid-kpis">
        <div class="kpi"><div class="kpi-title">Listas faltantes</div><div id="kpiMissingGroups" class="kpi-value">--</div><div class="kpi-note">Grupos sin registro</div></div>
        <div class="kpi"><div class="kpi-title">Alumnos estimados</div><div id="kpiStudentsTrace" class="kpi-value">--</div><div class="kpi-note">Afectados por listas faltantes</div></div>
        <div class="kpi"><div class="kpi-title">Personal</div><div id="kpiEmployee" class="kpi-value">--</div><div class="kpi-note">Faltas y retardos</div></div>
        <div class="kpi"><div class="kpi-title">Accesos sin scan</div><div id="kpiSecurityGap" class="kpi-value">--</div><div class="kpi-note">Brecha estimada</div></div>
        <div class="kpi"><div class="kpi-title">Académico</div><div id="kpiAcademic" class="kpi-value">--</div><div class="kpi-note">Pendientes de revisión</div></div>
        <div class="kpi"><div class="kpi-title">SAPF</div><div id="kpiSapf" class="kpi-value">--</div><div class="kpi-note">Fichas y seguimientos</div></div>
      </section>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Resumen</div><div class="section-title">Puntaje por plantel</div></div>
          <span class="stamp">Verde ≥ 78 · Amarillo 55-77 · Rojo &lt; 55</span>
        </div>
        <div class="section-body">
          <div class="chart-box"><canvas id="scoreChart"></canvas></div>
          <div id="scoreNote" class="note-box"></div>
        </div>
      </section>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Brechas</div><div class="section-title">Composición de brecha por área</div></div>
        </div>
        <div class="section-body">
          <div class="chart-box tall"><canvas id="gapStackChart"></canvas></div>
          <div id="gapStackNote" class="note-box"></div>
        </div>
      </section>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Asistencia</div><div class="section-title">Registro y asistencia real</div></div>
        </div>
        <div class="section-body">
          <div class="chart-box"><canvas id="attendanceChart"></canvas></div>
          <div id="attendanceNote" class="note-box"></div>
        </div>
      </section>

      <section class="section">
        <div class="section-head">
          <div><div class="section-eyebrow">Puntualidad</div><div class="section-title">Retardos de alumnos</div></div>
        </div>
        <div class="section-body">
          <div class="chart-box"><canvas id="studentTardiesChart"></canvas></div>
          <div id="studentTardiesNote" class="note-box"></div>
        </div>
      </section>

      <div class="two-col">
        <section class="section">
          <div class="section-head">
            <div><div class="section-eyebrow">Accesos</div><div class="section-title">Uso de Husky Pass</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box"><canvas id="huskyChart"></canvas></div>
            <div id="huskyNote" class="note-box"></div>
          </div>
        </section>
        <section class="section">
          <div class="section-head">
            <div><div class="section-eyebrow">Personal</div><div class="section-title">Asistencia laboral</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box"><canvas id="employeeChart"></canvas></div>
            <div id="employeeNote" class="note-box"></div>
          </div>
        </section>
      </div>

      <div class="two-col">
        <section class="section">
          <div class="section-head">
            <div><div class="section-eyebrow">Académico</div><div class="section-title">Revisión y observación</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box"><canvas id="academicChart"></canvas></div>
            <div id="academicNote" class="note-box"></div>
          </div>
        </section>
        <section class="section">
          <div class="section-head">
            <div><div class="section-eyebrow">SAPF</div><div class="section-title">Atención documentada</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box"><canvas id="sapfChart"></canvas></div>
            <div id="sapfNote" class="note-box"></div>
          </div>
        </section>
      </div>
    </div>
  </main>

  <script>
    const PLANTEL_ORDER = ["PT", "PM", "ST", "SM", "PREET", "PREEM"];
    const GREEN = "#22c55e";
    const YELLOW = "#f59e0b";
    const RED = "#ef4444";
    const GRAY = "#94a3b8";
    const INK = "#1f2937";
    const charts = {};
    const state = { scope: "month", selected: new Set(PLANTEL_ORDER), data: null };

    function fmt(value, digits = 0) {
      const num = Number(value || 0);
      return num.toLocaleString("es-MX", { maximumFractionDigits: digits, minimumFractionDigits: digits });
    }

    function pct(value) { return `${fmt(value, 1)}%`; }
    function scoreColor(score) { return Number(score || 0) < 55 ? RED : Number(score || 0) < 78 ? YELLOW : GREEN; }
    function riskColor(value) { return Number(value || 0) >= 45 ? RED : Number(value || 0) >= 22 ? YELLOW : GREEN; }
    function noteStatus(scoreOrRisk, mode = "score") {
      const value = Number(scoreOrRisk || 0);
      if (mode === "risk") return value >= 45 ? "critical" : value >= 22 ? "warning" : "fulfilled";
      return value < 55 ? "critical" : value < 78 ? "warning" : "fulfilled";
    }
    function fixedPlanteles() { return PLANTEL_ORDER.filter(code => state.selected.has(code)); }
    function plantelRows() { return (state.data?.planteles || []).slice().sort((a, b) => PLANTEL_ORDER.indexOf(a.plantel) - PLANTEL_ORDER.indexOf(b.plantel)); }

    function setNote(id, html, status = "warning") {
      const el = document.getElementById(id);
      el.className = `note-box callout-${status || "warning"}`;
      el.innerHTML = `<div class="note-title">Lectura</div><p>${html}</p>`;
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
        plugins: {
          legend: { position: "bottom", labels: { boxWidth: 12, usePointStyle: true, color: "#334155", font: { weight: "bold" } } },
          tooltip: { callbacks: { label: context => {
            const parsed = context.parsed || {};
            const value = context.chart?.options?.indexAxis === "y" ? parsed.x : parsed.y;
            return `${context.dataset.label}: ${fmt(value, 1)}`;
          } } }
        },
        ...extra,
      };
    }

    function initFilters() {
      document.querySelectorAll(".scope-btn").forEach(btn => {
        btn.addEventListener("click", () => {
          document.querySelectorAll(".scope-btn").forEach(b => b.classList.remove("active"));
          btn.classList.add("active");
          state.scope = btn.dataset.scope;
          const custom = state.scope === "range";
          document.getElementById("startDate").classList.toggle("hidden", !custom);
          document.getElementById("endDate").classList.toggle("hidden", !custom);
          if (custom && !document.getElementById("startDate").value) {
            const now = new Date();
            const start = new Date(now.getFullYear(), now.getMonth(), 1);
            document.getElementById("startDate").value = start.toISOString().slice(0, 10);
            document.getElementById("endDate").value = now.toISOString().slice(0, 10);
          }
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
        params.set("scope", state.scope || "month");
        params.set("planteles", fixedPlanteles().join(","));
        params.set("include_baselines", "false");
        if (state.scope === "range") {
          params.set("start_date", document.getElementById("startDate").value);
          params.set("end_date", document.getElementById("endDate").value);
        }
        const response = await fetch(`/api/v1/corporate-compliance-risk-index?${params.toString()}`, { cache: "no-store" });
        if (!response.ok) {
          const body = await response.text();
          const cleanBody = body.replace(/<[^>]+>/g, " ").replace(new RegExp("\\s+", "g"), " ").trim().slice(0, 360);
          throw new Error(`HTTP ${response.status}: ${cleanBody || response.statusText}`);
        }
        state.data = await response.json();
        renderDashboard();
        document.getElementById("dashboard").classList.remove("hidden");
      } catch (error) {
        const box = document.getElementById("errorState");
        box.textContent = `No se pudo cargar el tablero: ${error.message}`;
        box.classList.remove("hidden");
      } finally {
        document.getElementById("loadState").classList.add("hidden");
      }
    }

    function renderDashboard() {
      renderHero();
      renderKpis();
      renderScoreChart();
      renderGapStackChart();
      renderAttendanceChart();
      renderStudentTardiesChart();
      renderHuskyChart();
      renderEmployeeChart();
      renderAcademicChart();
      renderSapfChart();
    }

    function renderHero() {
      const agg = state.data.aggregate;
      const index = agg.corporate_index.score || 0;
      document.getElementById("indexNumber").textContent = fmt(index, 1);
      document.getElementById("indexLabel").textContent = agg.corporate_index.label || "Sin lectura";
      document.getElementById("indexNumber").style.color = scoreColor(index);
      document.getElementById("indexFill").style.width = `${Math.max(0, Math.min(100, index))}%`;
      document.getElementById("indexFill").style.background = scoreColor(index);
      const scopeLabel = (state.data.scope === "ciclo_escolar") ? "Ciclo" : (state.data.scope === "range") ? "Rango" : "Mes";
      document.getElementById("scopeStamp").textContent = scopeLabel;
      document.getElementById("dateStamp").textContent = `${agg.window.start} → ${agg.window.end} · ${agg.window.business_days} días hábiles`;
      document.getElementById("generatedStamp").textContent = `Actualizado: ${new Date(state.data.generated_at).toLocaleString("es-MX")}`;
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

    function renderScoreChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const values = rows.map(p => p.index.score || 0);
      renderChart("scoreChart", {
        type: "bar",
        data: { labels, datasets: [{ label: "Puntaje", data: values, backgroundColor: values.map(scoreColor), borderRadius: 12 }] },
        options: commonOptions({ indexAxis: "y", scales: { x: { min: 0, max: 100, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const worst = rows.reduce((a, b) => (Number(a.index.score || 0) < Number(b.index.score || 0) ? a : b), rows[0]);
      const best = rows.reduce((a, b) => (Number(a.index.score || 0) > Number(b.index.score || 0) ? a : b), rows[0]);
      const spread = Number(best.index.score || 0) - Number(worst.index.score || 0);
      setNote("scoreNote", `${best.plantel} tiene el puntaje más alto (${fmt(best.index.score, 1)}). ${worst.plantel} tiene la mayor brecha (${fmt(worst.index.score, 1)}). La diferencia entre ambos es de ${fmt(spread, 1)} puntos.`, noteStatus(worst.index.score));
    }

    function renderGapStackChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const domainLabels = [
        ["attendance", "Asistencia"],
        ["husky", "Accesos"],
        ["employee", "Personal"],
        ["academic", "Académico"],
        ["sapf", "SAPF"],
      ];
      renderChart("gapStackChart", {
        type: "bar",
        data: {
          labels,
          datasets: domainLabels.map(([key, label]) => ({
            label,
            data: rows.map(p => p.domain_scores[key]?.risk_score || 0),
            backgroundColor: rows.map(p => riskColor(p.domain_scores[key]?.risk_score || 0)),
            borderRadius: 8,
          }))
        },
        options: commonOptions({ indexAxis: "y", scales: { x: { stacked: true, beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { stacked: true, grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const all = rows.flatMap(p => domainLabels.map(([key, label]) => ({ plantel: p.plantel, label, risk: Number(p.domain_scores[key]?.risk_score || 0) })));
      const top = all.reduce((a, b) => a.risk > b.risk ? a : b, all[0]);
      setNote("gapStackNote", `La mayor brecha se ubica en ${top.plantel}, área ${top.label}, con ${fmt(top.risk, 1)} puntos de riesgo. Esta gráfica usa brecha por área, no nivel de actividad histórico.`, noteStatus(top.risk, "risk"));
    }

    function renderAttendanceChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const completion = rows.map(p => p.domains.attendance.completion_percent || 0);
      const attendance = rows.map(p => p.domains.attendance.attendance_rate_percent || 0);
      const affectedRate = rows.map(p => {
        const total = Number(p.domains.attendance.total_students_recorded || 0) + Number(p.domains.attendance.missing_expected_students || 0);
        return total > 0 ? (Number(p.domains.attendance.missing_expected_students || 0) / total) * 100 : 0;
      });
      renderChart("attendanceChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "% listas completas", data: completion, backgroundColor: completion.map(scoreColor), borderRadius: 10 },
          { label: "% asistencia real", data: attendance, backgroundColor: attendance.map(scoreColor), borderRadius: 10 },
          { label: "% alumnos estimados afectados", data: affectedRate, backgroundColor: affectedRate.map(v => v >= 12 ? RED : v >= 4 ? YELLOW : GREEN), borderRadius: 10 }
        ] },
        options: commonOptions({ scales: { y: { min: 0, max: 100, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, x: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const worst = rows.reduce((a, b) => (Number(a.domains.attendance.completion_percent || 0) < Number(b.domains.attendance.completion_percent || 0) ? a : b), rows[0]);
      const t = state.data.aggregate.totals;
      setNote("attendanceNote", `${worst.plantel} tiene el menor porcentaje de listas completas (${pct(worst.domains.attendance.completion_percent)}). En el periodo hay ${fmt(t.missing_groups)} listas faltantes y ${fmt(t.absent_students)} ausencias registradas.`, noteStatus(worst.domains.attendance.completion_percent));
    }

    function renderStudentTardiesChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const tardies = rows.map(p => p.domains.husky.student_tardies || 0);
      const repeaters = rows.map(p => p.domains.husky.repeat_tardy_students_count ?? (p.domains.husky.repeat_tardy_students || []).length);
      const avgDaily = rows.map(p => p.domains.husky.avg_tardies_per_business_day || 0);
      renderChart("studentTardiesChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "Retardos", data: tardies, backgroundColor: tardies.map(v => v >= 25 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10 },
          { label: "Reincidencia agregada", data: repeaters, backgroundColor: repeaters.map(v => v >= 8 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10 },
          { label: "Promedio diario", data: avgDaily, backgroundColor: GRAY, borderRadius: 10 }
        ] },
        options: commonOptions({ indexAxis: "y", scales: { x: { beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const worst = rows.reduce((a, b) => (Number(a.domains.husky.student_tardies || 0) > Number(b.domains.husky.student_tardies || 0) ? a : b), rows[0]);
      const t = state.data.aggregate.totals;
      const status = t.student_tardies >= 75 ? "critical" : t.student_tardies > 0 ? "warning" : "fulfilled";
      setNote("studentTardiesNote", `${worst.plantel} acumula más retardos (${fmt(worst.domains.husky.student_tardies)}). Total del periodo: ${fmt(t.student_tardies)} retardos y ${fmt(t.repeat_tardy_students)} reincidencias agregadas.`, status);
    }

    function renderHuskyChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const rates = rows.map(p => p.domains.husky.scan_rate_percent || 0);
      const gaps = rows.map(p => p.domains.husky.scan_gap || 0);
      renderChart("huskyChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "% entradas con scan", data: rates, backgroundColor: rates.map(scoreColor), borderRadius: 10 },
          { label: "Brecha", data: gaps, backgroundColor: gaps.map(v => v > 0 ? YELLOW : GREEN), borderRadius: 10, yAxisID: "y1" }
        ] },
        options: commonOptions({ scales: { y: { min: 0, max: 100, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { color: "#334155", font: { weight: "bold" } } }, x: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const worst = rows.reduce((a, b) => (Number(a.domains.husky.scan_rate_percent || 0) < Number(b.domains.husky.scan_rate_percent || 0) ? a : b), rows[0]);
      setNote("huskyNote", `${worst.plantel} tiene el menor uso registrado de Husky Pass (${pct(worst.domains.husky.scan_rate_percent)}). La brecha total estimada es ${fmt(state.data.aggregate.totals.security_scan_gap)} scans.`, noteStatus(worst.domains.husky.scan_rate_percent));
    }

    function renderEmployeeChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const absences = rows.map(p => p.domains.employee.employee_absences || 0);
      const tardies = rows.map(p => p.domains.employee.employee_tardies || 0);
      const minutes = rows.map(p => p.domains.employee.payroll_waste_minutes || 0);
      renderChart("employeeChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "Faltas", data: absences, backgroundColor: absences.map(v => v >= 12 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10, stack: "incidents" },
          { label: "Retardos", data: tardies, backgroundColor: tardies.map(v => v >= 40 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10, stack: "incidents" },
          { label: "Minutos", data: minutes, backgroundColor: GRAY, borderRadius: 10, yAxisID: "y1" }
        ] },
        options: commonOptions({ scales: { x: { stacked: true, grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } }, y: { stacked: true, beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { color: "#334155", font: { weight: "bold" } } } } })
      });
      const worst = rows.reduce((a, b) => (Number(a.domains.employee.employee_incidents || 0) > Number(b.domains.employee.employee_incidents || 0) ? a : b), rows[0]);
      const t = state.data.aggregate.totals;
      const status = t.employee_absences >= 12 || t.payroll_waste_minutes >= 1600 ? "critical" : t.employee_incidents > 0 ? "warning" : "fulfilled";
      setNote("employeeNote", `${worst.plantel} concentra más incidencias de personal (${fmt(worst.domains.employee.employee_incidents)}). Total: ${fmt(t.employee_absences)} faltas, ${fmt(t.employee_tardies)} retardos y ${fmt(t.payroll_waste_minutes)} minutos registrados.`, status);
    }

    function renderAcademicChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const pending = rows.map(p => p.domains.academic.planeaciones_pendientes || 0);
      const noObs = rows.map(p => p.domains.academic.docentes_sin_observacion_30_dias || 0);
      const never = rows.map(p => p.domains.academic.docentes_nunca_observados_ciclo || 0);
      renderChart("academicChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "Planeaciones pendientes", data: pending, backgroundColor: pending.map(v => v >= 18 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10 },
          { label: "Docentes sin observación", data: noObs, backgroundColor: noObs.map(v => v >= 12 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10 },
          { label: "Nunca observados", data: never, backgroundColor: never.map(v => v >= 8 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10 }
        ] },
        options: commonOptions({ indexAxis: "y", scales: { x: { beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const worst = rows.reduce((a, b) => (Number(a.domains.academic.supervision_backlog || 0) > Number(b.domains.academic.supervision_backlog || 0) ? a : b), rows[0]);
      const backlog = state.data.aggregate.totals.academic_backlog;
      setNote("academicNote", `${worst.plantel} tiene más pendientes académicos (${fmt(worst.domains.academic.supervision_backlog)}). Total del periodo: ${fmt(backlog)} pendientes entre revisión de planeaciones y observación docente.`, backlog >= 35 ? "critical" : backlog > 0 ? "warning" : "fulfilled");
    }

    function renderSapfChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const tickets = rows.map(p => p.domains.sapf.tickets_created || 0);
      const followups = rows.map(p => p.domains.sapf.followups || 0);
      const open = rows.map(p => p.domains.sapf.open_cases || 0);
      const complaints = rows.map(p => p.domains.sapf.complaints || 0);
      renderChart("sapfChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "Fichas", data: tickets, backgroundColor: GRAY, borderRadius: 10 },
          { label: "Seguimientos", data: followups, backgroundColor: followups.map(v => v > 0 ? GREEN : GRAY), borderRadius: 10 },
          { label: "Abiertos", data: open, backgroundColor: open.map(v => v >= 8 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10 },
          { label: "Quejas", data: complaints, backgroundColor: complaints.map(v => v >= 5 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10 }
        ] },
        options: commonOptions({ indexAxis: "y", scales: { x: { beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const t = state.data.aggregate.totals;
      const zeroRows = rows.filter(p => Number(p.domains.sapf.parent_interactions || 0) === 0).map(p => p.plantel);
      const status = t.sapf_complaints >= 8 || t.sapf_open_cases >= 20 ? "critical" : zeroRows.length || t.sapf_open_cases > 0 || t.sapf_complaints > 0 ? "warning" : "fulfilled";
      const zeroText = zeroRows.length ? ` Sin registros en: ${zeroRows.join(", ")}.` : "";
      setNote("sapfNote", `SAPF registra ${fmt(t.sapf_tickets_created)} fichas, ${fmt(t.sapf_followups)} seguimientos, ${fmt(t.sapf_open_cases)} casos abiertos y ${fmt(t.sapf_complaints)} quejas.${zeroText}`, status);
    }

    initFilters();
    loadDashboard();
  </script>
</body>
</html>
"""
