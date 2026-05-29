CORPORATE_COMPLIANCE_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Tablero operativo</title>
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
      --sans: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--text); font-family: var(--sans); line-height: 1.35; }
    button, input { font: inherit; }
    .topbar { position: sticky; top: 0; z-index: 50; background: rgba(255,255,255,0.96); border-bottom: 1px solid var(--line); backdrop-filter: blur(14px); }
    .topbar-inner { max-width: 1640px; margin: 0 auto; padding: 13px 22px; display: grid; grid-template-columns: minmax(240px, 1fr) auto; gap: 16px; align-items: center; }
    .brand-kicker { font-size: 11px; font-weight: 800; color: var(--muted); text-transform: uppercase; letter-spacing: 0.12em; }
    .brand-title { margin-top: 2px; font-size: 19px; font-weight: 850; letter-spacing: -0.025em; }
    .filters { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; justify-content: flex-end; }
    .filter-group { display: flex; align-items: center; gap: 6px; padding: 6px; background: var(--soft); border: 1px solid var(--line); border-radius: 999px; }
    .scope-btn, .plantel-pill, .refresh-btn { border: 1px solid transparent; background: transparent; color: #334155; padding: 9px 12px; border-radius: 999px; font-size: 12px; font-weight: 800; cursor: pointer; transition: 150ms ease; white-space: nowrap; }
    .scope-btn:hover, .plantel-pill:hover { background: #ffffff; border-color: var(--line); }
    .scope-btn.active, .plantel-pill.active { background: var(--ink); border-color: var(--ink); color: #ffffff; }
    .date-input { appearance: none; border: 1px solid var(--line); background: #ffffff; border-radius: 999px; padding: 9px 12px; color: var(--text); font-size: 12px; font-weight: 800; }
    .refresh-btn { background: var(--text); color: #ffffff; border-color: var(--text); padding-inline: 16px; }
    .page { max-width: 1640px; margin: 0 auto; padding: 22px 22px 70px; }
    .loading, .error-box, .card, .section { background: var(--panel); border: 1px solid var(--line); border-radius: var(--radius); box-shadow: var(--shadow); }
    .loading, .error-box { padding: 24px; color: var(--muted); font-weight: 750; }
    .error-box { border-color: rgba(239,68,68,0.35); color: #991b1b; background: #fff7f7; }
    .hidden { display: none !important; }
    .hero { display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(280px, 0.65fr); gap: 18px; align-items: stretch; }
    .card { padding: 22px; }
    h1 { margin: 0; font-size: clamp(30px, 4vw, 52px); line-height: 0.98; letter-spacing: -0.055em; }
    .lead { margin: 12px 0 0; max-width: 790px; font-size: 15px; color: #334155; font-weight: 560; }
    .stamp-row { margin-top: 18px; display: flex; flex-wrap: wrap; gap: 9px; }
    .stamp { display: inline-flex; align-items: center; gap: 8px; border: 1px solid var(--line); background: #ffffff; border-radius: 999px; padding: 8px 11px; font-size: 12px; font-weight: 800; color: #334155; }
    .score-card { display: grid; place-items: center; min-height: 220px; text-align: center; }
    .score-number { font-size: clamp(72px, 8vw, 112px); font-weight: 920; letter-spacing: -0.08em; line-height: 0.9; }
    .score-label { margin-top: 10px; font-size: 13px; font-weight: 850; text-transform: uppercase; letter-spacing: 0.08em; color: #334155; }
    .status-line { height: 8px; border-radius: 999px; background: var(--line); overflow: hidden; margin-top: 14px; }
    .status-fill { height: 100%; border-radius: 999px; }
    .grid-kpis { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 14px; margin: 18px 0; }
    .kpi { padding: 16px; min-height: 116px; }
    .kpi-title { font-size: 11px; font-weight: 850; color: var(--muted); text-transform: uppercase; letter-spacing: 0.10em; }
    .kpi-value { margin-top: 10px; font-size: 32px; font-weight: 900; letter-spacing: -0.045em; }
    .kpi-note { margin-top: 5px; font-size: 12px; color: var(--muted); font-weight: 650; }
    .layout { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
    .layout .section.wide { grid-column: 1 / -1; }
    .section { overflow: hidden; }
    .section-head { padding: 17px 20px; border-bottom: 1px solid var(--line); display: flex; justify-content: space-between; gap: 14px; align-items: flex-start; }
    .section-eyebrow { font-size: 11px; font-weight: 850; color: var(--muted); text-transform: uppercase; letter-spacing: 0.12em; }
    .section-title { margin-top: 4px; font-size: 21px; font-weight: 880; letter-spacing: -0.03em; }
    .section-body { padding: 18px 20px 20px; display: grid; grid-template-columns: minmax(0, 1.25fr) minmax(260px, 0.75fr); gap: 18px; align-items: stretch; }
    .chart-box { min-height: 320px; height: 360px; border: 1px solid var(--line); border-radius: 16px; padding: 14px; background: #ffffff; }
    .chart-box.short { height: 310px; }
    .chart-box.tall { height: 430px; }
    .note-box { border: 1px solid var(--line); border-radius: 16px; background: #f8fafc; padding: 16px; min-height: 150px; }
    .note-title { display: flex; align-items: center; gap: 8px; font-size: 12px; font-weight: 850; color: var(--text); text-transform: uppercase; letter-spacing: 0.10em; }
    .note-box p { margin: 12px 0 0; color: #334155; font-size: 14px; font-weight: 580; }
    .callout-critical { border-left: 6px solid var(--red); }
    .callout-warning { border-left: 6px solid var(--yellow); }
    .callout-fulfilled { border-left: 6px solid var(--green); }
    .callout-unavailable { border-left: 6px solid var(--gray); }
    @media (max-width: 1180px) { .topbar-inner, .hero, .section-body, .layout { grid-template-columns: 1fr; } .filters { justify-content: flex-start; } .grid-kpis { grid-template-columns: repeat(3, minmax(0, 1fr)); } .layout .section.wide { grid-column: auto; } }
    @media (max-width: 760px) { .page { padding: 16px 12px 42px; } .topbar-inner { padding: 12px; } .grid-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); } .section-head { display: block; } .chart-box { height: 320px; } }
  </style>
</head>
<body>
  <div class="topbar">
    <div class="topbar-inner">
      <div>
        <div class="brand-kicker">Planteles</div>
        <div class="brand-title">Tablero operativo</div>
      </div>
      <div class="filters">
        <div class="filter-group" aria-label="Periodo">
          <button class="scope-btn active" data-scope="month">Mes en curso</button>
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
      <section class="hero">
        <div class="card">
          <h1>Tablero operativo</h1>
          <p class="lead">Comparativo mensual por plantel. Asistencia, puntualidad, accesos, personal, revisión académica y SAPF.</p>
          <div class="stamp-row">
            <span id="scopeStamp" class="stamp">Mes en curso</span>
            <span id="dateStamp" class="stamp">Periodo</span>
            <span id="generatedStamp" class="stamp">Actualizado</span>
            <span class="stamp">PT · PM · ST · SM · PREET · PREEM</span>
          </div>
        </div>
        <div class="card score-card">
          <div>
            <div id="indexNumber" class="score-number">--</div>
            <div id="indexLabel" class="score-label">Sin lectura</div>
            <div class="status-line"><div id="indexFill" class="status-fill" style="width:0%"></div></div>
          </div>
        </div>
      </section>

      <section class="grid-kpis">
        <div class="card kpi"><div class="kpi-title">Listas completas</div><div id="kpiCompletion" class="kpi-value">--</div><div class="kpi-note">Promedio del periodo</div></div>
        <div class="card kpi"><div class="kpi-title">Asistencia real</div><div id="kpiAttendance" class="kpi-value">--</div><div class="kpi-note">Registros capturados</div></div>
        <div class="card kpi"><div class="kpi-title">Retardos alumnos</div><div id="kpiTardies" class="kpi-value">--</div><div class="kpi-note">Primer acceso del día</div></div>
        <div class="card kpi"><div class="kpi-title">Uso de accesos</div><div id="kpiScans" class="kpi-value">--</div><div class="kpi-note">Entradas con scan</div></div>
        <div class="card kpi"><div class="kpi-title">Personal</div><div id="kpiEmployee" class="kpi-value">--</div><div class="kpi-note">Faltas y retardos</div></div>
        <div class="card kpi"><div class="kpi-title">SAPF</div><div id="kpiSapf" class="kpi-value">--</div><div class="kpi-note">Fichas + seguimientos</div></div>
      </section>

      <div class="layout">
        <section class="section wide">
          <div class="section-head">
            <div><div class="section-eyebrow">Resumen</div><div class="section-title">Resultado por plantel</div></div>
            <span class="stamp">Verde ≥ 78 · Amarillo 55-77 · Rojo &lt; 55</span>
          </div>
          <div class="section-body">
            <div class="chart-box"><canvas id="scoreChart"></canvas></div>
            <div id="scoreNote" class="note-box"></div>
          </div>
        </section>

        <section class="section wide">
          <div class="section-head">
            <div><div class="section-eyebrow">Mes</div><div class="section-title">Tendencia diaria</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box"><canvas id="dailyChart"></canvas></div>
            <div id="dailyNote" class="note-box"></div>
          </div>
        </section>

        <section class="section wide">
          <div class="section-head">
            <div><div class="section-eyebrow">Brecha</div><div class="section-title">Áreas que mueven el resultado</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box"><canvas id="radarChart"></canvas></div>
            <div id="radarNote" class="note-box"></div>
          </div>
        </section>

        <section class="section">
          <div class="section-head">
            <div><div class="section-eyebrow">Asistencia</div><div class="section-title">Registro y asistencia</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box short"><canvas id="attendanceChart"></canvas></div>
            <div id="attendanceNote" class="note-box"></div>
          </div>
        </section>

        <section class="section">
          <div class="section-head">
            <div><div class="section-eyebrow">Puntualidad</div><div class="section-title">Retardos de alumnos</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box short"><canvas id="tardyChart"></canvas></div>
            <div id="tardyNote" class="note-box"></div>
          </div>
        </section>

        <section class="section">
          <div class="section-head">
            <div><div class="section-eyebrow">Accesos</div><div class="section-title">Husky Pass</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box short"><canvas id="accessChart"></canvas></div>
            <div id="accessNote" class="note-box"></div>
          </div>
        </section>

        <section class="section">
          <div class="section-head">
            <div><div class="section-eyebrow">Personal</div><div class="section-title">Asistencia laboral</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box short"><canvas id="peopleChart"></canvas></div>
            <div id="peopleNote" class="note-box"></div>
          </div>
        </section>

        <section class="section">
          <div class="section-head">
            <div><div class="section-eyebrow">Académico</div><div class="section-title">Revisión y observación</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box short"><canvas id="academicChart"></canvas></div>
            <div id="academicNote" class="note-box"></div>
          </div>
        </section>

        <section class="section">
          <div class="section-head">
            <div><div class="section-eyebrow">SAPF</div><div class="section-title">Atención documentada</div></div>
          </div>
          <div class="section-body">
            <div class="chart-box short"><canvas id="sapfChart"></canvas></div>
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
    function pct(value, digits = 1) { return `${fmt(value, digits)}%`; }
    function scoreColor(score) { const v = Number(score || 0); return v < 55 ? RED : v < 78 ? YELLOW : GREEN; }
    function riskColor(risk) { const v = Number(risk || 0); return v >= 45 ? RED : v >= 22 ? YELLOW : GREEN; }
    function scanColor(rate) { const v = Number(rate || 0); return v < 25 ? RED : v < 55 ? YELLOW : GREEN; }
    function statusForScore(score) { const v = Number(score || 0); return v < 55 ? "critical" : v < 78 ? "warning" : "fulfilled"; }
    function statusForScan(rate) { const v = Number(rate || 0); return v < 25 ? "critical" : v < 55 ? "warning" : "fulfilled"; }
    function statusForRisk(risk) { const v = Number(risk || 0); return v >= 45 ? "critical" : v >= 22 ? "warning" : "fulfilled"; }
    function plantelRows() { return (state.data?.planteles || []).slice().sort((a, b) => PLANTEL_ORDER.indexOf(a.plantel) - PLANTEL_ORDER.indexOf(b.plantel)); }
    function selectedPlanteles() { return PLANTEL_ORDER.filter(code => state.selected.has(code)); }
    function avg(values) { const clean = values.map(Number).filter(Number.isFinite); return clean.length ? clean.reduce((a,b)=>a+b,0) / clean.length : 0; }
    function sum(values) { return values.map(Number).filter(Number.isFinite).reduce((a,b)=>a+b,0); }

    function setNote(id, html, status = "warning") {
      const el = document.getElementById(id);
      el.className = `note-box callout-${status || "warning"}`;
      el.innerHTML = `<div class="note-title">Dato principal</div><p>${html}</p>`;
    }
    function destroyChart(id) { if (charts[id]) { charts[id].destroy(); delete charts[id]; } }
    function renderChart(id, config) { destroyChart(id); charts[id] = new Chart(document.getElementById(id), config); }
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
          if (state.selected.has(code) && state.selected.size > 1) { state.selected.delete(code); btn.classList.remove("active"); }
          else { state.selected.add(code); btn.classList.add("active"); }
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
        params.set("planteles", selectedPlanteles().join(","));
        params.set("include_baselines", "false");
        if (state.scope === "range") {
          params.set("start_date", document.getElementById("startDate").value);
          params.set("end_date", document.getElementById("endDate").value);
        }
        const response = await fetch(`/api/v1/corporate-compliance-risk-index?${params.toString()}`, { cache: "no-store" });
        if (!response.ok) {
          const body = await response.text();
          const cleanBody = body.replace(/<[^>]+>/g, " ").replace(new RegExp("\\s+", "g"), " ").trim().slice(0, 300);
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
      renderDailyChart();
      renderRadarChart();
      renderAttendanceChart();
      renderTardyChart();
      renderAccessChart();
      renderPeopleChart();
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
      const scopeLabel = (state.data.scope === "ciclo_escolar") ? "Ciclo" : (state.data.scope === "range") ? "Rango" : "Mes en curso";
      document.getElementById("scopeStamp").textContent = scopeLabel;
      document.getElementById("dateStamp").textContent = `${agg.window.start} → ${agg.window.end} · ${agg.window.business_days} días hábiles`;
      document.getElementById("generatedStamp").textContent = `Actualizado: ${new Date(state.data.generated_at).toLocaleString("es-MX")}`;
    }

    function renderKpis() {
      const rows = plantelRows();
      const t = state.data.aggregate.totals;
      const completion = avg(rows.map(p => p.domains.attendance.completion_percent));
      const attendance = avg(rows.map(p => p.domains.attendance.attendance_rate_percent));
      const scans = avg(rows.map(p => p.domains.husky.scan_rate_percent));
      document.getElementById("kpiCompletion").textContent = pct(completion);
      document.getElementById("kpiAttendance").textContent = pct(attendance);
      document.getElementById("kpiTardies").textContent = fmt(t.student_tardies);
      document.getElementById("kpiScans").textContent = pct(scans);
      document.getElementById("kpiEmployee").textContent = fmt(t.employee_incidents);
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
      const worst = rows.reduce((a, b) => Number(a.index.score || 0) < Number(b.index.score || 0) ? a : b, rows[0]);
      const best = rows.reduce((a, b) => Number(a.index.score || 0) > Number(b.index.score || 0) ? a : b, rows[0]);
      setNote("scoreNote", `${best.plantel} está arriba con ${fmt(best.index.score, 1)} puntos. ${worst.plantel} queda abajo con ${fmt(worst.index.score, 1)}. Brecha entre ambos: ${fmt(Number(best.index.score || 0) - Number(worst.index.score || 0), 1)} puntos.`, statusForScore(worst.index.score));
    }

    function renderDailyChart() {
      const series = state.data.aggregate.daily_series || [];
      const labels = series.map(p => p.date.slice(5));
      const completion = series.map(p => p.completion_percent || 0);
      const attendance = series.map(p => p.attendance_rate_percent || 0);
      const tardies = series.map(p => p.student_tardies || 0);
      renderChart("dailyChart", {
        type: "line",
        data: { labels, datasets: [
          { label: "% listas completas", data: completion, borderColor: GREEN, backgroundColor: "rgba(34,197,94,0.10)", tension: 0.25, fill: false, yAxisID: "y" },
          { label: "% asistencia", data: attendance, borderColor: INK, backgroundColor: "rgba(31,41,55,0.08)", tension: 0.25, fill: false, yAxisID: "y" },
          { label: "Retardos", data: tardies, borderColor: YELLOW, backgroundColor: "rgba(245,158,11,0.12)", tension: 0.25, fill: false, yAxisID: "y1" }
        ] },
        options: commonOptions({ scales: { y: { min: 0, max: 100, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { color: "#334155", font: { weight: "bold" } } }, x: { grid: { display: false }, ticks: { color: "#334155", maxRotation: 0, autoSkip: true } } } })
      });
      const worstDay = series.reduce((a, b) => Number(a.completion_percent || 100) < Number(b.completion_percent || 100) ? a : b, series[0] || {});
      const maxTardyDay = series.reduce((a, b) => Number(a.student_tardies || 0) > Number(b.student_tardies || 0) ? a : b, series[0] || {});
      setNote("dailyNote", `Día con menor registro: ${worstDay.date || "—"} (${pct(worstDay.completion_percent || 0)} listas completas). Día con más retardos: ${maxTardyDay.date || "—"} (${fmt(maxTardyDay.student_tardies || 0)}).`, statusForScore(worstDay.completion_percent || 100));
    }

    function renderRadarChart() {
      const rows = plantelRows();
      const labels = ["Asistencia", "Accesos", "Personal", "Académico", "SAPF"];
      const values = [
        avg(rows.map(p => p.domain_scores.attendance?.compliance_score || 0)),
        avg(rows.map(p => p.domain_scores.husky?.compliance_score || 0)),
        avg(rows.map(p => p.domain_scores.employee?.compliance_score || 0)),
        avg(rows.map(p => p.domain_scores.academic?.compliance_score || 0)),
        avg(rows.map(p => p.domain_scores.sapf?.compliance_score || 0)),
      ];
      renderChart("radarChart", {
        type: "radar",
        data: { labels, datasets: [{ label: "Promedio", data: values, borderColor: INK, backgroundColor: "rgba(31,41,55,0.12)", pointBackgroundColor: values.map(scoreColor) }] },
        options: commonOptions({ scales: { r: { min: 0, max: 100, ticks: { backdropColor: "transparent", color: "#64748b" }, grid: { color: "#e5e7eb" }, angleLines: { color: "#e5e7eb" }, pointLabels: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const minIndex = values.reduce((min, v, i) => v < values[min] ? i : min, 0);
      setNote("radarNote", `Área con menor resultado promedio: ${labels[minIndex]} (${fmt(values[minIndex], 1)}). Este gráfico sirve para ubicar la brecha principal sin mezclarla con el tamaño del plantel.`, statusForScore(values[minIndex]));
    }

    function renderAttendanceChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const completion = rows.map(p => p.domains.attendance.completion_percent || 0);
      const absence = rows.map(p => p.domains.attendance.absence_rate_percent || 0);
      renderChart("attendanceChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "% listas completas", data: completion, backgroundColor: completion.map(scoreColor), borderRadius: 10 },
          { label: "% ausentismo", data: absence, backgroundColor: absence.map(v => v >= 12 ? RED : v >= 7 ? YELLOW : GREEN), borderRadius: 10 }
        ] },
        options: commonOptions({ scales: { y: { min: 0, max: 100, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, x: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const low = rows.reduce((a, b) => Number(a.domains.attendance.completion_percent || 0) < Number(b.domains.attendance.completion_percent || 0) ? a : b, rows[0]);
      setNote("attendanceNote", `${low.plantel} tiene el menor registro de listas (${pct(low.domains.attendance.completion_percent)}). Ausentismo registrado: ${pct(low.domains.attendance.absence_rate_percent)}.`, statusForScore(low.domains.attendance.completion_percent));
    }

    function renderTardyChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const rates = rows.map(p => p.domains.husky.student_tardy_rate_percent || 0);
      const avgDaily = rows.map(p => p.domains.husky.avg_tardies_per_business_day || 0);
      renderChart("tardyChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "% alumnos/día tarde", data: rates, backgroundColor: rates.map(v => v >= 4 ? RED : v >= 1.5 ? YELLOW : GREEN), borderRadius: 10 },
          { label: "Promedio diario", data: avgDaily, backgroundColor: GRAY, borderRadius: 10, yAxisID: "y1" }
        ] },
        options: commonOptions({ scales: { y: { beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { color: "#334155", font: { weight: "bold" } } }, x: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const high = rows.reduce((a, b) => Number(a.domains.husky.student_tardy_rate_percent || 0) > Number(b.domains.husky.student_tardy_rate_percent || 0) ? a : b, rows[0]);
      const t = state.data.aggregate.totals;
      const status = Number(high.domains.husky.student_tardy_rate_percent || 0) >= 4 ? "critical" : Number(high.domains.husky.student_tardy_rate_percent || 0) >= 1.5 ? "warning" : "fulfilled";
      setNote("tardyNote", `${high.plantel} tiene la mayor tasa de retardo (${pct(high.domains.husky.student_tardy_rate_percent)}). Total del periodo: ${fmt(t.student_tardies)} retardos, ${fmt(t.unique_tardy_students)} alumnos distintos y ${fmt(t.repeat_tardy_students)} reincidencias.`, status);
    }

    function renderAccessChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const rates = rows.map(p => p.domains.husky.scan_rate_percent || 0);
      const gaps = rows.map(p => p.domains.husky.scan_gap || 0);
      renderChart("accessChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "% entradas con scan", data: rates, backgroundColor: rates.map(scanColor), borderRadius: 10 },
          { label: "Brecha estimada", data: gaps, backgroundColor: gaps.map(v => v > 0 ? YELLOW : GREEN), borderRadius: 10, yAxisID: "y1" }
        ] },
        options: commonOptions({ scales: { y: { min: 0, max: 100, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { color: "#334155", font: { weight: "bold" } } }, x: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const low = rows.reduce((a, b) => Number(a.domains.husky.scan_rate_percent || 0) < Number(b.domains.husky.scan_rate_percent || 0) ? a : b, rows[0]);
      setNote("accessNote", `${low.plantel} tiene el menor uso de entrada con scan (${pct(low.domains.husky.scan_rate_percent)}). Brecha estimada del periodo: ${fmt(state.data.aggregate.totals.security_scan_gap)} entradas.`, statusForScan(low.domains.husky.scan_rate_percent));
    }

    function renderPeopleChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const absences = rows.map(p => p.domains.employee.employee_absences || 0);
      const tardies = rows.map(p => p.domains.employee.employee_tardies || 0);
      renderChart("peopleChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "Faltas", data: absences, backgroundColor: absences.map(v => v >= 12 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10, stack: "incidents" },
          { label: "Retardos", data: tardies, backgroundColor: tardies.map(v => v >= 40 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10, stack: "incidents" }
        ] },
        options: commonOptions({ scales: { x: { stacked: true, grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } }, y: { stacked: true, beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } } } })
      });
      const high = rows.reduce((a, b) => Number(a.domains.employee.employee_incidents || 0) > Number(b.domains.employee.employee_incidents || 0) ? a : b, rows[0]);
      const t = state.data.aggregate.totals;
      const status = t.employee_absences >= 12 ? "critical" : t.employee_incidents > 0 ? "warning" : "fulfilled";
      setNote("peopleNote", `${high.plantel} concentra más incidencias de personal (${fmt(high.domains.employee.employee_incidents)}). Total: ${fmt(t.employee_absences)} faltas y ${fmt(t.employee_tardies)} retardos.`, status);
    }

    function renderAcademicChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const pending = rows.map(p => p.domains.academic.planeaciones_pendientes || 0);
      const noObs = rows.map(p => p.domains.academic.docentes_sin_observacion_30_dias || 0);
      renderChart("academicChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "Planeaciones pendientes", data: pending, backgroundColor: pending.map(v => v >= 18 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10 },
          { label: "Docentes sin observación", data: noObs, backgroundColor: noObs.map(v => v >= 12 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10 }
        ] },
        options: commonOptions({ indexAxis: "y", scales: { x: { beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const high = rows.reduce((a, b) => Number(a.domains.academic.supervision_backlog || 0) > Number(b.domains.academic.supervision_backlog || 0) ? a : b, rows[0]);
      const total = state.data.aggregate.totals.academic_backlog;
      setNote("academicNote", `${high.plantel} tiene más pendientes académicos (${fmt(high.domains.academic.supervision_backlog)}). Total del periodo: ${fmt(total)} pendientes.`, total >= 35 ? "critical" : total > 0 ? "warning" : "fulfilled");
    }

    function renderSapfChart() {
      const rows = plantelRows();
      const labels = rows.map(p => p.plantel);
      const tickets = rows.map(p => p.domains.sapf.tickets_created || 0);
      const followups = rows.map(p => p.domains.sapf.followups || 0);
      const open = rows.map(p => p.domains.sapf.open_cases || 0);
      renderChart("sapfChart", {
        type: "bar",
        data: { labels, datasets: [
          { label: "Fichas", data: tickets, backgroundColor: INK, borderRadius: 10 },
          { label: "Seguimientos", data: followups, backgroundColor: GREEN, borderRadius: 10 },
          { label: "Abiertos", data: open, backgroundColor: open.map(v => v >= 8 ? RED : v > 0 ? YELLOW : GREEN), borderRadius: 10 }
        ] },
        options: commonOptions({ indexAxis: "y", scales: { x: { beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } })
      });
      const t = state.data.aggregate.totals;
      const zero = rows.filter(p => Number(p.domains.sapf.parent_interactions || 0) === 0).map(p => p.plantel);
      const status = t.sapf_open_cases >= 20 ? "critical" : zero.length || t.sapf_open_cases > 0 ? "warning" : "fulfilled";
      setNote("sapfNote", `${fmt(t.sapf_tickets_created)} fichas y ${fmt(t.sapf_followups)} seguimientos en el periodo. Casos abiertos: ${fmt(t.sapf_open_cases)}.${zero.length ? ` Sin registros: ${zero.join(", ")}.` : ""}`, status);
    }

    initFilters();
    loadDashboard();
  </script>
</body>
</html>
"""
