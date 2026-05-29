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
      --bg: #f6f7f9;
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
      --radius: 16px;
      --shadow: 0 10px 24px rgba(15,23,42,.05);
      --sans: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--text); font-family: var(--sans); line-height: 1.35; }
    button, input, select { font: inherit; }
    .hidden { display: none !important; }
    .topbar { position: sticky; top: 0; z-index: 50; background: rgba(255,255,255,.97); border-bottom: 1px solid var(--line); backdrop-filter: blur(12px); }
    .topbar-inner { max-width: 1680px; margin: 0 auto; padding: 12px 22px; display: grid; grid-template-columns: minmax(220px, 1fr) auto; gap: 16px; align-items: center; }
    .brand-kicker { font-size: 11px; font-weight: 800; letter-spacing: .12em; text-transform: uppercase; color: var(--muted); }
    .brand-title { margin-top: 2px; font-size: 18px; font-weight: 850; letter-spacing: -.02em; }
    .filters { display: flex; flex-wrap: wrap; gap: 10px; justify-content: flex-end; align-items: center; }
    .filter-group { display: flex; align-items: center; gap: 6px; padding: 5px; border: 1px solid var(--line); border-radius: 999px; background: var(--soft); }
    .scope-btn, .plantel-pill, .refresh-btn { border: 1px solid transparent; background: transparent; color: #334155; padding: 8px 11px; border-radius: 999px; font-size: 12px; font-weight: 800; cursor: pointer; white-space: nowrap; }
    .scope-btn.active, .plantel-pill.active { color: #fff; background: var(--ink); border-color: var(--ink); }
    .refresh-btn { color: #fff; background: var(--text); border-color: var(--text); padding-inline: 16px; }
    .date-input { border: 1px solid var(--line); border-radius: 999px; padding: 8px 11px; font-size: 12px; font-weight: 800; background: #fff; color: var(--text); }
    .page { max-width: 1680px; margin: 0 auto; padding: 22px 22px 64px; }
    .loading, .error-box, .card, .section { background: var(--panel); border: 1px solid var(--line); border-radius: var(--radius); box-shadow: var(--shadow); }
    .loading, .error-box { padding: 22px; font-weight: 760; color: var(--muted); }
    .error-box { color: #991b1b; background: #fff7f7; border-color: rgba(239,68,68,.35); }
    .hero { display: grid; grid-template-columns: minmax(0, 1.15fr) minmax(420px, .85fr); gap: 18px; margin-bottom: 18px; }
    .card { padding: 22px; }
    h1 { margin: 0; font-size: clamp(30px, 4vw, 48px); letter-spacing: -.05em; line-height: 1; }
    .lead { margin: 12px 0 0; max-width: 960px; color: #334155; font-size: 15px; font-weight: 560; }
    .stamp-row { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 17px; }
    .stamp { display: inline-flex; align-items: center; gap: 7px; border: 1px solid var(--line); background: #fff; border-radius: 999px; padding: 7px 10px; font-size: 12px; font-weight: 780; color: #334155; }
    .quick { display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 12px; }
    .quick-card { padding: 16px; border-left: 5px solid var(--line); min-height: 122px; }
    .quick-card.fulfilled { border-left-color: var(--green); }
    .quick-card.warning { border-left-color: var(--yellow); }
    .quick-card.critical { border-left-color: var(--red); }
    .quick-card.unavailable { border-left-color: var(--gray); }
    .quick-label, .kpi-label, .section-kicker { color: var(--muted); font-size: 11px; font-weight: 850; letter-spacing: .10em; text-transform: uppercase; }
    .quick-value { margin-top: 8px; font-size: 20px; font-weight: 880; letter-spacing: -.03em; }
    .quick-detail { margin-top: 7px; color: #475569; font-size: 13px; font-weight: 560; }
    .kpis { display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 12px; margin: 18px 0; }
    .source-health { margin: 0 0 18px; padding: 14px; background: #fff; border: 1px solid var(--line); border-radius: var(--radius); box-shadow: var(--shadow); display: grid; grid-template-columns: repeat(6, minmax(0,1fr)); gap: 10px; }
    .source-chip { border: 1px solid var(--line); border-radius: 14px; padding: 12px; background: #f8fafc; min-height: 78px; }
    .source-chip.ok { border-left: 5px solid var(--green); }
    .source-chip.partial { border-left: 5px solid var(--yellow); }
    .source-chip.bad { border-left: 5px solid var(--red); }
    .source-chip.none { border-left: 5px solid var(--gray); }
    .source-label { font-size: 11px; font-weight: 850; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); }
    .source-value { margin-top: 6px; font-size: 20px; font-weight: 900; letter-spacing: -.035em; }
    .source-detail { margin-top: 4px; font-size: 11px; color: #64748b; font-weight: 680; }
    .kpi { min-height: 118px; padding: 17px; }
    .kpi-value { margin-top: 10px; font-size: 28px; font-weight: 900; letter-spacing: -.045em; }
    .kpi-note { margin-top: 5px; font-size: 12px; color: var(--muted); font-weight: 650; }
    .layout { display: grid; grid-template-columns: repeat(2, minmax(560px,1fr)); gap: 18px; align-items: start; }
    .section { overflow: hidden; }
    .section.wide { grid-column: 1 / -1; }
    .section-head { padding: 16px 18px; border-bottom: 1px solid var(--line); display: flex; justify-content: space-between; gap: 14px; align-items: flex-start; }
    .section-title { margin-top: 3px; font-size: 20px; font-weight: 860; letter-spacing: -.025em; }
    .section-sub { margin-top: 3px; font-size: 12px; color: var(--muted); font-weight: 620; }
    .section-body { padding: 18px; display: grid; grid-template-columns: minmax(0,2.25fr) minmax(340px,.75fr); gap: 18px; }
    .layout > .section:not(.wide) .section-body { grid-template-columns: 1fr; }
    .chart-box { height: 460px; min-height: 420px; border: 1px solid var(--line); border-radius: 14px; background: #fff; padding: 16px; position: relative; }
    .chart-box.tall { height: 560px; }
    .chart-box.empty canvas { display: none; }
    .empty-state { position: absolute; inset: 16px; display: flex; align-items: center; justify-content: center; text-align: center; border: 1px dashed #cbd5e1; border-radius: 12px; color: #64748b; font-weight: 800; background: #f8fafc; padding: 24px; }
    .note-box { border: 1px solid var(--line); border-radius: 14px; background: #f8fafc; padding: 16px; min-height: 132px; border-left: 5px solid var(--line); }
    .note-box.fulfilled { border-left-color: var(--green); }
    .note-box.warning { border-left-color: var(--yellow); }
    .note-box.critical { border-left-color: var(--red); }
    .note-box.unavailable { border-left-color: var(--gray); }
    .note-title { font-size: 11px; font-weight: 850; letter-spacing: .10em; text-transform: uppercase; color: #334155; }
    .note-line { margin-top: 10px; font-size: 14px; font-weight: 610; color: #334155; }
    .selector { border: 1px solid var(--line); border-radius: 999px; padding: 8px 12px; font-size: 12px; font-weight: 800; background: #fff; color: #334155; }
    .matrix { display: grid; grid-template-columns: 128px repeat(7, minmax(170px, 1fr)); gap: 1px; background: var(--line); border: 1px solid var(--line); border-radius: 14px; overflow-x: auto; }
    .mcell { background: #fff; min-height: 76px; padding: 12px; display: flex; flex-direction: column; justify-content: center; }
    .mhead { background: #f8fafc; color: #475569; font-size: 11px; font-weight: 850; text-transform: uppercase; letter-spacing: .08em; }
    .mplantel { font-size: 15px; font-weight: 900; }
    .mvalue { font-size: 18px; font-weight: 890; letter-spacing: -.035em; }
    .msub { font-size: 11px; color: #64748b; margin-top: 2px; font-weight: 650; }
    .tone-fulfilled { box-shadow: inset 5px 0 0 var(--green); }
    .tone-warning { box-shadow: inset 5px 0 0 var(--yellow); }
    .tone-critical { box-shadow: inset 5px 0 0 var(--red); }
    .tone-unavailable { box-shadow: inset 5px 0 0 var(--gray); color: #64748b; }
    @media (max-width: 1240px) { .topbar-inner, .hero, .section-body { grid-template-columns: 1fr; } .filters { justify-content: flex-start; } .quick { grid-template-columns: repeat(2,minmax(0,1fr)); } .kpis { grid-template-columns: repeat(4,minmax(0,1fr)); } .source-health { grid-template-columns: repeat(3,minmax(0,1fr)); } .layout { grid-template-columns: 1fr; } .section.wide { grid-column: auto; } .matrix { grid-template-columns: 128px repeat(7, minmax(170px,1fr)); overflow-x: auto; } }
    @media (max-width: 760px) { .page { padding: 16px 12px 42px; } .topbar-inner { padding: 12px; } .quick, .kpis, .source-health { grid-template-columns: repeat(2,minmax(0,1fr)); } .section-head { display:block; } .chart-box { height: 360px; min-height: 340px; } }
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
          <p class="lead">Lectura por plantel con datos agregados. Compara listas, asistencia, retardos, accesos, personal, revisión académica y SAPF.</p>
          <div class="stamp-row">
            <span id="scopeStamp" class="stamp">Mes en curso</span>
            <span id="dateStamp" class="stamp">Periodo</span>
            <span id="generatedStamp" class="stamp">Actualizado</span>
          </div>
        </div>
        <div id="quickRead" class="quick"></div>
      </section>

      <section class="kpis">
        <div class="card kpi"><div class="kpi-label">Listas</div><div id="kpiLists" class="kpi-value">--</div><div class="kpi-note">Completas</div></div>
        <div class="card kpi"><div class="kpi-label">Brecha listas</div><div id="kpiMissingLists" class="kpi-value">--</div><div class="kpi-note">Faltantes</div></div>
        <div class="card kpi"><div class="kpi-label">Asistencia</div><div id="kpiAttendance" class="kpi-value">--</div><div class="kpi-note">Registros capturados</div></div>
        <div class="card kpi"><div class="kpi-label">Ausencias</div><div id="kpiAbsence" class="kpi-value">--</div><div class="kpi-note">Por 100 registros</div></div>
        <div class="card kpi"><div class="kpi-label">Retardos</div><div id="kpiTardies" class="kpi-value">--</div><div class="kpi-note">Por 100 entradas</div></div>
        <div class="card kpi"><div class="kpi-label">Accesos</div><div id="kpiAccess" class="kpi-value">--</div><div class="kpi-note">Cobertura</div></div>
        <div class="card kpi"><div class="kpi-label">Personal</div><div id="kpiPeople" class="kpi-value">--</div><div class="kpi-note">Incidencias</div></div>
        <div class="card kpi"><div class="kpi-label">SAPF</div><div id="kpiSapf" class="kpi-value">--</div><div class="kpi-note">Fichas</div></div>
      </section>

      <section id="sourceHealth" class="source-health" aria-label="Lectura de fuentes"></section>

      <div class="layout">
        <section class="section wide">
          <div class="section-head">
            <div>
              <div class="section-kicker">Resumen</div>
              <div class="section-title">Mapa por plantel</div>
              <div class="section-sub">Una fila por plantel. Valores agregados del periodo seleccionado.</div>
            </div>
          </div>
          <div class="section-body" style="display:block">
            <div id="matrix" class="matrix"></div>
          </div>
        </section>

        <section class="section wide">
          <div class="section-head">
            <div>
              <div class="section-kicker">Evolución</div>
              <div class="section-title">Comparativo en el tiempo</div>
              <div class="section-sub">Mismo indicador para todos los planteles.</div>
            </div>
            <select id="trendMetric" class="selector">
              <option value="lists_completion">% listas completas</option>
              <option value="attendance_rate">% asistencia registrada</option>
              <option value="tardies_per_100">Retardos por 100 entradas</option>
              <option value="access_coverage">% cobertura de accesos</option>
            </select>
          </div>
          <div class="section-body">
            <div class="chart-box tall"><canvas id="trendChart"></canvas></div>
            <div id="trendNote" class="note-box"></div>
          </div>
        </section>

        <section class="section">
          <div class="section-head"><div><div class="section-kicker">Pase de lista</div><div class="section-title">Esperado vs capturado</div><div class="section-sub">Listas esperadas y listas faltantes.</div></div></div>
          <div class="section-body"><div class="chart-box"><canvas id="listsChart"></canvas></div><div id="listsNote" class="note-box"></div></div>
        </section>

        <section class="section">
          <div class="section-head"><div><div class="section-kicker">Asistencia</div><div class="section-title">Ausencias registradas</div><div class="section-sub">Ausencias por cada 100 registros capturados.</div></div></div>
          <div class="section-body"><div class="chart-box"><canvas id="attendanceChart"></canvas></div><div id="attendanceNote" class="note-box"></div></div>
        </section>

        <section class="section">
          <div class="section-head"><div><div class="section-kicker">Puntualidad</div><div class="section-title">Retardos de alumnos</div><div class="section-sub">Retardos sobre primeras entradas registradas.</div></div></div>
          <div class="section-body"><div class="chart-box"><canvas id="tardyChart"></canvas></div><div id="tardyNote" class="note-box"></div></div>
        </section>

        <section class="section">
          <div class="section-head"><div><div class="section-kicker">Accesos</div><div class="section-title">Cobertura de entrada</div><div class="section-sub">Entradas con scan contra entradas esperadas.</div></div></div>
          <div class="section-body"><div class="chart-box"><canvas id="accessChart"></canvas></div><div id="accessNote" class="note-box"></div></div>
        </section>

        <section class="section">
          <div class="section-head"><div><div class="section-kicker">Personal</div><div class="section-title">Incidencias laborales</div><div class="section-sub">Faltas y retardos del periodo.</div></div></div>
          <div class="section-body"><div class="chart-box"><canvas id="peopleChart"></canvas></div><div id="peopleNote" class="note-box"></div></div>
        </section>

        <section class="section">
          <div class="section-head"><div><div class="section-kicker">Académico</div><div class="section-title">Revisión y observación</div><div class="section-sub">Pendientes de revisión y observación docente.</div></div></div>
          <div class="section-body"><div class="chart-box"><canvas id="academicChart"></canvas></div><div id="academicNote" class="note-box"></div></div>
        </section>

        <section class="section wide">
          <div class="section-head"><div><div class="section-kicker">SAPF</div><div class="section-title">Atención documentada</div><div class="section-sub">Fichas, seguimientos y casos abiertos.</div></div></div>
          <div class="section-body"><div class="chart-box"><canvas id="sapfChart"></canvas></div><div id="sapfNote" class="note-box"></div></div>
        </section>
      </div>
    </div>
  </main>

  <script>
    const PLANTEL_ORDER = ["PT", "PM", "ST", "SM", "PREET", "PREEM"];
    const GREEN = "#22c55e", YELLOW = "#f59e0b", RED = "#ef4444", GRAY = "#94a3b8", INK = "#1f2937";
    const LINE_COLORS = ["#1f2937", "#64748b", "#0f766e", "#7c3aed", "#b45309", "#be123c"];
    const charts = {};
    const state = { scope: "month", selected: new Set(PLANTEL_ORDER), data: null };

    const $ = id => document.getElementById(id);
    const isMissing = value => value === null || value === undefined || value === "" || Number.isNaN(Number(value));
    const fmt = (value, digits = 0) => isMissing(value) ? "s/d" : Number(value).toLocaleString("es-MX", { maximumFractionDigits: digits, minimumFractionDigits: digits });
    const pct = (value, digits = 1) => isMissing(value) ? "s/d" : `${fmt(value, digits)}%`;
    const chartValue = value => isMissing(value) ? null : Number(value);
    const hasAnyNumber = values => (values || []).some(v => !isMissing(v));
    const allNumbersZero = values => { const nums = (values || []).filter(v => !isMissing(v)).map(Number); return nums.length > 0 && nums.every(v => Math.abs(v) < 0.000001); };
    const statusColor = status => status === "critical" ? RED : status === "warning" ? YELLOW : status === "fulfilled" ? GREEN : GRAY;
    const lowGoodColor = (value, warning, critical) => isMissing(value) ? GRAY : Number(value) >= critical ? RED : Number(value) >= warning ? YELLOW : GREEN;
    const highGoodColor = (value, warning, critical) => isMissing(value) ? GRAY : Number(value) <= critical ? RED : Number(value) <= warning ? YELLOW : GREEN;
    const labels = () => matrixRows().map(r => r.plantel);
    const matrixRows = () => (((state.data || {}).operational || {}).matrix || []).filter(r => state.selected.has(r.plantel));
    const network = () => (((state.data || {}).operational || {}).network || {});
    const trend = () => (((state.data || {}).operational || {}).trend || { labels: [], metrics: {} });

    function initFilters() {
      const holder = $("plantelFilters");
      holder.innerHTML = PLANTEL_ORDER.map(code => `<button class="plantel-pill active" data-plantel="${code}">${code}</button>`).join("");
      holder.querySelectorAll(".plantel-pill").forEach(btn => {
        btn.addEventListener("click", () => {
          const code = btn.dataset.plantel;
          if (state.selected.has(code) && state.selected.size > 1) state.selected.delete(code); else state.selected.add(code);
          btn.classList.toggle("active", state.selected.has(code));
          loadDashboard();
        });
      });
      document.querySelectorAll(".scope-btn").forEach(btn => {
        btn.addEventListener("click", () => {
          state.scope = btn.dataset.scope;
          document.querySelectorAll(".scope-btn").forEach(b => b.classList.toggle("active", b === btn));
          const custom = state.scope === "range";
          $("startDate").classList.toggle("hidden", !custom);
          $("endDate").classList.toggle("hidden", !custom);
          if (custom && !$("startDate").value) {
            const now = new Date();
            $("startDate").value = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().slice(0, 10);
            $("endDate").value = now.toISOString().slice(0, 10);
          }
          loadDashboard();
        });
      });
      $("refreshBtn").addEventListener("click", () => loadDashboard(true));
      $("startDate").addEventListener("change", () => loadDashboard(true));
      $("endDate").addEventListener("change", () => loadDashboard(true));
      $("trendMetric").addEventListener("change", renderTrendChart);
    }

    async function loadDashboard(force = false) {
      $("loadState").classList.remove("hidden");
      $("errorState").classList.add("hidden");
      $("dashboard").classList.add("hidden");
      const params = new URLSearchParams();
      params.set("scope", state.scope || "month");
      params.set("planteles", Array.from(state.selected).join(","));
      params.set("include_baselines", "false");
      if (force) params.set("force_refresh", "true");
      if (state.scope === "range") {
        if ($("startDate").value) params.set("start_date", $("startDate").value);
        if ($("endDate").value) params.set("end_date", $("endDate").value);
      }
      try {
        const response = await fetch(`/api/v1/corporate-compliance-risk-index?${params.toString()}`, { headers: { "Accept": "application/json" } });
        const raw = await response.text();
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${raw.slice(0, 280)}`);
        state.data = JSON.parse(raw);
        renderDashboard();
        $("dashboard").classList.remove("hidden");
      } catch (error) {
        $("errorState").textContent = `No se pudo cargar el tablero: ${String(error.message || error).slice(0, 360)}`;
        $("errorState").classList.remove("hidden");
      } finally {
        $("loadState").classList.add("hidden");
      }
    }

    function renderDashboard() {
      renderStamps();
      renderQuickRead();
      renderKpis();
      renderSourceHealth();
      renderMatrix();
      renderTrendChart();
      renderListsChart();
      renderAttendanceChart();
      renderTardyChart();
      renderAccessChart();
      renderPeopleChart();
      renderAcademicChart();
      renderSapfChart();
    }

    function renderStamps() {
      const agg = state.data.aggregate || {};
      const win = agg.window || {};
      const labels = { month: "Mes en curso", ciclo_escolar: "Ciclo", range: "Rango", today: "Hoy" };
      $("scopeStamp").textContent = labels[state.data.scope] || labels[state.scope] || "Mes en curso";
      $("dateStamp").textContent = `${win.start || "—"} → ${win.end || "—"} · ${win.business_days || 0} días hábiles`;
      const generated = state.data.generated_at ? new Date(state.data.generated_at) : null;
      $("generatedStamp").textContent = generated ? `Actualizado ${generated.toLocaleString("es-MX")}` : "Actualizado";
    }

    function renderQuickRead() {
      const rows = ((state.data.operational || {}).quick_read || []).slice(0, 4);
      $("quickRead").innerHTML = rows.map(item => `
        <div class="card quick-card ${item.status || ""}">
          <div class="quick-label">${escapeHtml(item.label)}</div>
          <div class="quick-value">${escapeHtml(item.value)}</div>
          <div class="quick-detail">${escapeHtml(item.detail)}</div>
        </div>`).join("");
    }

    function renderKpis() {
      const n = network();
      $("kpiLists").textContent = pct(n.attendance_lists_completion_pct);
      $("kpiMissingLists").textContent = fmt(n.attendance_lists_missing);
      $("kpiAttendance").textContent = pct(n.student_attendance_pct);
      $("kpiAbsence").textContent = fmt(n.absences_per_100, 1);
      $("kpiTardies").textContent = fmt(n.tardies_per_100_entries, 1);
      $("kpiAccess").textContent = pct(n.access_coverage_pct);
      $("kpiPeople").textContent = fmt(n.employee_incidents);
      $("kpiSapf").textContent = fmt(n.sapf_tickets);
    }

    function renderSourceHealth() {
      const holder = $("sourceHealth");
      const audit = (state.data.source_audit || ((state.data.aggregate || {}).source_audit || {}));
      const validity = audit.validity || {};
      const config = [
        ["attendance", "Listas"],
        ["husky", "Accesos"],
        ["retardos", "Retardos"],
        ["employee", "Personal"],
        ["academic", "Académico"],
        ["sapf", "SAPF"],
      ];
      holder.innerHTML = config.map(([key, label]) => {
        const item = validity[key] || {};
        const readable = Number(item.readable_planteles || 0);
        const total = Number(item.selected_planteles || matrixRows().length || state.selected.size || 0);
        const cls = readable === total && total > 0 ? "ok" : readable > 0 ? "partial" : "none";
        const domainAudit = audit[key] || {};
        const timeouts = Number(domainAudit.timeout || 0);
        const errors = Number(domainAudit.source_error || 0);
        const noRecords = Number(domainAudit.no_records || 0);
        const detail = timeouts || errors ? `${timeouts} timeout · ${errors} error` : noRecords ? `${noRecords} sin registros` : "lectura disponible";
        return `<div class="source-chip ${cls}"><div class="source-label">${label}</div><div class="source-value">${readable}/${total}</div><div class="source-detail">${escapeHtml(detail)}</div></div>`;
      }).join("");
    }

    function renderMatrix() {
      const rows = matrixRows();
      const cols = [
        ["Plantel", ""], ["Listas", "%"], ["Asistencia", "%"], ["Retardos", "/100"], ["Accesos", "%"], ["Personal", "inc."], ["Académico", "pend."], ["SAPF", "abiertos"]
      ];
      const header = cols.map(([h,s]) => `<div class="mcell mhead"><div>${h}</div><span class="msub">${s}</span></div>`).join("");
      const body = rows.map(r => {
        const cells = [
          `<div class="mcell"><div class="mplantel">${r.plantel}</div><span class="msub">${escapeHtml(r.resolved_name || "")}</span></div>`,
          matrixCell(r.attendance_lists.completion_pct, "%", r.attendance_lists.status, isMissing(r.attendance_lists.completion_pct) ? "sin lectura" : `${fmt(r.attendance_lists.missing)} faltantes`),
          matrixCell(r.student_attendance.attendance_pct, "%", r.student_attendance.status, isMissing(r.student_attendance.attendance_pct) ? "sin lectura" : `${fmt(r.student_attendance.absences_per_100,1)} aus./100`),
          matrixCell(r.student_tardies.tardies_per_100_entries, "", r.student_tardies.status, isMissing(r.student_tardies.tardies_per_100_entries) ? "sin entradas" : `${fmt(r.student_tardies.tardies)} retardos`),
          matrixCell(r.access.coverage_pct, "%", r.access.status, isMissing(r.access.coverage_pct) ? "sin lectura" : `${fmt(r.access.gap)} brecha`),
          matrixCell(r.employee_attendance.incidents, "", r.employee_attendance.status, isMissing(r.employee_attendance.incidents) ? "sin lectura" : `${fmt(r.employee_attendance.absences)} faltas`),
          matrixCell(r.academic.backlog, "", r.academic.status, isMissing(r.academic.backlog) ? "sin lectura" : `${fmt(r.academic.pending_lesson_reviews)} planeaciones`),
          matrixCell(r.sapf.open_cases, "", r.sapf.status, isMissing(r.sapf.open_cases) ? "sin lectura" : `${fmt(r.sapf.tickets)} fichas`),
        ];
        return cells.join("");
      }).join("");
      $("matrix").innerHTML = header + body;
    }

    function matrixCell(value, suffix, status, sub) {
      const missing = isMissing(value);
      const digits = !missing && Number(value) % 1 ? 1 : 0;
      const display = missing ? "s/d" : `${fmt(value, digits)}${suffix}`;
      return `<div class="mcell tone-${status || "unavailable"}"><div class="mvalue">${display}</div><span class="msub">${escapeHtml(sub || (missing ? "sin lectura" : ""))}</span></div>`;
    }

    function chartBase(extra = {}) {
      return Object.assign({
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { labels: { color: "#334155", font: { weight: "bold" } } }, tooltip: { mode: "index", intersect: false } },
        scales: { x: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } }, y: { beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } } }
      }, extra);
    }

    function clearChartBox(id) {
      const canvas = $(id);
      const box = canvas.parentElement;
      box.classList.remove("empty");
      canvas.style.display = "block";
      box.querySelectorAll(".empty-state").forEach(el => el.remove());
    }

    function renderChart(id, config) {
      if (charts[id]) charts[id].destroy();
      clearChartBox(id);
      charts[id] = new Chart($(id), config);
    }

    function renderEmptyChart(id, message) {
      if (charts[id]) charts[id].destroy();
      const canvas = $(id);
      const box = canvas.parentElement;
      box.classList.add("empty");
      canvas.style.display = "none";
      box.querySelectorAll(".empty-state").forEach(el => el.remove());
      const empty = document.createElement("div");
      empty.className = "empty-state";
      empty.textContent = message;
      box.appendChild(empty);
    }

    function setNote(id, title, lines, status = "fulfilled") {
      const box = $(id);
      box.className = `note-box ${status || "fulfilled"}`;
      box.innerHTML = `<div class="note-title">${escapeHtml(title)}</div>${[].concat(lines || []).map(line => `<div class="note-line">${escapeHtml(line)}</div>`).join("")}`;
    }

    function renderTrendChart() {
      const tr = trend();
      const metricKey = $("trendMetric").value;
      const metric = (tr.metrics || {})[metricKey] || { label: "", series: [] };
      const series = (metric.series || []).filter(s => state.selected.has(s.plantel));
      const values = series.flatMap(s => (s.values || []).filter(v => !isMissing(v)).map(Number));
      const highBad = metricKey === "tardies_per_100";
      if (!values.length) {
        renderEmptyChart("trendChart", "Sin denominador suficiente para este indicador.");
        return setNote("trendNote", "Lectura", "Sin datos suficientes para comparar este indicador en el periodo.", "unavailable");
      }
      renderChart("trendChart", {
        type: "line",
        data: { labels: tr.labels || [], datasets: series.map((s, i) => ({
          label: s.plantel,
          data: (s.values || []).map(chartValue),
          borderColor: LINE_COLORS[i % LINE_COLORS.length],
          backgroundColor: LINE_COLORS[i % LINE_COLORS.length],
          borderWidth: 3,
          tension: .22,
          pointRadius: 4,
          pointHoverRadius: 6,
          spanGaps: false
        })) },
        options: chartBase({ scales: { x: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } }, y: { beginAtZero: true, suggestedMax: values.length ? Math.max(...values) * 1.15 : 10, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } } } })
      });
      const all = series.flatMap(s => (s.values || []).map((v, idx) => isMissing(v) ? null : ({ plantel: s.plantel, value: Number(v), label: (tr.labels || [])[idx] })).filter(Boolean));
      if (!all.length) return setNote("trendNote", "Lectura", "Sin datos suficientes para comparar este indicador en el periodo.", "unavailable");
      const main = highBad ? all.reduce((a,b)=>a.value>b.value?a:b) : all.reduce((a,b)=>a.value<b.value?a:b);
      setNote("trendNote", "Lectura", `${main.plantel} marca ${fmt(main.value,1)}${metric.unit || ""} en ${main.label || "el periodo"}.`, highBad ? lowGoodColor(main.value, 4, 9) === RED ? "critical" : lowGoodColor(main.value,4,9) === YELLOW ? "warning" : "fulfilled" : highGoodColor(main.value, 75, 55) === RED ? "critical" : highGoodColor(main.value,75,55) === YELLOW ? "warning" : "fulfilled");
    }

    function renderListsChart() {
      const rows = matrixRows();
      const valid = rows.filter(r => !isMissing(r.attendance_lists.completion_pct));
      if (!valid.length) {
        renderEmptyChart("listsChart", "Sin lectura confiable de listas esperadas/capturadas.");
        return setNote("listsNote", "Lectura", "Sin denominador suficiente de pase de lista para el periodo.", "unavailable");
      }
      renderChart("listsChart", { type: "bar", data: { labels: labels(), datasets: [
        { label: "Capturadas", data: rows.map(r=>isMissing(r.attendance_lists.completion_pct) ? null : r.attendance_lists.captured), backgroundColor: GREEN, borderRadius: 8, stack: "lists" },
        { label: "Faltantes", data: rows.map(r=>isMissing(r.attendance_lists.completion_pct) ? null : r.attendance_lists.missing), backgroundColor: rows.map(r=>isMissing(r.attendance_lists.completion_pct) ? GRAY : r.attendance_lists.missing > 0 ? YELLOW : GREEN), borderRadius: 8, stack: "lists" }
      ]}, options: chartBase({ indexAxis: "y", scales: { x: { stacked: true, beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { stacked: true, grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } }) });
      const worst = valid.reduce((a,b)=>a.attendance_lists.completion_pct < b.attendance_lists.completion_pct ? a : b, valid[0]);
      setNote("listsNote", "Lectura", [`${worst.plantel}: ${fmt(worst.attendance_lists.missing)} listas faltantes.`, `${fmt(worst.attendance_lists.captured)} de ${fmt(worst.attendance_lists.expected)} listas capturadas (${pct(worst.attendance_lists.completion_pct)}).`], worst.attendance_lists.status);
    }

    function renderAttendanceChart() {
      const rows = matrixRows();
      const valid = rows.filter(r => !isMissing(r.student_attendance.absences_per_100));
      const values = valid.map(r => Number(r.student_attendance.absences_per_100));
      if (!valid.length) {
        renderEmptyChart("attendanceChart", "Sin registros capturados para calcular ausencias.");
        return setNote("attendanceNote", "Lectura", "Sin registros de asistencia para calcular ausencias del periodo.", "unavailable");
      }
      renderChart("attendanceChart", { type: "bar", data: { labels: labels(), datasets: [{ label: "Ausencias por 100 registros", data: rows.map(r=>chartValue(r.student_attendance.absences_per_100)), backgroundColor: rows.map(r=>lowGoodColor(r.student_attendance.absences_per_100, 8, 12)), borderRadius: 8 }] }, options: chartBase({ indexAxis: "y", scales: { x: { beginAtZero: true, suggestedMax: values.length ? Math.max(5, Math.max(...values) * 1.25) : 5, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } }) });
      const worst = valid.reduce((a,b)=>a.student_attendance.absences_per_100 > b.student_attendance.absences_per_100 ? a : b, valid[0]);
      setNote("attendanceNote", "Lectura", [`${worst.plantel}: ${fmt(worst.student_attendance.absences_per_100,1)} ausencias por cada 100 registros.`, `Asistencia registrada: ${pct(worst.student_attendance.attendance_pct)}.`], worst.student_attendance.status);
    }

    function renderTardyChart() {
      const rows = matrixRows();
      const valid = rows.filter(r => !isMissing(r.student_tardies.tardies_per_100_entries));
      const values = valid.map(r => Number(r.student_tardies.tardies_per_100_entries));
      if (!valid.length) {
        renderEmptyChart("tardyChart", "Sin entradas registradas para calcular retardos.");
        return setNote("tardyNote", "Lectura", "No se convierte la falta de denominador en 0 retardos.", "unavailable");
      }
      if (allNumbersZero(values)) {
        renderEmptyChart("tardyChart", "La consulta devolvió 0 retardos para todos los planteles. Revisar extracción de Husky antes de interpretar.");
        return setNote("tardyNote", "Lectura", "Resultado plano en 0. Se marca como lectura no confiable para evitar una conclusión falsa.", "unavailable");
      }
      renderChart("tardyChart", { type: "bar", data: { labels: labels(), datasets: [{ label: "Retardos por 100 entradas", data: rows.map(r=>chartValue(r.student_tardies.tardies_per_100_entries)), backgroundColor: rows.map(r=>lowGoodColor(r.student_tardies.tardies_per_100_entries, 4, 9)), borderRadius: 8 }] }, options: chartBase({ indexAxis: "y", scales: { x: { beginAtZero: true, suggestedMax: values.length ? Math.max(5, Math.max(...values) * 1.35) : 5, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } }) });
      const worst = valid.reduce((a,b)=>a.student_tardies.tardies_per_100_entries > b.student_tardies.tardies_per_100_entries ? a : b, valid[0]);
      setNote("tardyNote", "Lectura", [`${worst.plantel}: ${fmt(worst.student_tardies.tardies_per_100_entries,1)} retardos por 100 entradas.`, `${fmt(worst.student_tardies.tardies)} retardos; ${fmt(worst.student_tardies.unique_students)} alumnos distintos; ${pct(worst.student_tardies.repeat_share_pct)} reincidencia.`], worst.student_tardies.status);
    }

    function renderAccessChart() {
      const rows = matrixRows();
      const valid = rows.filter(r => !isMissing(r.access.coverage_pct));
      const values = valid.map(r => Number(r.access.coverage_pct));
      if (!valid.length) {
        renderEmptyChart("accessChart", "Sin denominador confiable para cobertura de entrada.");
        return setNote("accessNote", "Lectura", "Sin entradas registradas o población esperada para calcular cobertura.", "unavailable");
      }
      if (allNumbersZero(values)) {
        renderEmptyChart("accessChart", "La cobertura aparece en 0 para todos los planteles. Revisar extracción de accesos antes de interpretar.");
        return setNote("accessNote", "Lectura", "Resultado plano en 0. Se marca como lectura no confiable para evitar una conclusión falsa.", "unavailable");
      }
      renderChart("accessChart", { type: "bar", data: { labels: labels(), datasets: [{ label: "% cobertura de entrada", data: rows.map(r=>chartValue(r.access.coverage_pct)), backgroundColor: rows.map(r=>highGoodColor(r.access.coverage_pct, 75, 55)), borderRadius: 8 }] }, options: chartBase({ indexAxis: "y", scales: { x: { beginAtZero: true, max: 100, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } }) });
      const worst = valid.reduce((a,b)=>a.access.coverage_pct < b.access.coverage_pct ? a : b, valid[0]);
      setNote("accessNote", "Lectura", [`${worst.plantel}: ${pct(worst.access.coverage_pct)} cobertura de entrada.`, `${fmt(worst.access.scans)} entradas con scan; brecha estimada ${fmt(worst.access.gap)}.`], worst.access.status);
    }

    function renderPeopleChart() {
      const rows = matrixRows();
      const valid = rows.filter(r => r.employee_attendance.has_data);
      if (!valid.length) {
        renderEmptyChart("peopleChart", "Sin lectura confiable de asistencia laboral.");
        return setNote("peopleNote", "Lectura", "La fuente no devolvió empleados procesados para el periodo.", "unavailable");
      }
      renderChart("peopleChart", { type: "bar", data: { labels: labels(), datasets: [
        { label: "Faltas", data: rows.map(r=>r.employee_attendance.has_data ? r.employee_attendance.absences : null), backgroundColor: RED, borderRadius: 8, stack: "p" },
        { label: "Retardos", data: rows.map(r=>r.employee_attendance.has_data ? r.employee_attendance.tardies : null), backgroundColor: YELLOW, borderRadius: 8, stack: "p" }
      ] }, options: chartBase({ indexAxis: "y", scales: { x: { stacked: true, beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { stacked: true, grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } }) });
      const worst = valid.reduce((a,b)=>a.employee_attendance.incidents > b.employee_attendance.incidents ? a : b, valid[0]);
      setNote("peopleNote", "Lectura", [`${worst.plantel}: ${fmt(worst.employee_attendance.incidents)} incidencias.`, `${fmt(worst.employee_attendance.absences)} faltas y ${fmt(worst.employee_attendance.tardies)} retardos.`], worst.employee_attendance.status);
    }

    function renderAcademicChart() {
      const rows = matrixRows();
      const valid = rows.filter(r => r.academic.has_data);
      if (!valid.length) {
        renderEmptyChart("academicChart", "Sin lectura confiable de revisión académica.");
        return setNote("academicNote", "Lectura", "La fuente académica no devolvió denominadores suficientes para el periodo.", "unavailable");
      }
      renderChart("academicChart", { type: "bar", data: { labels: labels(), datasets: [
        { label: "Planeaciones pendientes", data: rows.map(r=>r.academic.has_data ? r.academic.pending_lesson_reviews : null), backgroundColor: YELLOW, borderRadius: 8 },
        { label: "Docentes sin observación", data: rows.map(r=>r.academic.has_data ? r.academic.teachers_without_observation : null), backgroundColor: RED, borderRadius: 8 }
      ] }, options: chartBase({ indexAxis: "y", scales: { x: { beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } }, y: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } } } }) });
      const worst = valid.reduce((a,b)=>a.academic.backlog > b.academic.backlog ? a : b, valid[0]);
      setNote("academicNote", "Lectura", [`${worst.plantel}: ${fmt(worst.academic.backlog)} pendientes académicos.`, `${fmt(worst.academic.pending_lesson_reviews)} planeaciones y ${fmt(worst.academic.teachers_without_observation)} docentes sin observación reciente.`], worst.academic.status);
    }

    function renderSapfChart() {
      const rows = matrixRows();
      const valid = rows.filter(r => r.sapf.has_data);
      if (!valid.length) {
        renderEmptyChart("sapfChart", "Sin lectura SAPF para el periodo.");
        return setNote("sapfNote", "Lectura", "No se muestran ceros cuando la fuente no devuelve registros confiables.", "unavailable");
      }
      renderChart("sapfChart", { type: "bar", data: { labels: labels(), datasets: [
        { label: "Fichas", data: rows.map(r=>r.sapf.has_data ? r.sapf.tickets : null), backgroundColor: INK, borderRadius: 8 },
        { label: "Seguimientos", data: rows.map(r=>r.sapf.has_data ? r.sapf.followups : null), backgroundColor: GREEN, borderRadius: 8 },
        { label: "Abiertos", data: rows.map(r=>r.sapf.has_data ? r.sapf.open_cases : null), backgroundColor: rows.map(r=>r.sapf.open_cases > 0 ? YELLOW : GREEN), borderRadius: 8 }
      ] }, options: chartBase({ scales: { x: { grid: { display: false }, ticks: { color: "#111827", font: { weight: "bold" } } }, y: { beginAtZero: true, grid: { color: "#e5e7eb" }, ticks: { color: "#334155", font: { weight: "bold" } } } } }) });
      const mostOpen = valid.reduce((a,b)=>a.sapf.open_cases > b.sapf.open_cases ? a : b, valid[0]);
      const noData = rows.filter(r=>!r.sapf.has_data).map(r=>r.plantel);
      const lines = [`${mostOpen.plantel}: ${fmt(mostOpen.sapf.open_cases)} casos abiertos.`, `${fmt(mostOpen.sapf.tickets)} fichas y ${fmt(mostOpen.sapf.followups)} seguimientos.`];
      if (noData.length) lines.push(`Sin lectura SAPF: ${noData.join(", ")}.`);
      setNote("sapfNote", "Lectura", lines, mostOpen.sapf.status);
    }

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, ch => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
    }

    initFilters();
    loadDashboard();
  </script>
</body>
</html>
"""
