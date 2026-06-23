CORPORATE_COMPLIANCE_HTML = r'''
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Reporte SIPAE</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@500;600;700&family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet" />
  <style>
    :root {
      --bg: #eef2f5;
      --paper: #ffffff;
      --paper-soft: #f7fafc;
      --text: #17212b;
      --muted: #67717f;
      --muted-2: #8d98a7;
      --line: #dfe6ee;
      --line-strong: #c7d2df;
      --green: #008f5a;
      --green-soft: #e5f7ee;
      --yellow: #c88400;
      --yellow-soft: #fff4d8;
      --red: #c63d35;
      --red-soft: #fde9e7;
      --blue: #2f6fe4;
      --coral: #ef6b5b;
      --gray: #667085;
      --gray-soft: #edf1f5;
      --ink: #17212b;
      --radius: 8px;
      --shadow: 0 22px 60px rgba(23, 33, 43, .10);
      --font: "Montserrat", ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      --title-font: "Fredoka", "Montserrat", ui-sans-serif, system-ui, sans-serif;
    }
    * { box-sizing: border-box; }
    html { background: var(--bg); }
    body {
      margin: 0;
      color: var(--text);
      background:
        linear-gradient(90deg, rgba(0,143,90,.10), rgba(47,111,228,.06), rgba(239,107,91,.08)) 0 0 / 100% 210px no-repeat,
        var(--bg);
      font-family: var(--font);
      font-size: 14px;
      line-height: 1.5;
    }
    button, input, select { font: inherit; }
    button { cursor: pointer; }
    h1, h2, h3 { font-family: var(--title-font); letter-spacing: 0; }
    .topbar {
      position: sticky;
      top: 0;
      z-index: 10;
      border-bottom: 1px solid rgba(199,210,223,.72);
      background: rgba(255,255,255,.94);
      backdrop-filter: blur(18px);
    }
    .topbar-inner {
      max-width: 1440px;
      margin: 0 auto;
      padding: 13px 24px;
      display: grid;
      grid-template-columns: minmax(260px, 1fr) auto;
      gap: 18px;
      align-items: center;
    }
    .brand-lockup { display: flex; align-items: center; gap: 14px; min-width: 0; }
    .brand-logo { width: 132px; height: auto; display: block; }
    .eyebrow { color: var(--muted); font-size: 11px; font-weight: 800; letter-spacing: 0; text-transform: uppercase; }
    .title { margin-top: 1px; font-family: var(--title-font); font-size: 20px; font-weight: 700; letter-spacing: 0; }
    .filters { display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 8px; align-items: center; }
    .chip-row { display: inline-flex; gap: 4px; padding: 4px; border: 1px solid var(--line); background: #f8fafc; border-radius: 999px; max-width: 100%; overflow-x: auto; }
    .chip { border: 0; border-radius: 999px; padding: 7px 12px; background: transparent; color: #384454; font-size: 12px; font-weight: 700; white-space: nowrap; }
    .chip.active { background: var(--ink); color: #fff; }
    .date-input { display: none; border: 1px solid var(--line); border-radius: 999px; padding: 7px 10px; color: #384454; background: #fff; font-size: 12px; font-weight: 700; }
    .date-input.visible { display: inline-block; }
    .refresh, .print-btn {
      border: 0;
      border-radius: 999px;
      padding: 9px 14px;
      background: var(--ink);
      color: #fff;
      font-size: 12px;
      font-weight: 800;
    }
    .print-btn { border: 1px solid var(--line); background: #fff; color: var(--ink); }
    .page { max-width: 1440px; margin: 0 auto; padding: 24px 24px 70px; }
    .hidden { display: none !important; }
    .state {
      padding: 18px 20px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--paper);
      box-shadow: var(--shadow);
      color: var(--muted);
      font-weight: 700;
    }
    .state.error { border-color: rgba(198,61,53,.38); background: #fff7f6; color: var(--red); }
    .report-sheet {
      background: var(--paper);
      border: 1px solid rgba(199,210,223,.92);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .executive-cover {
      position: relative;
      padding: 28px 30px 24px;
      background:
        linear-gradient(135deg, #ffffff 0%, #fbfcfd 58%, #ecf8f1 100%),
        var(--paper);
      border-bottom: 1px solid var(--line);
    }
    .executive-cover::before {
      content: "";
      position: absolute;
      inset: 0 0 auto;
      height: 7px;
      background: linear-gradient(90deg, var(--green) 0 40%, var(--blue) 40% 62%, var(--yellow) 62% 80%, var(--coral) 80% 100%);
    }
    .executive-cover::after {
      content: "";
      position: absolute;
      right: 28px;
      top: 30px;
      width: 280px;
      height: 130px;
      background:
        linear-gradient(135deg, transparent 0 46%, rgba(47,111,228,.08) 46% 48%, transparent 48% 100%),
        repeating-linear-gradient(90deg, rgba(0,143,90,.10) 0 1px, transparent 1px 18px);
      pointer-events: none;
      opacity: .72;
    }
    .report-identity {
      position: relative;
      z-index: 1;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
      margin-top: 9px;
    }
    .report-brand { display: flex; align-items: center; gap: 16px; min-width: 0; }
    .report-logo { width: 172px; height: auto; display: block; }
    .report-name {
      font-family: var(--title-font);
      font-size: 24px;
      line-height: 1;
      font-weight: 700;
      letter-spacing: 0;
      color: var(--ink);
      white-space: nowrap;
    }
    .report-badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 11px;
      border: 1px solid rgba(0,143,90,.22);
      border-radius: 999px;
      background: rgba(255,255,255,.80);
      color: #0a6b45;
      font-size: 12px;
      font-weight: 800;
    }
    .report-badge::before {
      content: "";
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: var(--green);
      box-shadow: 0 0 0 4px rgba(0,143,90,.12);
    }
    .cover-layout {
      position: relative;
      z-index: 1;
      display: grid;
      grid-template-columns: minmax(0, 1fr) 330px;
      gap: 28px;
      align-items: end;
      margin-top: 30px;
    }
    h1 {
      margin: 0;
      max-width: 860px;
      font-size: 42px;
      line-height: 1.04;
      font-weight: 700;
      letter-spacing: 0;
      color: var(--ink);
    }
    .subtitle { margin-top: 12px; max-width: 830px; color: #465365; font-size: 14px; line-height: 1.7; }
    .executive-summary {
      margin-top: 18px;
      max-width: 820px;
      padding-left: 14px;
      border-left: 4px solid var(--green);
      color: #263341;
      font-weight: 600;
      line-height: 1.6;
    }
    .report-meta-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      margin-top: 22px;
      max-width: 860px;
    }
    .meta-tile {
      border: 1px solid rgba(199,210,223,.88);
      border-radius: var(--radius);
      background: rgba(255,255,255,.74);
      padding: 12px;
      min-height: 78px;
    }
    .meta-label, .kpi-label, .section-label, .level-label, .chart-title {
      color: var(--muted);
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 0;
      text-transform: uppercase;
    }
    .meta-value {
      margin-top: 6px;
      color: var(--ink);
      font-size: 14px;
      font-weight: 800;
      line-height: 1.35;
    }
    .score-seal {
      min-height: 220px;
      border: 1px solid rgba(23,33,43,.10);
      border-radius: var(--radius);
      background:
        linear-gradient(180deg, rgba(255,255,255,.92), rgba(255,255,255,.70)),
        linear-gradient(135deg, rgba(0,143,90,.12), rgba(47,111,228,.10));
      padding: 22px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }
    .seal-label { color: var(--muted); font-size: 12px; font-weight: 800; text-transform: uppercase; letter-spacing: 0; }
    .seal-score { margin-top: 8px; font-family: var(--title-font); font-size: 56px; line-height: .92; font-weight: 700; letter-spacing: 0; }
    .seal-caption { color: #374558; font-size: 13px; font-weight: 700; line-height: 1.45; }
    .kpi-strip {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      padding: 18px 30px 30px;
      background: linear-gradient(180deg, #fff, #fbfcfd);
    }
    .kpi {
      min-height: 138px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: #fff;
      padding: 17px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      box-shadow: 0 10px 26px rgba(23,33,43,.045);
    }
    .kpi-value { margin-top: 12px; font-family: var(--title-font); font-size: 40px; line-height: .96; font-weight: 700; letter-spacing: 0; }
    .kpi-name { font-size: 15px; font-weight: 800; }
    .kpi-detail { margin-top: 5px; color: var(--muted); font-size: 12px; font-weight: 600; line-height: 1.45; }
    .score.green { color: var(--green); } .score.yellow { color: var(--yellow); } .score.red { color: var(--red); } .score.gray { color: var(--gray); }
    .dot { width: 9px; height: 9px; display: inline-block; border-radius: 99px; margin-right: 7px; vertical-align: middle; background: var(--gray); }
    .dot.green { background: var(--green); } .dot.yellow { background: var(--yellow); } .dot.red { background: var(--red); } .dot.gray { background: var(--gray); }
    .main-grid { display: grid; min-width: 0; }
    .report-section { min-width: 0; padding: 30px; border-top: 1px solid var(--line); }
    .section-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 18px; margin-bottom: 18px; }
    .section-title { margin-top: 4px; font-family: var(--title-font); font-size: 27px; line-height: 1.08; font-weight: 700; letter-spacing: 0; }
    .section-copy { margin-top: 7px; max-width: 780px; color: var(--muted); font-size: 13px; line-height: 1.6; }
    .period-inline {
      display: inline-flex;
      align-items: center;
      margin-left: 8px;
      padding: 5px 9px;
      border-radius: 999px;
      background: #eaf1ff;
      color: #274e9a;
      font-family: var(--font);
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 0;
      vertical-align: middle;
      white-space: nowrap;
    }
    .status-legend { display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 8px; min-width: 260px; }
    .stamp {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      padding: 7px 10px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #fff;
      color: #384454;
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
    }
    .matrix-wrap { width: 100%; max-width: 100%; min-width: 0; overflow-x: auto; padding-bottom: 2px; }
    table { width: 100%; }
    .matrix {
      min-width: 1320px;
      border-collapse: separate;
      border-spacing: 8px;
      table-layout: fixed;
    }
    .matrix th {
      padding: 10px 9px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: #f5f7fa;
      color: #526071;
      font-size: 10px;
      line-height: 1.2;
      letter-spacing: 0;
      text-transform: uppercase;
      font-weight: 800;
      text-align: center;
      vertical-align: middle;
    }
    .matrix th:first-child { text-align: left; width: 150px; }
    .metric-head {
      cursor: pointer;
      transition: border-color .15s ease, box-shadow .15s ease, background .15s ease;
    }
    .metric-head:hover, .metric-head.active {
      border-color: rgba(23,33,43,.42);
      background: #ffffff;
      box-shadow: inset 0 -3px 0 var(--green);
      color: var(--ink);
    }
    .matrix td {
      min-width: 112px;
      height: 94px;
      padding: 10px;
      border: 1px solid rgba(23,33,43,.10);
      border-radius: var(--radius);
      text-align: left;
      vertical-align: top;
    }
    .plantel-cell {
      position: sticky;
      left: 0;
      z-index: 2;
      background: #fff;
      border-color: var(--line-strong) !important;
      box-shadow: 8px 0 18px rgba(23,33,43,.06);
    }
    .plantel-cell.clickable { cursor: pointer; transition: border-color .15s ease, box-shadow .15s ease; }
    .plantel-cell.clickable:hover, .plantel-cell.clickable.active {
      border-color: rgba(23,33,43,.46) !important;
      box-shadow: inset 4px 0 0 var(--green), 8px 0 18px rgba(23,33,43,.06);
    }
    .plantel-code { font-family: var(--title-font); font-size: 22px; line-height: 1; font-weight: 700; letter-spacing: 0; }
    .plantel-name { margin-top: 5px; color: var(--muted); font-size: 11px; line-height: 1.35; }
    .heat {
      cursor: pointer;
      position: relative;
      overflow: hidden;
      transition: box-shadow .15s ease, transform .15s ease;
    }
    .heat::before {
      content: "";
      position: absolute;
      inset: 0;
      border-top: 4px solid rgba(255,255,255,.50);
      pointer-events: none;
    }
    .heat:hover { box-shadow: 0 10px 20px rgba(23,33,43,.10); transform: translateY(-1px); }
    .heat.active { outline: 2px solid rgba(23,33,43,.56); outline-offset: -2px; box-shadow: 0 13px 28px rgba(23,33,43,.14); }
    .heat.green { background: var(--green-soft); } .heat.yellow { background: var(--yellow-soft); } .heat.red { background: var(--red-soft); } .heat.gray { background: var(--gray-soft); }
    .heat-cell-top { display: flex; align-items: flex-start; justify-content: space-between; gap: 8px; position: relative; z-index: 1; }
    .cell-score { font-family: var(--title-font); font-size: 20px; line-height: 1; font-weight: 700; letter-spacing: 0; }
    .cell-status {
      padding: 3px 6px;
      border-radius: 999px;
      background: rgba(255,255,255,.64);
      color: #374558;
      font-size: 9px;
      line-height: 1.1;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0;
      white-space: nowrap;
    }
    .cell-label { margin-top: 8px; color: #3e4b5b; font-size: 11px; line-height: 1.35; position: relative; z-index: 1; }
    .info-title { color: #334155; font-size: 13px; line-height: 1.25; font-weight: 800; letter-spacing: 0; position: relative; z-index: 1; }
    .info-cell { background: linear-gradient(180deg, #f8fafc 0%, #eef2f7 100%) !important; }
    .metric-help {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 17px;
      height: 17px;
      margin-left: 5px;
      border-radius: 999px;
      border: 1px solid #cbd5e1;
      color: #475569;
      background: #fff;
      font-size: 11px;
      font-weight: 900;
      line-height: 1;
      cursor: help;
      vertical-align: middle;
    }
    .metric-help:hover { border-color: var(--ink); color: var(--ink); }
    .charts { display: grid; grid-template-columns: minmax(0, 1fr) minmax(330px, .48fr); gap: 14px; }
    .chart-card {
      min-height: 330px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: #fff;
      padding: 16px;
    }
    .chart-title { margin-bottom: 14px; color: #374558; font-size: 12px; }
    .bar-list { display: grid; gap: 12px; }
    .bar-row { display: grid; grid-template-columns: minmax(110px, .32fr) minmax(130px,1fr) 64px; gap: 12px; align-items: center; }
    .bar-name { font-size: 12px; font-weight: 800; color: #374558; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .track { height: 12px; border-radius: 999px; background: #edf1f5; overflow: hidden; }
    .fill { height: 100%; width: 0; border-radius: 999px; background: var(--gray); }
    .fill.green { background: linear-gradient(90deg, #00a66a, var(--green)); } .fill.yellow { background: linear-gradient(90deg, #e6a500, var(--yellow)); } .fill.red { background: linear-gradient(90deg, #e7594e, var(--red)); } .fill.gray { background: var(--gray); }
    .bar-value { text-align: right; font-size: 12px; font-weight: 800; }
    .level-access { display: grid; gap: 0; border: 1px solid var(--line); border-radius: var(--radius); overflow: hidden; background: #fff; }
    .level-row { display: grid; grid-template-columns: 150px minmax(160px,1fr) 125px 160px; gap: 14px; align-items: center; padding: 14px 16px; border-bottom: 1px solid var(--line); }
    .level-row:last-child { border-bottom: 0; }
    .level-name { font-weight: 800; color: #263341; }
    .level-meta { color: var(--muted); font-size: 12px; margin-top: 2px; line-height: 1.35; }
    .level-number { text-align: right; font-family: var(--title-font); font-size: 24px; font-weight: 700; color: var(--ink); letter-spacing: 0; }
    .level-label { text-align: right; }
    .trend-grid { display: grid; grid-template-columns: minmax(0, 1fr) 280px; gap: 14px; }
    .select { border: 1px solid var(--line); border-radius: 999px; background: #fff; color: #384454; padding: 8px 12px; font-size: 12px; font-weight: 800; }
    .line-chart { width: 100%; height: 350px; border: 1px solid var(--line); border-radius: var(--radius); background: #fff; display: block; }
    .trend-side { border: 1px solid var(--line); border-radius: var(--radius); background: #fff; padding: 18px; }
    .trend-big { margin-top: 14px; font-family: var(--title-font); font-size: 38px; font-weight: 700; letter-spacing: 0; }
    .trend-small { margin-top: 8px; color: var(--muted); font-size: 13px; line-height: 1.5; }
    .legend { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
    .legend-item { display: inline-flex; align-items: center; gap: 6px; color: #4b5563; font-size: 12px; font-weight: 700; }
    .legend-line { width: 18px; height: 3px; border-radius: 99px; background: var(--gray); }
    .detail-grid { display: grid; grid-template-columns: 280px minmax(0,1fr); gap: 14px; }
    .plantel-list-detail { display: grid; gap: 8px; }
    .plantel-option { border: 1px solid var(--line); background: #fff; border-radius: var(--radius); padding: 12px; display: flex; justify-content: space-between; gap: 12px; align-items: center; text-align: left; }
    .plantel-option.active { border-color: var(--ink); box-shadow: inset 4px 0 0 var(--ink); }
    .detail-table { border: 1px solid var(--line); border-radius: var(--radius); overflow: hidden; border-collapse: separate; border-spacing: 0; background: #fff; }
    .detail-table th, .detail-table td { padding: 12px 13px; border-bottom: 1px solid var(--line); text-align: left; }
    .detail-table tr:last-child td { border-bottom: 0; }
    .detail-table th { background: #f6f8fa; color: #4b5563; font-size: 11px; letter-spacing: 0; text-transform: uppercase; font-weight: 800; }
    .drilldown-section { background: linear-gradient(180deg, #fff, #fbfcfd); }
    .drilldown-shell {
      min-width: 0;
      display: grid;
      grid-template-columns: minmax(280px, .35fr) minmax(0, 1fr);
      gap: 14px;
      align-items: start;
    }
    .drill-panel {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: #fff;
      padding: 16px;
      box-shadow: 0 10px 26px rgba(23,33,43,.045);
    }
    .drill-kicker { color: var(--muted); font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing: 0; }
    .drill-heading { margin-top: 6px; font-family: var(--title-font); font-size: 25px; line-height: 1.04; font-weight: 700; color: var(--ink); }
    .drill-score { margin-top: 10px; font-family: var(--title-font); font-size: 42px; line-height: .95; font-weight: 700; letter-spacing: 0; }
    .drill-note { margin-top: 10px; color: #465365; font-size: 13px; line-height: 1.55; font-weight: 600; }
    .drill-stat-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 9px; margin-top: 14px; }
    .drill-stat { border: 1px solid var(--line); border-radius: var(--radius); background: #f8fafc; padding: 11px; min-height: 76px; }
    .drill-stat-label { color: var(--muted); font-size: 10px; line-height: 1.2; font-weight: 800; text-transform: uppercase; }
    .drill-stat-value { margin-top: 6px; color: var(--ink); font-size: 17px; line-height: 1.2; font-weight: 800; word-break: break-word; }
    .drill-method { display: grid; gap: 10px; }
    .method-card { border-left: 4px solid var(--green); padding: 3px 0 3px 12px; }
    .method-title { color: var(--ink); font-weight: 800; }
    .method-copy { margin-top: 3px; color: var(--muted); font-size: 12px; line-height: 1.5; }
    .component-grid { display: grid; gap: 8px; }
    .component-row { display: grid; grid-template-columns: minmax(130px, 1fr) 64px minmax(110px, .55fr); gap: 10px; align-items: center; }
    .component-name { color: #263341; font-size: 12px; font-weight: 800; line-height: 1.25; }
    .component-weight { color: var(--muted); font-size: 11px; font-weight: 800; text-align: right; }
    .month-table-wrap { width: 100%; max-width: 100%; min-width: 0; overflow-x: auto; border: 1px solid var(--line); border-radius: var(--radius); background: #fff; }
    .month-table { border-collapse: collapse; min-width: 720px; }
    .month-table th, .month-table td { padding: 10px 11px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
    .month-table th { background: #f6f8fa; color: #526071; font-size: 10px; line-height: 1.25; font-weight: 800; text-transform: uppercase; }
    .month-table tr:last-child td { border-bottom: 0; }
    .month-scorebar { width: 98px; height: 8px; margin-top: 5px; border-radius: 999px; background: #edf1f5; overflow: hidden; }
    .month-scorebar span { display: block; height: 100%; border-radius: 999px; background: var(--gray); }
    .month-scorebar .green { background: var(--green); } .month-scorebar .yellow { background: var(--yellow); } .month-scorebar .red { background: var(--red); } .month-scorebar .gray { background: var(--gray); }
    .evidence-chip-row { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 7px; }
    .evidence-chip { display: inline-flex; align-items: center; border: 1px solid var(--line); border-radius: 999px; padding: 4px 7px; color: #465365; background: #fff; font-size: 10px; font-weight: 800; line-height: 1; }
    .plantel-score-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 9px; }
    .plantel-score-card { border: 1px solid var(--line); border-radius: var(--radius); background: #fff; padding: 12px; cursor: pointer; text-align: left; }
    .plantel-score-card:hover { border-color: rgba(23,33,43,.42); box-shadow: 0 8px 18px rgba(23,33,43,.08); }
    .plantel-score-card.active { border-color: var(--ink); box-shadow: inset 4px 0 0 var(--green); }
    .plantel-score-top { display: flex; align-items: flex-start; justify-content: space-between; gap: 8px; }
    .empty-note { border: 1px dashed var(--line-strong); border-radius: var(--radius); padding: 15px; color: var(--muted); background: #fbfcfd; font-weight: 700; }
    .print-only { display: none; }
    .methodology-section { background: #fbfcfd; }
    .methodology-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px 16px; }
    .methodology-item { border-top: 1px solid var(--line); padding-top: 10px; }
    .methodology-name { font-weight: 800; color: var(--ink); }
    .methodology-copy { margin-top: 3px; color: var(--muted); font-size: 12px; line-height: 1.45; }
    @media (max-width: 1100px) {
      .topbar-inner, .cover-layout, .charts, .trend-grid, .detail-grid, .drilldown-shell { grid-template-columns: 1fr; }
      .filters { justify-content: flex-start; }
      .score-seal { min-height: auto; }
      .report-meta-grid, .kpi-strip, .methodology-grid, .plantel-score-grid { grid-template-columns: 1fr; }
      .status-legend { justify-content: flex-start; }
    }
    @media (max-width: 720px) {
      .page { padding: 14px 12px 48px; }
      .topbar-inner { padding: 12px; }
      .report-identity { align-items: flex-start; flex-direction: column; }
      .report-logo { width: 150px; }
      h1 { font-size: 31px; }
      .executive-cover, .report-section { padding: 22px 18px; }
      .kpi-strip { padding: 14px 18px 22px; grid-template-columns: 1fr; }
      .kpi-value { font-size: 36px; }
      .chart-card, .line-chart { min-height: 300px; height: 300px; }
      .level-row { grid-template-columns: 1fr; }
      .level-number, .level-label { text-align: left; }
    }

    @media print {
      @page { size: landscape; margin: 8mm; }
      * { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
      html, body { background: #fff !important; color: #17212b; font-size: 8.5pt; }
      body { line-height: 1.35; }
      .topbar, .refresh, .print-btn, #scopeFilters, #plantelFilters, .date-input, .metric-help, .select, .detail-section, .drilldown-section { display: none !important; }
      .print-only { display: block !important; }
      .page { max-width: none; padding: 0; }
      .report-sheet { border: 0; border-radius: 0; box-shadow: none; overflow: visible; }
      .executive-cover { padding: 0 0 2.5mm; background: #fff; border-bottom: .6pt solid var(--line-strong); break-inside: avoid; }
      .executive-cover::before { height: 3.5pt; top: -1mm; }
      .executive-cover::after { display: none; }
      .report-identity { margin-top: 2mm; }
      .report-logo { width: 30mm; }
      .report-name { font-size: 13pt; }
      .report-badge { padding: 1.3mm 2.4mm; font-size: 6.8pt; }
      .cover-layout { grid-template-columns: minmax(0,1fr) 38mm; gap: 4mm; margin-top: 3mm; }
      h1 { font-size: 18pt; line-height: 1.02; }
      .subtitle { margin-top: 1.5mm; max-width: 190mm; font-size: 7.2pt; line-height: 1.28; }
      .executive-summary { margin-top: 2mm; padding-left: 2.5mm; border-left-width: 1.6pt; font-size: 7.1pt; line-height: 1.28; }
      .report-meta-grid { grid-template-columns: repeat(3, minmax(0,1fr)); gap: 2mm; margin-top: 2.5mm; }
      .meta-tile { min-height: 13mm; padding: 2mm; border-width: .6pt; }
      .meta-label, .kpi-label, .section-label, .level-label, .chart-title { font-size: 5.9pt; }
      .meta-value { margin-top: .7mm; font-size: 7.4pt; line-height: 1.22; }
      .score-seal { min-height: 32mm; padding: 3mm; border-width: .6pt; box-shadow: none; }
      .seal-label { font-size: 6.2pt; }
      .seal-score { font-size: 22pt; }
      .seal-caption { font-size: 6.8pt; }
      .kpi-strip { grid-template-columns: repeat(3, minmax(0,1fr)); gap: 2.4mm; padding: 2.2mm 0 2.8mm; background: #fff; break-inside: avoid; }
      .kpi { min-height: 18mm; padding: 2mm; border-width: .6pt; box-shadow: none; }
      .kpi-value { margin-top: .8mm; font-size: 14pt; }
      .kpi-name { font-size: 7.1pt; }
      .kpi-detail { margin-top: .4mm; font-size: 5.8pt; line-height: 1.18; }
      .report-section { padding: 3mm 0; border-top: 0; }
      .heatmap-section { break-inside: avoid; }
      .section-head { margin-bottom: 1.7mm; gap: 2.5mm; }
      .section-title { font-size: 13pt; }
      .section-copy { margin-top: .7mm; max-width: 182mm; font-size: 6.5pt; line-height: 1.24; }
      .period-inline { margin-left: 1.2mm; padding: .7mm 1.7mm; font-size: 6.5pt; }
      .status-legend { gap: 1.5mm; min-width: 0; }
      .stamp { padding: .9mm 1.7mm; border-width: .6pt; font-size: 6.2pt; }
      .dot { width: 5pt; height: 5pt; margin-right: 3pt; }
      .matrix-wrap { overflow: visible; padding: 0; }
      .matrix { min-width: 0; width: 100%; border-spacing: 1.1mm; table-layout: fixed; }
      .matrix th { padding: 1.3mm .8mm; border-width: .6pt; font-size: 5.2pt; line-height: 1.08; }
      .matrix th:first-child { width: 26mm; }
      .matrix td { min-width: 0; height: 12.4mm; padding: 1.2mm; border-width: .6pt; }
      .plantel-cell { position: static; box-shadow: none; }
      .plantel-code { font-size: 10pt; }
      .plantel-name { margin-top: .6mm; font-size: 5pt; line-height: 1.1; }
      .heat::before { border-top-width: 1.6pt; }
      .cell-score { font-size: 8pt; }
      .cell-status { padding: .55mm .8mm; font-size: 4.3pt; }
      .cell-label { margin-top: .7mm; font-size: 4.8pt; line-height: 1.12; }
      .info-title { font-size: 5.6pt; }
      .methodology-section { break-before: page; background: #fff; }
      .methodology-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 2.4mm 5mm; }
      .methodology-item { padding-top: 1.6mm; border-top-width: .6pt; }
      .methodology-name { font-size: 7.4pt; }
      .methodology-copy { margin-top: .7mm; font-size: 6.4pt; line-height: 1.26; }
      .support-section { break-inside: avoid; }
      .charts { grid-template-columns: 1fr 1fr; gap: 3mm; }
      .chart-card { min-height: 54mm; padding: 3mm; border-width: .6pt; }
      .bar-list { gap: 2.2mm; }
      .bar-row { grid-template-columns: 28mm minmax(30mm,1fr) 15mm; gap: 2.5mm; }
      .bar-name, .bar-value { font-size: 6.6pt; }
      .track { height: 2.2mm; }
      .level-access { border-width: .6pt; }
      .level-row { grid-template-columns: 33mm minmax(36mm,1fr) 25mm 34mm; gap: 3mm; padding: 2.4mm 3mm; border-bottom-width: .6pt; }
      .level-name { font-size: 7.6pt; }
      .level-meta { font-size: 6.2pt; }
      .level-number { font-size: 12pt; }
      .trend-grid { grid-template-columns: minmax(0,1fr) 46mm; gap: 3mm; }
      .line-chart { height: 50mm; border-width: .6pt; }
      .trend-side { padding: 3mm; border-width: .6pt; }
      .trend-big { font-size: 18pt; }
      .trend-small, .legend-item { font-size: 6.5pt; }
    }
  </style>
</head>
<body>
  <header class="topbar">
    <div class="topbar-inner">
      <div class="brand-lockup">
        <img class="brand-logo" src="https://sipae.casitaapps.com/logo-h.png" alt="SIPAE Casita" />
        <div>
          <div class="eyebrow">Reporte ejecutivo</div>
          <div class="title">Reporte SIPAE</div>
        </div>
      </div>
      <div class="filters">
        <div class="chip-row" id="scopeFilters">
          <button class="chip" data-scope="month">Mes</button>
          <button class="chip" data-scope="today">Hoy</button>
          <button class="chip active" data-scope="ciclo_escolar">Ciclo</button>
          <button class="chip" data-scope="range">Rango</button>
        </div>
        <input id="startDate" class="date-input" type="date" />
        <input id="endDate" class="date-input" type="date" />
        <div class="chip-row" id="plantelFilters"></div>
        <button class="refresh" id="refreshBtn">Actualizar</button><button class="print-btn" id="printBtn">Imprimir PDF</button>
      </div>
    </div>
  </header>

  <main class="page">
    <div id="loading" class="state">Cargando reporte...</div>
    <div id="error" class="state error hidden"></div>

    <div id="report" class="hidden">
      <article class="report-sheet">
        <section class="executive-cover">
          <div class="report-identity">
            <div class="report-brand">
              <img class="report-logo" src="https://sipae.casitaapps.com/logo-h.png" alt="SIPAE Casita" />
              <div class="report-name">Reporte SIPAE</div>
            </div>
            <div class="report-badge">Informe institucional</div>
          </div>

          <div class="cover-layout">
            <div>
              <h1>Índice corporativo de cumplimiento</h1>
              <div class="subtitle">Vista ejecutiva del periodo evaluado con métricas de 1 a 100 por plantel. Los indicadores operativos se leen en positivo: 100 representa operación sana y los valores menores señalan atención requerida.</div>
              <div class="executive-summary" id="executiveSummary">Preparando lectura ejecutiva del periodo.</div>
              <div class="report-meta-grid">
                <div class="meta-tile"><div class="meta-label">Periodo del reporte</div><div class="meta-value" id="periodStamp">Periodo —</div></div>
                <div class="meta-tile"><div class="meta-label">Días hábiles</div><div class="meta-value" id="businessDaysStamp">—</div></div>
                <div class="meta-tile"><div class="meta-label">Generado</div><div class="meta-value" id="updatedStamp">—</div></div>
              </div>
            </div>
            <div class="score-seal">
              <div>
                <div class="seal-label">Índice general</div>
                <div class="seal-score score gray" id="coverGeneralScore">—</div>
              </div>
              <div>
                <div class="seal-caption" id="coverGeneralTitle">Sin datos</div>
                <div class="kpi-detail" id="coverGeneralDetail">—</div>
              </div>
            </div>
          </div>
        </section>

        <section class="kpi-strip">
          <div class="kpi"><div><div class="kpi-label">General</div><div class="kpi-value score gray" id="generalScore">—</div></div><div><div class="kpi-name" id="generalTitle">Sin datos</div><div class="kpi-detail" id="generalDetail">—</div></div></div>
          <div class="kpi"><div><div class="kpi-label">Mejor plantel</div><div class="kpi-value score gray" id="bestScore">—</div></div><div><div class="kpi-name" id="bestTitle">—</div><div class="kpi-detail" id="bestDetail">—</div></div></div>
          <div class="kpi"><div><div class="kpi-label">Menor resultado</div><div class="kpi-value score gray" id="worstScore">—</div></div><div><div class="kpi-name" id="worstTitle">—</div><div class="kpi-detail" id="worstDetail">—</div></div></div>
        </section>

        <div class="main-grid">
          <section class="report-section heatmap-section">
            <div class="section-head">
              <div>
                <div class="section-label">Centro ejecutivo</div>
                <div class="section-title">Mapa de cumplimiento <span class="period-inline" id="matrixPeriod">Periodo —</span></div>
                <div class="section-copy">Lectura comparativa por plantel y métrica. El color comunica prioridad inmediata; el porcentaje conserva la trazabilidad del cálculo aprobado.</div>
              </div>
              <div class="status-legend">
                <span class="stamp"><span class="dot green"></span>85-100 sano</span>
                <span class="stamp"><span class="dot yellow"></span>70-84 atención</span>
                <span class="stamp"><span class="dot red"></span>1-69 crítico</span>
              </div>
            </div>
            <div class="matrix-wrap"><table class="matrix" id="matrix"></table></div>
          </section>

          <section class="report-section drilldown-section" id="drilldownSection">
            <div class="section-head">
              <div>
                <div class="section-label">Auditoría del cálculo</div>
                <div class="section-title" id="drilldownTitle">Trazabilidad de métrica</div>
                <div class="section-copy" id="drilldownSubtitle">Selecciona una celda, un encabezado de métrica o un plantel para revisar la evidencia que sostiene el resultado.</div>
              </div>
            </div>
            <div class="drilldown-shell" id="drilldownBody"></div>
          </section>

          <section class="report-section methodology-section print-only">
            <div class="section-head">
              <div>
                <div class="section-label">Notas de lectura</div>
                <div class="section-title">Metodología de métricas</div>
                <div class="section-copy">Resumen breve para que el PDF sea autocontenido, con criterios, denominadores y exclusiones visibles.</div>
              </div>
            </div>
            <div class="methodology-grid" id="methodologyList"></div>
          </section>

          <section class="report-section support-section">
            <div class="section-head"><div><div class="section-label">Comparativo</div><div class="section-title">Planteles y áreas</div><div class="section-copy">Ranking ejecutivo para ubicar fortalezas y áreas que concentran seguimiento.</div></div></div>
            <div class="charts">
              <div class="chart-card"><div class="chart-title">Cumplimiento general por plantel</div><div id="plantelBars" class="bar-list"></div></div>
              <div class="chart-card"><div class="chart-title">Promedio por métrica</div><div id="metricBars" class="bar-list"></div></div>
            </div>
          </section>

          <section class="report-section support-section">
            <div class="section-head"><div><div class="section-label">Accesos</div><div class="section-title">Hora promedio de entrada por nivel</div><div class="section-copy">Síntesis de comportamiento de acceso para contextualizar puntualidad y flujo operativo.</div></div></div>
            <div class="level-access" id="levelAccess"></div>
          </section>

          <section class="report-section support-section">
            <div class="section-head">
              <div><div class="section-label">Tendencia</div><div class="section-title">Evolución del periodo</div><div class="section-copy">Seguimiento visual de la métrica seleccionada durante el periodo del reporte.</div></div>
              <select id="trendMetric" class="select">
                <option value="general">General</option>
                <option value="roll_call">Pase de lista</option>
                <option value="student_attendance">Asistencia alumnos</option>
                <option value="scans">Escaneos</option>
                <option value="scan_balance">Balance accesos</option>
                <option value="student_punctuality">Puntualidad alumnos</option>
              </select>
            </div>
            <div class="trend-grid">
              <div>
                <svg id="trendSvg" class="line-chart" viewBox="0 0 900 350" preserveAspectRatio="none" role="img" aria-label="Tendencia"></svg>
                <div id="trendLegend" class="legend"></div>
              </div>
              <div class="trend-side"><div class="section-label" id="trendName">Métrica</div><div class="trend-big score gray" id="trendAverage">—</div><div class="trend-small" id="trendText">Promedio del periodo seleccionado.</div></div>
            </div>
          </section>

        </div>
      </article>
    </div>
  </main>

  <script>
    var PLANTELES = ["PT", "PM", "ST", "SM", "PREET", "PREEM"];
    var METRIC_ORDER = ["general", "roll_call", "student_attendance", "scans", "scan_balance", "student_punctuality", "planning", "observations", "observation_coverage", "sapf"];
    var METRIC_LABELS = { general: "General", roll_call: "Pase de lista", student_attendance: "Asistencia alumnos", scans: "Escaneos", scan_balance: "Balance accesos", student_punctuality: "Puntualidad alumnos", planning: "Planeaciones", observations: "Observaciones", observation_coverage: "Cobertura obs.", sapf: "Seguimientos" };
    var METRIC_DESCRIPTIONS = {
      general: "Promedio ponderado de las métricas con cálculo real. En ciclo escolar usa el periodo completo salvo observaciones, que se promedian por mes.",
      roll_call: "Promedio del periodo: grupos/día con pase de lista capturado contra grupos esperados por día.",
      student_attendance: "Promedio diario del periodo: alumnos presentes dentro de listas capturadas.",
      scans: "Promedio diario del periodo: entradas registradas contra población esperada del plantel.",
      scan_balance: "Promedio diario del periodo: correspondencia entre entradas y salidas registradas; mayor balance = mejor.",
      student_punctuality: "Métrica positiva: 100 es sin retardos; baja según retardos contra oportunidades alumno-día del periodo.",
      planning: "Planeaciones creadas por docentes académicamente activos en el periodo que ya tienen revisión registrada. Excluye docentes sin actividad académica reciente.",
      observations: "Mes/rango corto: observaciones del mes contra meta 40. Ciclo escolar/rango largo: promedio mensual contra meta 40.",
      observation_coverage: "Mes/rango corto: docentes activos con 2+ observaciones. Ciclo escolar/rango largo: promedio mensual de esa cobertura.",
      sapf: "Puntaje combinado: 60% seguimientos reales contra meta poblacional del periodo, 25% fichas cerradas y 15% densidad de seguimiento por ficha."
    };
    var LINE_COLORS = ["#111827", "#009F5A", "#D1182C", "#D97706", "#2563eb", "#7c3aed"];
    var state = { scope: "ciclo_escolar", planteles: {}, data: null, selectedPlantel: null, selectedMetric: "general", drilldownMode: "cell" };
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
    function fmt(value, digits) {
      var n = num(value);
      if (n === null) return "—";
      var useDigits = digits === undefined ? (Math.abs(n % 1) > 0.001 ? 1 : 0) : digits;
      return n.toLocaleString("es-MX", { maximumFractionDigits: useDigits, minimumFractionDigits: useDigits });
    }
    function periodRangeText(win) {
      if (!win || !win.start || !win.end) return "Periodo sin fechas";
      return win.start + " a " + win.end;
    }
    function scoreWidth(value) {
      var n = num(value);
      return n === null ? 0 : Math.max(0, Math.min(100, n));
    }
    function colorFor(score) {
      var n = num(score);
      if (n === null) return "gray";
      if (n >= 85) return "green";
      if (n >= 70) return "yellow";
      return "red";
    }
    function statusText(metric) {
      if (isInformational(metric)) return "Info";
      var color = metricColor(metric);
      if (color === "green") return "Sano";
      if (color === "yellow") return "Atenc.";
      if (color === "red") return "Crít.";
      return "Sin dato";
    }
    function heatBackground(score) {
      var n = num(score);
      if (n === null) return "linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%)";
      var red = [198, 61, 53], yellow = [200, 132, 0], green = [0, 143, 90];
      var from = n < 70 ? red : (n < 85 ? yellow : green);
      var alpha = 0.18 + (Math.max(0, Math.min(100, n)) / 100) * 0.18;
      var top = 'rgba(' + from[0] + ',' + from[1] + ',' + from[2] + ',' + alpha.toFixed(3) + ')';
      var bottom = 'rgba(' + from[0] + ',' + from[1] + ',' + from[2] + ',' + (alpha * 0.46).toFixed(3) + ')';
      return 'linear-gradient(135deg, ' + top + ' 0%, ' + bottom + ' 100%)';
    }
    function heatStyle(metric) { return ' style="background:' + heatBackground(metric ? metric.score : null) + '"'; }
    function metricColor(metric) { return metric && metric.color ? metric.color : colorFor(metric ? metric.score : null); }
    function dot(metric) { return '<span class="dot ' + metricColor(metric) + '"></span>'; }
    function isInformational(metric) { return !!(metric && metric.informational); }
    function metricCellHtml(metric) {
      if (!metric) {
        return '<div class="heat-cell-top"><div class="info-title">Sin dato</div><span class="cell-status">Sin dato</span></div><div class="cell-label">No se recibió información para esta métrica.</div>';
      }
      if (isInformational(metric)) {
        return '<div class="heat-cell-top"><div class="info-title">' + esc(metric.label || 'Sin registros') + '</div><span class="cell-status">Info</span></div><div class="cell-label">' + esc(metric.detail || 'Dato informativo') + '</div>';
      }
      if (num(metric.score) === null) {
        return '<div class="heat-cell-top"><div class="info-title">' + esc(metric.label || 'Sin cálculo') + '</div><span class="cell-status">Sin dato</span></div><div class="cell-label">' + esc(metric.detail || 'No hay denominador suficiente para calcular.') + '</div>';
      }
      return '<div class="heat-cell-top"><div class="cell-score score ' + metricColor(metric) + '">' + pct(metric.score) + '</div><span class="cell-status">' + esc(statusText(metric)) + '</span></div><div class="cell-label">' + esc(metric.label || "Sin datos") + '</div>';
    }
    function metricValueHtml(metric) {
      if (isInformational(metric)) return '<span class="score gray">Informativo</span>';
      return '<span class="score ' + metricColor(metric) + '">' + pct(metric.score) + '</span>';
    }
    function selectedPlanteles() {
      var out = [];
      for (var i = 0; i < PLANTELES.length; i += 1) if (state.planteles[PLANTELES[i]]) out.push(PLANTELES[i]);
      return out;
    }
    function complianceCounts() {
      var rows = get(state.data, ["matrix"], []);
      var counts = { green: 0, yellow: 0, red: 0, gray: 0, total: 0 };
      for (var r = 0; r < rows.length; r += 1) {
        for (var c = 0; c < METRIC_ORDER.length; c += 1) {
          var metric = get(rows[r], ["cells", METRIC_ORDER[c]], {});
          if (isInformational(metric)) continue;
          var color = metricColor(metric);
          if (!hasOwn(counts, color)) color = "gray";
          counts[color] += 1;
          counts.total += 1;
        }
      }
      return counts;
    }
    function executiveSummaryText(aggregate, general) {
      var counts = complianceCounts();
      var best = aggregate.best_plantel || null;
      var worst = aggregate.worst_plantel || null;
      var alertText = "sin indicadores críticos";
      if (counts.red > 0) alertText = counts.red + " indicadores en zona crítica";
      else if (counts.yellow > 0) alertText = counts.yellow + " indicadores en zona de atención";
      var summary = "El índice general se ubica en " + pct(general.score) + " (" + (general.traffic_label || "sin clasificación") + "), con " + alertText + " dentro del mapa.";
      if (best && worst) summary += " Mejor desempeño: " + best.plantel + "; foco de seguimiento: " + worst.plantel + ".";
      return summary;
    }

    function monthName(index) {
      return ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"][index] || "";
    }
    function periodLabel(win) {
      if (!win || !win.start || !win.end) return "Periodo —";
      var start = new Date(win.start + "T00:00:00");
      var end = new Date(win.end + "T00:00:00");
      if (state.scope === "month") return monthName(end.getMonth()) + " " + end.getFullYear();
      if (state.scope === "today") return "Hoy · " + win.end;
      if (state.scope === "ciclo_escolar") {
        var cycleStart = start.getFullYear();
        var cycleEnd = end.getFullYear();
        return "Ciclo escolar " + cycleStart + "–" + cycleEnd;
      }
      return win.start + " → " + win.end;
    }
    function metricHead(key) {
      var label = METRIC_LABELS[key] || key;
      var desc = METRIC_DESCRIPTIONS[key] || "";
      return esc(label) + '<span class="metric-help" title="' + esc(desc) + '" aria-label="' + esc(desc) + '">?</span>';
    }
    function renderMethodology() {
      var target = byId("methodologyList");
      if (!target) return;
      var html = "";
      for (var i = 0; i < METRIC_ORDER.length; i += 1) {
        var key = METRIC_ORDER[i];
        html += '<div class="methodology-item"><div class="methodology-name">' + esc(METRIC_LABELS[key] || key) + '</div><div class="methodology-copy">' + esc(METRIC_DESCRIPTIONS[key] || "Métrica del reporte ejecutivo.") + '</div></div>';
      }
      target.innerHTML = html;
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
      byId("printBtn").addEventListener("click", printExecutiveReport);
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
      renderMethodology();
      renderBars();
      renderLevelAccess();
      renderTrend();
      renderDrilldown();
    }
    function renderHero() {
      var aggregate = get(state.data, ["aggregate"], {});
      var general = get(aggregate, ["corporate_index"], {});
      var win = get(aggregate, ["window"], {});
      var label = periodLabel(win);
      byId("periodStamp").textContent = label + " · " + (win.start || "—") + " → " + (win.end || "—");
      byId("matrixPeriod").textContent = label;
      byId("businessDaysStamp").textContent = (win.business_days || 0) + " días hábiles";
      var generated = get(state.data, ["generated_at"], null);
      byId("updatedStamp").textContent = generated ? new Date(generated).toLocaleString("es-MX") : "Actualizado —";
      byId("executiveSummary").textContent = executiveSummaryText(aggregate, general);
      byId("coverGeneralScore").className = "seal-score score " + metricColor(general);
      byId("coverGeneralScore").textContent = pct(general.score);
      byId("coverGeneralTitle").innerHTML = dot(general) + esc(general.traffic_label || "Sin datos");
      byId("coverGeneralDetail").textContent = general.detail || "—";
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
      var html = '<tr><th class="plantel-header">Plantel</th>';
      for (var h = 0; h < METRIC_ORDER.length; h += 1) {
        var headKey = METRIC_ORDER[h];
        var headActive = state.drilldownMode === "metric" && state.selectedMetric === headKey ? " active" : "";
        html += '<th class="metric-head' + headActive + '" data-metric="' + esc(headKey) + '">' + metricHead(headKey) + "</th>";
      }
      html += "</tr>";
      for (var r = 0; r < rows.length; r += 1) {
        var row = rows[r];
        var plantelActive = state.selectedPlantel === row.plantel && state.selectedMetric === "general" && state.drilldownMode !== "metric" ? " active" : "";
        html += '<tr><td class="plantel-cell clickable' + plantelActive + '" data-plantel="' + esc(row.plantel) + '"><div class="plantel-code">' + esc(row.plantel) + '</div><div class="plantel-name">' + esc(row.name) + "</div></td>";
        for (var c = 0; c < METRIC_ORDER.length; c += 1) {
          var key = METRIC_ORDER[c];
          var metric = get(row, ["cells", key], {});
          var infoClass = isInformational(metric) ? ' info-cell' : '';
          var active = state.drilldownMode !== "metric" && state.selectedPlantel === row.plantel && state.selectedMetric === key ? " active" : "";
          html += '<td class="heat ' + metricColor(metric) + infoClass + active + '" data-plantel="' + esc(row.plantel) + '" data-metric="' + esc(key) + '"' + heatStyle(metric) + '>' + metricCellHtml(metric) + '</td>';
        }
        html += "</tr>";
      }
      byId("matrix").innerHTML = html;
      var heads = document.querySelectorAll(".metric-head[data-metric]");
      for (var mh = 0; mh < heads.length; mh += 1) heads[mh].addEventListener("click", function () {
        state.selectedMetric = this.getAttribute("data-metric") || "general";
        state.drilldownMode = "metric";
        renderMatrix();
        renderDrilldown();
      });
      var plantelCells = document.querySelectorAll(".plantel-cell[data-plantel]");
      for (var pc = 0; pc < plantelCells.length; pc += 1) plantelCells[pc].addEventListener("click", function () {
        state.selectedPlantel = this.getAttribute("data-plantel");
        state.selectedMetric = "general";
        state.drilldownMode = "plantel";
        renderMatrix();
        renderDrilldown();
      });
      var cells = document.querySelectorAll(".heat[data-plantel][data-metric]");
      for (var i = 0; i < cells.length; i += 1) cells[i].addEventListener("click", function () {
        state.selectedPlantel = this.getAttribute("data-plantel");
        state.selectedMetric = this.getAttribute("data-metric") || "general";
        state.drilldownMode = "cell";
        renderMatrix();
        renderDrilldown();
      });
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
      html += '<rect x="' + padL + '" y="' + padT + '" width="' + plotW + '" height="' + (plotH * .15) + '" fill="#e5f7ee" opacity=".65" />';
      html += '<rect x="' + padL + '" y="' + (padT + plotH * .15) + '" width="' + plotW + '" height="' + (plotH * .15) + '" fill="#fff4d8" opacity=".72" />';
      html += '<rect x="' + padL + '" y="' + (padT + plotH * .30) + '" width="' + plotW + '" height="' + (plotH * .70) + '" fill="#fde9e7" opacity=".40" />';
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
        if (path) html += '<path d="' + path + '" fill="none" stroke="' + LINE_COLORS[s % LINE_COLORS.length] + '" stroke-width="3.4" stroke-linecap="round" stroke-linejoin="round" />';
      }
      byId("trendSvg").innerHTML = html;
    }
    function findPlantel(code) {
      var planteles = get(state.data, ["planteles"], []);
      for (var i = 0; i < planteles.length; i += 1) if (planteles[i].plantel === code) return planteles[i];
      return planteles.length ? planteles[0] : null;
    }
    function metricFromPlantel(plantel, key) {
      return get(plantel || {}, ["metrics", key], {});
    }
    function breakdownOf(metric) {
      return metric && metric.breakdown ? metric.breakdown : {};
    }
    function ratioText(numerator, denominator) {
      if (num(numerator) === null && num(denominator) === null) return "—";
      return fmt(numerator) + " / " + fmt(denominator, Math.abs((num(denominator) || 0) % 1) > 0.001 ? 1 : 0);
    }
    function evidenceReason(metric, breakdown) {
      if (!metric) return "No se recibió información para esta métrica.";
      if (num(metric.score) === null) return breakdown.aggregation || metric.detail || "No hay base suficiente para calcular el indicador.";
      var base = "Resultado final: " + pct(metric.score) + ".";
      if (breakdown.numerator_label || breakdown.denominator_label) {
        base += " " + (breakdown.numerator_label || "Numerador") + ": " + fmt(breakdown.numerator) + "; " + (breakdown.denominator_label || "Denominador") + ": " + fmt(breakdown.denominator, Math.abs((num(breakdown.denominator) || 0) % 1) > 0.001 ? 1 : 0) + ".";
      }
      if (breakdown.aggregation) base += " " + breakdown.aggregation;
      return base;
    }
    function chip(label, value) {
      if (value === null || value === undefined || value === "" || value === "—") return "";
      return '<span class="evidence-chip">' + esc(label) + ': ' + esc(value) + '</span>';
    }
    function componentHtml(components) {
      if (!components || !components.length) return "";
      var html = '<div class="method-card"><div class="method-title">Componentes del puntaje</div><div class="component-grid">';
      for (var i = 0; i < components.length; i += 1) {
        var item = components[i] || {};
        var score = num(item.score);
        var color = colorFor(score);
        html += '<div class="component-row"><div><div class="component-name">' + esc(item.label || item.key || "Componente") + '</div><div class="evidence-chip-row">' + chip("Base", ratioText(item.numerator, item.denominator)) + '</div></div><div class="component-weight">' + (item.weight ? esc(item.weight + "%") : "") + '</div><div><div class="bar-value score ' + color + '">' + pct(score) + '</div><div class="month-scorebar"><span class="' + color + '" style="width:' + scoreWidth(score) + '%"></span></div></div></div>';
      }
      html += '</div></div>';
      return html;
    }
    function monthlyEvidence(row) {
      var parts = "";
      parts += chip("Días", row.available_days !== undefined && row.business_days !== undefined ? (row.available_days + "/" + row.business_days) : null);
      parts += chip("Retardos", row.tardies);
      parts += chip("Fichas", row.fichas);
      parts += chip("Seguimientos", row.followups);
      parts += chip("Observadores", row.observed_teachers);
      parts += chip("Docentes 2+", row.teachers_with_2plus);
      if (row.unavailable_reason) parts += '<span class="evidence-chip">' + esc(row.unavailable_reason) + '</span>';
      return parts || '<span class="evidence-chip">Sin notas adicionales</span>';
    }
    function monthlyTableHtml(rows) {
      if (!rows || !rows.length) return '<div class="empty-note">Esta métrica no tiene desglose mensual aplicable para el periodo seleccionado.</div>';
      var html = '<div class="month-table-wrap"><table class="month-table"><thead><tr><th>Periodo</th><th>Score</th><th>Numerador</th><th>Denominador</th><th>Evidencia</th></tr></thead><tbody>';
      for (var i = 0; i < rows.length; i += 1) {
        var row = rows[i] || {};
        var color = colorFor(row.score);
        html += '<tr><td><strong>' + esc(row.label || row.period || "—") + '</strong></td><td><div class="score ' + color + '">' + pct(row.score) + '</div><div class="month-scorebar"><span class="' + color + '" style="width:' + scoreWidth(row.score) + '%"></span></div></td><td>' + esc(row.numerator_label || "Numerador") + '<br><strong>' + fmt(row.numerator) + '</strong></td><td>' + esc(row.denominator_label || "Denominador") + '<br><strong>' + fmt(row.denominator, Math.abs((num(row.denominator) || 0) % 1) > 0.001 ? 1 : 0) + '</strong></td><td><div class="evidence-chip-row">' + monthlyEvidence(row) + '</div></td></tr>';
      }
      html += '</tbody></table></div>';
      return html;
    }
    function excludedHtml(items) {
      if (!items || !items.length) return "";
      var html = '<div class="method-card"><div class="method-title">Excluido o no disponible</div><div class="method-copy">';
      for (var i = 0; i < items.length; i += 1) html += (i ? " · " : "") + esc(items[i]);
      return html + '</div></div>';
    }
    function plantelMetricHtml(plantel, key, metric) {
      var b = breakdownOf(metric);
      var win = get(state.data, ["aggregate", "window"], {});
      var method = b.method || "Valor del periodo";
      var formula = b.formula || METRIC_DESCRIPTIONS[key] || "Métrica del reporte ejecutivo.";
      var html = "";
      html += '<div class="drill-panel"><div class="drill-kicker">' + esc(plantel.plantel || "Plantel") + '</div><div class="drill-heading">' + esc(METRIC_LABELS[key] || key) + '</div><div class="drill-score score ' + metricColor(metric) + '">' + metricValueHtml(metric) + '</div><div class="drill-note">' + esc(evidenceReason(metric, b)) + '</div><div class="drill-stat-grid">';
      html += '<div class="drill-stat"><div class="drill-stat-label">' + esc(b.numerator_label || "Numerador") + '</div><div class="drill-stat-value">' + fmt(b.numerator) + '</div></div>';
      html += '<div class="drill-stat"><div class="drill-stat-label">' + esc(b.denominator_label || "Denominador") + '</div><div class="drill-stat-value">' + fmt(b.denominator, Math.abs((num(b.denominator) || 0) % 1) > 0.001 ? 1 : 0) + '</div></div>';
      html += '<div class="drill-stat"><div class="drill-stat-label">Periodo</div><div class="drill-stat-value">' + esc(periodLabel(win)) + '</div></div>';
      html += '<div class="drill-stat"><div class="drill-stat-label">Método</div><div class="drill-stat-value">' + esc(method) + '</div></div>';
      html += '</div></div>';
      html += '<div class="drill-panel drill-method"><div class="method-card"><div class="method-title">Cómo se obtuvo</div><div class="method-copy"><strong>' + esc(method) + '.</strong> ' + esc(formula) + '</div><div class="method-copy">' + esc(b.aggregation || "El valor usa el periodo seleccionado y únicamente datos con base calculable.") + '</div></div>';
      html += componentHtml(b.components || []);
      html += '<div class="method-card"><div class="method-title">Desglose mensual / del periodo</div></div>' + monthlyTableHtml(b.monthly || []);
      html += excludedHtml(b.excluded || []);
      html += '</div>';
      return html;
    }
    function aggregateMonthlyRows(key) {
      var planteles = get(state.data, ["planteles"], []);
      var buckets = {};
      for (var p = 0; p < planteles.length; p += 1) {
        var metric = metricFromPlantel(planteles[p], key);
        var rows = get(metric, ["breakdown", "monthly"], []);
        for (var i = 0; i < rows.length; i += 1) {
          var row = rows[i] || {};
          var period = row.period || row.label || ("periodo-" + i);
          if (!buckets[period]) buckets[period] = { period: period, label: row.label || period, scores: [], numerator: 0, denominator: 0, unavailable: 0, planteles: 0 };
          var bucket = buckets[period];
          var score = num(row.score);
          if (score !== null) bucket.scores.push(score);
          else bucket.unavailable += 1;
          if (num(row.numerator) !== null) bucket.numerator += Number(row.numerator);
          if (num(row.denominator) !== null) bucket.denominator += Number(row.denominator);
          bucket.planteles += 1;
        }
      }
      var out = [];
      var keys = Object.keys(buckets).sort();
      for (var k = 0; k < keys.length; k += 1) {
        var b = buckets[keys[k]];
        out.push({
          period: b.period,
          label: b.label,
          score: average(b.scores),
          numerator: b.numerator,
          denominator: b.denominator,
          numerator_label: "Suma de numeradores visibles",
          denominator_label: "Suma de denominadores visibles",
          available_days: b.scores.length,
          business_days: b.planteles,
          unavailable_reason: b.unavailable ? (b.unavailable + " planteles sin cálculo") : null,
        });
      }
      return out;
    }
    function plantelScoreCards(key) {
      var planteles = get(state.data, ["planteles"], []);
      var html = '<div class="plantel-score-grid">';
      for (var i = 0; i < planteles.length; i += 1) {
        var row = planteles[i];
        var metric = metricFromPlantel(row, key);
        var active = row.plantel === state.selectedPlantel ? " active" : "";
        html += '<button class="plantel-score-card' + active + '" data-plantel="' + esc(row.plantel) + '" data-metric="' + esc(key) + '"><div class="plantel-score-top"><div><strong>' + esc(row.plantel) + '</strong><div class="plantel-name">' + esc(row.resolved_name || "") + '</div></div><div class="score ' + metricColor(metric) + '">' + pct(metric.score) + '</div></div><div class="cell-label">' + esc(metric.label || metric.detail || "Sin datos") + '</div></button>';
      }
      return html + '</div>';
    }
    function renderAggregateDrilldown(key) {
      var aggregate = get(state.data, ["aggregate"], {});
      var metric = key === "general" ? get(aggregate, ["corporate_index"], {}) : get(aggregate, ["domain_scores", key], {});
      var win = get(aggregate, ["window"], {});
      var planteles = get(state.data, ["planteles"], []);
      var calculatedCount = 0;
      for (var cp = 0; cp < planteles.length; cp += 1) if (num(metricFromPlantel(planteles[cp], key).score) !== null) calculatedCount += 1;
      byId("drilldownTitle").textContent = (METRIC_LABELS[key] || key) + " · todos los planteles";
      byId("drilldownSubtitle").textContent = "Promedio corporativo del periodo " + periodRangeText(win) + ". Selecciona un plantel abajo para abrir la evidencia exacta de su celda.";
      var b = {
        method: "Promedio corporativo",
        formula: "Promedio de planteles con cálculo real para la métrica seleccionada.",
        aggregation: "Esta vista resume la métrica por plantel. Para defender una celda específica, abre el plantel correspondiente.",
        numerator: calculatedCount,
        denominator: planteles.length,
        numerator_label: "Planteles con cálculo",
        denominator_label: "Planteles evaluados",
        monthly: aggregateMonthlyRows(key),
      };
      var html = '<div class="drill-panel"><div class="drill-kicker">Corporativo</div><div class="drill-heading">' + esc(METRIC_LABELS[key] || key) + '</div><div class="drill-score score ' + metricColor(metric) + '">' + pct(metric.score) + '</div><div class="drill-note">' + esc(evidenceReason(metric, b)) + '</div><div class="drill-stat-grid"><div class="drill-stat"><div class="drill-stat-label">Periodo</div><div class="drill-stat-value">' + esc(periodLabel(win)) + '</div></div><div class="drill-stat"><div class="drill-stat-label">Planteles</div><div class="drill-stat-value">' + fmt(b.numerator) + ' / ' + fmt(b.denominator) + '</div></div></div></div>';
      html += '<div class="drill-panel drill-method"><div class="method-card"><div class="method-title">Vista por plantel</div><div class="method-copy">El encabezado muestra una lectura corporativa. La reconciliación exacta del score final está disponible en cada celda.</div></div>' + plantelScoreCards(key) + '<div class="method-card"><div class="method-title">Historia mensual corporativa</div></div>' + monthlyTableHtml(b.monthly) + '</div>';
      byId("drilldownBody").innerHTML = html;
      var cards = document.querySelectorAll(".plantel-score-card[data-plantel][data-metric]");
      for (var c = 0; c < cards.length; c += 1) cards[c].addEventListener("click", function () {
        state.selectedPlantel = this.getAttribute("data-plantel");
        state.selectedMetric = this.getAttribute("data-metric") || "general";
        state.drilldownMode = "cell";
        renderMatrix();
        renderDrilldown();
      });
    }
    function renderDrilldown() {
      var planteles = get(state.data, ["planteles"], []);
      if (!planteles.length) {
        byId("drilldownBody").innerHTML = '<div class="empty-note">Sin planteles para auditar.</div>';
        return;
      }
      if (!state.selectedMetric) state.selectedMetric = "general";
      if (state.drilldownMode === "metric") {
        renderAggregateDrilldown(state.selectedMetric);
        return;
      }
      var plantel = findPlantel(state.selectedPlantel);
      if (!plantel) return;
      state.selectedPlantel = plantel.plantel;
      var key = state.selectedMetric || "general";
      var metric = metricFromPlantel(plantel, key);
      var win = get(state.data, ["aggregate", "window"], {});
      byId("drilldownTitle").textContent = (METRIC_LABELS[key] || key) + " · " + plantel.plantel;
      byId("drilldownSubtitle").textContent = "Evidencia del periodo " + periodRangeText(win) + " para " + (plantel.resolved_name || plantel.plantel) + ".";
      byId("drilldownBody").innerHTML = plantelMetricHtml(plantel, key, metric);
    }

    function printDocumentCss() {
      return [
        "@page { size: landscape; margin: 8mm; }",
        "* { box-sizing: border-box; -webkit-print-color-adjust: exact; print-color-adjust: exact; }",
        "body { margin: 0; color: #17212b; background: #fff; font-family: 'Montserrat', Arial, sans-serif; font-size: 7pt; line-height: 1.28; }",
        "h1,h2,h3,.score,.brand-title { font-family: 'Fredoka', 'Montserrat', Arial, sans-serif; letter-spacing: 0; }",
        ".cover { position: relative; padding: 0 0 4mm; border-bottom: .6pt solid #c7d2df; break-inside: avoid; }",
        ".cover:before { content: ''; display: block; height: 3.5pt; background: linear-gradient(90deg, #008f5a 0 40%, #2f6fe4 40% 62%, #c88400 62% 80%, #ef6b5b 80% 100%); margin: 0 0 3mm; }",
        ".brand { display: flex; justify-content: space-between; align-items: center; gap: 8mm; }",
        ".brand-left { display: flex; align-items: center; gap: 4mm; }",
        ".logo { width: 32mm; height: auto; }",
        ".brand-title { font-size: 15pt; font-weight: 700; }",
        ".badge { border: .6pt solid rgba(0,143,90,.3); border-radius: 999px; padding: 1.2mm 2.3mm; color: #0a6b45; font-weight: 800; }",
        ".hero { display: grid; grid-template-columns: minmax(0,1fr) 38mm; gap: 5mm; margin-top: 4mm; align-items: end; }",
        "h1 { margin: 0; font-size: 18pt; line-height: 1.02; }",
        ".summary { margin-top: 1.8mm; max-width: 186mm; color: #465365; font-weight: 600; }",
        ".meta { display: grid; grid-template-columns: repeat(3,1fr); gap: 2mm; margin-top: 2.4mm; max-width: 190mm; }",
        ".meta-tile,.kpi,.seal { border: .6pt solid #dfe6ee; border-radius: 2mm; padding: 2mm; background: #fff; }",
        ".label { color: #67717f; font-size: 5.6pt; text-transform: uppercase; font-weight: 800; }",
        ".value { margin-top: .8mm; font-weight: 800; font-size: 7.2pt; }",
        ".seal { min-height: 31mm; background: linear-gradient(135deg, rgba(0,143,90,.09), rgba(47,111,228,.06)); display: flex; flex-direction: column; justify-content: space-between; }",
        ".seal .score { font-size: 22pt; }",
        ".signals { display: grid; grid-template-columns: repeat(3,1fr); gap: 2.2mm; margin-top: 2.4mm; max-width: 190mm; }",
        ".signal { border-top: .8pt solid #dfe6ee; padding-top: 1.4mm; }",
        ".signal strong { display: block; margin-top: .5mm; font-size: 8.6pt; }",
        ".signal .score { font-size: 9.5pt; font-weight: 800; }",
        ".kpis { display: none; }",
        ".kpi .score { font-size: 14pt; margin-top: .8mm; }",
        ".section { padding-top: 2.6mm; break-inside: avoid; }",
        ".section.break { break-before: page; }",
        ".section-head { display: flex; justify-content: space-between; gap: 4mm; align-items: end; margin-bottom: 1.6mm; }",
        "h2 { margin: 0; font-size: 12.8pt; line-height: 1.05; }",
        ".copy { margin-top: .8mm; color: #67717f; max-width: 170mm; }",
        ".period { display: inline-block; margin-left: 1.2mm; padding: .7mm 1.7mm; border-radius: 999px; background: #eaf1ff; color: #274e9a; font-family: 'Montserrat', Arial; font-size: 6.3pt; font-weight: 800; }",
        ".legend { display: flex; gap: 1.5mm; flex-wrap: wrap; justify-content: flex-end; }",
        ".stamp { border: .6pt solid #dfe6ee; border-radius: 999px; padding: .8mm 1.5mm; font-weight: 800; white-space: nowrap; }",
        ".matrix { width: 100%; border-collapse: separate; border-spacing: 1.05mm; table-layout: fixed; }",
        ".matrix th { padding: 1.2mm .7mm; border: .6pt solid #dfe6ee; border-radius: 1.8mm; background: #f5f7fa; color: #526071; font-size: 5pt; line-height: 1.08; text-transform: uppercase; font-weight: 800; text-align: center; }",
        ".matrix th:first-child { width: 25mm; text-align: left; }",
        ".matrix td { height: 12.4mm; min-width: 0; padding: 1.05mm; border: .6pt solid rgba(23,33,43,.12); border-radius: 1.8mm; vertical-align: top; overflow: hidden; }",
        ".plantel-code { font-family: 'Fredoka'; font-size: 9.5pt; font-weight: 700; }",
        ".plantel-name { margin-top: .3mm; color: #67717f; font-size: 4.8pt; line-height: 1.08; }",
        ".heat-cell-top { display: flex; justify-content: space-between; gap: 1mm; }",
        ".cell-score { font-size: 8pt; line-height: 1; font-weight: 700; }",
        ".cell-status { border-radius: 999px; background: rgba(255,255,255,.66); padding: .45mm .7mm; color: #374558; font-size: 4.1pt; line-height: 1; font-weight: 800; text-transform: uppercase; white-space: nowrap; }",
        ".cell-label { margin-top: .6mm; color: #3e4b5b; font-size: 4.65pt; line-height: 1.1; }",
        ".info-title { font-size: 5.2pt; font-weight: 800; line-height: 1.1; }",
        ".score.green { color: #008f5a; } .score.yellow { color: #c88400; } .score.red { color: #c63d35; } .score.gray { color: #667085; }",
        ".method-grid { display: grid; grid-template-columns: repeat(2,1fr); gap: 2.2mm 5mm; }",
        ".method-item { border-top: .6pt solid #dfe6ee; padding-top: 1.5mm; }",
        ".method-name { font-weight: 800; font-size: 7pt; }",
        ".method-copy { margin-top: .5mm; color: #67717f; font-size: 6pt; line-height: 1.22; }",
        ".trace-grid { display: grid; grid-template-columns: 50mm minmax(0,1fr); gap: 4mm; }",
        ".trace-card { border: .6pt solid #dfe6ee; border-radius: 2mm; padding: 2mm; }",
        ".month-table { width: 100%; border-collapse: collapse; }",
        ".month-table th,.month-table td { border-bottom: .6pt solid #dfe6ee; padding: 1.2mm; text-align: left; vertical-align: top; }",
        ".month-table th { background: #f6f8fa; color: #526071; font-size: 5.2pt; text-transform: uppercase; }",
      ].join("\n");
    }
    function printMatrixHtml() {
      var rows = get(state.data, ["matrix"], []);
      var html = '<table class="matrix"><thead><tr><th>Plantel</th>';
      for (var h = 0; h < METRIC_ORDER.length; h += 1) html += '<th>' + esc(METRIC_LABELS[METRIC_ORDER[h]] || METRIC_ORDER[h]) + '</th>';
      html += '</tr></thead><tbody>';
      for (var r = 0; r < rows.length; r += 1) {
        var row = rows[r];
        html += '<tr><td><div class="plantel-code">' + esc(row.plantel) + '</div><div class="plantel-name">' + esc(row.name || "") + '</div></td>';
        for (var c = 0; c < METRIC_ORDER.length; c += 1) {
          var metric = get(row, ["cells", METRIC_ORDER[c]], {});
          html += '<td class="heat ' + metricColor(metric) + '"' + heatStyle(metric) + '>' + metricCellHtml(metric) + '</td>';
        }
        html += '</tr>';
      }
      return html + '</tbody></table>';
    }
    function printMethodologyHtml() {
      var html = '<div class="method-grid">';
      for (var i = 0; i < METRIC_ORDER.length; i += 1) {
        var key = METRIC_ORDER[i];
        html += '<div class="method-item"><div class="method-name">' + esc(METRIC_LABELS[key] || key) + '</div><div class="method-copy">' + esc(METRIC_DESCRIPTIONS[key] || "Métrica del reporte ejecutivo.") + '</div></div>';
      }
      return html + '</div>';
    }
    function printTraceHtml() {
      var plantel = findPlantel(state.selectedPlantel);
      if (!plantel) return "";
      var key = state.selectedMetric || "general";
      var metric = metricFromPlantel(plantel, key);
      var b = breakdownOf(metric);
      var rows = b.monthly || [];
      var html = '<section class="section"><div class="section-head"><div><div class="label">Trazabilidad seleccionada</div><h2>' + esc(METRIC_LABELS[key] || key) + ' · ' + esc(plantel.plantel) + '</h2><div class="copy">' + esc(evidenceReason(metric, b)) + '</div></div><div class="score ' + metricColor(metric) + '" style="font-size:18pt">' + pct(metric.score) + '</div></div>';
      html += '<div class="trace-grid"><div class="trace-card"><div class="label">' + esc(b.numerator_label || "Numerador") + '</div><div class="value">' + fmt(b.numerator) + '</div><div class="label" style="margin-top:2mm">' + esc(b.denominator_label || "Denominador") + '</div><div class="value">' + fmt(b.denominator, Math.abs((num(b.denominator) || 0) % 1) > 0.001 ? 1 : 0) + '</div><div class="label" style="margin-top:2mm">Método</div><div class="value">' + esc(b.method || "Valor del periodo") + '</div></div><div>';
      html += '<table class="month-table"><thead><tr><th>Periodo</th><th>Score</th><th>Numerador</th><th>Denominador</th><th>Notas</th></tr></thead><tbody>';
      for (var i = 0; i < rows.length; i += 1) {
        var row = rows[i] || {};
        html += '<tr><td>' + esc(row.label || row.period || "—") + '</td><td class="score ' + colorFor(row.score) + '">' + pct(row.score) + '</td><td>' + fmt(row.numerator) + '</td><td>' + fmt(row.denominator, Math.abs((num(row.denominator) || 0) % 1) > 0.001 ? 1 : 0) + '</td><td>' + esc(row.unavailable_reason || "") + '</td></tr>';
      }
      html += '</tbody></table></div></div></section>';
      return html;
    }
    function buildPrintDocument() {
      var aggregate = get(state.data, ["aggregate"], {});
      var general = get(aggregate, ["corporate_index"], {});
      var win = get(aggregate, ["window"], {});
      var best = aggregate.best_plantel || {};
      var worst = aggregate.worst_plantel || {};
      var generated = get(state.data, ["generated_at"], null);
      var generatedText = generated ? new Date(generated).toLocaleString("es-MX") : "—";
      var title = periodLabel(win);
      var signals = '<div class="signals"><div class="signal"><div class="label">General</div><strong class="score ' + metricColor(general) + '">' + pct(general.score) + '</strong><div class="copy">' + esc(general.detail || general.traffic_label || "Sin datos") + '</div></div><div class="signal"><div class="label">Mejor plantel</div><strong class="score ' + metricColor(best) + '">' + pct(best.score) + ' · ' + esc(best.plantel || "—") + '</strong><div class="copy">' + esc(best.resolved_name || "—") + '</div></div><div class="signal"><div class="label">Foco de seguimiento</div><strong class="score ' + metricColor(worst) + '">' + pct(worst.score) + ' · ' + esc(worst.plantel || "—") + '</strong><div class="copy">' + esc(worst.resolved_name || "—") + '</div></div></div>';
      return '<!doctype html><html lang="es"><head><meta charset="utf-8"><title>Reporte SIPAE PDF</title><link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@500;600;700&family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet"><style>' + printDocumentCss() + '</style></head><body>' +
        '<section class="cover"><div class="brand"><div class="brand-left"><img class="logo" src="https://sipae.casitaapps.com/logo-h.png" alt="SIPAE Casita"><div class="brand-title">Reporte SIPAE</div></div><div class="badge">Informe institucional</div></div><div class="hero"><div><h1>Índice corporativo de cumplimiento</h1><div class="summary">' + esc(executiveSummaryText(aggregate, general)) + '</div><div class="meta"><div class="meta-tile"><div class="label">Periodo</div><div class="value">' + esc(title) + '<br>' + esc(periodRangeText(win)) + '</div></div><div class="meta-tile"><div class="label">Días hábiles</div><div class="value">' + esc(win.business_days || 0) + '</div></div><div class="meta-tile"><div class="label">Generado</div><div class="value">' + esc(generatedText) + '</div></div></div>' + signals + '</div><div class="seal"><div><div class="label">Índice general</div><div class="score ' + metricColor(general) + '">' + pct(general.score) + '</div></div><div class="copy">' + esc(general.detail || general.traffic_label || "Sin datos") + '</div></div></div></section>' +
        '<section class="section"><div class="section-head"><div><div class="label">Centro ejecutivo</div><h2>Mapa de cumplimiento <span class="period">' + esc(title) + '</span></h2><div class="copy">Mapa institucional con colores PDF-safe y trazabilidad de métrica preservada.</div></div><div class="legend"><span class="stamp">85-100 sano</span><span class="stamp">70-84 atención</span><span class="stamp">1-69 crítico</span></div></div>' + printMatrixHtml() + '</section>' +
        '<section class="section break"><div class="section-head"><div><div class="label">Notas de lectura</div><h2>Metodología de métricas</h2><div class="copy">Resumen autocontenido para lectura ejecutiva del PDF.</div></div></div>' + printMethodologyHtml() + '</section>' +
        printTraceHtml() +
        '<script>window.addEventListener("load",function(){setTimeout(function(){window.focus();window.print();},350);});<\/script></body></html>';
    }
    function printExecutiveReport() {
      if (!state.data) { window.print(); return; }
      var printWindow = window.open("", "sipae_print_report");
      if (!printWindow) { window.print(); return; }
      printWindow.document.open();
      printWindow.document.write(buildPrintDocument());
      printWindow.document.close();
    }
    window.buildSipaePrintDocument = buildPrintDocument;

    initControls();
    loadReport(false);
  </script>
</body>
</html>
'''
