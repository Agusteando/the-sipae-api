
    var PLANTELES = ["PT", "PM", "ST", "SM", "PREET", "PREEM"];
    var METRIC_ORDER = ["general", "roll_call", "student_attendance", "scans", "student_punctuality", "staff_attendance", "planning", "observations", "sapf"];
    var METRIC_LABELS = { general: "General", roll_call: "Pase de lista", student_attendance: "Asistencia alumnos", scans: "Escaneos", student_punctuality: "Puntualidad alumnos", staff_attendance: "Asistencia personal", planning: "Planeaciones", observations: "Observaciones", sapf: "SAPF" };
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
      if (!isFinite(parsed) || parsed <= 0) return null;
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
      renderTrend();
      renderDetail();
      renderDiagnostic();
    }
    function renderHero() {
      var aggregate = get(state.data, ["aggregate"], {});
      var general = get(aggregate, ["corporate_index"], {});
      var win = get(aggregate, ["window"], {});
      byId("periodStamp").textContent = (win.start || "—") + " → " + (win.end || "—");
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
          html += '<td class="heat ' + metricColor(metric) + '" data-plantel="' + esc(row.plantel) + '"><div class="cell-score score ' + metricColor(metric) + '">' + pct(metric.score) + '</div><div class="cell-label">' + esc(metric.label || "Sin datos") + '</div></td>';
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
      byId("trendText").textContent = "Promedio del periodo seleccionado.";
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
  