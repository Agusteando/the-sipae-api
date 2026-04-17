# Raw HTML string safely moved out of application layer
TEST_HUB_HTML = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SIPAE API Hub - Panel de Pruebas</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root { --bg: #1e1e2f; --panel: #2a2a40; --text: #e0e0e0; --accent: #ff4757; --accent-hover: #ff6b81; --success: #2ecc71; --warning: #f1c40f; --info: #3498db; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: var(--bg); color: var(--text); margin: 0; padding: 20px; }
        h1 { text-align: center; color: #fff; margin-bottom: 5px; }
        p.subtitle { text-align: center; color: #a0a0b0; margin-top: 0; margin-bottom: 30px; }
        
        .controls { background: var(--panel); padding: 20px; border-radius: 8px; display: flex; flex-wrap: wrap; gap: 15px; justify-content: center; align-items: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3); margin-bottom: 30px; }
        .controls label { font-weight: bold; font-size: 14px; color: #bbb; }
        .controls select, .controls input { background: #1e1e2f; color: #fff; border: 1px solid #444; padding: 8px 12px; border-radius: 4px; outline: none; }
        .controls button { background: var(--accent); color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-weight: bold; transition: 0.3s; }
        .controls button:hover { background: var(--accent-hover); }

        /* Grid System */
        .dashboard-grid { display: grid; grid-template-columns: 1fr 2fr; gap: 20px; max-width: 1300px; margin: 0 auto; display: none; }
        .card { background: var(--panel); padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); overflow: hidden; }
        .card h3 { margin-top: 0; border-bottom: 1px solid #444; padding-bottom: 10px; color: #fff; }
        
        /* Text & Status */
        .kpi-row { display: flex; justify-content: space-around; flex-wrap: wrap; margin-bottom: 15px; }
        .kpi-box { text-align: center; background: #1e1e2f; padding: 15px; border-radius: 6px; min-width: 120px; }
        .kpi-box .val { font-size: 26px; font-weight: bold; color: var(--info); }
        .kpi-box .lbl { font-size: 12px; color: #aaa; text-transform: uppercase; margin-top: 5px; }
        
        .status-indicator { text-align: center; padding: 15px; border-radius: 8px; margin-bottom: 15px; font-size: 18px; font-weight: bold; border: 2px solid transparent; }
        .status-complete { background: rgba(46, 204, 113, 0.1); border-color: var(--success); color: var(--success); }
        .status-missing { background: rgba(231, 76, 60, 0.1); border-color: var(--accent); color: var(--accent); }

        /* Tags for missing groups */
        .tag-container { display: flex; flex-wrap: wrap; gap: 8px; justify-content: center; margin-top: 15px; }
        .tag { background: rgba(255,255,255,0.1); padding: 5px 12px; border-radius: 20px; font-size: 14px; font-weight: bold; border: 1px solid #555; }
        .tag.tag-red { border-color: var(--accent); color: var(--accent); background: rgba(255, 71, 87, 0.1); }

        /* Tables */
        table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 14px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #444; }
        th { background-color: #1e1e2f; color: #bbb; position: sticky; top: 0; }
        tr:hover { background-color: rgba(255,255,255,0.05); }
        .table-container { max-height: 350px; overflow-y: auto; margin-bottom: 15px; }

        /* Code Area */
        #rawJson { background: #1e1e2f; padding: 15px; border-radius: 5px; font-family: monospace; color: #a8ff60; overflow-x: auto; max-height: 300px; }
        
        /* Context Views */
        .view-layer { display: none; }
        .view-layer.active { display: contents; }
        .full-col { grid-column: 1 / -1; }
        
        /* Chart Wrappers */
        .chart-container-relative { position: relative; height: 300px; width: 100%; }
    </style>
</head>
<body>

    <h1>🐺 SIPAE API Hub</h1>
    <p class="subtitle">Panel Central de Microservicios Operativos</p>

    <div class="controls">
        <div>
            <label>Servicio API:</label>
            <select id="apiSelector" onchange="clearDashboards()">
                <option value="attendance-detail">Asistencia - Análisis Detallado</option>
                <option value="husky">Husky Pass - Tasa de Captura</option>
            </select>
        </div>
        <div>
            <label>Plantel:</label>
            <select id="plantel">
                <option value="PT">Primaria Toluca (PT)</option>
                <option value="PM">Primaria Metepec (PM)</option>
                <option value="ST">Secundaria Toluca (ST)</option>
                <option value="SM">Secundaria Metepec (SM)</option>
                <option value="CT">ISSSTE Toluca (CT)</option>
                <option value="CM">ISSSTE Metepec (CM)</option>
            </select>
        </div>
        <div>
            <label>Desde:</label>
            <input type="date" id="startDate">
        </div>
        <div>
            <label>Hasta:</label>
            <input type="date" id="endDate">
        </div>
        <button onclick="fetchData()">⚡ Ejecutar Consulta</button>
    </div>

    <div class="dashboard-grid" id="mainDashboard">
        
        <!-- HUSKY PASS VIEW -->
        <div id="viewHusky" class="view-layer">
            <div class="card">
                <h3>📊 Tasa de Captura (Medidor)</h3>
                <canvas id="gaugeChart"></canvas>
                <div style="text-align:center; margin-top:15px;" id="gaugeStats"></div>
            </div>
            <div class="card">
                <h3>📈 Tendencia Diaria (Entradas vs Salidas)</h3>
                <!-- The wrapper below stops Chart.js from infinitely resizing the canvas -->
                <div class="chart-container-relative">
                    <canvas id="barChart"></canvas>
                </div>
            </div>
        </div>

        <!-- ATTENDANCE VIEW -->
        <div id="viewAttendance" class="view-layer">
            
            <!-- Range Mode Only: Trend Chart & Day Selector -->
            <div class="card full-col" id="attRangeWrapper" style="display:none;">
                <h3>📅 Tendencia Mensual / Rango de Fechas</h3>
                <div class="chart-container-relative" style="height: 250px;">
                    <canvas id="attendanceRangeChart"></canvas>
                </div>
                
                <div style="margin-top: 20px; background: #1e1e2f; padding: 15px; border-radius: 6px; text-align: center;">
                    <label style="font-weight:bold; margin-right:10px;">Inspeccionar día específico:</label>
                    <select id="daySelector" onchange="renderSpecificDay()"></select>
                </div>
            </div>

            <!-- Shared Daily Details (Used by Daily mode AND specific day selection in Range mode) -->
            <div class="card" id="attStatusCard" style="display:none;">
                <h3 id="attDayTitle">📋 Cobertura y KPIs del Día</h3>
                <div id="attendanceStatus" class="status-indicator"></div>
                
                <div class="kpi-row" id="kpiContainer"></div>
                
                <div id="missingGroupsContainer" style="display:none; margin-top: 15px; border-top: 1px solid #444; padding-top: 15px;">
                    <p style="text-align: center; color: #bbb; margin-bottom: 10px;">Grupos Faltantes Identificados:</p>
                    <div class="tag-container" id="missingGroupsList"></div>
                </div>
            </div>

            <div class="card" id="attDataCard" style="display:none;">
                <h3>📂 Desglose por Grado y Grupo</h3>
                <div class="table-container">
                    <table id="groupsTable">
                        <thead>
                            <tr>
                                <th>Grupo</th>
                                <th>Total Alumnos</th>
                                <th>Asistencia</th>
                                <th>Ausencia</th>
                                <th>Presencial</th>
                                <th>Virtual</th>
                            </tr>
                        </thead>
                        <tbody></tbody>
                    </table>
                </div>

                <h3 style="margin-top: 20px;">⚠️ Listado de Ausencias</h3>
                <div class="table-container">
                    <table id="absentsTable">
                        <thead>
                            <tr>
                                <th>Alumno</th>
                                <th>Grupo</th>
                                <th>Motivo</th>
                            </tr>
                        </thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>

        </div>

        <!-- RAW JSON LOG -->
        <div class="card full-col">
            <h3>⚙️ Carga de Datos en Crudo (JSON)</h3>
            <pre id="rawJson"></pre>
        </div>

    </div>

    <script>
        document.getElementById('startDate').valueAsDate = new Date();
        document.getElementById('endDate').valueAsDate = new Date();

        let charts = { gauge: null, bar: null, attRange: null };
        let currentGlobalData = null; 

        function clearDashboards() {
            document.getElementById('mainDashboard').style.display = 'none';
            if(charts.gauge) charts.gauge.destroy();
            if(charts.bar) charts.bar.destroy();
            if(charts.attRange) charts.attRange.destroy();
        }

        async function fetchData() {
            const api = document.getElementById('apiSelector').value;
            const plantel = document.getElementById('plantel').value;
            const start = document.getElementById('startDate').value;
            const end = document.getElementById('endDate').value;
            
            let url = '';
            if (api === 'husky') {
                url = `/api/v1/husky/scans/daily-rate?plantel=${plantel}&start_date=${start}&end_date=${end}`;
            } else {
                url = `/api/v1/attendance/detail?plantel=${plantel}&start_date=${start}&end_date=${end}`;
            }
            
            try {
                const response = await fetch(url);
                const data = await response.json();
                currentGlobalData = data;
                
                document.getElementById('mainDashboard').style.display = 'grid';
                document.getElementById('rawJson').innerText = JSON.stringify(data, null, 2);

                document.querySelectorAll('.view-layer').forEach(el => el.classList.remove('active'));
                
                if (api === 'husky') {
                    document.getElementById('viewHusky').classList.add('active');
                    renderHusky(data);
                } else {
                    document.getElementById('viewAttendance').classList.add('active');
                    renderAttendance(data);
                }
            } catch (error) {
                alert('Error en la conexión. Revisa la consola.');
                console.error(error);
            }
        }

        function renderHusky(data) {
            const pop = data.expected_population;
            const dates = Object.keys(data.daily_datapoints);
            let totalEntradas = 0, entradasArr = [], salidasArr = [];

            dates.forEach(date => {
                const dayData = data.daily_datapoints[date];
                totalEntradas += dayData.entrada;
                entradasArr.push(dayData.entrada);
                salidasArr.push(dayData.salida);
            });

            const capturedAvg = dates.length > 0 ? Math.round(totalEntradas / dates.length) : 0;
            const missingAvg = pop > capturedAvg ? pop - capturedAvg : 0;
            const percent = pop > 0 ? Math.round((capturedAvg / pop) * 100) : 0;

            const ctxGauge = document.getElementById('gaugeChart').getContext('2d');
            charts.gauge = new Chart(ctxGauge, {
                type: 'doughnut',
                data: {
                    labels: ['Promedio Entradas', 'Faltantes'],
                    datasets: [{ data: [capturedAvg, missingAvg], backgroundColor: ['#2ecc71', '#e74c3c'], borderWidth: 0 }]
                },
                options: { rotation: -90, circumference: 180, cutout: '75%', plugins: { legend: { labels: {color: '#fff'} } } }
            });

            document.getElementById('gaugeStats').innerHTML = `
                <div style="font-size:32px; font-weight:bold; color:var(--accent);">${percent}%</div>
                <div style="color:#aaa; font-size:14px;">del total esperado de ${pop}</div>
            `;

            const ctxBar = document.getElementById('barChart').getContext('2d');
            charts.bar = new Chart(ctxBar, {
                type: 'bar',
                data: {
                    labels: dates,
                    datasets: [
                        { label: 'Entradas', data: entradasArr, backgroundColor: '#3498db' },
                        { label: 'Salidas', data: salidasArr, backgroundColor: '#f1c40f' }
                    ]
                },
                options: { responsive: true, maintainAspectRatio: false, scales: { y: { grid: { color: '#444' }, ticks: { color: '#ccc'} }, x: { grid: { display: false }, ticks: { color: '#ccc'} } }, plugins: { legend: { labels: { color: '#fff' } } } }
            });
        }

        function renderAttendance(data) {
            const isDaily = data.mode === 'daily';
            
            document.getElementById('attRangeWrapper').style.display = isDaily ? 'none' : 'block';
            document.getElementById('attStatusCard').style.display = 'block';
            document.getElementById('attDataCard').style.display = 'block';
            
            if (isDaily) {
                injectDailyDetails(data, data.date_range.start);
            } else {
                const dates = Object.keys(data.daily_points);
                const sel = document.getElementById('daySelector');
                sel.innerHTML = dates.map(d => `<option value="${d}">${d}</option>`).join('');
                
                let assistArr = [], absArr = [];
                dates.forEach(d => {
                    assistArr.push(data.daily_points[d].summary.asistencia);
                    absArr.push(data.daily_points[d].summary.ausencia);
                });

                if(charts.attRange) charts.attRange.destroy();
                const ctxR = document.getElementById('attendanceRangeChart').getContext('2d');
                charts.attRange = new Chart(ctxR, {
                    type: 'line',
                    data: {
                        labels: dates,
                        datasets: [
                            { label: 'Asistencias', data: assistArr, borderColor: '#2ecc71', backgroundColor: 'rgba(46, 204, 113, 0.1)', fill: true, tension: 0.3 },
                            { label: 'Ausencias', data: absArr, borderColor: '#e74c3c', backgroundColor: 'transparent', borderDash: [5,5], tension: 0.3 }
                        ]
                    },
                    options: { responsive: true, maintainAspectRatio: false, scales: { y: { grid: { color: '#444' }, ticks: { color: '#ccc'} }, x: { grid: { display: false }, ticks: { color: '#ccc'} } }, plugins: { legend: { labels: { color: '#fff' } } } }
                });

                if (dates.length > 0) renderSpecificDay();
            }
        }

        function renderSpecificDay() {
            const day = document.getElementById('daySelector').value;
            if(currentGlobalData && currentGlobalData.daily_points[day]) {
                injectDailyDetails(currentGlobalData.daily_points[day], day);
            }
        }

        function injectDailyDetails(dayObj, dateLabel) {
            document.getElementById('attDayTitle').innerText = `📋 Cobertura del ${dateLabel}`;
            
            const statusEl = document.getElementById('attendanceStatus');
            const missingContainer = document.getElementById('missingGroupsContainer');
            const tagsContainer = document.getElementById('missingGroupsList');
            const md = dayObj.missing_groups_data;

            if (md.is_complete) {
                statusEl.className = 'status-indicator status-complete';
                statusEl.innerHTML = `✅ Cobertura Completa (${md.completion_percent}%)`;
                missingContainer.style.display = 'none';
            } else {
                statusEl.className = 'status-indicator status-missing';
                statusEl.innerHTML = `⚠️ Faltan ${md.missing_groups_count} Grupos (${md.completion_percent}%)`;
                missingContainer.style.display = 'block';
                tagsContainer.innerHTML = md.missing_groups.map(g => `<span class="tag tag-red">${g.grado}° ${g.grupo}</span>`).join('');
            }

            const sum = dayObj.summary;
            document.getElementById('kpiContainer').innerHTML = `
                <div class="kpi-box"><div class="val" style="color:var(--text);">${sum.total_students}</div><div class="lbl">Esperados</div></div>
                <div class="kpi-box"><div class="val" style="color:var(--success);">${sum.asistencia}</div><div class="lbl">Asistencias</div></div>
                <div class="kpi-box"><div class="val" style="color:var(--accent);">${sum.ausencia}</div><div class="lbl">Ausencias</div></div>
                <div class="kpi-box"><div class="val" style="color:#f39c12;">${sum.presencial}</div><div class="lbl">Presencial</div></div>
                <div class="kpi-box"><div class="val" style="color:#9b59b6;">${sum.virt}</div><div class="lbl">Virtual</div></div>
            `;

            const gBody = document.querySelector('#groupsTable tbody');
            if (dayObj.groups.length === 0) {
                gBody.innerHTML = `<tr><td colspan="6" style="text-align:center; color:#888;">No hay capturas para esta fecha.</td></tr>`;
            } else {
                gBody.innerHTML = dayObj.groups.map(g => `
                    <tr>
                        <td><b>${g.grado}° ${g.grupo}</b></td>
                        <td>${g.total_students_per_group}</td>
                        <td style="color:var(--success);">${g.asistencia}</td>
                        <td style="color:var(--accent);">${g.ausencia}</td>
                        <td>${g.presencial}</td>
                        <td>${g.virt}</td>
                    </tr>
                `).join('');
            }

            const aBody = document.querySelector('#absentsTable tbody');
            if (dayObj.absent_students.length === 0) {
                aBody.innerHTML = `<tr><td colspan="3" style="text-align:center; color:#888;">No se registraron ausencias.</td></tr>`;
            } else {
                aBody.innerHTML = dayObj.absent_students.map(a => `
                    <tr>
                        <td>${a.name}</td>
                        <td><b>${a.grado}° ${a.grupo}</b></td>
                        <td><span class="tag">${a.motivo || 'N/A'}</span></td>
                    </tr>
                `).join('');
            }
        }
    </script>
</body>
</html>
"""