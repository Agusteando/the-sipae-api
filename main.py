from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from typing import Optional
from datetime import date, timedelta
import httpx
import aiomysql

# Centralized imports
from core.config import settings
from core.database import get_husky_db_connection, get_attendance_db_connection

app = FastAPI(title="The SIPAE API Hub", version="1.2.0")

@app.get("/", include_in_schema=False)
async def redirect_root_to_hub():
    """Automatically redirect the base URL to our Test Hub."""
    return RedirectResponse(url="/test-hub")

# ==========================================
# 1. INTERNAL PLANTEL MAPPING
# ==========================================
# Maps standard internal codes to variations across different systems
PLANTEL_MAP = {
    "PM": {"db_code": "PM", "sheets_code": "PM", "name": "Primaria Metepec"},
    "PT": {"db_code": "PT", "sheets_code": "PT", "name": "Primaria Toluca"},
    "SM": {"db_code": "SM", "sheets_code": "SM", "name": "Secundaria Metepec"},
    "ST": {"db_code": "ST", "sheets_code": "ST", "name": "Secundaria Toluca"},
    "CM": {"db_code": "CM", "sheets_code": "CM", "name": "ISSSTE Metepec"},
    "CT": {"db_code": "CT", "sheets_code": "CT", "name": "ISSSTE Toluca"},
    "01": {"db_code": "01", "sheets_code": "PT", "name": "Primaria Toluca (01)"}, 
}

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
async def fetch_expected_population(sheets_code: str) -> int:
    """Fetches total active students per plantel from external API."""
    print(f"[DEBUG-HUSKY] Fetching population for sheets_code: {sheets_code}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                settings.external_bot_api_url, 
                json={"data": {"plantel": sheets_code}}, 
                timeout=15.0
            )
            if resp.status_code != 200:
                return 0
            
            data = resp.json()
            if not isinstance(data, list):
                return 0
            
            valid_students = [item for item in data if item.get('Grado') and item.get('Grupo')]
            return len(valid_students)
    except Exception as e:
        print(f"[DEBUG-HUSKY] Error fetching population: {e}")
        return 0

async def fetch_expected_groups(sheets_code: str) -> list:
    """Fetches unique expected grade-group combinations from external API."""
    print(f"[DEBUG-ATTENDANCE] Fetching expected groups for sheets_code: {sheets_code}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                settings.external_bot_api_url, 
                json={"data": {"plantel": sheets_code}}, 
                timeout=15.0
            )
            if resp.status_code != 200:
                print(f"[DEBUG-ATTENDANCE] API responded with {resp.status_code}")
                return []
            
            data = resp.json()
            if not isinstance(data, list):
                return []
            
            unique_groups = set()
            for item in data:
                g = item.get('Grado')
                gr = item.get('Grupo')
                if g and gr:
                    # Normalize formats securely
                    unique_groups.add((str(g).strip(), str(gr).strip()))
            
            # Return sorted structure
            return [{"grado": g, "grupo": gr} for g, gr in sorted(list(unique_groups))]
            
    except Exception as e:
        print(f"[DEBUG-ATTENDANCE] Error fetching expected groups: {e}")
        return []

# ==========================================
# 3. API ENDPOINTS
# ==========================================

@app.get("/api/v1/husky/scans/daily-rate", tags=["Husky Pass"])
async def get_husky_daily_rate(
    plantel: str = Query(..., description="Internal Plantel Code (e.g., PT, SM, CM, 01)"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    """
    Returns the rate of Husky Pass scans against the total expected population.
    """
    if not start_date:
        start_date = date.today()
    if not end_date:
        end_date = date.today()

    plantel_code_upper = plantel.upper()
    mapping = PLANTEL_MAP.get(plantel_code_upper)
    
    if not mapping:
        db_code = plantel_code_upper
        sheets_code = plantel_code_upper
    else:
        db_code = mapping["db_code"]
        sheets_code = mapping["sheets_code"]

    total_population = await fetch_expected_population(sheets_code)

    query = """
        SELECT 
            DATE(A.timestamp) as fecha,
            A.type as tipo_accion,
            COUNT(DISTINCT B.id) as total_scans
        FROM acceso A
        LEFT JOIN personas_autorizadas pa ON pa.id = A.ss_id
        LEFT JOIN users B ON pa.user_id = B.id
        WHERE B.plantel LIKE %s
          AND DATE(A.timestamp) BETWEEN %s AND %s
        GROUP BY DATE(A.timestamp), A.type
        ORDER BY DATE(A.timestamp) ASC
    """
    
    try:
        conn = await get_husky_db_connection()
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (f"{db_code}%", start_date, end_date))
            results = await cur.fetchall()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database Error: {str(e)}")

    daily_data = {}
    for row in results:
        f_date = str(row['fecha'])
        if f_date not in daily_data:
            daily_data[f_date] = {"entrada": 0, "salida": 0, "rate_entrada_percent": 0.0}
        
        tipo = row['tipo_accion'].lower()
        if tipo in ['entrada', 'salida']:
            daily_data[f_date][tipo] = row['total_scans']
            
            if tipo == 'entrada' and total_population > 0:
                rate = (row['total_scans'] / total_population) * 100
                daily_data[f_date]["rate_entrada_percent"] = round(rate, 2)

    return {
        "plantel_requested": plantel,
        "resolved_name": mapping["name"] if mapping else "Unknown",
        "expected_population": total_population,
        "date_range": {"start": start_date, "end": end_date},
        "daily_datapoints": daily_data
    }


@app.get("/api/v1/attendance/coverage", tags=["Attendance"])
async def get_attendance_coverage(
    plantel: str = Query(..., description="Internal Plantel Code (e.g., PT, SM, CM)"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    """
    Determines if attendance has been taken completely by finding missing groups.
    Calculates expected groups dynamically and queries actual groups day by day.
    """
    if not start_date:
        start_date = date.today()
    if not end_date:
        end_date = date.today()

    print(f"[DEBUG-ATTENDANCE] Requested coverage for {plantel} from {start_date} to {end_date}")

    # 1. Resolve mappings
    plantel_code_upper = plantel.upper()
    mapping = PLANTEL_MAP.get(plantel_code_upper)
    
    if not mapping:
        db_code = plantel_code_upper
        sheets_code = plantel_code_upper
    else:
        db_code = mapping["db_code"]
        sheets_code = mapping["sheets_code"]

    # 2. Fetch expected unique groups asynchronously
    expected_groups = await fetch_expected_groups(sheets_code)
    expected_set = {(g["grado"], g["grupo"]) for g in expected_groups}
    total_expected = len(expected_set)

    print(f"[DEBUG-ATTENDANCE] Found {total_expected} expected groups for {sheets_code}")

    # 3. Setup sequential dates dictionary to guarantee all days are returned
    daily_actuals = {}
    current_date = start_date
    while current_date <= end_date:
        daily_actuals[str(current_date)] = set()
        current_date += timedelta(days=1)

    # 4. Query the Attendance Database (control_coordinaciones)
    query = """
        SELECT 
            DATE(fecha) as d_fecha,
            grado,
            grupo
        FROM asistencia
        WHERE plantel = %s
          AND DATE(fecha) BETWEEN %s AND %s
        GROUP BY DATE(fecha), grado, grupo
    """
    
    try:
        conn = await get_attendance_db_connection()
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (db_code, start_date, end_date))
            results = await cur.fetchall()
        conn.close()
    except Exception as e:
        print(f"[DEBUG-ATTENDANCE] Error querying database: {e}")
        raise HTTPException(status_code=500, detail=f"Database Error: {str(e)}")

    # Populate actual grouped records per day
    for row in results:
        d_str = str(row['d_fecha'])
        if d_str in daily_actuals:
            daily_actuals[d_str].add((str(row['grado']).strip(), str(row['grupo']).strip()))

    # 5. Process coverage math day by day
    coverage_datapoints = {}
    for d, actuals_set in daily_actuals.items():
        missing_set = expected_set - actuals_set
        total_missing = len(missing_set)
        total_completed = total_expected - total_missing
        
        is_complete = (total_missing == 0) and (total_expected > 0)
        percentage = round((total_completed / total_expected * 100), 2) if total_expected > 0 else 0.0
        
        # Sort missing for predictable output
        missing_list = [{"grado": g, "grupo": gr} for g, gr in sorted(list(missing_set))]
        
        coverage_datapoints[d] = {
            "is_complete": is_complete,
            "total_expected": total_expected,
            "total_completed": total_completed,
            "total_missing": total_missing,
            "completion_percent": percentage,
            "missing_groups": missing_list
        }

    return {
        "plantel_requested": plantel,
        "resolved_name": mapping["name"] if mapping else "Unknown",
        "date_range": {"start": start_date, "end": end_date},
        "expected_groups": expected_groups,
        "daily_datapoints": coverage_datapoints
    }


# ==========================================
# 4. EXCLUSIVE TESTING UI (THE HUB DASHBOARD)
# ==========================================
@app.get("/test-hub", response_class=HTMLResponse, include_in_schema=False)
async def serve_test_hub():
    """
    A dedicated visual interface to test API endpoints independently of /docs.
    Includes context-aware UI switching for Husky and Attendance endpoints.
    """
    html_content = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>SIPAE API Hub - Panel de Pruebas</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            :root { --bg: #1e1e2f; --panel: #2a2a40; --text: #e0e0e0; --accent: #ff4757; --accent-hover: #ff6b81; --success: #2ecc71; --warning: #f1c40f; }
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: var(--bg); color: var(--text); margin: 0; padding: 20px; }
            h1 { text-align: center; color: #fff; margin-bottom: 5px; }
            p.subtitle { text-align: center; color: #a0a0b0; margin-top: 0; margin-bottom: 30px; }
            
            .controls { background: var(--panel); padding: 20px; border-radius: 8px; display: flex; flex-wrap: wrap; gap: 15px; justify-content: center; align-items: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3); margin-bottom: 30px; }
            .controls label { font-weight: bold; font-size: 14px; color: #bbb; }
            .controls select, .controls input { background: #1e1e2f; color: #fff; border: 1px solid #444; padding: 8px 12px; border-radius: 4px; outline: none; }
            .controls button { background: var(--accent); color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-weight: bold; transition: 0.3s; }
            .controls button:hover { background: var(--accent-hover); }

            /* Grid System */
            .dashboard-grid { display: grid; grid-template-columns: 1fr 2fr; gap: 20px; max-width: 1200px; margin: 0 auto; display: none; }
            .dashboard-full { grid-template-columns: 1fr; }
            .card { background: var(--panel); padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
            .card h3 { margin-top: 0; border-bottom: 1px solid #444; padding-bottom: 10px; color: #fff; }
            
            /* Text & Status */
            .stats-text { text-align: center; margin-top: 15px; }
            .stats-text .big-number { font-size: 32px; font-weight: bold; color: var(--accent); }
            
            .status-indicator { text-align: center; padding: 20px; border-radius: 8px; margin-bottom: 15px; font-size: 20px; font-weight: bold; border: 2px solid transparent; }
            .status-complete { background: rgba(46, 204, 113, 0.1); border-color: var(--success); color: var(--success); }
            .status-missing { background: rgba(231, 76, 60, 0.1); border-color: var(--accent); color: var(--accent); }

            /* Tags for missing groups */
            .tag-container { display: flex; flex-wrap: wrap; gap: 8px; justify-content: center; margin-top: 15px; }
            .tag { background: rgba(255,255,255,0.1); padding: 5px 12px; border-radius: 20px; font-size: 14px; font-weight: bold; border: 1px solid #555; }
            .tag.tag-red { border-color: var(--accent); color: var(--accent); background: rgba(255, 71, 87, 0.1); }

            /* Code Area */
            #rawJson { background: #1e1e2f; padding: 15px; border-radius: 5px; font-family: monospace; color: #a8ff60; overflow-x: auto; max-height: 300px; }
            
            /* Context Views */
            .view-layer { display: none; }
            .view-layer.active { display: contents; }
        </style>
    </head>
    <body>

        <h1>🐺 SIPAE API Hub</h1>
        <p class="subtitle">Panel Central de Microservicios Operativos</p>

        <div class="controls">
            <div>
                <label>Servicio API:</label>
                <select id="apiSelector" onchange="clearDashboards()">
                    <option value="attendance">Asistencia - Cobertura Diaria</option>
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

        <!-- MAIN DASHBOARD WRAPPER -->
        <div class="dashboard-grid" id="mainDashboard">
            
            <!-- HUSKY PASS VIEW -->
            <div id="viewHusky" class="view-layer">
                <div class="card">
                    <h3>📊 Tasa de Captura (Medidor)</h3>
                    <canvas id="gaugeChart"></canvas>
                    <div class="stats-text" id="gaugeStats"></div>
                </div>
                <div class="card">
                    <h3>📈 Tendencia Diaria (Entradas vs Salidas)</h3>
                    <canvas id="barChart"></canvas>
                </div>
            </div>

            <!-- ATTENDANCE VIEW -->
            <div id="viewAttendance" class="view-layer">
                <div class="card" id="attendanceSingleDayCard" style="display: none;">
                    <h3>📋 Estado de Captura de Asistencia</h3>
                    <div id="attendanceStatus" class="status-indicator"></div>
                    <div class="stats-text" id="attendanceStats"></div>
                    
                    <div id="missingGroupsContainer" style="display: none; margin-top: 20px; border-top: 1px solid #444; padding-top: 15px;">
                        <p style="text-align: center; color: #bbb; margin-bottom: 10px;">Grupos Faltantes Identificados:</p>
                        <div class="tag-container" id="missingGroupsList"></div>
                    </div>
                </div>
                <div class="card" id="attendanceRangeCard" style="display: none;">
                    <h3>📅 Cobertura de Asistencia por Día</h3>
                    <canvas id="attendanceRangeChart"></canvas>
                </div>
            </div>

            <!-- RAW JSON LOG -->
            <div class="card" style="grid-column: 1 / -1;">
                <h3>⚙️ Carga de Datos en Crudo (JSON)</h3>
                <pre id="rawJson"></pre>
            </div>

        </div>

        <script>
            // Init Dates
            document.getElementById('startDate').valueAsDate = new Date();
            document.getElementById('endDate').valueAsDate = new Date();

            let charts = { gauge: null, bar: null, attRange: null };

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
                    url = `/api/v1/attendance/coverage?plantel=${plantel}&start_date=${start}&end_date=${end}`;
                }
                
                try {
                    const response = await fetch(url);
                    const data = await response.json();
                    
                    document.getElementById('mainDashboard').style.display = 'grid';
                    document.getElementById('rawJson').innerText = JSON.stringify(data, null, 2);

                    document.querySelectorAll('.view-layer').forEach(el => el.classList.remove('active'));
                    
                    if (api === 'husky') {
                        document.getElementById('viewHusky').classList.add('active');
                        renderHusky(data);
                    } else {
                        document.getElementById('viewAttendance').classList.add('active');
                        renderAttendance(data, start, end);
                    }
                } catch (error) {
                    alert('Error en la conexión. Revisa la consola.');
                    console.error(error);
                }
            }

            // =======================================
            // RENDER LOGIC: HUSKY PASS
            // =======================================
            function renderHusky(data) {
                const pop = data.expected_population;
                const dates = Object.keys(data.daily_datapoints);
                
                let totalEntradas = 0;
                let entradasArr = [];
                let salidasArr = [];

                dates.forEach(date => {
                    const dayData = data.daily_datapoints[date];
                    totalEntradas += dayData.entrada;
                    entradasArr.push(dayData.entrada);
                    salidasArr.push(dayData.salida);
                });

                const avgEntradas = dates.length > 0 ? (totalEntradas / dates.length) : 0;
                const capturedAvg = Math.round(avgEntradas);
                const missingAvg = pop > capturedAvg ? pop - capturedAvg : 0;
                const percent = pop > 0 ? Math.round((capturedAvg / pop) * 100) : 0;

                const ctxGauge = document.getElementById('gaugeChart').getContext('2d');
                charts.gauge = new Chart(ctxGauge, {
                    type: 'doughnut',
                    data: {
                        labels: ['Asistencias (Promedio)', 'Faltantes'],
                        datasets: [{ data: [capturedAvg, missingAvg], backgroundColor: ['#2ecc71', '#e74c3c'], borderWidth: 0 }]
                    },
                    options: { rotation: -90, circumference: 180, cutout: '75%', plugins: { legend: { position: 'bottom', labels: {color: '#fff'} } } }
                });

                document.getElementById('gaugeStats').innerHTML = `
                    <div class="big-number">${percent}%</div>
                    <div style="color:#aaa; font-size:14px;">del total esperado de ${pop} alumnos</div>
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
                    options: { responsive: true, scales: { y: { beginAtZero: true, grid: { color: '#444' }, ticks: { color: '#ccc'} }, x: { grid: { display: false }, ticks: { color: '#ccc'} } }, plugins: { legend: { labels: { color: '#fff' } } } }
                });
            }

            // =======================================
            // RENDER LOGIC: ATTENDANCE COVERAGE
            // =======================================
            function renderAttendance(data, start, end) {
                const dates = Object.keys(data.daily_datapoints);
                const isSingleDay = start === end;
                
                const cardSingle = document.getElementById('attendanceSingleDayCard');
                const cardRange = document.getElementById('attendanceRangeCard');

                if (isSingleDay) {
                    cardSingle.style.display = 'block';
                    cardRange.style.display = 'none';
                    // We can span 2 columns since range is hidden
                    cardSingle.style.gridColumn = "1 / -1";

                    const dayData = data.daily_datapoints[dates[0]];
                    const statusEl = document.getElementById('attendanceStatus');
                    const missingContainer = document.getElementById('missingGroupsContainer');
                    const tagsContainer = document.getElementById('missingGroupsList');

                    if (dayData.is_complete) {
                        statusEl.className = 'status-indicator status-complete';
                        statusEl.innerHTML = '✅ Registros Completos';
                        missingContainer.style.display = 'none';
                    } else {
                        statusEl.className = 'status-indicator status-missing';
                        statusEl.innerHTML = `⚠️ Faltan ${dayData.total_missing} Grupos por Capturar`;
                        
                        missingContainer.style.display = 'block';
                        tagsContainer.innerHTML = dayData.missing_groups.map(g => 
                            `<span class="tag tag-red">${g.grado}° ${g.grupo}</span>`
                        ).join('');
                    }

                    document.getElementById('attendanceStats').innerHTML = `
                        <div class="big-number" style="color: ${dayData.is_complete ? 'var(--success)' : 'var(--accent)'}">${dayData.completion_percent}%</div>
                        <div style="color:#aaa; font-size:14px;">${dayData.total_completed} de ${dayData.total_expected} grupos capturados hoy</div>
                    `;

                } else {
                    cardSingle.style.display = 'none';
                    cardRange.style.display = 'block';
                    cardRange.style.gridColumn = "1 / -1";

                    let compArr = [];
                    let missArr = [];
                    
                    dates.forEach(d => {
                        compArr.push(data.daily_datapoints[d].total_completed);
                        missArr.push(data.daily_datapoints[d].total_missing);
                    });

                    const ctxAtt = document.getElementById('attendanceRangeChart').getContext('2d');
                    charts.attRange = new Chart(ctxAtt, {
                        type: 'bar',
                        data: {
                            labels: dates,
                            datasets: [
                                { label: 'Grupos Capturados', data: compArr, backgroundColor: '#2ecc71', stack: 'Stack 0' },
                                { label: 'Grupos Faltantes', data: missArr, backgroundColor: '#e74c3c', stack: 'Stack 0' }
                            ]
                        },
                        options: {
                            responsive: true,
                            scales: {
                                y: { stacked: true, beginAtZero: true, grid: { color: '#444' }, ticks: { color: '#ccc', stepSize: 1 } },
                                x: { stacked: true, grid: { display: false }, ticks: { color: '#ccc' } }
                            },
                            plugins: { legend: { labels: { color: '#fff' } } }
                        }
                    });
                }
            }
        </script>
    </body>
    </html>
    """
    return html_content

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)