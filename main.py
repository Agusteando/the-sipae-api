from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from typing import Optional
from datetime import date, timedelta
import httpx
import aiomysql

# Centralized imports
from core.config import settings
from core.database import get_husky_db_connection, get_attendance_db_connection

app = FastAPI(title="The SIPAE API Hub", version="1.3.0")

@app.get("/", include_in_schema=False)
async def redirect_root_to_hub():
    """Automatically redirect the base URL to our Test Hub."""
    return RedirectResponse(url="/test-hub")

# ==========================================
# 1. INTERNAL PLANTEL MAPPING
# ==========================================
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
    print(f"[DEBUG-API] Fetching population for sheets_code: {sheets_code}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                settings.external_bot_api_url, 
                json={"data": {"plantel": sheets_code}}, 
                timeout=15.0
            )
            if resp.status_code != 200: return 0
            data = resp.json()
            if not isinstance(data, list): return 0
            valid_students = [item for item in data if item.get('Grado') and item.get('Grupo')]
            return len(valid_students)
    except Exception as e:
        print(f"[DEBUG-API] Error fetching population: {e}")
        return 0

async def fetch_expected_groups(sheets_code: str) -> list:
    """Fetches unique expected grade-group combinations from external API."""
    print(f"[DEBUG-API] Fetching expected groups for sheets_code: {sheets_code}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                settings.external_bot_api_url, 
                json={"data": {"plantel": sheets_code}}, 
                timeout=15.0
            )
            if resp.status_code != 200:
                print(f"[DEBUG-API] API responded with {resp.status_code}")
                return []
            
            data = resp.json()
            if not isinstance(data, list):
                return []
            
            unique_groups = set()
            for item in data:
                g = item.get('Grado')
                gr = item.get('Grupo')
                if g and gr:
                    unique_groups.add((str(g).strip(), str(gr).strip()))
            
            return [{"grado": g, "grupo": gr} for g, gr in sorted(list(unique_groups))]
            
    except Exception as e:
        print(f"[DEBUG-API] Error fetching expected groups: {e}")
        return []

# ==========================================
# 3. API ENDPOINTS
# ==========================================

@app.get("/api/v1/husky/scans/daily-rate", tags=["Husky Pass"])
async def get_husky_daily_rate(
    plantel: str = Query(..., description="Internal Plantel Code"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    if not start_date: start_date = date.today()
    if not end_date: end_date = date.today()

    plantel_code_upper = plantel.upper()
    mapping = PLANTEL_MAP.get(plantel_code_upper)
    db_code = mapping["db_code"] if mapping else plantel_code_upper
    sheets_code = mapping["sheets_code"] if mapping else plantel_code_upper

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


@app.get("/api/v1/attendance/detail", tags=["Attendance"])
async def get_attendance_detail(
    plantel: str = Query(..., description="Internal Plantel Code (e.g., PT, SM)"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    """
    Comprehensive endpoint for attendance metrics including group summaries, 
    missing group coverage evaluation, and absent students details.
    Supports daily, monthly, and custom range modes.
    """
    if not start_date: start_date = date.today()
    if not end_date: end_date = date.today()

    print(f"[DEBUG-ATTENDANCE-DETAIL] Starting extraction for {plantel} ({start_date} to {end_date})")

    # 1. Resolve Plantel
    plantel_code_upper = plantel.upper()
    mapping = PLANTEL_MAP.get(plantel_code_upper)
    db_code = mapping["db_code"] if mapping else plantel_code_upper
    sheets_code = mapping["sheets_code"] if mapping else plantel_code_upper
    is_daily = (start_date == end_date)

    # 2. Fetch Expected Groups dynamically
    expected_groups_list = await fetch_expected_groups(sheets_code)
    expected_set = {(g["grado"], g["grupo"]) for g in expected_groups_list}
    total_expected = len(expected_set)
    print(f"[DEBUG-ATTENDANCE-DETAIL] Found {total_expected} expected unique groups.")

    # 3. Setup robust SQL queries using the explicit calculations required
    # Query A: Aggregated Group Statistics (using fallback for gender_list)
    query_stats_with_join = """
        SELECT
            DATE(A.fecha) as d_fecha,
            A.grado,
            A.grupo,
            COUNT(A.name) as total_students_per_group,
            SUM(CASE WHEN A.attendance = 1 THEN 1 ELSE 0 END) as asistencia,
            SUM(CASE WHEN A.attendance = 0 THEN 1 ELSE 0 END) as ausencia,
            SUM(CASE WHEN A.modalidad = 0 OR A.modalidad IS NULL THEN 1 ELSE 0 END) as ausencia2,
            SUM(CASE WHEN A.modalidad = 1 THEN 1 ELSE 0 END) as presencial,
            SUM(CASE WHEN A.modalidad = 2 THEN 1 ELSE 0 END) as virt,
            SUM(CASE WHEN B.gender IN ('F', 'FEMENINO', 'Mujer') THEN 1 ELSE 0 END) as girls,
            SUM(CASE WHEN B.gender IN ('M', 'MASCULINO', 'Hombre') THEN 1 ELSE 0 END) as boys
        FROM asistencia A
        LEFT JOIN gender_list B ON A.name = B.student
        WHERE A.plantel = %s AND DATE(A.fecha) BETWEEN %s AND %s
        GROUP BY DATE(A.fecha), A.grado, A.grupo
    """

    query_stats_fallback = """
        SELECT
            DATE(fecha) as d_fecha,
            grado,
            grupo,
            COUNT(name) as total_students_per_group,
            SUM(CASE WHEN attendance = 1 THEN 1 ELSE 0 END) as asistencia,
            SUM(CASE WHEN attendance = 0 THEN 1 ELSE 0 END) as ausencia,
            SUM(CASE WHEN modalidad = 0 OR modalidad IS NULL THEN 1 ELSE 0 END) as ausencia2,
            SUM(CASE WHEN modalidad = 1 THEN 1 ELSE 0 END) as presencial,
            SUM(CASE WHEN modalidad = 2 THEN 1 ELSE 0 END) as virt,
            0 as girls,
            0 as boys
        FROM asistencia
        WHERE plantel = %s AND DATE(fecha) BETWEEN %s AND %s
        GROUP BY DATE(fecha), grado, grupo
    """

    # Query B: Absent Students Detail
    query_absents = """
        SELECT DATE(fecha) as d_fecha, id, name, grado, grupo, plantel, motivo
        FROM asistencia
        WHERE attendance = 0 AND plantel = %s AND DATE(fecha) BETWEEN %s AND %s
        ORDER BY d_fecha ASC, grado ASC, grupo ASC
    """

    # 4. Execute Queries Safely
    stats_results = []
    absents_results = []
    conn = None
    try:
        conn = await get_attendance_db_connection()
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # Try Join first
            try:
                print(f"[DEBUG-ATTENDANCE-DETAIL] Executing Primary Join Query...")
                await cur.execute(query_stats_with_join, (db_code, start_date, end_date))
                stats_results = await cur.fetchall()
            except Exception as e:
                print(f"[DEBUG-ATTENDANCE-DETAIL] Join failed ({e}), using Fallback Query...")
                await cur.execute(query_stats_fallback, (db_code, start_date, end_date))
                stats_results = await cur.fetchall()

            # Execute Absents
            print(f"[DEBUG-ATTENDANCE-DETAIL] Executing Absents Query...")
            await cur.execute(query_absents, (db_code, start_date, end_date))
            absents_results = await cur.fetchall()

    except Exception as e:
        print(f"[DEBUG-ATTENDANCE-DETAIL] Database Error: {e}")
        raise HTTPException(status_code=500, detail=f"Database execution failed: {str(e)}")
    finally:
        if conn: conn.close()

    # 5. Process and Assemble Data into Sequential Days
    daily_points = {}
    current_date = start_date
    while current_date <= end_date:
        daily_points[str(current_date)] = {
            "summary": { "total_students": 0, "asistencia": 0, "ausencia": 0, "ausencia2": 0, "presencial": 0, "virt": 0, "girls": 0, "boys": 0 },
            "groups": [],
            "missing_groups_data": { "is_complete": False, "expected_groups_count": total_expected, "completed_groups_count": 0, "missing_groups_count": 0, "completion_percent": 0.0, "missing_groups": [] },
            "absent_students": [],
            "internal_actual_set": set()
        }
        current_date += timedelta(days=1)

    # 5a. Populate Groups and Accumulate Summary
    for row in stats_results:
        d_str = str(row['d_fecha'])
        if d_str not in daily_points: continue
        
        # Add to groups list
        grp_data = {
            "grado": str(row['grado']).strip(),
            "grupo": str(row['grupo']).strip(),
            "total_students_per_group": int(row['total_students_per_group']),
            "asistencia": int(row['asistencia']),
            "ausencia": int(row['ausencia']),
            "ausencia2": int(row['ausencia2']),
            "presencial": int(row['presencial']),
            "virt": int(row['virt']),
            "girls": int(row['girls']),
            "boys": int(row['boys'])
        }
        daily_points[d_str]["groups"].append(grp_data)
        daily_points[d_str]["internal_actual_set"].add((grp_data["grado"], grp_data["grupo"]))

        # Accumulate Summary
        summ = daily_points[d_str]["summary"]
        summ["total_students"] += grp_data["total_students_per_group"]
        summ["asistencia"] += grp_data["asistencia"]
        summ["ausencia"] += grp_data["ausencia"]
        summ["ausencia2"] += grp_data["ausencia2"]
        summ["presencial"] += grp_data["presencial"]
        summ["virt"] += grp_data["virt"]
        summ["girls"] += grp_data["girls"]
        summ["boys"] += grp_data["boys"]

    # 5b. Populate Absents
    for row in absents_results:
        d_str = str(row['d_fecha'])
        if d_str not in daily_points: continue
        daily_points[d_str]["absent_students"].append({
            "id": row["id"],
            "name": row["name"],
            "grado": str(row["grado"]).strip(),
            "grupo": str(row["grupo"]).strip(),
            "motivo": row["motivo"]
        })

    # 5c. Calculate Coverage Missing Logic
    for d_str, dt_obj in daily_points.items():
        actual_set = dt_obj["internal_actual_set"]
        missing_set = expected_set - actual_set
        
        total_miss = len(missing_set)
        total_comp = total_expected - total_miss
        is_comp = (total_miss == 0) and (total_expected > 0)
        pct = round((total_comp / total_expected * 100), 2) if total_expected > 0 else 0.0
        
        dt_obj["missing_groups_data"] = {
            "is_complete": is_comp,
            "expected_groups_count": total_expected,
            "completed_groups_count": total_comp,
            "missing_groups_count": total_miss,
            "completion_percent": pct,
            "missing_groups": [{"grado": g, "grupo": gr} for g, gr in sorted(list(missing_set))]
        }
        # Cleanup internal state
        del dt_obj["internal_actual_set"]
        
        # Sort groups logically
        dt_obj["groups"] = sorted(dt_obj["groups"], key=lambda x: (x["grado"], x["grupo"]))

    # 6. Build Final Payload structure based on Mode
    base_response = {
        "plantel_requested": plantel,
        "resolved_name": mapping["name"] if mapping else "Unknown",
        "mode": "daily" if is_daily else "range",
        "date_range": {"start": start_date, "end": end_date}
    }

    if is_daily:
        single_day_data = daily_points[str(start_date)]
        base_response.update({
            "summary": single_day_data["summary"],
            "groups": single_day_data["groups"],
            "missing_groups_data": single_day_data["missing_groups_data"],
            "absent_students": single_day_data["absent_students"],
            "daily_points": {} # Empty for daily mode compliance
        })
    else:
        base_response.update({
            "daily_points": daily_points
        })

    return base_response


# ==========================================
# 4. EXCLUSIVE TESTING UI (THE HUB DASHBOARD)
# ==========================================
@app.get("/test-hub", response_class=HTMLResponse, include_in_schema=False)
async def serve_test_hub():
    html_content = """
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
                    <canvas id="barChart"></canvas>
                </div>
            </div>

            <!-- ATTENDANCE VIEW -->
            <div id="viewAttendance" class="view-layer">
                
                <!-- Range Mode Only: Trend Chart & Day Selector -->
                <div class="card full-col" id="attRangeWrapper" style="display:none;">
                    <h3>📅 Tendencia Mensual / Rango de Fechas</h3>
                    <div style="height: 250px;"><canvas id="attendanceRangeChart"></canvas></div>
                    
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
            let currentGlobalData = null; // Stores API response for re-rendering specific days

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

            // =======================================
            // RENDER LOGIC: HUSKY PASS
            // =======================================
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

            // =======================================
            // RENDER LOGIC: ATTENDANCE DETAIL
            // =======================================
            function renderAttendance(data) {
                const isDaily = data.mode === 'daily';
                
                document.getElementById('attRangeWrapper').style.display = isDaily ? 'none' : 'block';
                document.getElementById('attStatusCard').style.display = 'block';
                document.getElementById('attDataCard').style.display = 'block';
                
                if (isDaily) {
                    injectDailyDetails(data, data.date_range.start);
                } else {
                    // Populate Chart & Select Dropdown for Range
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

                    // Render details for the first day by default
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
                
                // 1. Status Indicator
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

                // 2. KPIs
                const sum = dayObj.summary;
                document.getElementById('kpiContainer').innerHTML = `
                    <div class="kpi-box"><div class="val" style="color:var(--text);">${sum.total_students}</div><div class="lbl">Esperados</div></div>
                    <div class="kpi-box"><div class="val" style="color:var(--success);">${sum.asistencia}</div><div class="lbl">Asistencias</div></div>
                    <div class="kpi-box"><div class="val" style="color:var(--accent);">${sum.ausencia}</div><div class="lbl">Ausencias</div></div>
                    <div class="kpi-box"><div class="val" style="color:#f39c12;">${sum.presencial}</div><div class="lbl">Presencial</div></div>
                    <div class="kpi-box"><div class="val" style="color:#9b59b6;">${sum.virt}</div><div class="lbl">Virtual</div></div>
                `;

                // 3. Groups Table
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

                // 4. Absents Table
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
    return html_content

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)