import aiomysql
from datetime import date
from typing import Dict, List
from core.database import get_attendance_db_connection


SAPF_MONTHLY_BY_DEPARTMENT_SQL = """
    SELECT
        COALESCE(dm.department_name, src.department_email, 'Sin Departamento') AS department_name,
        YEAR(src.fecha) AS year,
        MONTH(src.fecha) AS month,
        DATE_FORMAT(src.fecha, '%%Y-%%m') AS period,
        COUNT(*) AS conteo
    FROM (
        SELECT fa.department_email, fa.fecha
        FROM fichas_atencion fa
        WHERE fa.school_code = %s
          AND fa.fecha >= %s
          AND fa.fecha < DATE_ADD(%s, INTERVAL 1 DAY)
          AND fa.department_email IS NOT NULL
          AND fa.department_email <> ''

        UNION ALL

        SELECT se.department_email, se.fecha
        FROM seguimiento se
        WHERE se.school_code = %s
          AND se.fecha >= %s
          AND se.fecha < DATE_ADD(%s, INTERVAL 1 DAY)
          AND se.department_email IS NOT NULL
          AND se.department_email <> ''
    ) src
    LEFT JOIN deptos_map dm
           ON dm.email = src.department_email
          AND dm.campus = %s
    GROUP BY department_name, year, month, period
    ORDER BY department_name ASC, year ASC, month ASC
"""

SAPF_MOTIVOS_BY_DEPARTMENT_SQL = """
    SELECT
        COALESCE(dm.department_name, fa.department_email, fa.area, 'Sin Departamento') AS department_name,
        COALESCE(NULLIF(TRIM(fa.motivo), ''), 'Sin Motivo') AS motivo,
        COUNT(*) AS conteo
    FROM fichas_atencion fa
    LEFT JOIN deptos_map dm
           ON dm.email = fa.department_email
          AND dm.campus = %s
    WHERE fa.school_code = %s
      AND fa.fecha >= %s
      AND fa.fecha < DATE_ADD(%s, INTERVAL 1 DAY)
    GROUP BY department_name, motivo
    ORDER BY conteo DESC, department_name ASC, motivo ASC
"""


async def get_sapf_monthly_stats(map_campus: str, data_campuses: List[str], start_date: date, end_date: date) -> list:
    """
    Recupera actividad SAPF con la semántica legacy:
    - deptos_map.campus usa el campus lógico.
    - fichas_atencion.school_code y seguimiento.school_code usan uno o más
      códigos físicos exactos.
    - PM se consolida desde PMA + PMB.
    - La métrica suma fichas_atencion + seguimiento por departamento.
    """
    rows: List[Dict] = []
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            for data_campus in data_campuses:
                await cur.execute(
                    SAPF_MONTHLY_BY_DEPARTMENT_SQL,
                    (
                        data_campus,
                        start_date,
                        end_date,
                        data_campus,
                        start_date,
                        end_date,
                        map_campus,
                    ),
                )
                rows.extend(await cur.fetchall())
        return rows
    finally:
        conn.close()


async def get_sapf_motivos_stats(map_campus: str, data_campuses: List[str], start_date: date, end_date: date) -> list:
    """
    Extrae motivos SAPF usando códigos físicos exactos y deptos_map para que el
    área/departamento coincida con la UI legacy.
    """
    rows: List[Dict] = []
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            for data_campus in data_campuses:
                await cur.execute(
                    SAPF_MOTIVOS_BY_DEPARTMENT_SQL,
                    (map_campus, data_campus, start_date, end_date),
                )
                rows.extend(await cur.fetchall())
        return rows
    finally:
        conn.close()
