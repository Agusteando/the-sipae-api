from datetime import date, timedelta
from core.utils import resolve_plantel
from core.logger import get_logger
from integrations.external_bot import fetch_expected_groups
from .repository import fetch_attendance_data

logger = get_logger("service.attendance")

async def get_attendance_detail_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Business logic for attendance metrics, group summarization, and coverage gap detection.
    Now rigorously enforcing the scope output contract.
    """
    logger.info(f"Starting extraction for {plantel} ({start_date} to {end_date} - Scope: {scope})")

    plantel_info = resolve_plantel(plantel)
    is_daily = (start_date == end_date)

    expected_groups_list = await fetch_expected_groups(plantel_info["sheets_code"])
    expected_set = {(g["grado"], g["grupo"]) for g in expected_groups_list}
    total_expected = len(expected_set)

    stats_results, absents_results = await fetch_attendance_data(
        plantel_info["db_code"], start_date, end_date
    )

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

    for row in stats_results:
        d_str = str(row['d_fecha'])
        if d_str not in daily_points: continue
        
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

        summ = daily_points[d_str]["summary"]
        summ["total_students"] += grp_data["total_students_per_group"]
        summ["asistencia"] += grp_data["asistencia"]
        summ["ausencia"] += grp_data["ausencia"]
        summ["ausencia2"] += grp_data["ausencia2"]
        summ["presencial"] += grp_data["presencial"]
        summ["virt"] += grp_data["virt"]
        summ["girls"] += grp_data["girls"]
        summ["boys"] += grp_data["boys"]

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
        del dt_obj["internal_actual_set"]
        dt_obj["groups"] = sorted(dt_obj["groups"], key=lambda x: (x["grado"], x["grupo"]))

    base_response = {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
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
            "daily_points": {}
        })
    else:
        base_response.update({
            "daily_points": daily_points
        })

    return base_response