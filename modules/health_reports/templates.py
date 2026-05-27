from html import escape
from typing import Any, Dict, List


def _status_style(status: str) -> Dict[str, str]:
    if status == "critical":
        return {"bg": "#3b0a18", "border": "#fb7185", "text": "#fb7185", "badge": "CRÍTICO"}
    if status == "warning":
        return {"bg": "#2b1d07", "border": "#f59e0b", "text": "#fbbf24", "badge": "ATENCIÓN"}
    return {"bg": "#06251a", "border": "#22c55e", "text": "#86efac", "badge": "CUMPLIDO"}


def _detail_rows(details: List[Dict[str, Any]]) -> str:
    if not details:
        return ""
    rows = []
    for item in details[:8]:
        left = escape(str(item.get("label") or item.get("name") or item.get("docente") or item.get("grupo") or ""))
        right = escape(str(item.get("value") or item.get("status") or item.get("detail") or ""))
        sub = escape(str(item.get("sub") or ""))
        rows.append(f"""
          <tr>
            <td style="padding:10px 0;border-top:1px solid #1f2937;color:#f8fafc;font-weight:700;font-size:14px;line-height:18px;">{left}<div style="color:#94a3b8;font-weight:600;font-size:12px;margin-top:3px;">{sub}</div></td>
            <td style="padding:10px 0;border-top:1px solid #1f2937;color:#f87171;font-weight:800;font-size:13px;text-align:right;white-space:nowrap;">{right}</td>
          </tr>
        """)
    return f"<table role='presentation' width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;margin-top:12px;'>{''.join(rows)}</table>"


def render_report_html(model: Dict[str, Any], open_url: str = "", click_url: str = "") -> str:
    plantel = escape(str(model.get("plantel_code") or ""))
    name = escape(str(model.get("resolved_name") or plantel))
    report_date = escape(str(model.get("report_date") or ""))
    generated_at = escape(str(model.get("generated_at") or ""))
    overall = model.get("overall_status") or "fulfilled"
    overall_style = _status_style(overall)
    top = model.get("top_insight") or {}
    cta = click_url or model.get("dashboard_url") or "https://sipae.casitaapps.com"

    cards_html = []
    for card in model.get("cards", []):
        style = _status_style(card.get("status") or "fulfilled")
        count = escape(str(card.get("count") if card.get("count") is not None else ""))
        title = escape(str(card.get("title") or ""))
        headline = escape(str(card.get("headline") or ""))
        summary = escape(str(card.get("summary") or ""))
        details = _detail_rows(card.get("details") or [])
        cards_html.append(f"""
        <div style="background:#0f172a;border:1px solid #1f2937;border-radius:24px;padding:22px;margin:0 0 18px 0;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
            <tr>
              <td style="vertical-align:top;">
                <div style="color:#f8fafc;font-size:18px;font-weight:900;letter-spacing:-0.02em;">{title}</div>
                <div style="color:#94a3b8;font-size:12px;font-weight:800;text-transform:uppercase;letter-spacing:0.16em;margin-top:3px;">{headline}</div>
              </td>
              <td style="vertical-align:top;text-align:right;width:120px;">
                <div style="display:inline-block;background:{style['bg']};border:1px solid {style['border']};color:{style['text']};border-radius:999px;padding:6px 10px;font-size:10px;font-weight:900;letter-spacing:0.12em;">{style['badge']}</div>
                <div style="color:{style['text']};font-size:40px;line-height:42px;font-weight:900;letter-spacing:-0.08em;margin-top:10px;">{count}</div>
              </td>
            </tr>
          </table>
          <div style="color:#cbd5e1;font-size:14px;line-height:21px;font-weight:600;margin-top:12px;">{summary}</div>
          {details}
        </div>
        """)

    pixel = f"<img src=\"{escape(open_url)}\" width=\"1\" height=\"1\" style=\"display:none;max-height:1px;overflow:hidden;\" alt=\"\" />" if open_url else ""

    return f"""<!doctype html>
<html lang="es">
  <head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
  <body style="margin:0;background:#020617;padding:24px;font-family:Arial,Helvetica,sans-serif;color:#f8fafc;">
    <div style="display:none;max-height:0;overflow:hidden;color:#020617;">{escape(str(model.get('preheader') or 'Cierre operativo SIPAE'))}</div>
    <div style="max-width:760px;margin:0 auto;background:#020617;">
      <div style="border:1px solid #1e293b;border-radius:28px;overflow:hidden;background:linear-gradient(180deg,#0b1220 0%,#020617 100%);">
        <div style="padding:30px 30px 20px 30px;border-bottom:1px solid #1e293b;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
            <tr>
              <td>
                <div style="font-size:34px;font-weight:900;letter-spacing:-0.07em;line-height:34px;color:#ffffff;">Cierre SIPAE</div>
                <div style="font-size:14px;color:#94a3b8;font-weight:700;margin-top:8px;">{name} · {plantel} · {report_date}</div>
              </td>
              <td style="text-align:right;vertical-align:top;">
                <div style="display:inline-block;background:{overall_style['bg']};border:1px solid {overall_style['border']};color:{overall_style['text']};border-radius:999px;padding:8px 12px;font-size:11px;font-weight:900;letter-spacing:0.13em;">{overall_style['badge']}</div>
              </td>
            </tr>
          </table>
        </div>

        <div style="padding:26px 30px;">
          <div style="background:#111827;border:1px solid #334155;border-radius:24px;padding:24px;margin-bottom:20px;">
            <div style="font-size:11px;color:#64748b;font-weight:900;text-transform:uppercase;letter-spacing:0.18em;">Insight principal</div>
            <div style="font-size:26px;line-height:29px;color:#ffffff;font-weight:900;letter-spacing:-0.05em;margin-top:8px;">{escape(str(top.get('title') or 'Indicadores revisados'))}</div>
            <div style="font-size:14px;line-height:21px;color:#cbd5e1;font-weight:600;margin-top:10px;">{escape(str(top.get('body') or 'Consulta el detalle de cierre operativo.'))}</div>
          </div>

          {''.join(cards_html)}

          <div style="padding:24px 0 6px 0;text-align:center;">
            <a href="{escape(cta)}" style="display:inline-block;background:#2563eb;color:#ffffff;text-decoration:none;border-radius:18px;padding:15px 24px;font-size:13px;font-weight:900;text-transform:uppercase;letter-spacing:0.14em;">Ver Panel de Salud SIPAE</a>
          </div>
          <div style="color:#64748b;text-align:center;font-size:11px;font-weight:700;margin-top:16px;">Generado {generated_at}. Los gerentes reciben copia directa de este cierre.</div>
        </div>
      </div>
    </div>
    {pixel}
  </body>
</html>"""


HEALTH_REPORTS_UI_HTML = """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Reportes SIPAE</title>
  <style>
    :root{--bg:#020617;--panel:#0f172a;--line:#1f2937;--text:#f8fafc;--muted:#94a3b8;--blue:#2563eb;--rose:#fb7185;--green:#22c55e;--amber:#f59e0b}
    *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,system-ui,Segoe UI,Arial,sans-serif;padding:28px}
    h1{font-size:42px;line-height:42px;letter-spacing:-.07em;margin:0 0 8px} p{color:var(--muted)}
    .bar,.card{background:rgba(15,23,42,.9);border:1px solid var(--line);border-radius:24px;padding:18px;margin-bottom:18px}.bar{display:flex;gap:12px;flex-wrap:wrap;align-items:end}
    label{display:block;font-size:11px;text-transform:uppercase;letter-spacing:.14em;color:var(--muted);font-weight:900;margin-bottom:6px}
    input,select{background:#020617;border:1px solid #334155;color:var(--text);border-radius:12px;padding:11px 12px;font-weight:700;min-width:160px}
    button{background:var(--blue);color:white;border:0;border-radius:14px;padding:12px 16px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;cursor:pointer}button.secondary{background:#1e293b}button.warn{background:var(--amber);color:#111827}
    .grid{display:grid;grid-template-columns:360px 1fr;gap:18px}.list{max-height:640px;overflow:auto}.row{border-bottom:1px solid var(--line);padding:12px 0;cursor:pointer}.row:hover{background:rgba(255,255,255,.03)}
    .pill{display:inline-block;border-radius:99px;padding:4px 8px;font-size:10px;font-weight:900;text-transform:uppercase;letter-spacing:.1em}.critical{background:#3b0a18;color:var(--rose);border:1px solid var(--rose)}.warning{background:#2b1d07;color:#fbbf24;border:1px solid var(--amber)}.fulfilled{background:#06251a;color:#86efac;border:1px solid var(--green)}
    iframe{width:100%;height:760px;border:1px solid var(--line);border-radius:20px;background:white}.muted{color:var(--muted);font-size:13px}.mono{font-family:ui-monospace,Menlo,monospace;font-size:12px;color:#cbd5e1;white-space:pre-wrap}.status{min-height:22px;color:#cbd5e1;font-weight:700}
    @media(max-width:960px){.grid{grid-template-columns:1fr}iframe{height:640px}}
  </style>
</head>
<body>
  <h1>Reportes SIPAE</h1>
  <p>Previsualiza el correo de cierre por plantel, envía pruebas, revisa historial y señales de lectura.</p>
  <div class="bar">
    <div><label>Token admin</label><input id="token" type="password" placeholder="HEALTH_REPORTS_ADMIN_TOKEN"></div>
    <div><label>Plantel</label><select id="plantel"><option>PT</option><option>PM</option><option>ST</option><option>SM</option><option>PREET</option><option>PREEM</option></select></div>
    <div><label>Fecha</label><input id="date" type="date"></div>
    <div><label>Email de prueba</label><input id="testEmail" type="email" placeholder="tu@casitaiedis.edu.mx"></div>
    <button onclick="preview()">Preview</button>
    <button class="warn" onclick="sendTest()">Enviar prueba</button>
    <button class="secondary" onclick="loadMessages()">Historial</button>
    <button class="secondary" onclick="syncRead()">Sincronizar lectura</button>
  </div>
  <div class="status" id="status"></div>
  <div class="grid">
    <div class="card list"><h2>Mensajes</h2><div id="messages"></div></div>
    <div class="card"><h2 id="previewTitle">Preview</h2><iframe id="frame"></iframe><div class="mono" id="meta"></div></div>
  </div>
<script>
const today = new Date().toISOString().slice(0,10); document.getElementById('date').value = today;
const hdr = () => ({'Content-Type':'application/json','X-Health-Reports-Admin-Token':document.getElementById('token').value});
function setStatus(t){document.getElementById('status').textContent=t||''}
async function preview(){
  setStatus('Generando preview...');
  const p=document.getElementById('plantel').value, d=document.getElementById('date').value;
  const r=await fetch(`/api/v1/health-reports/preview?plantel=${p}&date=${d}`,{headers:hdr()});
  const j=await r.json(); if(!r.ok){setStatus(j.detail||'Error');return}
  document.getElementById('frame').srcdoc=j.html; document.getElementById('previewTitle').textContent=j.subject;
  document.getElementById('meta').textContent=JSON.stringify({to:j.to,cc:j.cc,severity:j.severity,worst_metric:j.worst_metric},null,2); setStatus('Preview listo.');
}
async function sendTest(){
  setStatus('Enviando prueba...'); const body={plantel:document.getElementById('plantel').value,date:document.getElementById('date').value,test_email:document.getElementById('testEmail').value};
  const r=await fetch('/api/v1/health-reports/send-test',{method:'POST',headers:hdr(),body:JSON.stringify(body)}); const j=await r.json();
  setStatus(r.ok?`Prueba enviada: ${j.message_id||''}`:(j.detail||'Error'));
  if(j.html){document.getElementById('frame').srcdoc=j.html; document.getElementById('previewTitle').textContent=j.subject;}
}
async function loadMessages(){
  setStatus('Cargando historial...'); const d=document.getElementById('date').value,p=document.getElementById('plantel').value;
  const r=await fetch(`/api/v1/health-reports/messages?date=${d}&plantel=${p}&limit=80`,{headers:hdr()}); const j=await r.json();
  if(!r.ok){setStatus(j.detail||'Error');return} document.getElementById('messages').innerHTML='';
  j.messages.forEach(m=>{const div=document.createElement('div');div.className='row';div.onclick=()=>loadHtml(m.id);div.innerHTML=`<span class="pill ${m.severity}">${m.severity}</span><div style="font-weight:900;margin-top:8px">${m.subject}</div><div class="muted">${m.recipient} · ${m.status} · opens ${m.open_count} · clicks ${m.click_count}</div>`;document.getElementById('messages').appendChild(div)});
  setStatus(`${j.messages.length} mensajes.`);
}
async function loadHtml(id){
  const r=await fetch(`/api/v1/health-reports/messages/${id}/html`,{headers:hdr()}); const j=await r.json(); if(!r.ok){setStatus(j.detail||'Error');return}
  document.getElementById('frame').srcdoc=j.html; document.getElementById('previewTitle').textContent=j.subject; document.getElementById('meta').textContent=JSON.stringify(j.meta,null,2);
}
async function syncRead(){
  setStatus('Sincronizando lectura via Gmail...'); const r=await fetch('/api/v1/health-reports/sync-read-status',{method:'POST',headers:hdr(),body:'{}'}); const j=await r.json(); setStatus(r.ok?`Revisados: ${j.checked}, actualizados: ${j.updated}`:(j.detail||'Error'));
}
preview();
</script>
</body></html>"""
