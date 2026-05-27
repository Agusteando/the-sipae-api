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
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Reportes SIPAE</title>
  <style>
    :root{--bg:#020617;--panel:#0f172a;--panel2:#111827;--line:#1e293b;--muted:#94a3b8;--text:#f8fafc;--blue:#3b82f6;--violet:#8b5cf6;--green:#22c55e;--amber:#f59e0b;--rose:#f43f5e}
    *{box-sizing:border-box}body{margin:0;height:100vh;overflow:hidden;background:radial-gradient(circle at 10% 0%,rgba(59,130,246,.2),transparent 32%),linear-gradient(135deg,#020617 0%,#0b1120 56%,#020617 100%);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:var(--text)}button,input,select{font:inherit}button{border:0;cursor:pointer}.app{height:100vh;display:grid;grid-template-rows:64px 118px minmax(0,1fr);gap:12px;padding:12px;max-width:1920px;margin:0 auto}.top{display:flex;align-items:center;justify-content:space-between;background:rgba(15,23,42,.8);border:1px solid rgba(148,163,184,.16);border-radius:24px;padding:12px 16px;backdrop-filter:blur(18px)}.brand{display:flex;align-items:center;gap:12px}.mark{width:38px;height:38px;border-radius:14px;background:linear-gradient(135deg,#2563eb,#8b5cf6);display:flex;align-items:center;justify-content:center;font-weight:950}.title h1{margin:0;font-size:25px;letter-spacing:-.07em}.title p{margin:2px 0 0;color:var(--muted);font-size:11px;font-weight:750}.badges{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}.badge,.pill{display:inline-flex;align-items:center;border-radius:999px;padding:5px 8px;font-size:9px;font-weight:950;text-transform:uppercase;letter-spacing:.12em;border:1px solid #334155;color:#94a3b8}.ok{color:#86efac!important;border-color:rgba(34,197,94,.35)!important;background:rgba(34,197,94,.08)!important}.bad,.critical,.failed{color:#fda4af!important;border-color:rgba(244,63,94,.35)!important;background:rgba(244,63,94,.08)!important}.warn,.warning{color:#fcd34d!important;border-color:rgba(245,158,11,.35)!important;background:rgba(245,158,11,.08)!important}.fulfilled,.sent{color:#86efac!important;border-color:rgba(34,197,94,.35)!important;background:rgba(34,197,94,.08)!important}.generated{color:#93c5fd!important;border-color:rgba(59,130,246,.35)!important;background:rgba(59,130,246,.08)!important}.controls{display:grid;grid-template-columns:120px 148px 240px 1fr 390px;gap:10px}.card{background:rgba(15,23,42,.84);border:1px solid rgba(148,163,184,.16);border-radius:22px;padding:12px;min-width:0;box-shadow:0 20px 60px rgba(0,0,0,.15)}.field label,.schedule label{display:block;margin:0 0 6px;color:#64748b;text-transform:uppercase;letter-spacing:.14em;font-size:9px;font-weight:950}input,select{width:100%;height:38px;border-radius:13px;border:1px solid #334155;background:#020617;color:#fff;padding:0 11px;outline:none;font-size:13px;font-weight:800}.actions{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:7px}.actions button,.schedule button{height:38px;border-radius:13px;background:#2563eb;color:#fff;font-size:10px;font-weight:950;text-transform:uppercase;letter-spacing:.1em}.actions .test{background:#0f766e}.actions .real{background:#be123c}.actions .all{background:#7c2d12}.actions .ghost,.schedule .ghost{background:#1e293b;color:#cbd5e1}.schedule{display:grid;grid-template-columns:70px 70px 1fr 86px;gap:8px;align-items:end}.days{display:flex;gap:5px;flex-wrap:wrap}.day{height:24px;min-width:34px;border-radius:999px;border:1px solid #334155;color:#94a3b8;background:#020617;font-size:9px;font-weight:950}.day.active{background:#2563eb;color:#fff;border-color:#60a5fa}.toggle{display:flex;align-items:center;gap:7px;color:#cbd5e1;font-size:11px;font-weight:900}.toggle input{width:auto;height:auto}.main{display:grid;grid-template-columns:minmax(0,1fr) 390px;gap:12px;min-height:0}.preview{display:grid;grid-template-rows:50px minmax(0,1fr) 48px;min-height:0}.sectionHead{display:flex;align-items:center;justify-content:space-between;gap:12px}.sectionHead h2{margin:0;font-size:17px;letter-spacing:-.04em}.sectionHead p{margin:2px 0 0;color:#94a3b8;font-size:11px;font-weight:700}.frameWrap{min-height:0;border-radius:18px;overflow:hidden;background:#fff;border:1px solid #1e293b}iframe{width:100%;height:100%;border:0;background:#fff}.statusbar{display:grid;grid-template-columns:minmax(0,1fr) 340px;gap:10px;align-items:center}.status{background:#020617;border:1px solid #1e293b;border-radius:14px;padding:9px 11px;color:#cbd5e1;font-size:11px;font-weight:750;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.miniStats{display:grid;grid-template-columns:repeat(4,1fr);gap:7px}.stat{background:#020617;border:1px solid #1e293b;border-radius:14px;padding:7px;text-align:center}.stat b{display:block;font-size:15px}.stat span{display:block;color:#64748b;font-size:8px;font-weight:950;text-transform:uppercase}.side{display:grid;grid-template-rows:152px minmax(0,1fr) 132px;gap:12px;min-height:0}.panel{min-height:0;overflow:hidden}.panel h3{margin:0 0 9px;font-size:13px;text-transform:uppercase;letter-spacing:.12em}.recipients{font-size:11px;color:#cbd5e1;line-height:1.45}.recipients b{color:#fff}.rows{height:100%;overflow:auto;padding-right:4px}.row{border:1px solid #1e293b;background:#020617;border-radius:15px;padding:10px;margin-bottom:8px;cursor:pointer}.rowtitle{font-size:12px;font-weight:850;line-height:1.25;margin:5px 0;color:#fff}.rowmeta{font-size:10px;color:#94a3b8;line-height:1.35}.meta{height:100%;overflow:auto;background:#020617;border:1px solid #1e293b;border-radius:14px;padding:9px;color:#94a3b8;font-size:10px;line-height:14px}.small{height:30px;padding:0 10px;border-radius:11px;font-size:9px}.loading{position:fixed;inset:0;background:rgba(2,6,23,.55);backdrop-filter:blur(4px);z-index:10;display:none;align-items:center;justify-content:center;color:#fff;font-weight:950;letter-spacing:.12em;text-transform:uppercase}.loading.show{display:flex}.spin{width:18px;height:18px;border:2px solid rgba(255,255,255,.2);border-top-color:#fff;border-radius:99px;margin-right:10px;animation:spin .8s linear infinite}@keyframes spin{to{transform:rotate(360deg)}}@media(max-width:1280px){body{overflow:auto}.app{height:auto;min-height:100vh;grid-template-rows:auto auto auto}.controls{grid-template-columns:repeat(2,1fr)}.main{grid-template-columns:1fr}.side{grid-template-rows:auto 330px 132px}.preview{height:76vh}.actions{grid-template-columns:repeat(2,1fr)}.schedule{grid-template-columns:1fr 1fr}}@media(max-width:720px){.controls{grid-template-columns:1fr}.top{height:auto}.badges{display:none}.statusbar{grid-template-columns:1fr}.preview{height:74vh}}
  </style>
</head>
<body>
  <div id="loading" class="loading"><span class="spin"></span><span id="loadingText">Procesando</span></div>
  <div class="app">
    <header class="top"><div class="brand"><div class="mark">S</div><div class="title"><h1>Reportes SIPAE</h1><p>Consola operativa: preview, destinatarios, envío real, agenda, historial y lectura.</p></div></div><div class="badges" id="badges"><span class="badge warn">Cargando</span></div></header>
    <section class="controls">
      <div class="card field"><label>Plantel</label><select id="plantel" onchange="refreshAll()"><option>PT</option><option>PM</option><option>ST</option><option>SM</option><option>PREET</option><option>PREEM</option></select></div>
      <div class="card field"><label>Fecha</label><input id="date" type="date" onchange="refreshAll()"></div>
      <div class="card field"><label>Email de prueba</label><input id="testEmail" type="email" placeholder="desarrollo.tecnologico@casitaiedis.edu.mx"></div>
      <div class="card actions"><button onclick="preview()">Preview</button><button class="test" onclick="sendTest()">Enviar prueba</button><button class="real" onclick="sendRealPlantel()">Enviar real plantel</button><button class="all" onclick="sendRealAll()">Enviar real todos</button><button class="ghost" onclick="syncRead()">Lectura</button></div>
      <div class="card schedule"><div><label>Hora</label><input id="hour" type="number" min="0" max="23"></div><div><label>Min</label><input id="minute" type="number" min="0" max="59"></div><div><label>Días</label><div class="days" id="days"></div><div class="toggle"><input id="enabled" type="checkbox"> envío automático</div></div><button onclick="saveSchedule()">Guardar</button></div>
    </section>
    <main class="main">
      <section class="card preview"><div class="sectionHead"><div><h2>Preview del correo</h2><p id="previewTitle">Generando preview inicial…</p></div><button class="ghost small" onclick="copyMeta()">Copiar meta</button></div><div class="frameWrap"><iframe id="frame" title="Preview HTML del reporte"></iframe></div><div class="statusbar"><div class="status" id="status">Inicializando consola…</div><div class="miniStats"><div class="stat"><b id="statMsgs">—</b><span>mensajes</span></div><div class="stat"><b id="statSent">—</b><span>sent</span></div><div class="stat"><b id="statOpened">—</b><span>open</span></div><div class="stat"><b id="statClicked">—</b><span>click</span></div></div></div></section>
      <section class="side"><div class="card panel"><h3>Destinatarios detectados</h3><div id="recipients" class="recipients">Cargando…</div></div><div class="card panel"><div class="sectionHead"><h3>Historial</h3><button class="ghost small" onclick="loadMessages()">Recargar</button></div><div id="messages" class="rows"><div class="rowmeta">Sin historial cargado.</div></div></div><div class="card panel"><h3>Meta / error técnico</h3><pre id="meta" class="meta">{}</pre></div></section>
    </main>
  </div>
<script>
const $=(id)=>document.getElementById(id);$('date').value=new Date().toISOString().slice(0,10);const state={lastMeta:{},days:['mon','tue','wed','thu','fri']};const dayLabels={mon:'Lun',tue:'Mar',wed:'Mié',thu:'Jue',fri:'Vie',sat:'Sáb',sun:'Dom'};
function headers(){return {'Content-Type':'application/json'}}function setBusy(on,text='Procesando'){$('loadingText').textContent=text;$('loading').classList.toggle('show',!!on)}function setStatus(text,kind=''){$('status').innerHTML=kind?`<b>${kind}</b> · ${escapeHtml(text)}`:escapeHtml(text)}function setMeta(value){state.lastMeta=value||{};$('meta').textContent=JSON.stringify(state.lastMeta,null,2)}function detailToText(detail){if(!detail)return '';if(typeof detail==='string')return detail;if(typeof detail==='object')return [detail.message,detail.type&&('Tipo: '+detail.type),detail.error&&('Error: '+detail.error)].filter(Boolean).join(' · ')||JSON.stringify(detail);return String(detail)}async function parseResponse(r){const raw=await r.text();let j={};try{j=raw?JSON.parse(raw):{}}catch(_){j={detail:raw||'Respuesta no JSON'}}if(!r.ok){const e=new Error(detailToText(j.detail)||`HTTP ${r.status}`);e.status=r.status;e.body=j;e.url=r.url;throw e}return j}function handleError(e,ctx){setStatus(`${ctx}: HTTP ${e.status||'ERR'} · ${e.message||e}`,'Error');setMeta({context:ctx,status:e.status,error:e.message,body:e.body,url:e.url});}
function renderDays(days){state.days=days||[];$('days').innerHTML=Object.entries(dayLabels).map(([k,v])=>`<button type="button" class="day ${state.days.includes(k)?'active':''}" onclick="toggleDay('${k}')">${v}</button>`).join('')}function toggleDay(k){state.days=state.days.includes(k)?state.days.filter(d=>d!==k):[...state.days,k];renderDays(state.days)}
async function loadConfig(){try{const [cRes,sRes]=await Promise.all([fetch('/api/v1/health-reports/config-status'),fetch('/api/v1/health-reports/schedule')]);const c=await parseResponse(cRes);const s=await parseResponse(sRes);const items=[['SIPAE DB',c.sipae_db_configured],['Gmail',c.gmail_sender_configured],['Service Account',c.google_service_account_configured],['Base URL',c.public_base_url_configured],['Auto',s.active]];$('badges').innerHTML=items.map(([label,ok])=>`<span class="badge ${ok?'ok':'bad'}">${label}</span>`).join('');$('enabled').checked=!!s.config.enabled;$('hour').value=s.config.hour;$('minute').value=s.config.minute;renderDays(s.config.days||['mon','tue','wed','thu','fri']);return {config:c,schedule:s}}catch(e){$('badges').innerHTML='<span class="badge bad">Config error</span>';handleError(e,'config/schedule')}}
async function loadRecipients(){const p=$('plantel').value;try{const r=await fetch(`/api/v1/health-reports/recipients?plantel=${encodeURIComponent(p)}`);const j=await parseResponse(r);const rec=(j.recipients||[])[0];if(!rec){$('recipients').innerHTML='<span class="bad">No se detectó dirección para este plantel.</span>';return null}$('recipients').innerHTML=`<b>Para:</b> ${escapeHtml(rec.principal_email||'—')}<br><b>Manager CC:</b> ${escapeHtml(rec.manager_email||'—')}<br><b>CC final:</b> ${escapeHtml((rec.cc_emails||[]).join(', ')||'—')}<br><b>Plantel:</b> ${escapeHtml(rec.resolved_name||rec.plantel_code||p)}<br><b>Coord:</b> ${escapeHtml(rec.coord_name||'—')}`;return rec}catch(e){$('recipients').innerHTML='<span class="bad">Error al detectar destinatarios.</span>';handleError(e,'recipients');return null}}
async function preview(){setBusy(true,'Generando preview');const p=$('plantel').value,d=$('date').value;try{const r=await fetch(`/api/v1/health-reports/preview?plantel=${encodeURIComponent(p)}&date=${encodeURIComponent(d)}`);const j=await parseResponse(r);$('frame').srcdoc=j.html||'';$('previewTitle').textContent=j.subject||'Sin asunto';setMeta({to:j.to,cc:j.cc,severity:j.severity,worst_metric:j.worst_metric,resolver_error:j.resolver_error});setStatus(j.resolver_error?`Preview listo con aviso: ${j.resolver_error}`:'Preview listo.')}catch(e){handleError(e,'preview')}finally{setBusy(false)}}
async function sendTest(){const email=$('testEmail').value.trim();if(!email){setStatus('Escribe un email de prueba.','Falta email');return}return sendPost('/api/v1/health-reports/send-test',{plantel:$('plantel').value,date:$('date').value,test_email:email},'Enviando prueba','send-test')}
async function sendRealPlantel(){if(!confirm('Esto enviará el reporte REAL al director del plantel y copiará a su manager. ¿Continuar?'))return;return sendPost('/api/v1/health-reports/send-now',{plantel:$('plantel').value,date:$('date').value,send:true},'Enviando real plantel','send-real-plantel')}
async function sendRealAll(){if(!confirm('Esto enviará reportes REALES a todos los planteles detectados y sus managers. ¿Continuar?'))return;return sendPost('/api/v1/health-reports/send-now',{date:$('date').value,send:true},'Enviando real todos','send-real-todos')}
async function sendPost(url,body,busy,ctx){setBusy(true,busy);try{const r=await fetch(url,{method:'POST',headers:headers(),body:JSON.stringify(body)});const j=await parseResponse(r);if(j.html){$('frame').srcdoc=j.html;$('previewTitle').textContent=j.subject||ctx}setMeta(j);setStatus(`${ctx}: ${j.status||j.sent||0} enviados · ${j.generated||0} generados · ${j.failed||0} fallidos`);await loadMessages(false)}catch(e){handleError(e,ctx)}finally{setBusy(false)}}
async function saveSchedule(){setBusy(true,'Guardando agenda');try{const payload={enabled:$('enabled').checked,hour:Number($('hour').value||15),minute:Number($('minute').value||55),days:state.days};const r=await fetch('/api/v1/health-reports/schedule',{method:'POST',headers:headers(),body:JSON.stringify(payload)});const j=await parseResponse(r);setMeta(j);setStatus(`Agenda ${j.active?'activa':'inactiva'} · próxima ejecución: ${j.next_run_time||'sin ejecución'}`);await loadConfig()}catch(e){handleError(e,'schedule')}finally{setBusy(false)}}
async function loadMessages(show=true){if(show)setBusy(true,'Cargando historial');const d=$('date').value,p=$('plantel').value;try{const r=await fetch(`/api/v1/health-reports/messages?date=${encodeURIComponent(d)}&plantel=${encodeURIComponent(p)}&limit=80`);const j=await parseResponse(r);renderMessages(j.messages||[]);if(show)setStatus(`${(j.messages||[]).length} mensajes para ${p} en ${d}.`)}catch(e){handleError(e,'historial')}finally{if(show)setBusy(false)}}
function renderMessages(messages){$('messages').innerHTML='';if(!messages.length){$('messages').innerHTML='<div class="rowmeta">No hay mensajes para este filtro.</div>'}let sent=0,opened=0,clicked=0;messages.forEach(m=>{sent+=m.status==='sent'?1:0;opened+=Number(m.open_count||0)>0?1:0;clicked+=Number(m.click_count||0)>0?1:0;const div=document.createElement('div');div.className='row';div.onclick=()=>loadHtml(m.id);div.innerHTML=`<span class="pill ${m.severity||'fulfilled'}">${m.severity||'sin severidad'}</span> <span class="pill ${m.status||'generated'}">${m.status||'generated'}</span><div class="rowtitle">${escapeHtml(m.subject||'Sin asunto')}</div><div class="rowmeta">${escapeHtml(m.recipient||'')} · manager: ${escapeHtml(m.manager||'—')}</div><div class="rowmeta">opens ${m.open_count||0} · clicks ${m.click_count||0} · ${m.sent_at||'sin envío'}</div>`;$('messages').appendChild(div)});$('statMsgs').textContent=messages.length;$('statSent').textContent=sent;$('statOpened').textContent=opened;$('statClicked').textContent=clicked}
async function loadHtml(id){setBusy(true,'Abriendo correo');try{const r=await fetch(`/api/v1/health-reports/messages/${id}/html`);const j=await parseResponse(r);$('frame').srcdoc=j.html||'';$('previewTitle').textContent=j.subject||'Mensaje';setMeta(j.meta||{});setStatus('Mensaje cargado.')}catch(e){handleError(e,'html-message')}finally{setBusy(false)}}async function syncRead(){setBusy(true,'Sincronizando lectura');try{const r=await fetch('/api/v1/health-reports/sync-read-status',{method:'POST',headers:headers(),body:'{}'});const j=await parseResponse(r);setMeta(j);setStatus(`Lectura sincronizada: ${j.checked||0} revisados, ${j.updated||0} actualizados.`);await loadMessages(false)}catch(e){handleError(e,'sync-read-status')}finally{setBusy(false)}}
async function refreshAll(){await loadRecipients();await preview();await loadMessages(false)}function copyMeta(){navigator.clipboard.writeText(JSON.stringify(state.lastMeta,null,2));setStatus('Meta copiada al portapapeles.')}function escapeHtml(s){return String(s||'').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]))}(async()=>{await loadConfig();await loadRecipients();await preview();await loadMessages(false)})();
</script>
</body>
</html>"""
