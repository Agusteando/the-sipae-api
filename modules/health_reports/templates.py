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
    *{box-sizing:border-box} body{margin:0;height:100vh;overflow:hidden;background:radial-gradient(circle at top left,rgba(59,130,246,.18),transparent 34%),linear-gradient(135deg,#020617 0%,#0b1120 55%,#020617 100%);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:var(--text)}
    .app{height:100vh;display:grid;grid-template-rows:72px 96px minmax(0,1fr);gap:14px;padding:14px;max-width:1920px;margin:0 auto}
    .top,.controls,.main>section{border:1px solid rgba(148,163,184,.14);background:rgba(15,23,42,.78);box-shadow:0 20px 70px rgba(0,0,0,.30);backdrop-filter:blur(18px);border-radius:26px}
    .top{display:flex;align-items:center;justify-content:space-between;padding:0 20px 0 24px;gap:18px}.brand{display:flex;align-items:center;gap:14px;min-width:0}.mark{width:44px;height:44px;border-radius:16px;background:linear-gradient(135deg,var(--blue),var(--violet));display:grid;place-items:center;font-weight:950;letter-spacing:-.08em}.title{min-width:0}.title h1{margin:0;font-size:28px;letter-spacing:-.07em;line-height:28px}.title p{margin:4px 0 0;color:var(--muted);font-size:12px;font-weight:750;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.badges{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}.badge{border:1px solid rgba(148,163,184,.18);background:rgba(255,255,255,.045);border-radius:999px;padding:8px 10px;font-size:10px;font-weight:950;text-transform:uppercase;letter-spacing:.14em;color:#cbd5e1}.badge.ok{color:#86efac;border-color:rgba(34,197,94,.32);background:rgba(34,197,94,.10)}.badge.warn{color:#fbbf24;border-color:rgba(245,158,11,.32);background:rgba(245,158,11,.10)}.badge.bad{color:#fb7185;border-color:rgba(244,63,94,.35);background:rgba(244,63,94,.11)}
    .controls{display:grid;grid-template-columns:130px 170px minmax(220px,1fr) repeat(4,148px);gap:10px;align-items:end;padding:14px}.field{min-width:0}.field label{display:block;font-size:10px;font-weight:950;letter-spacing:.16em;text-transform:uppercase;color:#64748b;margin:0 0 7px 2px}input,select{width:100%;height:42px;border:1px solid rgba(148,163,184,.18);background:#020617;color:#f8fafc;border-radius:15px;padding:0 13px;font-size:13px;font-weight:800;outline:none}input:focus,select:focus{border-color:rgba(59,130,246,.75);box-shadow:0 0 0 3px rgba(59,130,246,.12)}button{height:42px;border:0;border-radius:15px;background:#2563eb;color:white;font-size:11px;font-weight:950;letter-spacing:.12em;text-transform:uppercase;cursor:pointer;transition:transform .15s ease,background .15s ease,opacity .15s ease}button:hover{transform:translateY(-1px);background:#3b82f6}button.secondary{background:#1e293b;color:#cbd5e1;border:1px solid rgba(148,163,184,.16)}button.secondary:hover{background:#334155}button.send{background:#7c3aed}button.send:hover{background:#8b5cf6}button.danger{background:#be123c}button:disabled{opacity:.45;cursor:not-allowed;transform:none}
    .main{min-height:0;display:grid;grid-template-columns:minmax(0,1.45fr) minmax(390px,.55fr);gap:14px}.main>section{min-height:0;overflow:hidden}.preview{display:grid;grid-template-rows:58px minmax(0,1fr) 70px}.sectionHead{height:58px;display:flex;align-items:center;justify-content:space-between;padding:0 16px 0 18px;border-bottom:1px solid rgba(148,163,184,.12);gap:14px}.sectionHead h2{font-size:14px;margin:0;font-weight:950;letter-spacing:-.02em}.sectionHead p{margin:3px 0 0;color:#64748b;font-size:11px;font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:760px}.subject{font-size:12px;font-weight:900;color:#cbd5e1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:760px}.frameWrap{padding:12px;background:#020617;min-height:0}.frameWrap iframe{width:100%;height:100%;border:0;border-radius:18px;background:white}.statusbar{display:grid;grid-template-columns:minmax(0,1fr) 260px;gap:10px;padding:10px 12px;border-top:1px solid rgba(148,163,184,.12)}.status{border:1px solid rgba(148,163,184,.12);background:rgba(2,6,23,.65);border-radius:16px;padding:10px 12px;color:#cbd5e1;font-size:12px;font-weight:750;line-height:16px;overflow:hidden}.status b{color:white}.miniStats{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.stat{border:1px solid rgba(148,163,184,.12);background:rgba(255,255,255,.035);border-radius:14px;padding:8px 7px;text-align:center}.stat b{display:block;font-size:18px;line-height:18px;letter-spacing:-.05em}.stat span{display:block;margin-top:3px;color:#64748b;font-size:9px;font-weight:950;text-transform:uppercase;letter-spacing:.12em}
    .side{display:grid;grid-template-rows:150px minmax(0,1fr) 128px}.config{padding:14px;border-bottom:1px solid rgba(148,163,184,.12)}.config h3,.history h3,.meta h3{margin:0 0 10px;font-size:12px;font-weight:950;text-transform:uppercase;letter-spacing:.13em;color:#94a3b8}.configGrid{display:grid;grid-template-columns:1fr 1fr;gap:8px}.cfg{display:flex;align-items:center;justify-content:space-between;gap:8px;background:rgba(2,6,23,.55);border:1px solid rgba(148,163,184,.10);border-radius:14px;padding:9px 10px;font-size:11px;font-weight:850;color:#cbd5e1}.dot{width:9px;height:9px;border-radius:999px;background:#64748b;box-shadow:0 0 0 3px rgba(100,116,139,.13)}.dot.ok{background:#22c55e;box-shadow:0 0 0 3px rgba(34,197,94,.16)}.dot.bad{background:#f43f5e;box-shadow:0 0 0 3px rgba(244,63,94,.16)}.history{min-height:0;display:grid;grid-template-rows:38px minmax(0,1fr);padding:14px;border-bottom:1px solid rgba(148,163,184,.12)}.historyHead{display:flex;justify-content:space-between;align-items:center}.rows{overflow:auto;padding-right:4px}.row{border:1px solid rgba(148,163,184,.12);background:rgba(2,6,23,.45);border-radius:16px;padding:11px;margin-bottom:8px;cursor:pointer}.row:hover{border-color:rgba(59,130,246,.45);background:rgba(30,41,59,.55)}.pill{display:inline-flex;align-items:center;border-radius:999px;padding:4px 7px;font-size:9px;font-weight:950;letter-spacing:.10em;text-transform:uppercase;margin-right:5px;background:rgba(148,163,184,.11);color:#cbd5e1}.pill.critical,.pill.failed{background:rgba(244,63,94,.12);color:#fb7185}.pill.warning,.pill.generated{background:rgba(245,158,11,.12);color:#fbbf24}.pill.fulfilled,.pill.sent{background:rgba(34,197,94,.12);color:#86efac}.rowtitle{margin-top:8px;font-size:12px;font-weight:900;color:#f8fafc;line-height:15px}.rowmeta{margin-top:5px;font-size:10px;font-weight:750;color:#64748b;line-height:13px}.empty{height:100%;display:grid;place-items:center;color:#64748b;text-align:center;font-size:12px;font-weight:850;padding:18px}.meta{padding:12px 14px}.meta pre{height:76px;margin:0;overflow:auto;border:1px solid rgba(148,163,184,.10);background:#020617;border-radius:14px;padding:9px;color:#94a3b8;font-size:10px;line-height:14px}.small{height:30px;padding:0 10px;border-radius:11px;font-size:9px}.loading{position:fixed;inset:0;background:rgba(2,6,23,.55);backdrop-filter:blur(4px);z-index:10;display:none;align-items:center;justify-content:center;color:#fff;font-weight:950;letter-spacing:.12em;text-transform:uppercase}.loading.show{display:flex}.spin{width:18px;height:18px;border:2px solid rgba(255,255,255,.2);border-top-color:#fff;border-radius:99px;margin-right:10px;animation:spin .8s linear infinite}@keyframes spin{to{transform:rotate(360deg)}}
    @media(max-width:1180px){.app{overflow:auto;height:auto;min-height:100vh}.controls{grid-template-columns:repeat(2,1fr)}.main{grid-template-columns:1fr}.side{grid-template-rows:auto 360px 128px}.preview{height:72vh}}@media(max-width:720px){.top{height:auto;align-items:flex-start;padding:16px}.badges{display:none}.controls{grid-template-columns:1fr}.statusbar{grid-template-columns:1fr}.main{display:block}.preview{height:72vh;margin-bottom:14px}.side{height:78vh}.title h1{font-size:24px}}
  </style>
</head>
<body>
  <div id="loading" class="loading"><span class="spin"></span><span id="loadingText">Procesando</span></div>
  <div class="app">
    <header class="top">
      <div class="brand"><div class="mark">S</div><div class="title"><h1>Reportes SIPAE</h1><p>Consola de cierre: preview, envío de prueba, historial y lectura. Sin token admin.</p></div></div>
      <div class="badges" id="badges"><span class="badge warn">Cargando configuración</span></div>
    </header>

    <section class="controls">
      <div class="field"><label>Plantel</label><select id="plantel"><option>PT</option><option>PM</option><option>ST</option><option>SM</option><option>PREET</option><option>PREEM</option></select></div>
      <div class="field"><label>Fecha</label><input id="date" type="date"></div>
      <div class="field"><label>Email de prueba</label><input id="testEmail" type="email" placeholder="desarrollo.tecnologico@casitaiedis.edu.mx"></div>
      <button onclick="preview()">Preview</button>
      <button class="send" onclick="sendTest()">Enviar prueba</button>
      <button class="secondary" onclick="loadMessages()">Historial</button>
      <button class="secondary" onclick="syncRead()">Lectura</button>
    </section>

    <main class="main">
      <section class="preview">
        <div class="sectionHead"><div><h2>Preview del correo</h2><p id="previewTitle">Generando preview inicial…</p></div><button class="secondary small" onclick="copyMeta()">Copiar meta</button></div>
        <div class="frameWrap"><iframe id="frame" title="Preview HTML del reporte"></iframe></div>
        <div class="statusbar"><div class="status" id="status">Inicializando consola…</div><div class="miniStats"><div class="stat"><b id="statMsgs">—</b><span>mensajes</span></div><div class="stat"><b id="statSent">—</b><span>sent</span></div><div class="stat"><b id="statOpened">—</b><span>open</span></div><div class="stat"><b id="statClicked">—</b><span>click</span></div></div></div>
      </section>

      <section class="side">
        <div class="config"><h3>Configuración</h3><div class="configGrid" id="configGrid"><div class="cfg">SIPAE DB <span class="dot"></span></div><div class="cfg">Gmail <span class="dot"></span></div><div class="cfg">Service Account <span class="dot"></span></div><div class="cfg">Base URL <span class="dot"></span></div></div></div>
        <div class="history"><div class="historyHead"><h3>Historial</h3><button class="secondary small" onclick="loadMessages()">Recargar</button></div><div id="messages" class="rows"><div class="empty">Sin historial cargado.</div></div></div>
        <div class="meta"><h3>Meta / error técnico</h3><pre id="meta">{}</pre></div>
      </section>
    </main>
  </div>
<script>
const $ = (id) => document.getElementById(id);
$('date').value = new Date().toISOString().slice(0,10);
const state = {lastMeta:{}};
function headers(){return {'Content-Type':'application/json'}}
function setBusy(on,text='Procesando'){$('loadingText').textContent=text;$('loading').classList.toggle('show',!!on)}
function setStatus(text,kind=''){$('status').innerHTML = kind ? `<b>${kind}</b> · ${escapeHtml(text)}` : escapeHtml(text)}
function setMeta(value){state.lastMeta=value||{};$('meta').textContent=JSON.stringify(state.lastMeta,null,2)}
function detailToText(detail){if(!detail)return ''; if(typeof detail==='string')return detail; if(typeof detail==='object'){return [detail.message,detail.type&&('Tipo: '+detail.type),detail.error&&('Error: '+detail.error)].filter(Boolean).join(' · ') || JSON.stringify(detail)} return String(detail)}
async function parseResponse(r){const raw=await r.text();let j={};try{j=raw?JSON.parse(raw):{}}catch(_){j={detail:raw||'Respuesta no JSON'}} if(!r.ok){const e=new Error(detailToText(j.detail)||`HTTP ${r.status}`);e.status=r.status;e.body=j;e.url=r.url;throw e}return j}
function handleError(e,ctx){setStatus(`${ctx}: HTTP ${e.status||'ERR'} · ${e.message||e}`,'Error');setMeta({context:ctx,status:e.status,error:e.message,body:e.body,url:e.url});}
async function loadConfig(){try{const r=await fetch('/api/v1/health-reports/config-status',{headers:headers()});const c=await parseResponse(r);const items=[['SIPAE DB',c.sipae_db_configured],['Gmail',c.gmail_sender_configured],['Service Account',c.google_service_account_configured],['Base URL',c.public_base_url_configured],['Test email',c.test_recipient_configured]];$('configGrid').innerHTML=items.map(([label,ok])=>`<div class="cfg">${label}<span class="dot ${ok?'ok':'bad'}"></span></div>`).join('');$('badges').innerHTML=items.map(([label,ok])=>`<span class="badge ${ok?'ok':'bad'}">${label}</span>`).join('');return c}catch(e){$('badges').innerHTML='<span class="badge bad">Config error</span>';handleError(e,'config-status')}}
async function preview(){setBusy(true,'Generando preview');const p=$('plantel').value,d=$('date').value;try{const r=await fetch(`/api/v1/health-reports/preview?plantel=${encodeURIComponent(p)}&date=${encodeURIComponent(d)}`,{headers:headers()});const j=await parseResponse(r);$('frame').srcdoc=j.html||'';$('previewTitle').textContent=j.subject||'Sin asunto';setMeta({to:j.to,cc:j.cc,severity:j.severity,worst_metric:j.worst_metric,resolver_error:j.resolver_error});setStatus(j.resolver_error?`Preview listo con aviso de destinatarios: ${j.resolver_error}`:'Preview listo.')}catch(e){handleError(e,'preview')}finally{setBusy(false)}}
async function sendTest(){const email=$('testEmail').value.trim();if(!email){setStatus('Escribe un email de prueba.','Falta email');return}setBusy(true,'Enviando prueba');try{const body={plantel:$('plantel').value,date:$('date').value,test_email:email};const r=await fetch('/api/v1/health-reports/send-test',{method:'POST',headers:headers(),body:JSON.stringify(body)});const j=await parseResponse(r);if(j.html){$('frame').srcdoc=j.html;$('previewTitle').textContent=j.subject||'Prueba enviada'}setMeta({message_id:j.message_id,status:j.status,error:j.error,resolver_error:j.resolver_error});setStatus(`Prueba procesada: ${j.status||'sin estado'}${j.error?' · '+j.error:''}${j.resolver_error?' · '+j.resolver_error:''}`);await loadMessages(false)}catch(e){handleError(e,'send-test')}finally{setBusy(false)}}
async function loadMessages(show=true){if(show)setBusy(true,'Cargando historial');const d=$('date').value,p=$('plantel').value;try{const r=await fetch(`/api/v1/health-reports/messages?date=${encodeURIComponent(d)}&plantel=${encodeURIComponent(p)}&limit=80`,{headers:headers()});const j=await parseResponse(r);renderMessages(j.messages||[]);if(show)setStatus(`${(j.messages||[]).length} mensajes para ${p} en ${d}.`)}catch(e){handleError(e,'historial')}finally{if(show)setBusy(false)}}
function renderMessages(messages){$('messages').innerHTML='';if(!messages.length){$('messages').innerHTML='<div class="empty">No hay mensajes para este filtro.</div>'}let sent=0,opened=0,clicked=0;messages.forEach(m=>{sent+=m.status==='sent'?1:0;opened+=Number(m.open_count||0)>0?1:0;clicked+=Number(m.click_count||0)>0?1:0;const div=document.createElement('div');div.className='row';div.onclick=()=>loadHtml(m.id);div.innerHTML=`<span class="pill ${m.severity||'fulfilled'}">${m.severity||'sin severidad'}</span><span class="pill ${m.status||'generated'}">${m.status||'generated'}</span><div class="rowtitle">${escapeHtml(m.subject||'Sin asunto')}</div><div class="rowmeta">${escapeHtml(m.recipient||'')} · manager: ${escapeHtml(m.manager||'—')}</div><div class="rowmeta">opens ${m.open_count||0} · clicks ${m.click_count||0} · ${m.sent_at||'sin envío'}</div>`;$('messages').appendChild(div)});$('statMsgs').textContent=messages.length;$('statSent').textContent=sent;$('statOpened').textContent=opened;$('statClicked').textContent=clicked}
async function loadHtml(id){setBusy(true,'Abriendo correo');try{const r=await fetch(`/api/v1/health-reports/messages/${id}/html`,{headers:headers()});const j=await parseResponse(r);$('frame').srcdoc=j.html||'';$('previewTitle').textContent=j.subject||'Mensaje';setMeta(j.meta||{});setStatus('Mensaje cargado.')}catch(e){handleError(e,'html-message')}finally{setBusy(false)}}
async function syncRead(){setBusy(true,'Sincronizando lectura');try{const r=await fetch('/api/v1/health-reports/sync-read-status',{method:'POST',headers:headers(),body:'{}'});const j=await parseResponse(r);setMeta(j);setStatus(`Lectura sincronizada: ${j.checked||0} revisados, ${j.updated||0} actualizados.`);await loadMessages(false)}catch(e){handleError(e,'sync-read-status')}finally{setBusy(false)}}
function copyMeta(){navigator.clipboard.writeText(JSON.stringify(state.lastMeta,null,2));setStatus('Meta copiada al portapapeles.')}
function escapeHtml(s){return String(s||'').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]))}
(async()=>{await loadConfig();await preview();await loadMessages(false);})();
</script>
</body>
</html>"""
