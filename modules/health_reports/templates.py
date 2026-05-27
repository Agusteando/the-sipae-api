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
    :root{--bg:#050816;--panel:#0f172a;--panel2:#111827;--line:#233044;--text:#f8fafc;--muted:#94a3b8;--blue:#2563eb;--blue2:#60a5fa;--rose:#fb7185;--green:#22c55e;--amber:#f59e0b;--violet:#8b5cf6}
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(circle at 20% 0%,rgba(37,99,235,.22),transparent 34%),radial-gradient(circle at 82% 4%,rgba(139,92,246,.18),transparent 30%),var(--bg);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,Segoe UI,Arial,sans-serif;min-height:100vh;padding:28px}
    .shell{max-width:1440px;margin:0 auto}.hero{display:flex;justify-content:space-between;gap:24px;align-items:flex-end;margin-bottom:22px}.eyebrow{color:var(--blue2);font-size:11px;font-weight:950;letter-spacing:.22em;text-transform:uppercase;margin-bottom:10px}h1{font-size:52px;line-height:48px;letter-spacing:-.08em;margin:0}p{color:var(--muted);font-weight:650;line-height:1.55}.hero p{max-width:720px;margin:12px 0 0}.health{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;min-width:460px}.stat{background:rgba(15,23,42,.74);border:1px solid rgba(148,163,184,.16);border-radius:22px;padding:14px}.stat b{display:block;font-size:24px;letter-spacing:-.05em}.stat span{display:block;margin-top:4px;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.14em;font-weight:900}
    .panel{background:rgba(15,23,42,.82);border:1px solid rgba(148,163,184,.16);box-shadow:0 28px 90px rgba(0,0,0,.26);border-radius:30px}.toolbar{padding:18px;display:grid;grid-template-columns:1.4fr repeat(3,minmax(150px,.5fr)) auto auto auto auto;gap:12px;align-items:end;margin-bottom:18px}.field label{display:block;font-size:10px;text-transform:uppercase;letter-spacing:.16em;color:var(--muted);font-weight:950;margin:0 0 7px}input,select{width:100%;background:#020617;border:1px solid #334155;color:var(--text);border-radius:15px;padding:13px 13px;font-weight:800;outline:none}input:focus,select:focus{border-color:var(--blue2);box-shadow:0 0 0 3px rgba(96,165,250,.14)}button{background:var(--blue);color:white;border:0;border-radius:16px;padding:14px 16px;font-weight:950;letter-spacing:.1em;text-transform:uppercase;cursor:pointer;white-space:nowrap}button:hover{filter:brightness(1.08)}button.secondary{background:#1e293b}button.warn{background:var(--amber);color:#111827}button.danger{background:#be123c;color:white}.tokenstate{padding:11px 13px;border-radius:14px;font-size:12px;font-weight:900;border:1px solid var(--line);background:#020617;color:var(--muted)}.tokenstate.ok{color:#86efac;border-color:#22c55e;background:#06251a}.tokenstate.bad{color:#fecaca;border-color:#fb7185;background:#3b0a18}.tokenstate.warn{color:#fde68a;border-color:#f59e0b;background:#2b1d07}
    .status{min-height:46px;border-radius:18px;background:rgba(15,23,42,.78);border:1px solid rgba(148,163,184,.14);padding:14px 18px;color:#cbd5e1;font-weight:800;margin-bottom:18px}.layout{display:grid;grid-template-columns:410px 1fr;gap:18px}.card{background:rgba(15,23,42,.86);border:1px solid rgba(148,163,184,.16);border-radius:30px;padding:18px}.card h2{margin:0 0 14px;font-size:18px;letter-spacing:-.03em}.messages{height:780px;overflow:auto;padding-right:4px}.row{border:1px solid transparent;border-bottom-color:rgba(148,163,184,.13);padding:14px 12px;cursor:pointer;border-radius:18px}.row:hover{background:rgba(255,255,255,.035);border-color:rgba(148,163,184,.15)}.rowtitle{font-weight:950;margin-top:9px;line-height:1.25}.rowmeta{color:var(--muted);font-size:12px;font-weight:750;margin-top:7px;line-height:1.45}.pill{display:inline-block;border-radius:99px;padding:5px 8px;font-size:10px;font-weight:950;text-transform:uppercase;letter-spacing:.1em}.critical{background:#3b0a18;color:var(--rose);border:1px solid var(--rose)}.warning{background:#2b1d07;color:#fbbf24;border:1px solid var(--amber)}.fulfilled{background:#06251a;color:#86efac;border:1px solid var(--green)}.failed{background:#3b0a18;color:var(--rose);border:1px solid var(--rose)}.sent{background:#0b2347;color:#93c5fd;border:1px solid #3b82f6}.generated{background:#1e1b4b;color:#c4b5fd;border:1px solid var(--violet)}
    .previewhead{display:flex;gap:16px;align-items:start;justify-content:space-between;margin-bottom:12px}.subject{font-size:20px;font-weight:950;letter-spacing:-.04em;line-height:1.15}.meta{font-family:ui-monospace,Menlo,monospace;font-size:12px;color:#cbd5e1;white-space:pre-wrap;background:#020617;border:1px solid #1f2937;border-radius:18px;padding:12px;margin-top:12px;max-height:210px;overflow:auto}iframe{width:100%;height:720px;border:1px solid var(--line);border-radius:22px;background:white}.split{display:grid;grid-template-columns:1fr 1fr;gap:10px}.hint{color:#94a3b8;font-size:12px;font-weight:700;margin-top:8px}.empty{padding:26px;border:1px dashed #334155;border-radius:20px;text-align:center;color:#94a3b8;font-weight:800}.smallactions{display:flex;gap:8px;flex-wrap:wrap}
    @media(max-width:1180px){.toolbar{grid-template-columns:1fr 1fr}.layout{grid-template-columns:1fr}.messages{height:380px}.hero{display:block}.health{margin-top:18px;min-width:0}}@media(max-width:720px){body{padding:16px}.toolbar{grid-template-columns:1fr}.health{grid-template-columns:1fr 1fr}h1{font-size:42px;line-height:40px}}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div>
        <div class="eyebrow">Automatización de cierre institucional</div>
        <h1>Reportes SIPAE</h1>
        <p>Previsualiza exactamente qué correo recibirá cada plantel, envía una prueba controlada, revisa historial y sincroniza señales de lectura sin exponer datos internos.</p>
      </div>
      <div class="health">
        <div class="stat"><b id="statMsgs">—</b><span>mensajes</span></div>
        <div class="stat"><b id="statSent">—</b><span>enviados</span></div>
        <div class="stat"><b id="statOpened">—</b><span>aperturas</span></div>
        <div class="stat"><b id="statClicked">—</b><span>clicks</span></div>
      </div>
    </section>

    <section class="panel toolbar">
      <div class="field"><label>Token admin</label><input id="token" type="password" placeholder="HEALTH_REPORTS_ADMIN_TOKEN" autocomplete="off"></div>
      <div class="field"><label>Plantel</label><select id="plantel"><option>PT</option><option>PM</option><option>ST</option><option>SM</option><option>PREET</option><option>PREEM</option></select></div>
      <div class="field"><label>Fecha</label><input id="date" type="date"></div>
      <div class="field"><label>Email de prueba</label><input id="testEmail" type="email" placeholder="tu@casitaiedis.edu.mx"></div>
      <button onclick="preview()">Preview</button>
      <button class="warn" onclick="sendTest()">Enviar prueba</button>
      <button class="secondary" onclick="loadMessages()">Historial</button>
      <button class="secondary" onclick="syncRead()">Sincronizar</button>
    </section>

    <div class="status" id="status">Listo. El token se guarda sólo en este navegador.</div>

    <section class="layout">
      <aside class="card">
        <div class="previewhead">
          <div><h2>Mensajes generados</h2><div class="hint">Selecciona un registro para abrir el HTML enviado.</div></div>
          <div id="authState" class="tokenstate warn">Token no verificado</div>
        </div>
        <div id="messages" class="messages"><div class="empty">Carga el historial para revisar mensajes anteriores.</div></div>
      </aside>
      <main class="card">
        <div class="previewhead">
          <div><h2>Preview de correo</h2><div class="subject" id="previewTitle">Sin preview generado</div></div>
          <div class="smallactions"><button class="secondary" onclick="checkAuth()">Probar token</button><button class="secondary" onclick="copyMeta()">Copiar meta</button></div>
        </div>
        <iframe id="frame"></iframe>
        <div class="meta" id="meta">{}</div>
      </main>
    </section>
  </div>
<script>
const $ = (id) => document.getElementById(id);
const today = new Date().toISOString().slice(0,10); $('date').value = today;
const storedToken = localStorage.getItem('healthReportsAdminToken') || ''; $('token').value = storedToken;
$('token').addEventListener('input', () => { localStorage.setItem('healthReportsAdminToken', cleanToken()); updateAuthState('warn','Token sin verificar'); });
function cleanToken(){return ($('token').value || '').trim().replace(/^Bearer\s+/i,'').replace(/^['\"]|['\"]$/g,'').trim()}
function hdr(){return {'Content-Type':'application/json','X-Health-Reports-Admin-Token':cleanToken(),'Authorization':'Bearer '+cleanToken()}}
function setStatus(t){$('status').textContent=t||''}
function updateAuthState(kind,text){$('authState').className='tokenstate '+kind;$('authState').textContent=text}
function detailToText(detail){
  if(!detail) return '';
  if(typeof detail === 'string') return detail;
  if(typeof detail === 'object'){
    const parts=[];
    if(detail.message) parts.push(detail.message);
    if(detail.type) parts.push('Tipo: '+detail.type);
    if(detail.error) parts.push('Error: '+detail.error);
    return parts.length ? parts.join(' · ') : JSON.stringify(detail);
  }
  return String(detail);
}
async function parseResponse(r){
  const raw = await r.text();
  let j={};
  try{j = raw ? JSON.parse(raw) : {}}catch(_){j={detail:raw || 'Respuesta inválida del servidor'}}
  if(!r.ok){
    const err = new Error(detailToText(j.detail) || ('HTTP '+r.status));
    err.status = r.status; err.body = j; err.url = r.url;
    throw err;
  }
  return j;
}
function handleError(e, context){
  const authFail = e && e.status === 403;
  updateAuthState(authFail ? 'bad' : 'ok', authFail ? 'Token inválido' : 'Token validado');
  const status = e && e.status ? `HTTP ${e.status}` : 'Error';
  const url = e && e.url ? ` · ${e.url}` : '';
  setStatus(`${context}: ${status} · ${e.message || e}${url}`);
  $('meta').textContent = JSON.stringify({context, status:e.status, error:e.message, body:e.body, url:e.url}, null, 2);
}
async function checkAuth(){
  setStatus('Verificando token contra el API en ejecución...');
  try{const r=await fetch('/api/v1/health-reports/auth-status',{headers:hdr()}); const j=await parseResponse(r); updateAuthState(j.valid?'ok':(j.configured?'bad':'warn'), j.valid?'Token válido':(j.configured?'Token inválido':'Token no configurado')); let config=null; if(j.valid){try{const cr=await fetch('/api/v1/health-reports/config-status',{headers:hdr()}); config=await parseResponse(cr);}catch(_){}} $('meta').textContent=JSON.stringify({auth:j,config},null,2); setStatus(j.valid?'Token válido.':`Token no válido. Configurado: ${j.configured}, recibido: ${j.received}, fuente: ${j.source}`); return j.valid;}catch(e){handleError(e,'auth-status');return false;}
}
async function preview(){
  setStatus('Generando preview...');
  const p=$('plantel').value, d=$('date').value;
  try{const r=await fetch(`/api/v1/health-reports/preview?plantel=${encodeURIComponent(p)}&date=${encodeURIComponent(d)}`,{headers:hdr()}); const j=await parseResponse(r); $('frame').srcdoc=j.html; $('previewTitle').textContent=j.subject; $('meta').textContent=JSON.stringify({to:j.to,cc:j.cc,severity:j.severity,worst_metric:j.worst_metric,resolver_error:j.resolver_error},null,2); updateAuthState('ok','Token válido'); setStatus(j.resolver_error ? `Preview listo con aviso: ${j.resolver_error}` : 'Preview listo.');}catch(e){handleError(e,'preview')}
}
async function sendTest(){
  const email=$('testEmail').value.trim(); if(!email){setStatus('Escribe un email de prueba.');return}
  setStatus('Enviando prueba...'); const body={plantel:$('plantel').value,date:$('date').value,test_email:email};
  try{const r=await fetch('/api/v1/health-reports/send-test',{method:'POST',headers:hdr(),body:JSON.stringify(body)}); const j=await parseResponse(r); updateAuthState('ok','Token válido'); const extra = j.error ? ` · ${j.error}` : (j.resolver_error ? ` · Aviso destinatarios: ${j.resolver_error}` : ''); setStatus(`Prueba procesada: ${j.status || 'sin estado'} ${j.message_id ? '#'+j.message_id : ''}${extra}`); if(j.html){$('frame').srcdoc=j.html; $('previewTitle').textContent=j.subject; $('meta').textContent=JSON.stringify({message_id:j.message_id,status:j.status,error:j.error,resolver_error:j.resolver_error},null,2);} await loadMessages(false);}catch(e){handleError(e,'send-test')}
}
async function loadMessages(showStatus=true){
  if(showStatus)setStatus('Cargando historial...'); const d=$('date').value,p=$('plantel').value;
  try{const r=await fetch(`/api/v1/health-reports/messages?date=${encodeURIComponent(d)}&plantel=${encodeURIComponent(p)}&limit=80`,{headers:hdr()}); const j=await parseResponse(r); updateAuthState('ok','Token válido'); renderMessages(j.messages || []); if(showStatus)setStatus(`${j.messages.length} mensajes.`);}catch(e){handleError(e,'historial')}
}
function renderMessages(messages){
  $('messages').innerHTML='';
  if(!messages.length){$('messages').innerHTML='<div class="empty">No hay mensajes para este filtro.</div>'}
  let sent=0,opened=0,clicked=0;
  messages.forEach(m=>{sent += m.status==='sent'?1:0; opened += Number(m.open_count||0)>0?1:0; clicked += Number(m.click_count||0)>0?1:0; const div=document.createElement('div');div.className='row';div.onclick=()=>loadHtml(m.id);div.innerHTML=`<span class="pill ${m.severity||'fulfilled'}">${m.severity||'sin severidad'}</span> <span class="pill ${m.status||'generated'}">${m.status||'generated'}</span><div class="rowtitle">${escapeHtml(m.subject||'Sin asunto')}</div><div class="rowmeta">${escapeHtml(m.recipient||'')} · gerente: ${escapeHtml(m.manager||'—')}</div><div class="rowmeta">opens ${m.open_count||0} · clicks ${m.click_count||0} · ${m.sent_at||'sin envío'}</div>`;$('messages').appendChild(div)});
  $('statMsgs').textContent=messages.length;$('statSent').textContent=sent;$('statOpened').textContent=opened;$('statClicked').textContent=clicked;
}
async function loadHtml(id){
  setStatus('Abriendo HTML enviado...');
  try{const r=await fetch(`/api/v1/health-reports/messages/${id}/html`,{headers:hdr()}); const j=await parseResponse(r); $('frame').srcdoc=j.html; $('previewTitle').textContent=j.subject; $('meta').textContent=JSON.stringify(j.meta,null,2); setStatus('Mensaje cargado.')}catch(e){handleError(e,'html-message')}
}
async function syncRead(){
  setStatus('Sincronizando lectura vía Gmail...');
  try{const r=await fetch('/api/v1/health-reports/sync-read-status',{method:'POST',headers:hdr(),body:'{}'}); const j=await parseResponse(r); updateAuthState('ok','Token válido'); setStatus(`Revisados: ${j.checked}, actualizados: ${j.updated}`); await loadMessages(false);}catch(e){handleError(e,'sync-read-status')}
}
function copyMeta(){navigator.clipboard.writeText($('meta').textContent || '{}'); setStatus('Meta copiada.')}
function escapeHtml(s){return String(s||'').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]))}
checkAuth().then(()=>preview());
</script>
</body></html>"""
