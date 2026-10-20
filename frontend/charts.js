/* Shared helpers + dependency-free SVG line chart. */
const $ = (s, r = document) => r.querySelector(s);
const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmt = (v, d = 3) => v === null || v === undefined ? '-' : (typeof v === 'number' ? (Math.abs(v) >= 1e5 ? v.toExponential(2) : +v.toFixed(d)).toLocaleString() : esc(v));
const COLORS = ['#4da3ff', '#3fb68b', '#e0a526', '#ef6b5b', '#b48cff', '#4dd0e1', '#ff8a65', '#9ccc65'];

function toast(msg, kind = 'ok') {
  const t = $('#toast'); t.textContent = msg; t.className = kind; t.style.display = 'block';
  clearTimeout(toast.t); toast.t = setTimeout(() => t.style.display = 'none', kind === 'err' ? 7000 : 3500);
}

async function api(path, opts = {}) {
  let res;
  try { res = await fetch('/api' + path, opts); }
  catch (e) { throw new Error('Cannot reach the API server: ' + e.message); }
  const ct = res.headers.get('content-type') || '';
  const body = ct.includes('json') ? await res.json() : await res.text();
  if (!res.ok) {
    const d = body && body.detail;
    const msg = typeof d === 'string' ? d : (d && d.message) || (Array.isArray(d) ? d.map(x => x.msg).join('; ') : res.statusText);
    const err = new Error(msg); err.status = res.status; err.detail = d; throw err;
  }
  return body;
}
const post = (p, body) => api(p, { method: 'POST', headers: body ? { 'Content-Type': 'application/json' } : {}, body: body ? JSON.stringify(body) : undefined });

/* series: [{name, x:[], y:[], color}]; bands: [{x0,x1,color,label,onclick}]; xFmt: x => label */
function lineChart(series, { bands = [], xFmt = x => fmt(x, 1), unit = '', height = 300, highlight = null } = {}) {
  const W = 900, H = height, m = { l: 54, r: 12, t: 10, b: 24 };
  const xs = series.flatMap(s => s.x), ys = series.flatMap(s => s.y.filter(v => v !== null));
  if (!xs.length || !ys.length) return '<div class="empty">No data points for this selection.</div>';
  let x0 = Math.min(...xs), x1 = Math.max(...xs), y0 = Math.min(...ys), y1 = Math.max(...ys);
  if (x1 === x0) x1 = x0 + 1; if (y1 === y0) { y1 += 1; y0 -= 1; }
  const pad = (y1 - y0) * 0.06; y0 -= pad; y1 += pad;
  const X = v => m.l + (v - x0) / (x1 - x0) * (W - m.l - m.r), Y = v => H - m.b - (v - y0) / (y1 - y0) * (H - m.t - m.b);
  let g = '';
  for (let i = 0; i <= 4; i++) {
    const v = y0 + (y1 - y0) * i / 4, y = Y(v);
    g += `<line x1="${m.l}" x2="${W - m.r}" y1="${y}" y2="${y}" stroke="#222c38"/><text x="${m.l - 6}" y="${y + 3}" text-anchor="end">${fmt(v, 2)}</text>`;
  }
  for (let i = 0; i <= 5; i++) {
    const v = x0 + (x1 - x0) * i / 5;
    g += `<text x="${X(v)}" y="${H - 6}" text-anchor="middle">${esc(xFmt(v))}</text>`;
  }
  const b = bands.map(d => {
    const a = Math.max(X(d.x0), m.l), z = Math.min(Math.max(X(d.x1), X(d.x0) + 3), W - m.r);
    return a < W - m.r && z > m.l ? `<rect x="${a}" y="${m.t}" width="${z - a}" height="${H - m.t - m.b}" fill="${d.color || '#e0a526'}" opacity=".22" ${d.id ? `data-band="${esc(d.id)}" style="cursor:pointer"` : ''}><title>${esc(d.label || '')}</title></rect>` : '';
  }).join('');
  const hl = highlight ? `<rect x="${X(highlight[0])}" y="${m.t}" width="${Math.max(3, X(highlight[1]) - X(highlight[0]))}" height="${H - m.t - m.b}" fill="#e0a526" opacity=".28"/>` : '';
  const lines = series.map((s, i) => {
    const c = s.color || COLORS[i % COLORS.length]; let d = '', pen = false;
    s.x.forEach((xv, k) => { const yv = s.y[k]; if (yv === null) { pen = false; return; } d += (pen ? 'L' : 'M') + X(xv).toFixed(1) + ' ' + Y(yv).toFixed(1); pen = true; });
    return `<path d="${d}" fill="none" stroke="${c}" stroke-width="1.3"/>`;
  }).join('');
  const legend = series.map((s, i) => `<span><i style="background:${s.color || COLORS[i % COLORS.length]}"></i>${esc(s.name)}</span>`).join('');
  return `<div class="legend">${legend}<span class="mut">${esc(unit)}</span></div><svg class="chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="Time-series chart">${g}${b}${hl}${lines}</svg>`;
}
const sevBadge = s => `<span class="${esc(s)}">${esc(s)}</span>`;
const table = (cols, rows, { cls = '', rowAttr = () => '' } = {}) => !rows.length ? '<div class="empty">Nothing to show.</div>' :
  `<div class="scroll ${cls}"><table><thead><tr>${cols.map(c => `<th class="${c.num ? 'num' : ''}">${esc(c.h)}</th>`).join('')}</tr></thead><tbody>${rows.map(r =>
    `<tr ${rowAttr(r)}>${cols.map(c => `<td class="${c.num ? 'num' : ''}">${c.f ? c.f(r) : esc(r[c.k])}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
const isoShort = s => s ? s.replace('T', ' ').replace(/\.\d+/, '').replace('+00:00', 'Z') : '-';
