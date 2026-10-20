/* OrbitalPulse dashboard. Every control calls the real backend API; no mock data. */
const S = { runs: [], sensors: [], sel: { run: null, compare: [] } };
const pages = {};
const app = $('#app');

async function loadBase() {
  [S.runs, S.sensors] = await Promise.all([api('/runs'), api('/config/sensors').then(c => c.sensors)]);
  if (!S.sel.run && S.runs.length) S.sel.run = S.runs[0].run_id;
}
const runOptions = (sel, any) => (any ? `<option value="">${any}</option>` : '') + S.runs.map(r => `<option ${r.run_id === sel ? 'selected' : ''}>${esc(r.run_id)}</option>`).join('');
const noData = `<div class="empty">No data loaded yet. <button class="pri" id="loadDemo">Load synthetic demo campaign</button><div class="mut" style="margin-top:8px">or import a CSV on the Ingestion page</div></div>`;
function wireDemo() { const b = $('#loadDemo'); if (b) b.onclick = async () => { b.disabled = true; try { await post('/demo/load'); toast('Demo campaign loaded'); await loadBase(); route(); } catch (e) { toast(e.message, 'err'); b.disabled = false; } }; }
const safe = fn => async (...a) => { try { await fn(...a); } catch (e) { app.innerHTML = `<div class="note">Error: ${esc(e.message)}</div>`; toast(e.message, 'err'); } };

/* ---------------- Overview ---------------- */
pages.overview = async () => {
  const [o, st] = await Promise.all([api('/overview'), api('/status')]);
  const sev = ['INFO', 'WARNING', 'ERROR', 'CRITICAL'];
  const stat = (v, l) => `<div class="card stat"><b>${v}</b><span>${l}</span></div>`;
  app.innerHTML = `<h1>Overview</h1>
  ${o.runs ? '' : noData}
  <div class="grid g4">${stat(o.campaigns, 'campaigns')}${stat(o.runs, 'test runs')}${stat(o.events_valid.toLocaleString(), 'stored (valid) records')}
  ${stat(o.quarantined.toLocaleString(), 'quarantined records')}${stat(o.duplicates_ignored, 'duplicates ignored')}${stat(o.completeness_pct == null ? '-' : fmt(o.completeness_pct, 1) + '%', 'avg completeness (estimate)')}</div>
  <div class="grid g2" style="margin-top:12px">
   <div class="card"><h2>Quality issues by severity</h2>${sev.map(s => `<div>${sevBadge(s)} <b>${(o.issues_by_severity[s] || 0).toLocaleString()}</b></div>`).join('')}
     <div class="mut" style="margin-top:6px">Severity describes data quality only, not engine safety.</div>
     <h3>Findings by review state</h3>${Object.entries(o.findings_by_review).map(([k, v]) => `<span class="badge">${esc(k)}: ${v}</span> `).join('') || '<span class="mut">none</span>'}</div>
   <div class="card"><h2>Streaming service status</h2><div class="kv">
     <div>Mode</div><div><span class="badge ${st.mode === 'LIVE_KAFKA' ? 'b-ok' : 'b-warn'}">${esc(st.mode)}</span> <span class="mut">${esc(st.mode_explanation)}</span></div>
     <div>Kafka broker</div><div>${st.kafka.reachable ? '<span class="b-ok">reachable</span>' : '<span class="b-err">not reachable</span>'} <span class="mut">${esc(st.kafka.bootstrap)}</span></div>
     <div>Consumer</div><div>${esc(st.consumer.state)}${st.consumer.alive ? ' (heartbeat ' + st.consumer.age_s + 's ago)' : ''}</div>
     <div>Consumer lag</div><div>${st.consumer.consumer_lag == null ? '<span class="mut">not available</span>' : fmt(st.consumer.consumer_lag, 0)}</div></div></div>
   <div class="card"><h2>Recent processing activity</h2>${table([{ h: 'Time', f: r => isoShort(r.ts) }, { h: 'Kind', k: 'kind' }, { h: 'Message', k: 'message' }], o.recent_activity)}</div>
   <div class="card"><h2>Recent ingestion jobs</h2>${table([{ h: 'Job', k: 'job_id' }, { h: 'Source', k: 'source_type' }, { h: 'Status', k: 'status' }, { h: 'Rows', num: 1, k: 'rows_total' }, { h: 'Imported', num: 1, k: 'imported' }, { h: 'Dup', num: 1, k: 'duplicates' }, { h: 'Quar.', num: 1, k: 'quarantined' }], o.recent_jobs)}</div></div>`;
  wireDemo();
};

/* ---------------- Campaigns & Runs ---------------- */
pages.runs = async () => {
  if (!S.runs.length) { app.innerHTML = `<h1>Campaigns &amp; Runs</h1>${noData}`; return wireDemo(); }
  app.innerHTML = `<h1>Campaigns &amp; Runs</h1><div class="card">${table([
    { h: 'Run', k: 'run_id' }, { h: 'Campaign', k: 'campaign_id' }, { h: 'Scenario', f: r => esc(r.scenario || '-') }, { h: 'Source', k: 'source_type' },
    { h: 'Records', num: 1, f: r => r.summary.event_count.toLocaleString() }, { h: 'Quarantined', num: 1, f: r => r.summary.quarantined_count },
    { h: 'Issues', num: 1, f: r => r.summary.issue_count }, { h: 'Findings', num: 1, f: r => r.summary.finding_count },
    { h: 'Completeness %', num: 1, f: r => fmt(r.summary.completeness_pct, 1) }], S.runs, { rowAttr: r => `class="click ${r.run_id === S.sel.run ? 'sel' : ''}" data-run="${esc(r.run_id)}"` })}</div>
  <div id="runDetail" style="margin-top:12px"></div>`;
  app.querySelectorAll('[data-run]').forEach(tr => tr.onclick = () => { S.sel.run = tr.dataset.run; app.querySelectorAll('[data-run]').forEach(x => x.classList.toggle('sel', x === tr)); runDetail(); });
  runDetail();
};
const phaseColor = { PREPARATION: '#46576b', STARTUP: '#2f6fb0', TEST_WINDOW: '#2f8f6d', SHUTDOWN: '#a8742a', POST_TEST: '#6b4f8f', UNKNOWN: '#555' };
async function runDetail() {
  const box = $('#runDetail'); if (!box || !S.sel.run) return;
  const [d, tl] = await Promise.all([api('/runs/' + encodeURIComponent(S.sel.run)), api('/runs/' + encodeURIComponent(S.sel.run) + '/timeline')]);
  const sp = x => `${(x / tl.duration_s * 100).toFixed(2)}%`;
  const marker = (items, color, label, kind) => items.filter(i => i.start_s != null).map(i => `<div class="seg" data-${kind}="${esc(i.finding_id || i.issue_id)}" title="${esc(i.rule_id + (i.sensor_id ? ' - ' + i.sensor_id : ''))}" style="left:${sp(Math.max(0, i.start_s))};width:max(4px,${sp(Math.max(0, i.end_s - i.start_s))});background:${color};cursor:pointer;padding:0"></div>`).join('');
  const maxc = Math.max(...tl.event_volume.counts, 1);
  box.innerHTML = `<div class="grid g2"><div class="card"><h2>${esc(d.run_id)}</h2><div class="kv">
    <div>Campaign</div><div>${esc(d.campaign_id)}</div><div>Scenario</div><div>${esc(d.scenario || '-')}</div><div>Synthetic</div><div>${d.synthetic ? 'yes' : 'no'}</div>
    <div>Source</div><div>${esc(d.source_type)}</div><div>Start</div><div>${isoShort(tl.start)}</div><div>End</div><div>${isoShort(tl.end)}</div><div>Duration</div><div>${fmt(tl.duration_s, 1)} s</div>
    <div>Analysed</div><div>${isoShort(d.analyzed_at)} (config ${esc(d.config_version)})</div></div>
    <h3>Ingestion jobs</h3>${table([{ h: 'Job', k: 'job_id' }, { h: 'Source', k: 'source_name' }, { h: 'Status', k: 'status' }, { h: 'Imported', num: 1, k: 'imported' }, { h: 'Dup', num: 1, k: 'duplicates' }, { h: 'Quar.', num: 1, k: 'quarantined' }], d.ingestion_jobs)}
    <div class="bar" style="margin-top:8px"><a href="#/explorer">Open in Explorer</a><a href="/api/report?run_ids=${encodeURIComponent(d.run_id)}&format=html">HTML report</a></div></div>
   <div class="card timeline"><h2>Timeline</h2>
    <div class="tl-lab">Phases</div><div class="tl-row">${tl.phases.map(p => `<div class="seg" style="left:${sp(p.start_s)};width:${sp(p.end_s - p.start_s)};background:${phaseColor[p.phase] || '#555'}">${esc(p.phase)}</div>`).join('')}</div>
    <div class="tl-lab">Event volume over time</div><div class="tl-row" style="height:34px;display:flex;align-items:flex-end">${tl.event_volume.counts.map(c => `<div style="flex:1;margin:0 .5px;background:#4da3ff;height:${(c / maxc * 100).toFixed(0)}%" title="${c} events"></div>`).join('')}</div>
    <div class="tl-lab">Data gaps</div><div class="tl-row">${marker(tl.gaps, '#ef6b5b', 'gap', 'finding')}</div>
    <div class="tl-lab">Quality issues (click for detail)</div><div class="tl-row">${marker(tl.quality_issues, '#e0a526', 'issue', 'issue')}</div>
    <div class="tl-lab">Anomaly findings (click for evidence)</div><div class="tl-row">${marker(tl.findings, '#b48cff', 'finding', 'finding')}</div>
    <div class="tl-lab">Ingestion interruptions: ${tl.ingestion_interruptions.length ? tl.ingestion_interruptions.map(j => esc(j.job_id + ' ' + j.status)).join(', ') : 'none recorded'}</div></div></div>`;
  box.querySelectorAll('[data-finding]').forEach(e => e.onclick = () => { S.openFinding = e.dataset.finding; location.hash = '#/anomalies'; });
  box.querySelectorAll('[data-issue]').forEach(e => e.onclick = () => { S.openIssue = e.dataset.issue; S.qtab = 'issues'; location.hash = '#/quality'; });
}

/* ---------------- Telemetry Explorer ---------------- */
pages.explorer = async () => {
  if (!S.runs.length) { app.innerHTML = `<h1>Telemetry Explorer</h1>${noData}`; return wireDemo(); }
  const picked = new Set(S.exSensors || ['chamber_pressure']);
  app.innerHTML = `<h1>Telemetry Explorer</h1><div class="card"><div class="bar">
   <label>Run <select id="exRun">${runOptions(S.sel.run)}</select></label>
   <label>Phase <select id="exPhase"><option value="">all</option>${['PREPARATION', 'STARTUP', 'TEST_WINDOW', 'SHUTDOWN', 'POST_TEST'].map(p => `<option>${p}</option>`).join('')}</select></label>
   <label>From (s) <input id="exFrom" size="5" placeholder="0"></label><label>To (s) <input id="exTo" size="5" placeholder="end"></label>
   <label><input type="checkbox" id="exMarkers" checked> anomaly markers</label></div>
   <div class="bar" id="exSensors">${S.sensors.map(s => `<label><input type="checkbox" value="${esc(s.sensor_id)}" ${picked.has(s.sensor_id) ? 'checked' : ''}> ${esc(s.display_name)} <span class="mut">[${esc(s.unit)}]</span></label>`).join('')}</div>
   <div id="exChart"></div><div id="exInfo" class="mut"></div></div>
   <div class="card" style="margin-top:12px"><h2>Underlying records</h2><div class="bar"><label>Sensor <select id="evSensor">${S.sensors.map(s => `<option>${esc(s.sensor_id)}</option>`).join('')}</select></label>
   <button id="evPrev">Prev</button><button id="evNext">Next</button><span id="evPos" class="mut"></span></div><div id="evTable"></div></div>`;
  let offset = 0; const PAGE = 25;
  const draw = safe(async () => {
    S.sel.run = $('#exRun').value; const ids = [...app.querySelectorAll('#exSensors input:checked')].map(i => i.value); S.exSensors = ids;
    if (!ids.length) { $('#exChart').innerHTML = '<div class="empty">Select at least one sensor.</div>'; return; }
    const run = S.runs.find(r => r.run_id === S.sel.run); const t0 = run && run.summary.first_timestamp ? Date.parse(run.summary.first_timestamp) / 1000 : 0;
    const q = new URLSearchParams({ sensor_ids: ids.join(',') }); const ph = $('#exPhase').value; if (ph) q.set('phase', ph);
    const f = $('#exFrom').value, t = $('#exTo').value; if (f !== '') q.set('start', t0 + +f); if (t !== '') q.set('end', t0 + +t);
    const [ts, fin] = await Promise.all([api(`/runs/${encodeURIComponent(S.sel.run)}/timeseries?${q}`), $('#exMarkers').checked ? api(`/findings?run_id=${encodeURIComponent(S.sel.run)}&limit=500`) : { items: [] }]);
    const units = [...new Set(ts.series.map(s => s.unit))];
    const series = ts.series.map(s => ({ name: `${s.display_name} (${s.unit})`, x: s.x.map(v => v - t0), y: s.y }));
    const bands = fin.items.filter(i => i.window_start && (!i.sensor_id || ids.includes(i.sensor_id))).map(i => ({ x0: Date.parse(i.window_start) / 1000 - t0, x1: Date.parse(i.window_end) / 1000 - t0, label: `${i.rule_id}: ${i.explanation}`, color: i.rule_id === 'AN_MISSING_BURST' ? '#ef6b5b' : '#e0a526' }));
    $('#exChart').innerHTML = (units.length > 1 ? '<div class="note">Selected sensors use different units; they share one axis for shape comparison only.</div>' : '') + lineChart(series, { bands, xFmt: v => v.toFixed(0) + 's', unit: units.join(' / ') });
    $('#exInfo').innerHTML = ts.series.map(s => `${esc(s.sensor_id)}: ${s.total_points} stored points${s.downsampled ? ` (min/max-downsampled to ${s.returned_points} for display)` : ''}, expected interval ${fmt(s.expected_interval_s)} s`).join(' &middot; ') + '<br>Shaded bands = anomaly findings (hover for rule). X axis = seconds since run start (display only).';
  });
  const rows = safe(async () => {
    const q = new URLSearchParams({ sensor_id: $('#evSensor').value, limit: PAGE, offset }); const ph = $('#exPhase').value; if (ph) q.set('phase', ph);
    const r = await api(`/runs/${encodeURIComponent($('#exRun').value)}/events?${q}`);
    $('#evTable').innerHTML = table([{ h: 'Event', k: 'event_id' }, { h: 'Event time', f: r => isoShort(r.event_ts) }, { h: 'Seq', num: 1, k: 'sequence_number' }, { h: 'Value', num: 1, f: r => r.value === null ? '<span class="WARNING">missing</span>' : fmt(r.value, 4) }, { h: 'Unit', k: 'unit' }, { h: 'Phase', k: 'phase' }, { h: 'Status', k: 'quality_status' }, { h: 'Job', k: 'job_id' }], r.items);
    $('#evPos').textContent = `${r.total ? offset + 1 : 0}-${Math.min(offset + PAGE, r.total)} of ${r.total}`; $('#evPrev').disabled = offset === 0; $('#evNext').disabled = offset + PAGE >= r.total;
  });
  ['exRun', 'exPhase', 'exFrom', 'exTo', 'exMarkers'].forEach(id => $('#' + id).onchange = () => { offset = 0; draw(); rows(); });
  $('#exSensors').onchange = draw; $('#evSensor').onchange = () => { offset = 0; rows(); };
  $('#evPrev').onclick = () => { offset = Math.max(0, offset - PAGE); rows(); }; $('#evNext').onclick = () => { offset += PAGE; rows(); };
  draw(); rows();
};

/* ---------------- Data Quality ---------------- */
pages.quality = async () => {
  if (!S.runs.length) { app.innerHTML = `<h1>Data Quality</h1>${noData}`; return wireDemo(); }
  const tab = S.qtab || 'issues';
  app.innerHTML = `<h1>Data Quality</h1><div class="card"><div class="bar"><button id="tIssues" class="${tab === 'issues' ? 'pri' : ''}">Quality issues</button><button id="tQuar" class="${tab === 'quar' ? 'pri' : ''}">Quarantine</button><span class="sp"></span>
   <label>Run <select id="fRun">${runOptions('', 'all runs')}</select></label><label>Rule <select id="fRule"><option value="">all rules</option></select></label>
   ${tab === 'issues' ? `<label>Severity <select id="fSev"><option value="">all</option><option>INFO</option><option>WARNING</option><option>ERROR</option><option>CRITICAL</option></select></label>` : ''}
   <label>Sensor <select id="fSensor"><option value="">all</option>${S.sensors.map(s => `<option>${esc(s.sensor_id)}</option>`).join('')}</select></label><a id="export" href="#">Export CSV</a></div>
   <div id="qList"></div><div class="bar"><button id="qPrev">Prev</button><button id="qNext">Next</button><span id="qPos" class="mut"></span></div></div><div id="qDetail" class="card" style="margin-top:12px;display:none"></div>`;
  let offset = 0; const PAGE = 20; const g = id => ($('#' + id) || {}).value || '';
  const params = () => { const q = new URLSearchParams(); [['run_id', 'fRun'], ['rule_id', 'fRule'], ['severity', 'fSev'], ['sensor_id', 'fSensor']].forEach(([k, id]) => g(id) && q.set(k, g(id))); return q; };
  const load = safe(async () => {
    const q = params(); q.set('limit', PAGE); q.set('offset', offset);
    const base = tab === 'issues' ? '/quality/issues' : '/quarantine'; const r = await api(`${base}?${q}`);
    if (tab === 'issues') { const rs = r.facets.rules, cur = g('fRule'); $('#fRule').innerHTML = '<option value="">all rules</option>' + rs.map(x => `<option ${x === cur ? 'selected' : ''}>${x}</option>`).join(''); }
    else { const cur = g('fRule'); $('#fRule').innerHTML = '<option value="">all rules</option>' + Object.keys(r.by_rule).map(x => `<option ${x === cur ? 'selected' : ''}>${x}</option>`).join(''); }
    const cols = tab === 'issues' ? [{ h: 'Severity', f: x => sevBadge(x.severity) }, { h: 'Rule', k: 'rule_id' }, { h: 'Run', k: 'run_id' }, { h: 'Sensor', k: 'sensor_id' }, { h: 'Observed', k: 'observed' }, { h: 'Expected', k: 'expected' }, { h: 'Source', k: 'source_ref' }]
      : [{ h: 'Rule', k: 'rule_id' }, { h: 'Severity', f: x => sevBadge(x.severity) }, { h: 'Run', k: 'run_id' }, { h: 'Sensor', k: 'sensor_id' }, { h: 'Observed', k: 'observed' }, { h: 'Source', k: 'source_ref' }, { h: 'Replayed', k: 'replayed' }];
    $('#qList').innerHTML = table(cols, r.items, { rowAttr: x => `class="click" data-id="${esc(tab === 'issues' ? x.issue_id : x.quarantine_id)}"` });
    $('#qPos').textContent = `${r.total ? offset + 1 : 0}-${Math.min(offset + PAGE, r.total)} of ${r.total}`; $('#qPrev').disabled = !offset; $('#qNext').disabled = offset + PAGE >= r.total;
    $('#export').href = `/api/${tab === 'issues' ? 'quality/issues.csv' : 'quarantine.csv'}?${params()}`;
    app.querySelectorAll('#qList tr[data-id]').forEach(tr => tr.onclick = () => detail(r.items.find(x => String(tab === 'issues' ? x.issue_id : x.quarantine_id) === tr.dataset.id)));
    if (S.openIssue && tab === 'issues') { const id = S.openIssue; S.openIssue = null; detail(await api('/quality/issues/' + id)); }
  });
  const detail = safe(async it => {
    let raw = it.raw_payload; if (tab === 'issues' && raw === undefined) it = await api('/quality/issues/' + it.issue_id), raw = it.raw_payload;
    if (tab === 'quar') { try { raw = JSON.parse(it.raw_payload); } catch { raw = it.raw_payload; } }
    const box = $('#qDetail'); box.style.display = 'block';
    box.innerHTML = `<h2>${esc(it.rule_id)} ${sevBadge(it.severity)}</h2><p>${esc(it.explanation)}</p><div class="kv"><div>Observed</div><div>${esc(it.observed)}</div><div>Expected</div><div>${esc(it.expected)}</div>
     <div>Run / sensor</div><div>${esc(it.run_id || '-')} / ${esc(it.sensor_id || '-')}</div><div>Event ID</div><div>${esc(it.event_id || '-')}</div><div>Source reference</div><div>${esc(it.source_ref || '-')}</div><div>Job</div><div>${esc(it.job_id || '-')}</div></div>
     <h3>Original payload (raw, preserved)</h3><pre>${raw ? esc(JSON.stringify(raw, null, 2)) : 'Run-level issue: derived from stored events, no single raw payload.'}</pre>`;
    box.scrollIntoView({ behavior: 'smooth' });
  });
  $('#tIssues').onclick = () => { S.qtab = 'issues'; route(); }; $('#tQuar').onclick = () => { S.qtab = 'quar'; route(); };
  ['fRun', 'fRule', 'fSev', 'fSensor'].forEach(id => { const e = $('#' + id); if (e) e.onchange = () => { offset = 0; load(); }; });
  $('#qPrev').onclick = () => { offset = Math.max(0, offset - PAGE); load(); }; $('#qNext').onclick = () => { offset += PAGE; load(); }; load();
};

/* ---------------- Anomalies ---------------- */
pages.anomalies = async () => {
  if (!S.runs.length) { app.innerHTML = `<h1>Anomalies</h1>${noData}`; return wireDemo(); }
  app.innerHTML = `<h1>Anomaly findings</h1><div class="note info">Rule-based screening results: "unusual sensor pattern / review recommended". Never a confirmed malfunction.</div>
   <div class="grid g2"><div class="card"><div class="bar"><label>Run <select id="aRun">${runOptions('', 'all runs')}</select></label><label>Rule <select id="aRule"><option value="">all</option></select></label>
    <label>Review <select id="aRev"><option value="">all</option><option>OPEN</option><option>REVIEWED</option><option>FALSE_POSITIVE</option></select></label></div><div id="aList"></div></div><div id="aDetail" class="card"><div class="empty">Select a finding to see its evidence.</div></div></div>`;
  const g = id => $('#' + id).value;
  const load = safe(async () => {
    const q = new URLSearchParams({ limit: 200 }); [['run_id', 'aRun'], ['rule_id', 'aRule'], ['review_status', 'aRev']].forEach(([k, id]) => g(id) && q.set(k, g(id)));
    const r = await api('/findings?' + q); const cur = g('aRule');
    $('#aRule').innerHTML = '<option value="">all</option>' + r.facets.rules.map(x => `<option ${x === cur ? 'selected' : ''}>${x}</option>`).join('');
    $('#aList').innerHTML = table([{ h: 'Rule', f: x => esc(x.rule_id.replace('AN_', '')) }, { h: 'Run', f: x => esc(x.run_id.replace('RUN-', '')) }, { h: 'Sensor', f: x => esc(x.sensor_id || 'run-level') }, { h: 'Sev.', f: x => sevBadge(x.severity[0]) }, { h: 'Review', f: x => esc(x.review_status) }], r.items, { rowAttr: x => `class="click" data-id="${x.finding_id}"` });
    app.querySelectorAll('#aList tr[data-id]').forEach(tr => tr.onclick = () => show(tr.dataset.id));
    if (S.openFinding) { const id = S.openFinding; S.openFinding = null; show(id); }
  });
  const show = safe(async id => {
    const f = await api('/findings/' + id); const box = $('#aDetail'); const ev = f.evidence_events || [];
    const chart = ev.length ? lineChart([{ name: `${f.sensor_id} (${ev[0].unit})`, x: ev.map(e => e.event_epoch), y: ev.map(e => e.value) }], { height: 220, xFmt: v => new Date(v * 1000).toISOString().substr(11, 8), highlight: [Date.parse(f.window_start) / 1000, Date.parse(f.window_end) / 1000] }) : '<div class="mut">No sensor-level evidence window for run-level findings.</div>';
    box.innerHTML = `<h2>${esc(f.rule_name)} <span class="badge">${esc(f.category.replaceAll('_', ' '))}</span></h2><p>${esc(f.explanation)}</p>${chart}
     <h3>Measured evidence</h3><pre>${esc(JSON.stringify(f.evidence, null, 2))}</pre><h3>Configured threshold (synthetic demo setting)</h3><pre>${esc(JSON.stringify(f.threshold, null, 2))}</pre>
     <h3>Limitations / alternative explanations</h3><p class="mut">${esc(f.limitations)}</p><div class="kv"><div>Run</div><div>${esc(f.run_id)}</div><div>Window</div><div>${isoShort(f.window_start)} &rarr; ${isoShort(f.window_end)}</div><div>Config / processing</div><div>${esc(f.config_version)} / ${esc(f.processing_version)}</div>
     <div>Review</div><div>${esc(f.review_status)}${f.reviewed_at ? ' at ' + isoShort(f.reviewed_at) : ''}${f.reviewer ? ' by ' + esc(f.reviewer) : ''}${f.review_note ? ' - ' + esc(f.review_note) : ''}</div></div>
     <h3>Analyst review</h3><div class="bar"><input id="rvName" placeholder="reviewer" size="12"><input id="rvNote" placeholder="note" size="28"><button data-s="REVIEWED">Mark reviewed</button><button data-s="FALSE_POSITIVE">False positive</button><button data-s="OPEN">Reopen</button></div>`;
    box.querySelectorAll('[data-s]').forEach(b => b.onclick = safe(async () => { await api(`/findings/${id}/review`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ status: b.dataset.s, reviewer: $('#rvName').value, note: $('#rvNote').value }) }); toast('Review saved'); await load(); show(id); }));
  });
  ['aRun', 'aRule', 'aRev'].forEach(id => $('#' + id).onchange = load); load();
};

/* ---------------- Run Comparison ---------------- */
pages.compare = async () => {
  if (S.runs.length < 2) { app.innerHTML = `<h1>Run Comparison</h1>${S.runs.length ? '<div class="empty">At least two runs are needed.</div>' : noData}`; return wireDemo(); }
  if (S.sel.compare.length < 2) S.sel.compare = S.runs.slice(0, 2).map(r => r.run_id);
  app.innerHTML = `<h1>Run Comparison</h1><div class="card"><div class="bar">${S.runs.map(r => `<label><input type="checkbox" class="cr" value="${esc(r.run_id)}" ${S.sel.compare.includes(r.run_id) ? 'checked' : ''}> ${esc(r.run_id)}</label>`).join('')}
   <span class="sp"></span><label>Sensor <select id="cSensor">${S.sensors.map(s => `<option>${esc(s.sensor_id)}</option>`).join('')}</select></label><label><input type="checkbox" id="cAlign" checked> align by elapsed time</label><a id="cReport" href="#">HTML report</a><a id="cJson" href="#">JSON</a></div>
   <div id="cWarn"></div><div id="cChart"></div></div><div id="cBody"></div>`;
  const run = safe(async () => {
    const ids = [...app.querySelectorAll('.cr:checked')].map(i => i.value); S.sel.compare = ids;
    if (ids.length < 2) { $('#cBody').innerHTML = '<div class="empty">Select at least two runs.</div>'; return; }
    const c = await api('/compare?run_ids=' + ids.map(encodeURIComponent).join(','));
    $('#cReport').href = `/api/report?run_ids=${ids.join(',')}&format=html`; $('#cJson').href = `/api/report?run_ids=${ids.join(',')}&format=json`;
    $('#cWarn').innerHTML = c.warnings.map(w => `<div class="note ${w.level === 'INFO' ? 'info' : ''}">${esc(w.message)}</div>`).join('') + `<div class="mut">${esc(c.method_note)}</div>`;
    const sid = $('#cSensor').value, align = $('#cAlign').checked ? 'elapsed' : 'absolute';
    const sers = await Promise.all(ids.map(r => api(`/runs/${encodeURIComponent(r)}/timeseries?sensor_ids=${sid}&align=${align}`).then(x => ({ name: r, x: x.series[0].x, y: x.series[0].y, unit: x.series[0].unit }))));
    $('#cChart').innerHTML = lineChart(sers, { xFmt: v => align === 'elapsed' ? v.toFixed(0) + 's' : new Date(v * 1000).toISOString().substr(11, 8), unit: sers[0].unit });
    const pct = (s, r) => { const d = (s.diff_vs_baseline[r] || {}).mean; return d && d.pct != null ? `${d.pct >= 0 ? '+' : ''}${d.pct.toFixed(1)}%` : '-'; };
    $('#cBody').innerHTML = `<div class="card" style="margin-top:12px"><h2>Run summary</h2>${table([{ h: 'Metric', k: 'm' }, ...c.runs.map(r => ({ h: r.run_id, k: r.run_id, num: 1 }))], [
      ['Records', 'event_count'], ['Quarantined', 'quarantined_count'], ['Completeness % (est.)', 'completeness_pct'], ['Quality issues', 'issue_count'], ['Findings', 'finding_count'], ['Duration (s)', 'duration_s']].map(([m, k]) => Object.fromEntries([['m', m], ...c.runs.map(r => [r.run_id, fmt(r[k], 1)])])))}
     <h3>Anomaly counts by rule</h3>${table([{ h: 'Rule', k: 'm' }, ...ids.map(r => ({ h: r, k: r, num: 1 }))], Object.entries(c.anomaly_counts_by_rule).map(([rule, v]) => ({ m: rule, ...v })))}
     <h3>Phase durations (s)</h3>${table([{ h: 'Phase', k: 'm' }, ...ids.map(r => ({ h: r, k: r, num: 1 }))], ['PREPARATION', 'STARTUP', 'TEST_WINDOW', 'SHUTDOWN', 'POST_TEST'].map(p => Object.fromEntries([['m', p], ...ids.map(r => [r, fmt(c.phase_durations_s[r][p], 1)])])))}</div>
     <div class="card" style="margin-top:12px"><h2>Per-sensor metrics (mean / std; change vs baseline ${esc(c.baseline_run)})</h2>${table([{ h: 'Sensor', k: 'sensor_id' }, { h: 'Unit', f: s => esc(s.unit || '-') }, { h: 'Status', f: s => `<span class="${s.status === 'OK' ? 'b-ok' : s.status === 'CAUTION' ? 'b-warn' : 'b-err'}" title="${esc(s.notes.join(' '))}">${s.status}</span>` },
      ...ids.map(r => ({ h: r, num: 1, f: s => s.per_run[r] ? `${fmt(s.per_run[r].mean, 2)} / ${fmt(s.per_run[r].std, 2)}${r !== ids[0] ? ` <span class="mut">(${pct(s, r)})</span>` : ''}` : '-' }))], c.sensors)}</div>`;
  });
  app.querySelectorAll('.cr').forEach(i => i.onchange = run); $('#cSensor').onchange = run; $('#cAlign').onchange = run; run();
};

/* ---------------- Ingestion & Pipeline ---------------- */
pages.ingest = async () => {
  const st = await api('/status');
  const procLine = (n, p) => p.managed_by_api ? `${n}: ${p.running ? 'running (pid ' + p.pid + ')' : 'stopped (exit ' + p.exit_code + ')'}` : `${n}: not started from this dashboard`;
  app.innerHTML = `<h1>Ingestion &amp; Pipeline</h1><div class="grid g2">
   <div class="card"><h2>CSV import (batch path)</h2><div class="bar"><input type="file" id="file" accept=".csv"><button id="btnPrev">Preview</button></div><div id="prev"></div></div>
   <div class="card"><h2>Pipeline mode</h2><div class="kv"><div>Mode</div><div><span class="badge ${st.mode === 'LIVE_KAFKA' ? 'b-ok' : 'b-warn'}">${esc(st.mode)}</span></div><div>Broker</div><div>${st.kafka.reachable ? '<span class="b-ok">reachable</span>' : '<span class="b-err">not reachable</span> <span class="mut">(' + esc(st.kafka.error) + ')</span>'} ${esc(st.kafka.bootstrap)}</div>
    <div>Topic</div><div>${esc(st.kafka.topic_raw)}</div><div>Consumer</div><div>${esc(st.consumer.state)} ${st.consumer.counts ? '- processed ' + st.consumer.counts.total + ', imported ' + st.consumer.counts.imported + ', dup ' + st.consumer.counts.duplicates + ', quarantined ' + st.consumer.counts.quarantined : ''}</div>
    <div>Consumer lag</div><div>${st.consumer.consumer_lag == null ? '<span class="mut">not available</span>' : st.consumer.consumer_lag}</div><div>Processes</div><div>${procLine('producer', st.producer_process)}<br>${procLine('consumer', st.consumer_process)}</div></div>
    ${st.kafka.reachable ? '' : '<div class="note">Kafka is not running. Batch/demo mode still works. Start the broker with scripts\\kafka_start.ps1 (see README).</div>'}
    <div class="bar" style="margin-top:8px"><button id="cStart" ${st.kafka.reachable ? '' : 'disabled'}>Start consumer</button><button id="cStop">Stop consumer</button><button id="pStart" ${st.kafka.reachable ? '' : 'disabled'}>Start synthetic generator</button><button id="pStop">Stop generator</button></div>
    <h3>Demo data</h3><div class="bar"><button class="pri" id="demo">Load demo campaign</button><button class="danger" id="reset">Reset ALL data</button></div></div></div>
   <div class="card" style="margin-top:12px"><h2>Ingestion jobs</h2><div id="jobs"></div></div>`;
  const jobs = safe(async () => { const r = await api('/ingestions?limit=30'); $('#jobs').innerHTML = table([{ h: 'Job', k: 'job_id' }, { h: 'Type', k: 'source_type' }, { h: 'Source', k: 'source_name' }, { h: 'Status', k: 'status' }, { h: 'Rows', num: 1, k: 'rows_total' }, { h: 'Imported', num: 1, k: 'imported' }, { h: 'Duplicates', num: 1, k: 'duplicates' }, { h: 'Quarantined', num: 1, k: 'quarantined' }, { h: 'Started', f: x => isoShort(x.started_at) }], r.items); });
  jobs();
  const act = (id, fn, msg) => $('#' + id).onclick = async () => { try { await fn(); toast(msg); await loadBase(); route(); } catch (e) { toast(e.message, 'err'); } };
  act('demo', () => post('/demo/load'), 'Demo loaded (already-imported files are skipped)');
  $('#reset').onclick = async () => { if (!confirm('Delete ALL stored events, quarantine, findings and jobs? Config is kept.')) return; try { await post('/demo/reset'); toast('All data reset'); await loadBase(); route(); } catch (e) { toast(e.message, 'err'); } };
  act('cStart', () => post('/stream/consumer/start'), 'Consumer started'); act('cStop', () => post('/stream/consumer/stop'), 'Consumer stopped');
  act('pStart', () => post('/stream/producer/start', { rate: 300, redeliver: 0.02 }), 'Generator started (2% duplicate redelivery to demo idempotency)'); act('pStop', () => post('/stream/producer/stop'), 'Generator stopped');
  let mapping = null;
  const preview = safe(async () => {
    const f = $('#file').files[0]; if (!f) return toast('Choose a CSV file first', 'err');
    const fd = new FormData(); fd.append('file', f); if (mapping) fd.append('mapping', JSON.stringify(mapping));
    const p = await api('/ingest/csv/preview', { method: 'POST', body: fd }); mapping = p.applied_mapping;
    const internal = ['test_run_id', 'sensor_id', 'event_timestamp', 'sensor_value', 'event_id', 'campaign_id', 'sequence_number', 'sensor_unit', 'test_phase'];
    $('#prev').innerHTML = `<div class="mut">${p.row_count} data rows, ${p.columns.length} columns. Extra columns (kept in raw archive): ${p.extra_columns.map(esc).join(', ') || 'none'}</div>
     ${p.missing_required_fields.length ? `<div class="note">Required fields not mapped: ${p.missing_required_fields.join(', ')}. Choose columns below.</div>` : ''}
     <h3>Column mapping</h3><div class="kv">${internal.map(k => `<div>${k}${['test_run_id', 'sensor_id', 'event_timestamp', 'sensor_value'].includes(k) ? ' *' : ''}</div><div><select data-map="${k}"><option value="">(none)</option>${p.columns.map(c => `<option ${mapping[k] === c ? 'selected' : ''}>${esc(c)}</option>`).join('')}</select></div>`).join('')}</div>
     <h3>Sample rows</h3>${table(p.columns.map(c => ({ h: c, f: r => esc(r[c] ?? (r._malformed_row ? 'MALFORMED' : '')) })), p.sample_rows)}
     <div class="bar" style="margin-top:8px"><label>Campaign <input id="camp" value="UPLOAD" size="12"></label><label>Default run id <input id="defRun" size="12" placeholder="optional"></label><button class="pri" id="doImport">Import</button></div><div id="impRes"></div>`;
    app.querySelectorAll('[data-map]').forEach(s => s.onchange = () => { mapping = Object.fromEntries([...app.querySelectorAll('[data-map]')].filter(x => x.value).map(x => [x.dataset.map, x.value])); });
    $('#doImport').onclick = safe(async () => {
      const fd2 = new FormData(); fd2.append('file', f); fd2.append('mapping', JSON.stringify(mapping)); fd2.append('default_campaign', $('#camp').value); if ($('#defRun').value) fd2.append('default_run_id', $('#defRun').value);
      try { const r = await api('/ingest/csv', { method: 'POST', body: fd2 }); $('#impRes').innerHTML = `<div class="note info">Imported job ${esc(r.job_id)}: ${r.rows_total} rows - ${r.imported} imported, ${r.duplicates} duplicates ignored, ${r.quarantined} quarantined. Runs: ${r.runs.map(esc).join(', ')}</div>`; toast('Import finished'); await loadBase(); jobs(); }
      catch (e) { $('#impRes').innerHTML = `<div class="note">Import refused: ${esc(e.message)}</div>`; toast(e.message, 'err'); }
    });
  });
  $('#btnPrev').onclick = () => { mapping = null; preview(); };
};

/* ---------------- Settings ---------------- */
pages.settings = async () => {
  const [sc, st, sch] = await Promise.all([api('/config/sensors'), api('/config/settings'), api('/schema')]);
  const num = (v) => v === null || v === undefined ? '' : v;
  app.innerHTML = `<h1>Settings</h1><div class="note">All values are <b>synthetic demonstration settings</b>, not engineering limits. Saving re-analyses every stored run.</div>
   <div class="card"><h2>Sensor metadata (${esc(sc.version)})</h2><div class="scroll" style="max-height:none"><table><thead><tr><th>Sensor</th><th>Unit</th><th>Interval s</th><th>Valid min</th><th>Valid max</th><th>Op. min</th><th>Op. max</th><th>Enabled</th></tr></thead><tbody>
   ${sc.sensors.map((s, i) => `<tr data-i="${i}"><td>${esc(s.display_name)}<div class="mut">${esc(s.sensor_id)}</div></td><td>${esc(s.unit)}</td>${['expected_interval_s', 'validation_min', 'validation_max', 'operating_min', 'operating_max'].map(k => `<td><input size="7" data-k="${k}" value="${num(s[k])}"></td>`).join('')}<td><input type="checkbox" data-k="enabled" ${s.enabled ? 'checked' : ''}></td></tr>`).join('')}</tbody></table></div>
   <div class="bar" style="margin-top:8px"><button class="pri" id="saveSensors">Save sensors</button></div></div>
   <div class="card" style="margin-top:12px"><h2>Quality &amp; anomaly thresholds (${esc(st.version)})</h2><div class="grid g4">${['quality', 'anomaly'].flatMap(g => Object.entries(st[g]).map(([k, v]) => `<label style="display:block"><span class="mut">${g}.${k}</span><br><input size="10" data-g="${g}" data-t="${k}" value="${v}"></label>`)).join('')}</div>
   <div class="bar" style="margin-top:8px"><button class="pri" id="saveSettings">Save thresholds</button><button id="resetCfg" class="danger">Reset to shipped defaults</button><a href="/api/config/export">Export configuration (JSON)</a></div></div>
   <div class="card" style="margin-top:12px"><h2>Schema versions</h2><div>Current: <b>${esc(sch.current)}</b>; supported: ${sch.supported.join(', ')}. Severity levels: ${sch.severity_levels.join(', ')} (data-quality semantics only).</div>
   <details><summary class="mut">Event schema (JSON Schema)</summary><pre>${esc(JSON.stringify(sch.event_schema, null, 2))}</pre></details></div>`;
  const parse = v => v.trim() === '' ? null : Number(v);
  $('#saveSensors').onclick = safe(async () => {
    const out = JSON.parse(JSON.stringify(sc)); app.querySelectorAll('tr[data-i]').forEach(tr => { const s = out.sensors[+tr.dataset.i]; tr.querySelectorAll('[data-k]').forEach(i => { s[i.dataset.k] = i.type === 'checkbox' ? i.checked : parse(i.value); }); });
    try { const r = await api('/config/sensors', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(out) }); toast(`Saved; ${r.reanalyzed.length} run(s) re-analysed`); } catch (e) { toast('Not saved: ' + e.message, 'err'); }
  });
  $('#saveSettings').onclick = safe(async () => {
    const out = JSON.parse(JSON.stringify(st)); app.querySelectorAll('[data-t]').forEach(i => { out[i.dataset.g][i.dataset.t] = Number(i.value); });
    try { const r = await api('/config/settings', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(out) }); toast(`Saved; ${r.reanalyzed.length} run(s) re-analysed`); } catch (e) { toast('Not saved: ' + e.message, 'err'); }
  });
  $('#resetCfg').onclick = safe(async () => { await post('/config/reset'); toast('Defaults restored'); route(); });
};

/* ---------------- router ---------------- */
async function updateBadge() { try { const s = await api('/status'); const b = $('#modeBadge'); b.textContent = s.mode === 'LIVE_KAFKA' ? 'Live Kafka mode' : 'Batch / demo mode'; b.className = 'badge ' + (s.mode === 'LIVE_KAFKA' ? 'b-ok' : 'b-warn'); } catch { $('#modeBadge').textContent = 'API unreachable'; $('#modeBadge').className = 'badge b-err'; } }
const route = safe(async () => {
  const page = (location.hash.replace('#/', '') || 'overview').split('?')[0]; const fn = pages[page] || pages.overview;
  document.querySelectorAll('.side a[data-page]').forEach(a => a.classList.toggle('on', a.dataset.page === page));
  app.innerHTML = '<div class="mut">Loading...</div>'; await loadBase(); await fn(); updateBadge();
});
window.addEventListener('hashchange', route); route();
