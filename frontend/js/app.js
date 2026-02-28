/* ════════════════════════════════════════════════════════════
   AntiFraud CRM — Frontend Logic
════════════════════════════════════════════════════════════ */
const API = window.location.origin;
const charts = {};

// Состояние сортировки
const sortState = {
  tx:    { col: 'transaction_date', dir: 'desc' },
  fraud: { col: 'transaction_date', dir: 'desc' },
};

// ── Router ────────────────────────────────────────────────────
function navigate(page) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById(`page-${page}`)?.classList.add('active');
  document.querySelector(`[data-page="${page}"]`)?.classList.add('active');
  if (page === 'transactions') loadTransactions(1);
  if (page === 'fraud')        loadFraud(1);
  if (page === 'dashboard')    loadDashboard();
  if (page === 'eda')          loadEDA();
  if (page === 'upload')       loadEtlLogs();
}
document.querySelectorAll('.nav-item').forEach(el =>
  el.addEventListener('click', e => { e.preventDefault(); navigate(el.dataset.page); })
);

// ── API Status ────────────────────────────────────────────────
async function checkApiStatus() {
  try {
    const data = await fetch(`${API}/api/dashboard/stats`).then(r => r.json());
    document.getElementById('api-dot').className = 'status-dot online';
    document.getElementById('api-status').textContent = 'Система онлайн';
    document.getElementById('fraud-badge').textContent = fmtNum(data.fraud_transactions || 0);
  } catch {
    document.getElementById('api-dot').className = 'status-dot error';
    document.getElementById('api-status').textContent = 'Нет соединения';
  }
}

// ── Banks ─────────────────────────────────────────────────────
async function loadBanks() {
  try {
    const banks = await fetch(`${API}/api/banks`).then(r => r.json());
    ['f-bank','ff-bank'].forEach(id => {
      const sel = document.getElementById(id);
      if (!sel) return;
      const cur = sel.value;
      while (sel.options.length > 1) sel.remove(1);
      banks.filter(b => b && b !== 'None').forEach(b => {
        const o = document.createElement('option');
        o.value = b; o.textContent = b; sel.appendChild(o);
      });
      if (cur) sel.value = cur;
    });
  } catch(e) { console.warn('loadBanks:', e); }
}

// ── Helpers ───────────────────────────────────────────────────
const fmtNum = n => (n == null || n === '') ? '—' : Number(n).toLocaleString('ru-RU');
const fmtDate = s => s ? s.replace('T',' ').slice(0,16) : '—';

function fmtAmount(n) {
  if (n == null) return '—';
  const v = Number(n);
  if (v >= 1e6) return (v/1e6).toFixed(1)+'М ₸';
  if (v >= 1e3) return (v/1e3).toFixed(1)+'К ₸';
  return fmtNum(v)+' ₸';
}

const statusBadge = s => {
  const m = {completed:'badge-green',pending:'badge-yellow',failed:'badge-red',cancelled:'badge-gray'};
  return `<span class="badge ${m[s]||'badge-gray'}">${s||'—'}</span>`;
};
const fraudBadge = v => v
  ? `<span class="badge badge-red">FRAUD</span>`
  : `<span class="badge badge-green">OK</span>`;
const shortId = id => id
  ? `<span class="tx-id" title="${id}">${id.slice(0,18)}…</span>` : '—';
const deviceIcon = d =>
  ({mobile_app:'📱',web:'🌐',pos_terminal:'💳',atm:'🏧',ussd:'📞'})[d]||'';

function fraudReasonBadge(r) {
  if (!r) return '<span class="badge badge-gray">—</span>';
  const m = {
    high_amount_unusual_time:    ['badge-red',    '🌙 Сумма ночью'],
    international_transfer:      ['badge-red',    '🌐 Международный'],
    multiple_small_transactions: ['badge-yellow', '🔢 Дробление'],
    rapid_transactions:          ['badge-yellow', '⚡ Быстрые'],
    unusual_location:            ['badge-blue',   '📍 Локация'],
  };
  const [cls,lbl] = m[r]||['badge-gray',r];
  return `<span class="badge ${cls}">${lbl}</span>`;
}

// ── Параметры фильтров ────────────────────────────────────────
function getTxParams() {
  const p = new URLSearchParams();
  const v = id => document.getElementById(id)?.value.trim()||'';
  if (v('f-tx-id'))     p.set('transaction_id', v('f-tx-id'));
  if (v('f-client-id')) p.set('client_id', v('f-client-id'));
  if (v('f-bank'))      p.set('bank', v('f-bank'));
  if (v('f-status'))    p.set('status', v('f-status'));
  if (v('f-date-from')) p.set('date_from', v('f-date-from'));
  if (v('f-date-to'))   p.set('date_to', v('f-date-to'));
  p.set('sort_by',  sortState.tx.col);
  p.set('sort_dir', sortState.tx.dir);
  return p;
}

function getFraudParams() {
  const p = new URLSearchParams();
  p.set('fraud_only','true');
  const v = id => document.getElementById(id)?.value.trim()||'';
  if (v('ff-tx-id'))     p.set('transaction_id', v('ff-tx-id'));
  if (v('ff-client-id')) p.set('client_id', v('ff-client-id'));
  if (v('ff-bank'))      p.set('bank', v('ff-bank'));
  p.set('sort_by',  sortState.fraud.col);
  p.set('sort_dir', sortState.fraud.dir);
  return p;
}

// ── Сортировка ────────────────────────────────────────────────
function sortBy(table, col) {
  const s = sortState[table];
  s.dir = (s.col === col && s.dir === 'desc') ? 'asc' : 'desc';
  s.col = col;

  // Обновить иконки
  const pageId = table === 'tx' ? 'page-transactions' : 'page-fraud';
  document.querySelectorAll(`#${pageId} .sort-th`).forEach(th => {
    th.classList.remove('sort-active');
    const icon = th.querySelector('.sort-icon');
    if (icon) icon.textContent = ' ↕';
  });
  const activeTh = document.querySelector(`#${pageId} [data-col="${col}"]`);
  if (activeTh) {
    activeTh.classList.add('sort-active');
    const icon = activeTh.querySelector('.sort-icon');
    if (icon) icon.textContent = s.dir === 'asc' ? ' ↑' : ' ↓';
  }

  if (table === 'tx')    loadTransactions(1);
  if (table === 'fraud') loadFraud(1);
}

// ── Upload ────────────────────────────────────────────────────
document.getElementById('drop-zone').addEventListener('dragover', e => {
  e.preventDefault(); e.currentTarget.classList.add('dragover');
});
document.getElementById('drop-zone').addEventListener('dragleave', e =>
  e.currentTarget.classList.remove('dragover')
);
document.getElementById('drop-zone').addEventListener('drop', e => {
  e.preventDefault(); e.currentTarget.classList.remove('dragover');
  const f = e.dataTransfer.files[0];
  if (f?.name.endsWith('.csv')) uploadFile(f);
});
document.getElementById('csv-input').addEventListener('change', e => {
  if (e.target.files[0]) uploadFile(e.target.files[0]);
  e.target.value = '';
});

async function uploadFile(file) {
  const prog = document.getElementById('etl-progress');
  const res  = document.getElementById('etl-result');
  prog.style.display = 'block'; res.style.display = 'none';
  res.className = 'etl-result';
  setStage('extract','active'); setProgress(20);

  const fd = new FormData(); fd.append('file', file);
  setTimeout(()=>{ setStage('extract','done'); setStage('transform','active'); setProgress(55); }, 800);

  try {
    const data = await fetch(`${API}/api/upload`,{method:'POST',body:fd}).then(r=>r.json());
    setStage('transform','done'); setStage('load','active'); setProgress(85);
    setTimeout(()=>{
      setStage('load','done'); setProgress(100);
      if (data.status==='success') {
        res.className='etl-result success';
        res.innerHTML=`
          <div style="font-family:'Unbounded',sans-serif;font-size:14px;color:var(--green);margin-bottom:16px">
            ✅ ETL завершён — ${file.name}
          </div>
          <div class="result-grid">
            <div class="result-item"><div class="result-label">Всего строк</div><div class="result-val">${fmtNum(data.total_rows)}</div></div>
            <div class="result-item"><div class="result-label">Валидных</div><div class="result-val green">${fmtNum(data.valid_rows)}</div></div>
            <div class="result-item"><div class="result-label">Удалено</div><div class="result-val">${fmtNum(data.invalid_rows)}</div></div>
            <div class="result-item"><div class="result-label">Фрод</div><div class="result-val red">${fmtNum(data.fraud_detected)}</div></div>
            <div class="result-item"><div class="result-label">Загружено</div><div class="result-val green">${fmtNum(data.inserted)}</div></div>
            <div class="result-item"><div class="result-label">Пропущено</div><div class="result-val">${fmtNum(data.skipped)}</div></div>
          </div>`;
      } else {
        res.className='etl-result error';
        res.innerHTML=`<div style="color:var(--red)">❌ Ошибка: ${data.error||'Неизвестная'}</div>`;
      }
      res.style.display='block';
      loadEtlLogs(); checkApiStatus(); loadBanks();
    }, 600);
  } catch(e) {
    res.className='etl-result error';
    res.innerHTML=`<div style="color:var(--red)">❌ Ошибка соединения: ${e.message}</div>`;
    res.style.display='block'; setProgress(0);
  }
}

const setStage    = (id,s) => { const el=document.getElementById(`stage-${id}`); if(el) el.className=`etl-stage ${s}`; };
const setProgress = p => document.getElementById('progress-bar').style.width = p+'%';

async function loadEtlLogs() {
  try {
    const logs = await fetch(`${API}/api/etl/logs`).then(r=>r.json());
    const c = document.getElementById('etl-logs-table');
    if (!logs.length) { c.innerHTML=`<div class="empty-state"><p>Загрузок пока нет</p></div>`; return; }
    c.innerHTML = logs.map(l=>`
      <div class="etl-log-row">
        <span class="etl-log-file">📄 ${l.filename}</span>
        <span class="etl-log-meta">${fmtNum(l.total_rows)} строк</span>
        <span class="etl-log-meta">${fmtNum(l.fraud_detected)} фрод</span>
        <span class="badge ${l.status==='success'?'badge-green':l.status==='running'?'badge-yellow':'badge-red'}">${l.status}</span>
        <span class="etl-log-meta">${fmtDate(l.started_at)}</span>
      </div>`).join('');
  } catch {}
}

// ── Transactions ──────────────────────────────────────────────
async function loadTransactions(page=1) {
  const params = getTxParams();
  params.set('page', page);
  params.set('per_page', 20);

  document.getElementById('tx-body').innerHTML =
    `<tr><td colspan="10" style="text-align:center;padding:40px;color:var(--text3)">⏳ Загрузка...</td></tr>`;

  try {
    const data = await fetch(`${API}/api/transactions?${params}`).then(r=>r.json());
    renderTxTable(data, page);
  } catch(e) {
    document.getElementById('tx-body').innerHTML =
      `<tr><td colspan="10" style="color:var(--red);text-align:center;padding:40px">Ошибка: ${e.message}</td></tr>`;
  }
}

function renderTxTable(data, page) {
  const tbody = document.getElementById('tx-body');
  const count = document.getElementById('tx-count');
  if (!data.data?.length) {
    tbody.innerHTML=`<tr><td colspan="10"><div class="empty-state"><p>Транзакции не найдены</p></div></td></tr>`;
    document.getElementById('tx-pagination').innerHTML='';
    count.textContent='0 записей'; return;
  }
  count.textContent = `${fmtNum(data.total)} записей`;
  tbody.innerHTML = data.data.map(tx=>`
    <tr class="${tx.is_fraud?'fraud-row':''}">
      <td>${shortId(tx.transaction_id)}</td>
      <td><code style="font-size:11px">${tx.client_id||'—'}</code></td>
      <td>${tx.bank||'—'}</td>
      <td style="font-weight:600;color:${tx.is_fraud?'var(--red)':'var(--text)'}">
        ${fmtNum(tx.amount_kzt)} ₸</td>
      <td>${tx.category||'—'}</td>
      <td>${tx.city||'—'}</td>
      <td>${deviceIcon(tx.device_type)} ${tx.device_type||'—'}</td>
      <td style="color:var(--text2)">${fmtDate(tx.transaction_date)}</td>
      <td>${statusBadge(tx.status)}</td>
      <td>${fraudBadge(tx.is_fraud)}</td>
    </tr>`).join('');
  renderPagination('tx-pagination', data.page, data.pages, data.total, loadTransactions);
}

function searchTransactions() { loadTransactions(1); }

function clearFilters(type) {
  if (type==='tx') {
    ['f-tx-id','f-client-id','f-date-from','f-date-to'].forEach(id=>{
      const el=document.getElementById(id); if(el) el.value='';
    });
    document.getElementById('f-bank').value='';
    document.getElementById('f-status').value='';
    loadTransactions(1);
  } else {
    ['ff-tx-id','ff-client-id'].forEach(id=>{
      const el=document.getElementById(id); if(el) el.value='';
    });
    document.getElementById('ff-bank').value='';
    loadFraud(1);
  }
}

// ── Fraud ─────────────────────────────────────────────────────
async function loadFraud(page=1) {
  const params = getFraudParams();
  params.set('page', page);
  params.set('per_page', 20);

  document.getElementById('fraud-body').innerHTML =
    `<tr><td colspan="9" style="text-align:center;padding:40px;color:var(--text3)">⏳ Загрузка...</td></tr>`;

  try {
    const data = await fetch(`${API}/api/transactions?${params}`).then(r=>r.json());
    renderFraudTable(data, page);
  } catch {}
}

function renderFraudTable(data, page) {
  const tbody = document.getElementById('fraud-body');
  const count = document.getElementById('fraud-count');
  if (!data.data?.length) {
    tbody.innerHTML=`<tr><td colspan="9"><div class="empty-state"><p>Мошеннические транзакции не найдены</p></div></td></tr>`;
    document.getElementById('fraud-pagination').innerHTML='';
    count.textContent='0 записей'; return;
  }
  count.textContent = `${fmtNum(data.total)} записей`;
  tbody.innerHTML = data.data.map(tx=>`
    <tr class="fraud-row">
      <td>${shortId(tx.transaction_id)}</td>
      <td><code style="font-size:11px">${tx.client_id||'—'}</code></td>
      <td>${tx.bank||'—'}</td>
      <td style="font-weight:600;color:var(--red)">${fmtNum(tx.amount_kzt)} ₸</td>
      <td>${tx.category||'—'}</td>
      <td>${tx.city||'—'}</td>
      <td>${fraudReasonBadge(tx.fraud_reason)}</td>
      <td style="color:var(--text2)">${fmtDate(tx.transaction_date)}</td>
      <td>${statusBadge(tx.status)}</td>
    </tr>`).join('');
  renderPagination('fraud-pagination', data.page, data.pages, data.total, loadFraud);
}

function searchFraud() { loadFraud(1); }

// ── CSV Export ────────────────────────────────────────────────
function downloadCSV(btnId) {
  const btn = document.getElementById(btnId);
  if (!btn) return;

  const params = btnId==='btn-export-fraud' ? getFraudParams() : getTxParams();
  params.set('page', 1);
  params.set('per_page', 99999);

  const orig = btn.innerHTML;
  btn.innerHTML='⏳ Подготовка...'; btn.disabled=true;

  fetch(`${API}/api/transactions?${params}`)
    .then(r=>r.json())
    .then(data=>{
      if (!data.data?.length){ alert('Нет данных для экспорта'); return; }
      triggerDownload(buildCSV(data.data), buildFilename(params));
    })
    .catch(e=>alert('Ошибка: '+e.message))
    .finally(()=>{ btn.innerHTML=orig; btn.disabled=false; });
}

function buildCSV(rows) {
  const cols = ['transaction_id','client_id','bank','sender_account','receiver_account',
    'amount_kzt','category','city','device_type','transaction_date',
    'status','is_fraud','fraud_reason','description'];
  const esc = v => { const s=String(v??'').replace(/"/g,'""'); return s.includes(',')||s.includes('"')||s.includes('\n')?`"${s}"`:s; };
  return '\uFEFF' + [cols.join(','), ...rows.map(r=>cols.map(c=>esc(r[c])).join(','))].join('\n');
}

function buildFilename(params) {
  const p=['transactions'];
  if (params.get('fraud_only')==='true') p.push('fraud');
  if (params.get('bank'))      p.push(params.get('bank').replace(/\s+/g,'_'));
  if (params.get('date_from')) p.push(params.get('date_from'));
  if (params.get('date_to'))   p.push('to_'+params.get('date_to'));
  p.push(new Date().toISOString().slice(0,10));
  return p.join('_')+'.csv';
}

function triggerDownload(content, filename) {
  const a = Object.assign(document.createElement('a'),{
    href: URL.createObjectURL(new Blob([content],{type:'text/csv;charset=utf-8;'})),
    download: filename,
  });
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
}

// ── Pagination ────────────────────────────────────────────────
function renderPagination(id, page, pages, total, fn) {
  const c = document.getElementById(id); if (!c) return;
  if (pages<=1){ c.innerHTML=''; return; }
  let h=`<span class="pagination-info">Всего: ${fmtNum(total)}</span>`;
  h+=`<button ${page<=1?'disabled':''} onclick="${fn.name}(${page-1})">←</button>`;
  const s=Math.max(1,page-2), e=Math.min(pages,page+2);
  if(s>1){h+=`<button onclick="${fn.name}(1)">1</button>`;if(s>2)h+=`<span style="color:var(--text3);padding:0 4px">…</span>`;}
  for(let i=s;i<=e;i++) h+=`<button class="${i===page?'active':''}" onclick="${fn.name}(${i})">${i}</button>`;
  if(e<pages){if(e<pages-1)h+=`<span style="color:var(--text3);padding:0 4px">…</span>`;h+=`<button onclick="${fn.name}(${pages})">${pages}</button>`;}
  h+=`<button ${page>=pages?'disabled':''} onclick="${fn.name}(${page+1})">→</button>`;
  c.innerHTML=h;
}

// ── Dashboard ─────────────────────────────────────────────────
async function loadDashboard() {
  await Promise.all([loadKPIs(),loadByDate(),loadByBank(),loadByDevice(),
    loadByHour(),loadByCategory(),loadFraudReasons(),loadByCity()]);
}

async function loadKPIs() {
  try {
    const d=await fetch(`${API}/api/dashboard/stats`).then(r=>r.json());
    document.getElementById('kpi-total').textContent        = fmtNum(d.total_transactions);
    document.getElementById('kpi-fraud').textContent        = fmtNum(d.fraud_transactions);
    document.getElementById('kpi-rate').textContent         = d.fraud_rate+'%';
    document.getElementById('kpi-clients').textContent      = fmtNum(d.unique_clients);
    document.getElementById('kpi-amount').textContent       = fmtAmount(d.total_amount_kzt);
    document.getElementById('kpi-fraud-amount').textContent = fmtAmount(d.fraud_amount_kzt);
    document.getElementById('fraud-badge').textContent      = fmtNum(d.fraud_transactions);
  } catch {}
}

async function loadByDate() {
  const days=document.getElementById('days-select')?.value||90;
  try {
    const data=await fetch(`${API}/api/dashboard/by_date?days=${days}`).then(r=>r.json());
    renderChart('chart-by-date','line',data.map(d=>d.date),[
      {label:'Всего',data:data.map(d=>d.total),borderColor:'#4f8ef7',backgroundColor:'#4f8ef715',tension:0.4,fill:true},
      {label:'Фрод', data:data.map(d=>d.fraud),borderColor:'#ff3d57',backgroundColor:'#ff3d5715',tension:0.4,fill:true},
    ]);
  } catch {}
}

async function loadByBank() {
  try {
    const data=await fetch(`${API}/api/dashboard/by_bank`).then(r=>r.json());
    const labels=data.map(d=>d.bank);
    const totals=data.map(d=>d.total);
    const frauds=data.map(d=>d.fraud);
    const pcts  =data.map(d=>d.total>0?+((d.fraud/d.total)*100).toFixed(1):0);
    renderChart('chart-by-bank','bar',labels,[
      {label:'Всего',        data:totals, backgroundColor:'#4f8ef777', yAxisID:'y'},
      {label:'Фрод',         data:frauds, backgroundColor:'#ff3d5777', yAxisID:'y'},
      {label:'Фрод %',       data:pcts,   backgroundColor:'#ffc93c99', yAxisID:'y2',
        type:'line', borderColor:'#ffc93c', borderWidth:2, pointRadius:3, fill:false},
    ],{indexAxis:'y', dualAxis:true});
  } catch {}
}

async function loadByDevice() {
  try {
    const data=await fetch(`${API}/api/dashboard/by_device`).then(r=>r.json());
    const total=data.reduce((s,d)=>s+d.total,0);
    const labels=data.map(d=>`${d.device_type} (${total>0?((d.total/total)*100).toFixed(1):0}%)`);
    renderChart('chart-by-device','doughnut',labels,
      [{data:data.map(d=>d.total),backgroundColor:PALETTE}],{showPct:true});
  } catch {}
}

async function loadByHour() {
  try {
    const data=await fetch(`${API}/api/dashboard/by_hour`).then(r=>r.json());
    const labels=Array.from({length:24},(_,i)=>`${i}:00`);
    const tots=new Array(24).fill(0), frds=new Array(24).fill(0);
    data.forEach(d=>{ tots[d.hour]=d.total; frds[d.hour]=d.fraud; });
    renderChart('chart-by-hour','bar',labels,[
      {label:'Всего',data:tots,backgroundColor:'#4f8ef744'},
      {label:'Фрод', data:frds,backgroundColor:'#ff3d5766'},
    ]);
  } catch {}
}

async function loadByCategory() {
  try {
    const data=await fetch(`${API}/api/dashboard/by_category`).then(r=>r.json());
    const total=data.reduce((s,d)=>s+d.total,0);
    const labels=data.map(d=>`${d.category} (${total>0?((d.total/total)*100).toFixed(1):0}%)`);
    renderChart('chart-by-category','pie',labels,
      [{data:data.map(d=>d.total),backgroundColor:PALETTE}],{showPct:true});
  } catch {}
}

async function loadFraudReasons() {
  try {
    const data=await fetch(`${API}/api/dashboard/fraud_reasons`).then(r=>r.json());
    const total=data.reduce((s,d)=>s+d.count,0);
    const labels=data.map(d=>`${d.reason} (${total>0?((d.count/total)*100).toFixed(1):0}%)`);
    renderChart('chart-fraud-reasons','doughnut',labels,
      [{data:data.map(d=>d.count),backgroundColor:PALETTE_RED}],{showPct:true});
  } catch {}
}

async function loadByCity() {
  try {
    const data=await fetch(`${API}/api/dashboard/by_city`).then(r=>r.json());
    const labels=data.map(d=>d.city);
    const totals=data.map(d=>d.total);
    const frauds=data.map(d=>d.fraud);
    const pcts  =data.map(d=>d.total>0?+((d.fraud/d.total)*100).toFixed(1):0);
    renderChart('chart-by-city','bar',labels,[
      {label:'Всего',  data:totals,backgroundColor:'#00ff8755',yAxisID:'y'},
      {label:'Фрод',   data:frauds,backgroundColor:'#ff3d5766',yAxisID:'y'},
      {label:'Фрод %', data:pcts,  backgroundColor:'#ffc93c99',yAxisID:'y2',
        type:'line',borderColor:'#ffc93c',borderWidth:2,pointRadius:3,fill:false},
    ],{indexAxis:'y', dualAxis:true});
  } catch {}
}

// ── EDA ───────────────────────────────────────────────────────
async function loadEDA() {
  try {
    const [summary,dist]=await Promise.all([
      fetch(`${API}/api/eda/summary`).then(r=>r.json()),
      fetch(`${API}/api/eda/amount_distribution`).then(r=>r.json()),
    ]);
    const s=summary.amount_stats;
    document.getElementById('eda-amount-stats').innerHTML=[
      ['MIN',fmtNum(s.min)],['MAX',fmtNum(s.max)],['СРЕДНЕЕ',fmtNum(s.mean)],
      ['P25',fmtNum(s.p25)],['МЕДИАНА',fmtNum(s.p50)],['P75',fmtNum(s.p75)],
      ['P95',fmtNum(s.p95)],['P99',fmtNum(s.p99)],['КОЛ-ВО',fmtNum(s.count)],
    ].map(([k,v])=>`<div class="stat-cell"><div class="stat-key">${k}</div><div class="stat-val">${v}</div></div>`).join('');

    // Гистограмма сумм + % от общего
    const distTotal=dist.reduce((s,d)=>s+d.count,0);
    const distLabels=dist.map(d=>`${d.bucket} (${distTotal>0?((d.count/distTotal)*100).toFixed(1):0}%)`);
    renderChart('chart-amount-dist','bar',distLabels,[
      {label:'Всего',data:dist.map(d=>d.count),backgroundColor:'#4f8ef766'},
      {label:'Фрод', data:dist.map(d=>d.fraud),backgroundColor:'#ff3d5766'},
    ]);

    // Уровень фрода по банкам — только % (это уже процент)
    const bd=summary.bank_fraud_rates;
    renderChart('chart-bank-fraud-rate','bar',bd.map(d=>d.bank),[{
      label:'Уровень фрода %',
      data:bd.map(d=>d.fraud_rate),
      backgroundColor:bd.map(d=>d.fraud_rate>10?'#ff3d5788':'#4f8ef788'),
    }],{indexAxis:'y'});

    const tc=summary.top_clients;
    document.getElementById('top-clients-table').innerHTML=`
      <table class="mini-table">
        <thead><tr><th>#</th><th>ИИН</th><th>Транзакций</th><th>Фрод</th><th>Оборот (₸)</th></tr></thead>
        <tbody>${tc.map((c,i)=>`
          <tr>
            <td style="color:var(--text3)">${i+1}</td>
            <td><code>${c.client_id}</code></td>
            <td>${fmtNum(c.tx_count)}</td>
            <td>${c.fraud_count>0?`<span class="badge badge-red">${fmtNum(c.fraud_count)}</span>`:'<span class="badge badge-green">0</span>'}</td>
            <td>${fmtAmount(c.total_amount)}</td>
          </tr>`).join('')}
        </tbody>
      </table>`;
  } catch(e){ console.error('EDA:',e); }
}

// ── Charts ────────────────────────────────────────────────────
const PALETTE=['#4f8ef7','#00ff87','#ffc93c','#ff6b9d','#a78bfa','#34d399','#f97316','#60a5fa','#e879f9','#22d3ee'];
const PALETTE_RED=['#ff3d57','#ff6b7a','#ff9ea8','#ffc4cb','#ff3d57cc'];

const CHART_DEFAULTS={
  responsive:true,
  plugins:{
    legend:{labels:{color:'#8899aa',font:{family:'JetBrains Mono',size:11},boxWidth:12}},
    tooltip:{
      backgroundColor:'#161b24',titleColor:'#e2e8f0',bodyColor:'#8899aa',
      borderColor:'#1e2735',borderWidth:1,
      titleFont:{family:'JetBrains Mono'},bodyFont:{family:'JetBrains Mono'},
      callbacks:{
        label: ctx => {
          const ds=ctx.chart.data.datasets[ctx.datasetIndex];
          const val=ctx.parsed.y??ctx.parsed;
          if (ds.label?.includes('%')) return ` ${ds.label}: ${val}%`;
          return ` ${ds.label||''}: ${Number(val).toLocaleString('ru-RU')}`;
        }
      }
    }
  },
  scales:{
    x:{ticks:{color:'#4a5568',font:{family:'JetBrains Mono',size:10}},grid:{color:'#1e273520'}},
    y:{ticks:{color:'#4a5568',font:{family:'JetBrains Mono',size:10}},grid:{color:'#1e2735'}},
  }
};

function renderChart(id, type, labels, datasets, extra={}) {
  const ctx=document.getElementById(id); if(!ctx) return;
  if(charts[id]) charts[id].destroy();

  const opts=JSON.parse(JSON.stringify(CHART_DEFAULTS));

  if(type==='doughnut'||type==='pie'){
    delete opts.scales;
    // Показать % в tooltip для pie/doughnut
    opts.plugins.tooltip.callbacks.label = ctx => {
      const total=ctx.chart.data.datasets[0].data.reduce((a,b)=>a+b,0);
      const pct=total>0?((ctx.parsed/total)*100).toFixed(1):0;
      return ` ${ctx.label?.split(' (')[0]||''}: ${Number(ctx.parsed).toLocaleString('ru-RU')} (${pct}%)`;
    };
  }

  if(extra.indexAxis==='y') opts.indexAxis='y';

  // Двойная ось Y для графиков с процентами
  if(extra.dualAxis){
    opts.scales.y2={
      type:'linear', position:'right',
      ticks:{color:'#ffc93c',font:{family:'JetBrains Mono',size:10},callback:v=>v+'%'},
      grid:{drawOnChartArea:false},
    };
  }

  charts[id]=new Chart(ctx,{type,data:{labels,datasets},options:opts});
}

// ── Init ──────────────────────────────────────────────────────
async function init() {
  await checkApiStatus();
  await loadBanks();
  loadEtlLogs();
}
init();
setInterval(checkApiStatus, 30000);