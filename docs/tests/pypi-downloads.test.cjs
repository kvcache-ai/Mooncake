const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { sum, recent, rangeTotal, series, breakdown } = require('../source/_static/pypi-downloads.js');
const pkg = { name: 'mooncake-transfer-engine', endpoints: {
  overall: { payload: { data: [
    {date:'2026-10-01', category:'without_mirrors', downloads:10},
    {date:'2026-10-01', category:'with_mirrors', downloads:12},
    {date:'2026-10-03', category:'without_mirrors', downloads:0},
  ]}},
  recent: { payload: { data: {last_day:0, last_week:10, last_month:50}}},
  system: { payload: { data: [
    {date:'2026-10-01', category:'Linux', downloads:8},
    {date:'2026-10-01', category:null, downloads:2},
  ]}},
}};
const missing = {name:'mooncake-transfer-engine-cuda13', endpoints:{}};
test('missing values, zeros and mirror categories stay distinct', () => {
  assert.equal(sum([null, null]), null);
  assert.equal(sum([0, null]), 0);
  assert.equal(recent(pkg, 'last_day'), 0);
  assert.equal(recent(missing, 'last_day'), null);
  assert.equal(rangeTotal(pkg, '2026-10-01', '2026-10-03', 'without_mirrors'), 10);
  assert.equal(rangeTotal(pkg, '2026-10-01', '2026-10-03', 'with_mirrors'), 12);
});
test('daily series retains gaps and partial coverage', () => {
  const data = series([pkg, missing], '2026-10-01', '2026-10-03', 'without_mirrors');
  assert.deepEqual(data.map(d=>d.total), [10,null,0]);
  assert.deepEqual(data.map(d=>d.reporting), [1,0,1]);
  assert.ok(data.every(d=>d.values[1]===null));
});
test('environment categories retain unknown values and honor dates', () => {
  assert.deepEqual(breakdown([pkg], 'system', '2026-10-01', '2026-10-03'), [['Linux',8],['Unknown',2]]);
  assert.deepEqual(breakdown([pkg], 'system', '2026-10-02', '2026-10-03'), []);
});
test('real snapshot chart totals match an independent sum', () => {
  const data = JSON.parse(fs.readFileSync(path.join(__dirname, '../source/_static/pypi-downloads.json')));
  const expected = data.packages.flatMap(p=>p.endpoints.overall.payload.data).filter(r=>r.category==='without_mirrors').reduce((n,r)=>n+r.downloads,0);
  const dates = data.packages.flatMap(p=>p.endpoints.overall.payload.data.map(r=>r.date)).sort();
  assert.equal(sum(series(data.packages,dates[0],dates.at(-1),'without_mirrors').map(d=>d.total)),expected);
});
