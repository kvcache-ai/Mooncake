// Static, same-origin PyPIStats snapshot. No browser requests to PyPIStats.
(() => {
  "use strict";
  const PREFIX = "mooncake-transfer-engine";
  const COLORS = ["#2674d9", "#149b89", "#9371ce", "#ba8029", "#cf647b", "#4c94b1", "#9b7559", "#697d99", "#a45fa8"];
  const shortName = name => name === PREFIX ? "Original" : name.slice(PREFIX.length + 1);
  const format = value => value == null ? "—" : value.toLocaleString("en-US");
  const rows = (pkg, endpoint) => {
    const data = pkg.endpoints[endpoint]?.payload?.data;
    return Array.isArray(data) ? data : [];
  };
  const recent = (pkg, key) => pkg.endpoints.recent?.payload?.data?.[key] ?? null;
  const sum = values => values.some(v => v != null) ? values.reduce((a, b) => a + (b ?? 0), 0) : null;
  const offset = (date, days) => new Date(Date.parse(date + "T00:00:00Z") + days * 86400000).toISOString().slice(0, 10);
  const inRange = (row, start, end) => row.date >= start && row.date <= end;
  const rangeTotal = (pkg, start, end, category) => sum(rows(pkg, "overall")
    .filter(r => inRange(r, start, end) && r.category === category).map(r => r.downloads));
  function series(packages, start, end, category) {
    if (!start || !end || start > end) return [];
    const maps = packages.map(p => new Map(rows(p, "overall").filter(r => r.category === category).map(r => [r.date, r.downloads])));
    const result = [];
    for (let date = start; date <= end; date = offset(date, 1)) {
      const values = maps.map(m => m.get(date) ?? null);
      result.push({ date, values, total: sum(values), reporting: values.filter(v => v != null).length });
    }
    return result;
  }
  function breakdown(packages, endpoint, start, end) {
    const values = new Map();
    packages.flatMap(p => rows(p, endpoint)).filter(r => inRange(r, start, end)).forEach(r => {
      const key = r.category == null || r.category === "null" ? "Unknown" : r.category;
      values.set(key, (values.get(key) ?? 0) + r.downloads);
    });
    return [...values].sort((a, b) => b[1] - a[1]);
  }
  // Pure aggregation functions are also exercised by the Node test runner.
  if (typeof module !== "undefined") module.exports = { sum, recent, rangeTotal, series, breakdown };
  if (typeof document === "undefined") return;
  const el = (tag, props = {}, ...children) => {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(props)) {
      if (key === "text") node.textContent = value;
      else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
      else node.setAttribute(key, value);
    }
    for (const child of children.flat()) if (child != null) node.append(child instanceof Node ? child : String(child));
    return node;
  };
  const svg = (tag, props, text) => {
    const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
    Object.entries(props).forEach(([key, value]) => node.setAttribute(key, value));
    if (text != null) node.textContent = text;
    return node;
  };
  function plot(days, packages, compare) {
    const chart = svg("svg", { viewBox: "0 0 720 225", role: "img", "aria-label": "Daily downloads for selected packages" });
    const max = Math.max(1, ...days.flatMap(d => compare ? d.values : [d.total]));
    const x = i => 52 + i / Math.max(1, days.length - 1) * 648;
    const y = value => 190 - value / max * 165;
    for (let i = 0; i <= 4; i++) {
      const value = max * i / 4;
      chart.append(svg("line", { x1: 52, x2: 700, y1: y(value), y2: y(value), class: "pypi-grid" }));
      chart.append(svg("text", { x: 42, y: y(value) + 4, "text-anchor": "end" }, Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(value)));
    }
    [0, Math.floor((days.length - 1) / 2), days.length - 1].filter((v, i, a) => a.indexOf(v) === i).forEach(i => {
      chart.append(svg("text", { x: x(i), y: 217, "text-anchor": "middle" }, days[i].date.slice(5)));
    });
    const lines = compare ? packages.map((p, i) => ({ name: shortName(p.name), values: days.map(d => d.values[i]), color: COLORS[i % COLORS.length] }))
      : [{ name: "Total", values: days.map(d => d.total), color: COLORS[0] }];
    lines.forEach(line => {
      let path = "", pen = false;
      line.values.forEach((v, i) => {
        if (v == null) { pen = false; return; }
        path += `${pen ? "L" : "M"}${x(i)},${y(v)} `;
        pen = true;
        const point = svg("circle", { cx: x(i), cy: y(v), r: 3, fill: line.color, class: "pypi-point", tabindex: "0" });
        const label = `${days[i].date} · ${line.name}: ${format(v)} · ${days[i].reporting}/${packages.length} packages reporting`;
        point.setAttribute("aria-label", label);
        point.append(svg("title", {}, label));
        chart.append(point);
      });
      chart.append(svg("path", { d: path, fill: "none", stroke: line.color, "stroke-width": 2, "pointer-events": "none" }));
    });
    const legend = el("div", { class: "pypi-legend" }, lines.map(l => el("span", {}, el("i", { style: `background:${l.color}` }), l.name)));
    return el("div", { class: "pypi-chart" }, chart, legend);
  }
  async function mount(root) {
    try {
      const response = await fetch(root.dataset.source);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      const selected = new Set(data.catalog.names);
      const dates = data.packages.flatMap(p => rows(p, "overall").map(r => r.date)).sort();
      const first = dates[0], last = dates.at(-1);
      if (!first || !last) throw new Error("No daily records in this snapshot");
      const controls = el("div", { class: "pypi-controls" });
      const output = el("div", { "aria-live": "polite" });
      const period = el("select", { "aria-label": "Date range", onchange: render },
        [[7, "7 days"], [30, "30 days"], [90, "90 days"], [180, "180 days"], [0, "All dates"]].map(([v, label]) => el("option", { value: v }, label)));
      period.value = "30";
      const mode = el("select", { "aria-label": "Chart mode", onchange: render }, el("option", { value: "total" }, "Total"), el("option", { value: "packages" }, "By package"));
      const mirrors = el("input", { type: "checkbox", onchange: render });
      const chips = el("div", { class: "pypi-packages", "aria-label": "Packages" });
      data.catalog.names.forEach(name => chips.append(el("button", { type: "button", "aria-pressed": "true", title: name, onclick: event => {
        selected.has(name) ? selected.delete(name) : selected.add(name);
        event.currentTarget.setAttribute("aria-pressed", String(selected.has(name))); render();
      } }, shortName(name))));
      const all = el("button", { type: "button", onclick: () => {
        data.catalog.names.forEach(name => selected.add(name));
        chips.querySelectorAll("button").forEach(b => b.setAttribute("aria-pressed", "true")); render();
      } }, "Select all");
      controls.append(el("label", {}, "Range ", period), mode, el("label", {}, mirrors, " Include mirrors"), all);
      const oldest = data.packages.flatMap(p => Object.values(p.endpoints)).filter(r => r.payload && r.fetchedAt).map(r => r.fetchedAt).sort()[0];
      const issues = data.packages.flatMap(p => Object.values(p.endpoints)).filter(r => r.status !== "ok").length;
      const age = oldest ? Math.floor((Date.now() - Date.parse(oldest)) / 86400000) : 0;
      const status = `Data through ${last} · ${data.catalog.names.length} packages · Collected ${oldest ? oldest.slice(0, 16).replace("T", " ") + " UTC" : "unknown"}`;
      root.replaceChildren(el("p", { class: "pypi-status" }, status), controls, chips, output);
      if (issues || age > 2 || data.catalog.status !== "ok") root.insertBefore(el("p", { class: "pypi-warning" }, `Some data may be outdated or incomplete (${issues} endpoint warnings). Available records are retained.`), controls);
      function render() {
        const packages = data.packages.filter(p => selected.has(p.name));
        const start = Number(period.value) ? [first, offset(last, 1 - Number(period.value))].sort().at(-1) : first;
        const category = mirrors.checked ? "with_mirrors" : "without_mirrors";
        output.replaceChildren();
        if (!packages.length) { output.append(el("p", {}, "Select a package to view statistics.")); return; }
        const cards = el("div", { class: "pypi-metrics" });
        [["Last month · API", "last_month"], ["Last week · API", "last_week"], ["Last day · API", "last_day"]].forEach(([label, key]) => {
          const values = packages.map(p => recent(p, key));
          cards.append(el("div", {}, el("span", {}, label), el("strong", {}, format(sum(values))), el("small", {}, `${values.filter(v => v != null).length}/${packages.length} packages · Excludes mirrors`)));
        });
        const total = sum(packages.map(p => rangeTotal(p, start, last, category)));
        const days = series(packages, start, last, category);
        output.append(cards, el("div", { class: "pypi-chart-heading" }, el("strong", {}, `Selected range: ${format(total)}`), el("span", {}, `${start} — ${last} · ${mirrors.checked ? "Includes" : "Excludes"} mirrors`)), plot(days, packages, mode.value === "packages"));
        const partial = days.filter(d => d.reporting < packages.length).length;
        if (partial) output.append(el("p", { class: "pypi-status" }, `${partial} days have incomplete coverage. The chart sums returned records; missing values are not zero.`));
        const table = el("table", {}, el("thead", {}, el("tr", {}, ["Package", "Day · API", "Week · API", "Month · API", "Selected range"].map(t => el("th", { scope: "col" }, t)))));
        const body = el("tbody");
        [...packages].sort((a, b) => (rangeTotal(b, start, last, category) ?? -1) - (rangeTotal(a, start, last, category) ?? -1)).forEach(p => {
          body.append(el("tr", {}, el("th", { scope: "row" }, el("a", { href: `https://pypistats.org/packages/${p.name}`, title: p.name }, shortName(p.name))), ...["last_day", "last_week", "last_month"].map(k => el("td", {}, format(recent(p, k)))), el("td", {}, format(rangeTotal(p, start, last, category)))));
        });
        table.append(body);
        output.append(el("div", { class: "pypi-table" }, table), el("p", { class: "pypi-status" }, "Recent API totals can differ from daily sums. Selected range follows the date and mirror filters."));
        const python = el("select", { "aria-label": "Python version level" }, el("option", { value: "python_minor" }, "Minor versions"), el("option", { value: "python_major" }, "Major versions"));
        const py = el("div"), os = el("div");
        function bars(target, endpoint) {
          const values = breakdown(packages, endpoint, start, last), total = sum(values.map(v => v[1]));
          const reporting = packages.filter(p => rows(p, endpoint).some(r => inRange(r, start, last))).length;
          target.replaceChildren(el("p", { class: "pypi-status" }, `${reporting}/${packages.length} packages with data · Excludes mirrors`));
          values.forEach(([label, value]) => target.append(el("div", { class: "pypi-bar" }, el("span", {}, label, el("span", {}, `${format(value)} · ${total ? (value / total * 100).toFixed(1) : "0.0"}%`)), el("meter", { min: "0", max: total || 1, value, "aria-label": label }))));
          if (!values.length) target.append(el("p", {}, "No data for this range."));
        }
        python.addEventListener("change", () => bars(py, python.value));
        bars(py, python.value); bars(os, "system");
        output.append(el("details", { class: "pypi-environment" }, el("summary", {}, "Environment breakdown"), el("div", { class: "pypi-columns" }, el("section", {}, el("h2", {}, "Python versions"), python, py), el("section", {}, el("h2", {}, "Operating systems"), os))));
      }
      render();
    } catch (error) {
      root.replaceChildren(el("p", { role: "alert" }, "Download statistics are unavailable. ", el("a", { href: "https://pypistats.org/packages/mooncake-transfer-engine" }, "View PyPIStats")));
      console.error("PyPI downloads:", error);
    }
  }
  document.querySelectorAll("[data-pypi-downloads]").forEach(mount);
})();
