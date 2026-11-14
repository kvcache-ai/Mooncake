// Live dashboard of Mooncake-related pull requests in downstream inference
// engines. The docs are a static site, so PRs are fetched in the browser from
// the public GitHub search API. Unauthenticated search is limited to 10
// requests per minute per IP, so results are cached in localStorage and shared
// across tabs; each refresh issues one request per repository.
(() => {
  "use strict";

  const REPOS = [
    { id: "sglang", name: "SGLang", fullName: "sgl-project/sglang" },
    { id: "vllm", name: "vLLM", fullName: "vllm-project/vllm" },
    { id: "tokenspeed", name: "TokenSpeed", fullName: "lightseekorg/tokenspeed" },
  ];
  const KEYWORD = "mooncake";
  const SEARCH_API = "https://api.github.com/search/issues";
  const CACHE_KEY = "mooncake-pr-dashboard:v1";
  const SCOPES = {
    title: { label: "Title", qualifier: "in:title" },
    body: { label: "Title + description", qualifier: "in:title,body" },
  };
  const SCOPE_LABELS = Object.fromEntries(
    Object.entries(SCOPES).map(([key, scope]) => [key, scope.label]),
  );
  const STATES = {
    all: "All",
    open: "Open",
    draft: "Draft",
    merged: "Merged",
    closed: "Closed",
  };
  const SORTS = { updated: "Recently updated", created: "Newest" };
  const PAGE_SIZE = 25;
  const MAX_LABELS = 4;
  const ACTIVE_WINDOW_MS = 7 * 24 * 3600 * 1000;

  const el = (tag, props = {}, ...children) => {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(props)) {
      if (value === undefined || value === null || value === false) continue;
      if (key === "class") node.className = value;
      else if (key === "text") node.textContent = value;
      else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
      else node.setAttribute(key, value === true ? "" : String(value));
    }
    for (const child of children.flat()) {
      if (child === null || child === undefined || child === false) continue;
      node.append(child instanceof Node ? child : String(child));
    }
    return node;
  };

  const rtf = new Intl.RelativeTimeFormat("en", { numeric: "auto" });
  const TIME_UNITS = [
    ["year", 31536000],
    ["month", 2592000],
    ["week", 604800],
    ["day", 86400],
    ["hour", 3600],
    ["minute", 60],
  ];
  const relativeTime = (timestamp) => {
    const seconds = (new Date(timestamp).getTime() - Date.now()) / 1000;
    for (const [unit, size] of TIME_UNITS) {
      if (Math.abs(seconds) >= size) return rtf.format(Math.round(seconds / size), unit);
    }
    return "just now";
  };
  const absoluteTime = (timestamp) => new Date(timestamp).toLocaleString();

  const searchQuery = (repo, scope) =>
    `${KEYWORD} ${SCOPES[scope].qualifier} is:pr repo:${repo.fullName}`;

  const githubSearchUrl = (repo, scope) => {
    const query = `is:pr ${KEYWORD} ${SCOPES[scope].qualifier} sort:updated-desc`;
    return `https://github.com/${repo.fullName}/pulls?q=${encodeURIComponent(query)}`;
  };

  const headerNumber = (response, name) => {
    const value = response.headers.get(name);
    return value === null ? NaN : Number(value);
  };

  const normalize = (item, repoId) => {
    const mergedAt = item.pull_request && item.pull_request.merged_at;
    let state = item.state;
    if (mergedAt) state = "merged";
    else if (item.state === "open" && item.draft) state = "draft";
    const user = item.user || {};
    return {
      repoId,
      number: item.number,
      title: item.title,
      url: item.html_url,
      author: user.login || "ghost",
      authorUrl: user.html_url || null,
      avatar: user.avatar_url || null,
      state,
      createdAt: item.created_at,
      updatedAt: item.updated_at,
      comments: item.comments || 0,
      labels: (item.labels || []).map((label) => ({ name: label.name, color: label.color })),
    };
  };

  const fetchRepo = async (repo, scope, perPage) => {
    const params = new URLSearchParams({
      q: searchQuery(repo, scope),
      sort: "updated",
      order: "desc",
      per_page: String(perPage),
    });
    const response = await fetch(`${SEARCH_API}?${params}`, {
      headers: { Accept: "application/vnd.github+json" },
    });
    if (!response.ok) {
      const remaining = headerNumber(response, "x-ratelimit-remaining");
      const reset = headerNumber(response, "x-ratelimit-reset");
      if ((response.status === 403 || response.status === 429) && remaining === 0) {
        const retryAt = Number.isNaN(reset) ? "" : ` until ${new Date(reset * 1000).toLocaleTimeString()}`;
        throw new Error(`GitHub API rate limit reached${retryAt}`);
      }
      throw new Error(`GitHub API returned HTTP ${response.status}`);
    }
    const data = await response.json();
    return {
      total: data.total_count,
      items: data.items.map((item) => normalize(item, repo.id)),
      fetchedAt: Date.now(),
    };
  };

  const loadCache = () => {
    try {
      const parsed = JSON.parse(window.localStorage.getItem(CACHE_KEY));
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch (error) {
      return {};
    }
  };

  const saveCache = (cache) => {
    try {
      window.localStorage.setItem(CACHE_KEY, JSON.stringify(cache));
    } catch (error) {
      // Storage may be full or disabled; the dashboard still works in memory.
    }
  };

  const labelTextColor = (hex) => {
    const r = parseInt(hex.slice(0, 2), 16);
    const g = parseInt(hex.slice(2, 4), 16);
    const b = parseInt(hex.slice(4, 6), 16);
    return 0.299 * r + 0.587 * g + 0.114 * b > 150 ? "#1f2328" : "#ffffff";
  };

  const debounce = (fn, ms) => {
    let timer;
    return (...args) => {
      clearTimeout(timer);
      timer = setTimeout(() => fn(...args), ms);
    };
  };

  class Dashboard {
    constructor(root) {
      this.root = root;
      const minutes = Number(root.dataset.refreshMinutes);
      this.refreshMs = (minutes > 0 ? minutes : 5) * 60 * 1000;
      const perPage = Number(root.dataset.perPage);
      this.perPage = perPage >= 1 && perPage <= 100 ? Math.floor(perPage) : 100;

      const params = new URLSearchParams(window.location.search);
      const pick = (name, allowed, fallback) =>
        allowed.includes(params.get(name)) ? params.get(name) : fallback;
      this.filters = {
        repo: pick("repo", REPOS.map((repo) => repo.id), "all"),
        state: pick("state", Object.keys(STATES), "all"),
        scope: pick("scope", Object.keys(SCOPES), "title"),
        sort: pick("sort", Object.keys(SORTS), "updated"),
        text: params.get("q") || "",
      };
      this.visibleCount = PAGE_SIZE;
      this.loading = false;
      this.errors = {};
      this.cache = {};
      this.syncFromStorage();

      this.build();
      this.render();
      this.refreshIfStale();

      setInterval(() => {
        this.refreshIfStale();
        this.renderStatus();
      }, 60 * 1000);
      document.addEventListener("visibilitychange", () => this.refreshIfStale());
      window.addEventListener("storage", (event) => {
        if (event.key !== CACHE_KEY) return;
        this.syncFromStorage();
        this.render();
      });
    }

    get entry() {
      return this.cache[this.filters.scope] || { repos: {} };
    }

    // Adopt entries another tab fetched more recently, without dropping
    // in-memory data when storage is unavailable.
    syncFromStorage() {
      for (const [scope, stored] of Object.entries(loadCache())) {
        if (!(scope in SCOPES) || !stored || typeof stored.repos !== "object") continue;
        const current = this.cache[scope];
        if (!current || (stored.attemptedAt || 0) > (current.attemptedAt || 0)) {
          this.cache[scope] = stored;
        }
      }
    }

    refreshIfStale() {
      if (document.hidden || this.loading) return;
      this.syncFromStorage();
      const attemptedAt = this.entry.attemptedAt || 0;
      if (Date.now() - attemptedAt >= this.refreshMs) this.refresh();
      else this.render();
    }

    async refresh() {
      if (this.loading) return;
      const scope = this.filters.scope;
      this.loading = true;
      this.renderStatus();

      const results = await Promise.allSettled(
        REPOS.map((repo) => fetchRepo(repo, scope, this.perPage)),
      );
      this.syncFromStorage();
      const entry = this.cache[scope] || { repos: {} };
      const errors = {};
      results.forEach((result, index) => {
        const repo = REPOS[index];
        if (result.status === "fulfilled") entry.repos[repo.id] = result.value;
        else errors[repo.id] = result.reason.message || String(result.reason);
      });
      entry.attemptedAt = Date.now();
      this.cache[scope] = entry;
      saveCache(this.cache);
      this.errors[scope] = errors;
      this.loading = false;
      this.render();
    }

    setFilter(name, value) {
      if (this.filters[name] === value) return;
      this.filters[name] = value;
      this.visibleCount = PAGE_SIZE;
      this.syncUrl();
      if (name === "scope") this.refreshIfStale();
      this.render();
    }

    syncUrl() {
      const url = new URL(window.location.href);
      const defaults = { repo: "all", state: "all", scope: "title", sort: "updated", text: "" };
      for (const [name, value] of Object.entries(this.filters)) {
        const param = name === "text" ? "q" : name;
        if (value === defaults[name]) url.searchParams.delete(param);
        else url.searchParams.set(param, value);
      }
      window.history.replaceState(null, "", url);
    }

    segmented(name, options, ariaLabel) {
      const group = el("div", { class: "prd-segmented", role: "group", "aria-label": ariaLabel });
      const buttons = {};
      for (const [value, label] of Object.entries(options)) {
        const button = el("button", {
          type: "button",
          class: "prd-segment",
          onclick: () => this.setFilter(name, value),
        });
        button.append(el("span", { text: label }), el("span", { class: "prd-count" }));
        buttons[value] = button;
        group.append(button);
      }
      return { group, buttons };
    }

    select(name, options, ariaLabel) {
      const node = el("select", {
        class: "prd-select",
        "aria-label": ariaLabel,
        onchange: (event) => this.setFilter(name, event.target.value),
      });
      for (const [value, label] of Object.entries(options)) {
        node.append(el("option", { value, text: label }));
      }
      node.value = this.filters[name];
      return node;
    }

    build() {
      this.statusText = el("span", { class: "prd-status-text" });
      this.refreshButton = el("button", {
        type: "button",
        class: "prd-refresh",
        text: "Refresh",
        onclick: () => this.refresh(),
      });
      this.errorBox = el("div", { class: "prd-errors", role: "alert" });
      this.cards = el("div", { class: "prd-cards" });

      const repoOptions = { all: "All" };
      for (const repo of REPOS) repoOptions[repo.id] = repo.name;
      this.repoControl = this.segmented("repo", repoOptions, "Repository");
      this.stateControl = this.segmented("state", STATES, "Pull request state");

      this.searchInput = el("input", {
        type: "search",
        class: "prd-search",
        placeholder: "Filter by title, author, label, or #number",
        "aria-label": "Filter pull requests",
        value: this.filters.text,
        oninput: debounce((event) => this.setFilter("text", event.target.value.trim()), 150),
      });

      this.list = el("ol", { class: "prd-list" });
      this.summary = el("p", { class: "prd-summary" });
      this.moreButton = el("button", {
        type: "button",
        class: "prd-more",
        text: "Show more",
        onclick: () => {
          this.visibleCount += PAGE_SIZE;
          this.renderList();
        },
      });

      this.root.replaceChildren(
        el(
          "div",
          { class: "prd-status", "aria-live": "polite" },
          el("span", { class: "prd-live-dot", "aria-hidden": "true" }),
          this.statusText,
          this.refreshButton,
        ),
        this.errorBox,
        this.cards,
        el(
          "div",
          { class: "prd-controls" },
          this.repoControl.group,
          this.stateControl.group,
          el(
            "div",
            { class: "prd-control-row" },
            this.searchInput,
            el("label", { class: "prd-select-label" }, "Match ", this.select("scope", SCOPE_LABELS, "Match keyword in")),
            el("label", { class: "prd-select-label" }, "Sort ", this.select("sort", SORTS, "Sort order")),
          ),
        ),
        this.summary,
        this.list,
        this.moreButton,
      );
    }

    allItems() {
      const repos = this.entry.repos || {};
      return REPOS.flatMap((repo) => (repos[repo.id] ? repos[repo.id].items : []));
    }

    filteredItems() {
      const { repo, state, sort, text } = this.filters;
      const needle = text.toLowerCase().replace(/^#/, "");
      const sortKey = sort === "created" ? "createdAt" : "updatedAt";
      return this.allItems()
        .filter((item) => repo === "all" || item.repoId === repo)
        .filter((item) => state === "all" || item.state === state)
        .filter((item) => {
          if (!needle) return true;
          return (
            item.title.toLowerCase().includes(needle) ||
            item.author.toLowerCase().includes(needle) ||
            String(item.number) === needle ||
            item.labels.some((label) => label.name.toLowerCase().includes(needle))
          );
        })
        .sort((a, b) => new Date(b[sortKey]) - new Date(a[sortKey]));
    }

    render() {
      this.renderStatus();
      this.renderCards();
      this.renderControls();
      this.renderList();
    }

    renderStatus() {
      const repos = this.entry.repos || {};
      const fetchedTimes = REPOS.map((repo) => repos[repo.id] && repos[repo.id].fetchedAt).filter(Boolean);
      const minutes = Math.round(this.refreshMs / 60000);
      let text;
      if (this.loading) text = "Fetching the latest pull requests from GitHub…";
      else if (fetchedTimes.length) {
        const oldest = Math.min(...fetchedTimes);
        text = `Updated ${relativeTime(oldest)} · refreshes every ${minutes} minute${minutes === 1 ? "" : "s"}`;
      } else text = "No data loaded yet.";
      this.statusText.textContent = text;
      this.refreshButton.disabled = this.loading;
      this.root.classList.toggle("prd-is-loading", this.loading);

      const errors = this.errors[this.filters.scope] || {};
      this.errorBox.replaceChildren(
        ...REPOS.filter((repo) => errors[repo.id]).map((repo) =>
          el(
            "p",
            {},
            `${repo.name}: ${errors[repo.id]}. `,
            repos[repo.id] ? "Showing cached results. " : "",
            el("a", { href: githubSearchUrl(repo, this.filters.scope), target: "_blank", rel: "noopener" }, "Open the search on GitHub"),
            ".",
          ),
        ),
      );
    }

    renderCards() {
      const repos = this.entry.repos || {};
      const now = Date.now();
      this.cards.replaceChildren(
        ...REPOS.map((repo) => {
          const data = repos[repo.id];
          const items = data ? data.items : [];
          const counts = { open: 0, draft: 0, merged: 0, closed: 0 };
          for (const item of items) counts[item.state] += 1;
          const active = items.filter((item) => now - new Date(item.updatedAt).getTime() < ACTIVE_WINDOW_MS).length;
          return el(
            "div",
            { class: `prd-card prd-repo-${repo.id}` },
            el(
              "div",
              { class: "prd-card-head" },
              el("span", { class: "prd-card-name", text: repo.name }),
              el("a", {
                class: "prd-card-link",
                href: githubSearchUrl(repo, this.filters.scope),
                target: "_blank",
                rel: "noopener",
                text: repo.fullName,
              }),
            ),
            el("div", { class: "prd-card-total", text: data ? data.total.toLocaleString() : "–" }),
            el("div", { class: "prd-card-caption", text: "matching pull requests" }),
            items.length < (data ? data.total : 0)
              ? el("div", { class: "prd-card-caption prd-card-sample", text: `Latest ${items.length} by state:` })
              : null,
            el(
              "div",
              { class: "prd-card-states" },
              ["open", "draft", "merged", "closed"].map((state) =>
                el("span", { class: `prd-pill prd-state-${state}`, title: `${STATES[state]} among loaded PRs` }, `${counts[state]} ${STATES[state].toLowerCase()}`),
              ),
            ),
            el("div", { class: "prd-card-caption", text: `${active} updated in the last 7 days` }),
          );
        }),
      );
    }

    renderControls() {
      const items = this.allItems();
      const { repo, state } = this.filters;
      const repoCount = (id) => items.filter((item) => id === "all" || item.repoId === id).length;
      const stateCount = (id) =>
        items.filter((item) => (repo === "all" || item.repoId === repo) && (id === "all" || item.state === id)).length;
      for (const [id, button] of Object.entries(this.repoControl.buttons)) {
        button.setAttribute("aria-pressed", String(id === repo));
        button.lastChild.textContent = String(repoCount(id));
      }
      for (const [id, button] of Object.entries(this.stateControl.buttons)) {
        button.setAttribute("aria-pressed", String(id === state));
        button.lastChild.textContent = String(stateCount(id));
      }
    }

    renderList() {
      const items = this.filteredItems();
      const shown = items.slice(0, this.visibleCount);
      const loaded = this.allItems().length;
      if (!loaded) {
        this.summary.textContent = this.loading ? "" : "No pull requests loaded.";
      } else {
        this.summary.textContent =
          `Showing ${shown.length} of ${items.length} matching pull requests ` +
          `(from the ${this.perPage} most recently updated per repository).`;
      }
      this.list.replaceChildren(...shown.map((item) => this.renderItem(item)));
      if (loaded && !items.length) {
        this.list.append(el("li", { class: "prd-empty", text: "No pull requests match the current filters." }));
      }
      this.moreButton.hidden = shown.length >= items.length;
    }

    renderItem(item) {
      const repo = REPOS.find((candidate) => candidate.id === item.repoId);
      const labels = item.labels
        .filter((label) => /^[0-9a-f]{6}$/i.test(label.color || ""))
        .slice(0, MAX_LABELS)
        .map((label) =>
          el("span", {
            class: "prd-label",
            style: `background-color:#${label.color};color:${labelTextColor(label.color)}`,
            text: label.name,
          }),
        );
      const dateField = this.filters.sort === "created" ? "createdAt" : "updatedAt";
      const dateVerb = this.filters.sort === "created" ? "opened" : "updated";
      return el(
        "li",
        { class: "prd-item" },
        item.avatar
          ? el("img", {
              class: "prd-avatar",
              src: `${item.avatar}${item.avatar.includes("?") ? "&" : "?"}s=64`,
              alt: "",
              loading: "lazy",
              width: 32,
              height: 32,
            })
          : el("span", { class: "prd-avatar" }),
        el(
          "div",
          { class: "prd-item-main" },
          el(
            "div",
            { class: "prd-item-title" },
            el("span", { class: `prd-pill prd-state-${item.state}`, text: STATES[item.state] }),
            el("a", { href: item.url, target: "_blank", rel: "noopener", text: item.title }),
          ),
          el(
            "div",
            { class: "prd-item-meta" },
            el("span", { class: `prd-repo-chip prd-repo-${item.repoId}`, text: repo ? repo.name : item.repoId }),
            ` #${item.number} by `,
            item.authorUrl
              ? el("a", { href: item.authorUrl, target: "_blank", rel: "noopener", text: item.author })
              : item.author,
            " · ",
            el("time", { datetime: item[dateField], title: absoluteTime(item[dateField]), text: `${dateVerb} ${relativeTime(item[dateField])}` }),
            item.comments ? ` · ${item.comments} comment${item.comments === 1 ? "" : "s"}` : "",
          ),
          labels.length ? el("div", { class: "prd-labels" }, labels) : null,
        ),
      );
    }
  }

  const init = () => {
    for (const root of document.querySelectorAll("[data-pr-dashboard]")) {
      new Dashboard(root);
    }
  };

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
