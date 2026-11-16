# Ecosystem PR Dashboard

This dashboard tracks pull requests that mention Mooncake in the inference
engines that integrate it: [SGLang](https://github.com/sgl-project/sglang),
[vLLM](https://github.com/vllm-project/vllm), and
[TokenSpeed](https://github.com/lightseekorg/tokenspeed).

PRs are fetched live from the GitHub search API in your browser and refresh
automatically while this page is open. By default, a PR matches when its title
contains `mooncake`; switch **Match** to *Title + description* for broader
results. Filters are kept in the page URL, so a filtered view can be shared as
a link.

:::{raw} html
<div class="pr-dashboard" data-pr-dashboard data-refresh-minutes="5" data-per-page="100">
  <p>The live dashboard needs JavaScript. Browse the PRs on GitHub instead:</p>
  <ul>
    <li><a href="https://github.com/sgl-project/sglang/pulls?q=is%3Apr+mooncake+in%3Atitle+sort%3Aupdated-desc">SGLang</a></li>
    <li><a href="https://github.com/vllm-project/vllm/pulls?q=is%3Apr+mooncake+in%3Atitle+sort%3Aupdated-desc">vLLM</a></li>
    <li><a href="https://github.com/lightseekorg/tokenspeed/pulls?q=is%3Apr+mooncake+in%3Atitle+sort%3Aupdated-desc">TokenSpeed</a></li>
  </ul>
</div>
:::

:::{note}
Unauthenticated GitHub search allows 10 requests per minute per IP address, and
each refresh uses one request per repository. Results are cached in your browser
and shared across tabs. If the limit is reached, the dashboard keeps showing
cached results and retries on the next refresh.
:::
