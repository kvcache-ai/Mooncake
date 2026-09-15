# CI Monitor Scripts

Scripts for automated CI health monitoring.

## ci_failures_analysis.py

Fetches recent CI workflow runs, identifies consistently failing jobs, and
produces a JSON report. Runs every 12 hours via GitHub Actions.

```bash
python ci_failures_analysis.py --token $GITHUB_TOKEN --limit 50 --output report.json
```
