# politeia-data

Static JSON endpoint and daily pipeline for Daily Better News.

## Published paths
- `https://<github-username>.github.io/politeia-data/notallbad/usa.json`
- `https://data.politeiaproject.org/notallbad/usa.json` (after Pages custom domain + DNS CNAME)
- `https://data.politeiaproject.org/support/`
- `https://data.politeiaproject.org/privacy/`

## Repository layout
- `notallbad/*.json`: country payloads consumed by the app
- `pipeline/run_daily.py`: feed ingestion and filtering pipeline
- `pipeline/config/feeds.json`: official RSS/Atom sources per country
- `pipeline/config/categories.json`: keyword-based category mapping
- `.github/workflows/daily.yml`: daily automation job

## Pipeline behavior
- Reads RSS/Atom feeds from official institutional sources.
- Looks back 72 hours (UTC).
- Keeps items with numeric deltas/metrics or official approval milestones.
- Filters obvious negative events.
- Deduplicates similar stories.
- Publishes up to 10 items/country, preferring category diversity.
- Validates JSON schema before publish.
- Uses atomic writes (tmp + rename).
- Writes run logs under `pipeline/logs/`.
- If all feeds fail for a country, publishes valid JSON with `items: []`.

## GitHub Pages
Set GitHub Pages to:
- Branch: `main`
- Folder: `/ (root)`

`CNAME` is set to `data.politeiaproject.org`.

## One manual GoDaddy DNS step
Add this DNS record:
- Type: `CNAME`
- Host: `data`
- Points to: `<github-username>.github.io`
