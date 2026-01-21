# DataCenterMap USA Scraper

This repo provides a small script to scrape data center listings from the USA page on DataCenterMap.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
python scrape_datacentermap.py \
  --url "https://www.datacentermap.com/usa/" \
  --output "output/datacenters_usa.csv" \
  --dump-html "output/datacentermap_usa.html"
```

The script crawls the USA index, state, and city pages to collect data center names.
It tries to parse JSON-LD data first and falls back to extracting data center links from the page.
If the site layout changes, use `--dump-html` to inspect the HTML and adjust the selectors.

## Notes

Some environments or corporate proxies may block access to `datacentermap.com` and return a `403`.
If that happens, run the script from a network that can access the site.
