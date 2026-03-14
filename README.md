# CULTIVE HK Event Scraper v2.2

Apify Actor built with [Crawlee](https://crawlee.dev) + [Cheerio](https://cheerio.js.org/) that scrapes Hong Kong event listings for the [CULTIVE](https://cultive.hk) platform.

## v2.2 Changes
- **Removed Time Out** — enough data collected, focused scraper on active sources
- **CULTIVE field mapping** — unified mapToCultiveSchema() with multi-name field resolution
- **Eventbrite HK** — ticketed events with JSON-LD extraction
- **HK Art Gallery Association** — exhibition cards + detail page parsing

## Target Sites
- **Eventbrite HK** — ticketed events across all categories
- **HK Art Gallery Association** — exhibitions with gallery info

## Deploy to Apify
1. Push this repo to GitHub
2. Go to Apify Console → your Actor → Rebuild
3. Click Start — results appear in the Dataset tab
