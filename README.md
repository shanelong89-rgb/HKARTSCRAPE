# CULTIVE HK Event Scraper

Apify Actor built with [Crawlee](https://crawlee.dev) + [Cheerio](https://cheerio.js.org/) that scrapes Hong Kong event listings for the [CULTIVE](https://cultive.hk) platform.

## Target Sites

- **Lifestyle Asia HK** — https://www.lifestyleasia.com/hk/whats-on/events-whats-on/
- **Time Out Hong Kong** — https://www.timeout.com/hong-kong/things-to-do/things-to-do-in-hong-kong-this-weekend

## Output Format

Each scraped event includes:

| Field        | Description                              |
|-------------|------------------------------------------|
| title       | Event name                                |
| date        | Event date (ISO or text)                  |
| time        | Start time                                |
| venueName   | Venue / location name                     |
| description | Event description (max 500 chars)         |
| image       | Image URL                                 |
| price       | Ticket price                              |
| address     | Full address                              |
| lat / lng   | Coordinates (if available)                |
| artists     | Array of performer names                  |
| district    | Auto-detected HK district                 |
| category    | Auto-detected category                    |
| modes       | Auto-detected experience modes            |
| sourceUrl   | Link to the original event page           |

## Extraction Strategy

1. **JSON-LD** — Tries structured data first (most reliable)
2. **Site-specific CSS selectors** — Tailored for each target site
3. **Generic fallback** — Works on any site with standard HTML patterns

## Local Development

\`\`\`bash
npm install
npx apify-cli run
\`\`\`

## Deploy to Apify

1. Push this repo to GitHub
2. Go to [Apify Console](https://console.apify.com/actors) -> Create new -> Link from GitHub
3. Select this repository
4. Apify builds and deploys automatically on each push

## License

MIT