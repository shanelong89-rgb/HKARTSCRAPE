// CULTIVE HK Events Apify Actor
// Deploy on Apify (js-crawlee-cheerio template) to scrape HK events.
// Output format matches CULTIVE's import pipeline exactly.
//
// Template: https://console.apify.com/actors/templates/js-crawlee-cheerio

import { CheerioCrawler, Dataset } from '@crawlee/cheerio';
import { Actor } from 'apify';
import { setTimeout } from 'node:timers/promises';

await Actor.init();

// Graceful abort handling
Actor.on('aborting', async () => {
    await setTimeout(1000);
    await Actor.exit();
});

// ── Input Configuration ──────────────────────────────────────
const {
    startUrls = [
        'https://www.lifestyleasia.com/hk/whats-on/events-whats-on/',
        'https://www.timeout.com/hong-kong/things-to-do/things-to-do-in-hong-kong-this-weekend',
    ],
    maxRequestsPerCrawl = 100,
    followLinks = true,
} = (await Actor.getInput()) ?? {};

const proxyConfiguration = await Actor.createProxyConfiguration({ checkAccess: true });

// ── Helpers ──────────────────────────────────────────────────

/** Try multiple selectors and return the first non-empty text */
function tryText($el, selectors) {
    for (const sel of selectors) {
        const text = $el.find(sel).first().text().trim();
        if (text && text.length > 1) return text;
    }
    return '';
}

/** Try to extract an image URL */
function tryImage($el) {
    const img = $el.find('img').first();
    return img.attr('src') || img.attr('data-src') || img.attr('data-lazy-src') || '';
}

/** Resolve relative URLs */
function resolveUrl(url, base) {
    if (!url) return '';
    if (url.startsWith('http')) return url;
    try {
        return new URL(url, base).href;
    } catch {
        return url;
    }
}

/** Detect HK district from text */
function detectDistrict(text) {
    const districts = [
        'Central', 'Wan Chai', 'Causeway Bay', 'Admiralty', 'Sheung Wan',
        'Tsim Sha Tsui', 'Mong Kok', 'Jordan', 'Sham Shui Po', 'Kowloon',
        'Kwun Tong', 'Wong Tai Sin', 'Sha Tin', 'Tai Po', 'Tuen Mun',
        'Yuen Long', 'Tsuen Wan', 'Sai Kung', 'Lantau', 'Aberdeen',
        'Stanley', 'Repulse Bay', 'Happy Valley', 'Kennedy Town',
        'Sai Ying Pun', 'North Point', 'Quarry Bay', 'Tai Koo',
        'Chai Wan', 'Shek Tong Tsui', 'Mid-Levels', 'The Peak',
        'Discovery Bay', 'Tung Chung', 'Hung Hom', 'To Kwa Wan',
        'Whampoa', 'Diamond Hill', 'Lok Fu', 'Cheung Sha Wan',
        'Lai Chi Kok', 'Mei Foo', 'Olympic', 'West Kowloon',
    ];
    const lower = text.toLowerCase();
    return districts.find((d) => lower.includes(d.toLowerCase())) || '';
}

/** Detect event category from text */
function detectCategory(text) {
    const categories = {
        'Music': ['concert', 'music', 'dj', 'live band', 'gig', 'festival', 'rave', 'jazz', 'hip hop', 'electronic'],
        'Art': ['art', 'exhibition', 'gallery', 'museum', 'sculpture', 'painting', 'installation'],
        'Food & Drink': ['food', 'restaurant', 'dining', 'brunch', 'cocktail', 'wine', 'beer', 'tasting', 'bar'],
        'Nightlife': ['club', 'nightclub', 'party', 'nightlife', 'lounge', 'rooftop'],
        'Wellness': ['yoga', 'meditation', 'wellness', 'fitness', 'spa', 'retreat', 'workshop'],
        'Film': ['film', 'movie', 'cinema', 'screening', 'documentary'],
        'Theatre': ['theatre', 'theater', 'performance', 'dance', 'ballet', 'opera', 'comedy', 'stand-up'],
        'Sports': ['sport', 'run', 'marathon', 'rugby', 'football', 'tennis', 'hike', 'hiking', 'cycling'],
        'Markets': ['market', 'flea', 'bazaar', 'fair', 'pop-up'],
        'Community': ['community', 'charity', 'volunteer', 'meetup', 'networking', 'social'],
    };
    const lower = text.toLowerCase();
    for (const [cat, keywords] of Object.entries(categories)) {
        if (keywords.some((kw) => lower.includes(kw))) return cat;
    }
    return 'Events';
}

/** Detect experience modes from text */
function detectModes(text) {
    const modes = [];
    const lower = text.toLowerCase();
    if (['outdoor', 'rooftop', 'garden', 'beach', 'park', 'harbour'].some((k) => lower.includes(k))) modes.push('Outdoor');
    if (['indoor', 'gallery', 'museum', 'theatre', 'cinema', 'restaurant'].some((k) => lower.includes(k))) modes.push('Indoor');
    if (['free', 'no cover', 'complimentary'].some((k) => lower.includes(k))) modes.push('Free');
    if (['family', 'kid', 'children'].some((k) => lower.includes(k))) modes.push('Family-Friendly');
    if (['date', 'romantic', 'couples'].some((k) => lower.includes(k))) modes.push('Date Night');
    return modes.length > 0 ? modes : ['Indoor'];
}

// ── Site-Specific Handlers ───────────────────────────────────

function isLifestyleAsia(url) {
    return url.includes('lifestyleasia.com');
}

function isTimeOut(url) {
    return url.includes('timeout.com');
}

function extractLifestyleAsiaEvents($, url) {
    const events = [];
    $('article, .post-card, .story-card, [class*="post-item"], [class*="article-card"]').each((i, el) => {
        const $el = $(el);
        const title = tryText($el, ['h2', 'h3', '.title', '.post-title', '.story-title', 'a[class*="title"]']);
        if (!title || title.length < 5) return;

        const link = $el.find('a').first().attr('href') || '';
        const image = tryImage($el);
        const dateText = tryText($el, ['time', '[class*="date"]', '.meta-date', '[datetime]']);
        const description = tryText($el, ['p', '.excerpt', '.description', '.summary', '.dek']);

        const fullText = title + ' ' + description;
        events.push({
            title,
            date: dateText,
            time: '',
            venueName: '',
            description,
            image: resolveUrl(image, url),
            price: '',
            address: '',
            lat: null,
            lng: null,
            artists: [],
            district: detectDistrict(fullText),
            category: detectCategory(fullText),
            modes: detectModes(fullText),
            sourceUrl: resolveUrl(link, url),
            scrapedFrom: url,
            scrapedAt: new Date().toISOString(),
        });
    });
    return events;
}

function extractTimeOutEvents($, url) {
    const events = [];
    $('article, [class*="card"], [class*="tile"], [class*="listing-item"], li[class*="event"]').each((i, el) => {
        const $el = $(el);
        const title = tryText($el, ['h2', 'h3', '.card-title', '[class*="title"]', 'a']);
        if (!title || title.length < 5) return;

        const link = $el.find('a').first().attr('href') || '';
        const image = tryImage($el);
        const dateText = tryText($el, ['time', '[class*="date"]', '[class*="when"]', '.meta']);
        const venue = tryText($el, ['[class*="venue"]', '[class*="location"]', '[class*="place"]']);
        const description = tryText($el, ['p', '.summary', '.description', '.excerpt']);
        const price = tryText($el, ['[class*="price"]', '[class*="cost"]']);

        const fullText = title + ' ' + description + ' ' + venue;
        events.push({
            title,
            date: dateText,
            time: '',
            venueName: venue,
            description,
            image: resolveUrl(image, url),
            price,
            address: '',
            lat: null,
            lng: null,
            artists: [],
            district: detectDistrict(fullText),
            category: detectCategory(fullText),
            modes: detectModes(fullText),
            sourceUrl: resolveUrl(link, url),
            scrapedFrom: url,
            scrapedAt: new Date().toISOString(),
        });
    });
    return events;
}

/** Generic fallback extractor */
function extractGenericEvents($, url) {
    const events = [];
    $('article, [class*="event"], [class*="card"], [class*="listing"]').each((i, el) => {
        const $el = $(el);
        const title = tryText($el, ['h1', 'h2', 'h3', '.title', '[class*="title"]', 'a']);
        if (!title || title.length < 5) return;

        const link = $el.find('a').first().attr('href') || '';
        const image = tryImage($el);
        const dateText = tryText($el, ['time', '[class*="date"]', '.date']);
        const venue = tryText($el, ['[class*="venue"]', '[class*="location"]']);
        const description = tryText($el, ['p', '.description', '.excerpt']);
        const price = tryText($el, ['[class*="price"]', '[class*="cost"]']);

        const fullText = title + ' ' + description + ' ' + venue;
        events.push({
            title,
            date: dateText,
            time: '',
            venueName: venue,
            description: description.slice(0, 500),
            image: resolveUrl(image, url),
            price,
            address: '',
            lat: null,
            lng: null,
            artists: [],
            district: detectDistrict(fullText),
            category: detectCategory(fullText),
            modes: detectModes(fullText),
            sourceUrl: resolveUrl(link, url),
            scrapedFrom: url,
            scrapedAt: new Date().toISOString(),
        });
    });
    return events;
}

// ── Also try JSON-LD structured data ─────────────────────────

function extractJsonLdEvents($, url) {
    const events = [];
    $('script[type="application/ld+json"]').each((i, el) => {
        try {
            const data = JSON.parse($(el).html());
            const items = Array.isArray(data) ? data : data['@graph'] ? data['@graph'] : [data];
            for (const item of items) {
                if (item['@type'] !== 'Event') continue;
                const title = item.name || '';
                if (!title) continue;
                const fullText = title + ' ' + (item.description || '') + ' ' + (item.location?.name || '');
                events.push({
                    title,
                    date: item.startDate || '',
                    time: item.startDate ? new Date(item.startDate).toLocaleTimeString() : '',
                    venueName: item.location?.name || '',
                    description: (item.description || '').slice(0, 500),
                    image: item.image?.url || item.image || '',
                    price: item.offers?.price ? `${item.offers.priceCurrency || 'HKD'} ${item.offers.price}` : '',
                    address: item.location?.address?.streetAddress || item.location?.address || '',
                    lat: item.location?.geo?.latitude || null,
                    lng: item.location?.geo?.longitude || null,
                    artists: item.performer
                        ? (Array.isArray(item.performer) ? item.performer.map((p) => p.name) : [item.performer.name])
                        : [],
                    district: detectDistrict(fullText),
                    category: detectCategory(fullText),
                    modes: detectModes(fullText),
                    sourceUrl: item.url || url,
                    scrapedFrom: url,
                    scrapedAt: new Date().toISOString(),
                });
            }
        } catch (e) {
            // ignore invalid JSON-LD
        }
    });
    return events;
}

// ── Crawler ──────────────────────────────────────────────────

const crawler = new CheerioCrawler({
    proxyConfiguration,
    maxRequestsPerCrawl,
    async requestHandler({ request, $, log, enqueueLinks }) {
        const url = request.url;
        log.info(`Scraping: ${url}`);

        // 1. Try JSON-LD first (most reliable)
        let events = extractJsonLdEvents($, url);

        // 2. If no JSON-LD events, try site-specific extractors
        if (events.length === 0) {
            if (isLifestyleAsia(url)) {
                events = extractLifestyleAsiaEvents($, url);
            } else if (isTimeOut(url)) {
                events = extractTimeOutEvents($, url);
            } else {
                events = extractGenericEvents($, url);
            }
        }

        // 3. Deduplicate by title
        const seen = new Set();
        const unique = events.filter((e) => {
            const key = e.title.toLowerCase();
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });

        if (unique.length > 0) {
            await Dataset.pushData(unique);
            log.info(`Extracted ${unique.length} events from ${url}`);
        } else {
            log.warning(`No events found on ${url}`);
        }

        // Optionally follow links on the same domain
        if (followLinks) {
            await enqueueLinks({
                globs: [
                    'https://www.lifestyleasia.com/hk/whats-on/**',
                    'https://www.timeout.com/hong-kong/**',
                ],
            });
        }
    },
    failedRequestHandler({ request, log }) {
        log.error(`Failed: ${request.url}`);
    },
});

await crawler.run(startUrls);

// ── Summary ──────────────────────────────────────────────────
const dataset = await Dataset.open();
const info = await dataset.getInfo();
console.log(`Done! Total events scraped: ${info?.itemCount ?? 0}`);

await Actor.exit();
