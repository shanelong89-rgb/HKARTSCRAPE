// CULTIVE HK Events Apify Actor v2.5
// v2.5: Description extraction from DOM body text + Google Maps coords
//       Address validation (must contain digits). 3-strategy desc fallback.
// v2.4: HK-AGA detail page crawling with Wix JSON mining (desc=0, didn't work)
// v2.3: Fixed HK-AGA card extraction (line splitting vs DOM traversal)
// Deploy: push to GitHub -> Apify Console -> Rebuild -> Start

import { CheerioCrawler, Dataset } from '@crawlee/cheerio';
import { Actor } from 'apify';
await Actor.init();

const {
  startUrls = [
    'https://www.eventbrite.hk/d/hong-kong/events/',
    'https://www.eventbrite.hk/d/hong-kong/music--events/',
    'https://www.eventbrite.hk/d/hong-kong/food-and-drink--events/',
    'https://www.eventbrite.hk/d/hong-kong/arts--events/',
    'https://www.hk-aga.org/exhibitions',
  ],
  maxRequestsPerCrawl = 120,
} = (await Actor.getInput()) ?? {};

const proxyConfig = await Actor.createProxyConfiguration({ checkAccess: true });
const seen = new Set();

// ── Helpers ─────────────────────���──────────────────────────

function tryText($el, sels) {
  for (const s of sels) {
    const t = $el.find(s).first().text().trim();
    if (t && t.length > 1) return t;
  }
  return '';
}

function resolveUrl(url, base) {
  if (!url) return '';
  if (url.startsWith('http')) return url;
  try { return new URL(url, base).href; } catch { return url; }
}

function normalizeDate(text) {
  if (!text) return '';
  if (/^\d{4}-\d{2}-\d{2}/.test(text)) return text;
  const until = text.match(/until\s+(\d{1,2}\s+\w+\s+\d{4})/i);
  if (until) { const d = new Date(until[1]); if (!isNaN(d)) return d.toISOString().split('T')[0]; }
  const range = text.match(/(\d{1,2})\s+(\w{3,9})\s*[-]\s*\d{1,2}\s+\w{3,9},?\s*(\d{4})/);
  if (range) { const d = new Date(range[1]+' '+range[2]+' '+range[3]); if (!isNaN(d)) return d.toISOString().split('T')[0]; }
  try { const d = new Date(text); if (!isNaN(d) && d.getFullYear() > 2020) return d.toISOString().split('T')[0]; } catch {}
  return text;
}

const HK_DISTRICTS = [
  'Central','Wan Chai','Causeway Bay','Admiralty','Sheung Wan',
  'Tsim Sha Tsui','Mong Kok','Jordan','Sham Shui Po','Kowloon',
  'Kwun Tong','Wong Tai Sin','Sha Tin','Tai Po','Tuen Mun',
  'Yuen Long','Tsuen Wan','Sai Kung','Lantau','Aberdeen',
  'Stanley','Kennedy Town','Sai Ying Pun','North Point',
  'Quarry Bay','Tai Koo','Chai Wan','West Kowloon',
  'Wong Chuk Hang','Hung Hom','Diamond Hill','Tai Kwun','PMQ',
];

function detectDistrict(text) {
  const lower = text.toLowerCase();
  return HK_DISTRICTS.find(d => lower.includes(d.toLowerCase())) || '';
}

const CATEGORY_KEYWORDS = {
  Music: ['concert','music','dj','live band','gig','jazz','orchestra'],
  Art: ['art','exhibition','gallery','museum','sculpture','painting'],
  'Food & Drink': ['food','dining','brunch','cocktail','wine','beer','tasting'],
  Nightlife: ['club','nightclub','party','nightlife','rave'],
  Wellness: ['yoga','meditation','wellness','fitness','sound bath'],
  Film: ['film','movie','cinema','screening'],
  Theatre: ['theatre','theater','performance','dance','ballet','comedy'],
  Sports: ['sport','run','marathon','hike','hiking','cycling'],
  Markets: ['market','flea','bazaar','fair','craft'],
  Community: ['community','charity','volunteer','meetup','workshop'],
};

function detectCategory(text) {
  const lower = text.toLowerCase();
  for (const [cat, kws] of Object.entries(CATEGORY_KEYWORDS))
    if (kws.some(k => lower.includes(k))) return cat;
  return 'Events';
}

function detectModes(text) {
  const modes = [], lower = text.toLowerCase();
  if (['outdoor','rooftop','garden','beach','park','terrace'].some(k => lower.includes(k))) modes.push('Outdoor');
  if (['indoor','gallery','museum','theatre','cinema','studio'].some(k => lower.includes(k))) modes.push('Indoor');
  if (['free','no cover','complimentary'].some(k => lower.includes(k))) modes.push('Free');
  if (['family','kid','children'].some(k => lower.includes(k))) modes.push('Family-Friendly');
  return modes.length > 0 ? modes : ['Indoor'];
}

// ── CULTIVE Field Mapping ────────────────────────────────────

function mapToCultiveSchema(raw, url) {
  const r = raw || {};
  const title = r.title || r.name || r.eventTitle || '';
  const date = normalizeDate(r.date || r.startDate || r.eventDate || '');
  const time = r.time || r.startTime || '';
  const venueName = r.venueName || r.venue || r.locationName || r.place ||
    (r.location && typeof r.location === 'object' ? r.location.name : '') || '';
  const description = (r.description || r.text || r.content || r.summary || '');
  const imgRaw = r.image || r.imageUrl || r.photo || r.thumbnail || r.flyerFront || '';
  const image = typeof imgRaw === 'object' ? (imgRaw.url || imgRaw.src || '') : imgRaw;
  const price = r.price || r.cost || r.ticketPrice ||
    (r.offers && r.offers.price ? (r.offers.priceCurrency||'HKD')+' '+r.offers.price : '') || '';
  let address = r.address || r.fullAddress || '';
  if (!address && r.location) {
    if (typeof r.location === 'string') address = r.location;
    else if (r.location.address) address = typeof r.location.address === 'string'
      ? r.location.address : r.location.address.streetAddress || '';
  }
  let lat = r.lat||r.latitude||null, lng = r.lng||r.longitude||null;
  if (!lat && r.geo) { lat = r.geo.lat||r.geo.latitude||null; lng = r.geo.lng||r.geo.longitude||null; }
  if (!lat && r.coordinates) { lat = r.coordinates.lat||null; lng = r.coordinates.lng||null; }
  if (!lat && r.location?.geo) { lat = r.location.geo.latitude||null; lng = r.location.geo.longitude||null; }
  let artists = r.artists || [];
  if (typeof artists === 'string') artists = artists.split(',').map(a => a.trim()).filter(Boolean);
  if (!Array.isArray(artists)) artists = [];
  if (artists.length === 0 && r.performer) {
    artists = Array.isArray(r.performer) ? r.performer.map(p => p.name||p) : r.performer.name ? [r.performer.name] : [];
  }
  const fullText = [title, description, venueName, address].join(' ');
  return {
    title, date, time, venueName, description,
    image: resolveUrl(image, url), price, address,
    lat: lat ? Number(lat) : null, lng: lng ? Number(lng) : null,
    artists,
    district: r.district || detectDistrict(fullText),
    category: r.category || detectCategory(fullText),
    modes: r.modes || detectModes(fullText),
    sourceUrl: r.sourceUrl || r.url || url,
    scrapedFrom: url, scrapedAt: new Date().toISOString(),
  };
}

// ── JSON-LD extraction ───────────────────────────────────────

function extractJsonLdEvents($, url) {
  const events = [];
  $('script[type="application/ld+json"]').each((i, el) => {
    try {
      const data = JSON.parse($(el).html());
      const items = Array.isArray(data) ? data : data['@graph'] ? data['@graph'] : [data];
      for (const item of items) {
        if (item['@type'] !== 'Event' || !item.name) continue;
        events.push(mapToCultiveSchema({
          title: item.name, startDate: item.startDate,
          venueName: item.location?.name || '',
          description: item.description || '', image: item.image,
          address: item.location?.address?.streetAddress || '',
          lat: item.location?.geo?.latitude, lng: item.location?.geo?.longitude,
          performer: item.performer, sourceUrl: item.url || url,
          price: item.offers?.price ? (item.offers.priceCurrency||'HKD')+' '+item.offers.price : '',
        }, url));
      }
    } catch {}
  });
  return events;
}

// ── Eventbrite ───────────────────────────────────────────────

function isEventbrite(url) { return url.includes('eventbrite.hk') || url.includes('eventbrite.com'); }

function extractEventbriteListingLinks($, url) {
  const links = new Set();
  $('a[href*="/e/"]').each((i, el) => {
    const href = $(el).attr('href');
    if (href && href.includes('/e/')) links.add(resolveUrl(href, url));
  });
  return [...links];
}

function extractEventbriteDetail($, url) {
  const title = $('h1').first().text().trim() || $('meta[property="og:title"]').attr('content') || '';
  if (!title || title.length < 5) return [];

  // Date: prefer time[datetime], fallback to text pattern
  let date = $('time').first().attr('datetime') || '';
  if (!date) {
    const bodyText = $('body').text();
    const dateP = bodyText.match(/((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,\s+\w+\s+\d{1,2}(?:,\s*\d{4})?)/);
    if (dateP) date = dateP[1];
  }

  // Venue: extract first text node only, strip trailing region/date
  let venueName = '';
  const venueEl = $('[class*="location-info"]').first();
  if (venueEl.length) {
    venueName = venueEl.find('p').first().text().trim()
      || venueEl.find('strong').first().text().trim() || '';
    // Cut before day-of-week to avoid "VenueCentral, HKIMonday Mar 30..."
    venueName = venueName.split(/(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?/)[0].trim();
    // Remove trailing region codes
    venueName = venueName.replace(/,?\s*(?:HKI|KOW|NT|NTW)\s*$/, '').trim();
  }

  // Address: try second <p> inside location-info, clean same way
  let address = '';
  if (venueEl.length) {
    const pEls = venueEl.find('p');
    if (pEls.length > 1) address = $(pEls[1]).text().trim();
    if (!address) address = venueEl.text().trim();
    address = address.split(/(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?/)[0].trim();
    if (venueName && address.startsWith(venueName)) address = address.slice(venueName.length).trim();
  }

  // Price
  let price = '';
  $('[class*="conversion-bar"] [class*="price"], [class*="ticket"] [class*="price"]').each((i, el) => {
    const t = $(el).text().trim();
    if (t && !price && /\$|HKD|Free|\d/.test(t)) price = t;
  });

  return [mapToCultiveSchema({
    title,
    description: $('meta[property="og:description"]').attr('content') || '',
    image: $('meta[property="og:image"]').attr('content') || '',
    date, time: '', venueName, address, price, sourceUrl: url,
  }, url)];
}

// ── HK Art Gallery Association ───────────────────────────────

function isHkAga(url) { return url.includes('hk-aga.org'); }

function isHkAgaListing(url) {
  return /\/exhibitions\/?($|\?)/.test(new URL(url).pathname + new URL(url).search);
}

// HK-AGA listing page: each <a> wraps an entire card with structured text:
//   Line 1: DISTRICT (all caps, e.g. "SAI WAN (WESTERN)")
//   Line 2: Exhibition title
//   Line 3: Date range (e.g. "14 Mar – 8 Apr, 2026")
//   Line 4: Gallery/venue name
//   Line 5: Type label (e.g. "Art Galleries" / "Art Spaces")
// Parse this blob directly instead of traversing DOM containers.
function extractHkAgaCards($, url) {
  const cards = [];
  const seen = new Set();
  $('a[href]').each((i, el) => {
    const href = $(el).attr('href');
    if (!href) return;
    const full = resolveUrl(href, url);
    if (!/hk-aga\.org\/exhibitions\/\d+/.test(full)) return;
    if (seen.has(full)) return;
    seen.add(full);

    // Split the link text blob into clean lines
    const rawText = $(el).text();
    const lines = rawText.split('\n').map(l => l.trim()).filter(l => l.length > 0);
    if (lines.length < 2) return;

    // Parse structured lines
    let district = '', title = '', date = '', venue = '', typeLabel = '';

    for (const line of lines) {
      if (!district && line === line.toUpperCase() && line.length >= 3 && line.length < 40
        && !/^(ART |CURRENTLY|UPCOMING|PAST|FILTER)/.test(line)
        && !/\d{1,2}\s+\w{3,9}/.test(line)) {
        district = line; continue;
      }
      if (/^Art\s+(Galleries|Spaces|Gallery)$/i.test(line)) { typeLabel = line; continue; }
      if (!date && /\d{1,2}\s+\w{3,9}\s*[\-\u2013]\s*\d{1,2}\s+\w{3,9},?\s*\d{4}/.test(line)) {
        date = line; continue;
      }
      if (!title && line.length >= 3) { title = line; continue; }
      if (title && !venue && line.length >= 2) { venue = line; continue; }
    }

    if (!title || title.length < 3) return;
    const image = $(el).find('img').first().attr('src') || '';

    cards.push({ url: full, title, date, venue, district, image: image ? resolveUrl(image, url) : '' });
  });
  return cards;
}

// Mine HK-AGA detail pages for description, address, coords
// The Wix SPA DOES server-render body text, so we extract from DOM + body text
function extractWixData($, url) {
  const result = { description: '', address: '', lat: null, lng: null };
  const body = $('body').text() || '';

  // ── Address: find text after "Address:" label ──
  // Use body text split by common field labels
  const addrMatch = body.match(/Address[:\s]+([^\n]+)/i);
  if (addrMatch) {
    let addr = addrMatch[1].trim();
    // Stop before Phone/Email/Website labels if on same line
    addr = addr.split(/\s*(?:Phone|Email|Website|Tel)[:\s]/i)[0].trim();
    // Avoid catching description text: valid addresses have numbers + road/street keywords
    if (/\d/.test(addr) && (addr.length < 200)) {
      result.address = addr;
    }
  }

  // ── Description: collect substantial text paragraphs from DOM ──
  // Strategy A: Find all text nodes > 80 chars that look like descriptions
  const descParts = [];
  $('p, div, span').each((i, el) => {
    const text = $(el).clone().children().remove().end().text().trim();
    if (text.length < 80) return;
    // Skip if it looks like address, title, date, nav, or boilerplate
    if (/^Address|^Phone|^Email|^Website|^Click here|FILTER|CURRENTLY|UPCOMING/i.test(text)) return;
    if (/^\d{1,2}\s+\w{3,9}\s*[\-\u2013]/.test(text)) return; // date range
    if (text === text.toUpperCase() && text.length < 50) return; // district label
    descParts.push(text);
  });

  if (descParts.length > 0) {
    // Concatenate all description paragraphs, deduplicate overlaps
    descParts.sort((a, b) => b.length - a.length);
    // Filter out parts that are substrings of longer parts
    const unique = descParts.filter((p, i) => !descParts.slice(0, i).some(longer => longer.includes(p)));
    result.description = unique.join(' ').replace(/\\s+/g, ' ').trim();
  }

  // Strategy B: If no long paragraphs found, try extracting text between
  // the header section and "Click here" / "Address:" markers
  if (!result.description) {
    const clickIdx = body.indexOf('Click here');
    const addrIdx = body.indexOf('Address');
    const endIdx = clickIdx > 0 ? clickIdx : (addrIdx > 0 ? addrIdx : body.length);
    // Skip the first ~100 chars (title, date, venue header)
    const chunk = body.slice(Math.min(150, endIdx), endIdx).trim();
    if (chunk.length > 80) {
      result.description = chunk.replace(/\\s+/g, ' ').trim();
    }
  }

  // Strategy C: og:description meta tag
  if (!result.description) {
    const ogDesc = ($('meta[property="og:description"]').attr('content') || '').trim();
    if (ogDesc && ogDesc.length > 30) result.description = ogDesc;
  }

  // ── Coordinates: from Google Maps iframe/embed URL ──
  $('iframe[src*="maps"], iframe[src*="google"]').each((i, el) => {
    const src = $(el).attr('src') || '';
    // Pattern: q=LAT,LNG or @LAT,LNG or ll=LAT,LNG or center=LAT,LNG
    const coordMatch = src.match(/(?:q=|@|ll=|center=)([0-9.]+)[,]([0-9.]+)/);
    if (coordMatch && !result.lat) {
      result.lat = Number(coordMatch[1]);
      result.lng = Number(coordMatch[2]);
    }
  });

  // Also try script tags for embedded coordinates
  if (!result.lat) {
    $('script').each((i, el) => {
      const text = $(el).html() || '';
      if (text.length < 50) return;
      const latMatch = text.match(/"(?:lat|latitude)"\s*:\s*([0-9]{1,3}\.[0-9]+)/);
      const lngMatch = text.match(/"(?:lng|longitude)"\s*:\s*([0-9]{1,3}\.[0-9]+)/);
      if (latMatch && lngMatch && !result.lat) {
        result.lat = Number(latMatch[1]);
        result.lng = Number(lngMatch[1]);
      }
    });
  }

  return result;
}

// ── Crawler ──────────────────────────────────────────────────

const crawler = new CheerioCrawler({
  proxyConfiguration: proxyConfig,
  maxRequestsPerCrawl, maxRequestRetries: 2, requestHandlerTimeoutSecs: 30,

  async requestHandler({ request, $, log }) {
    const url = request.url;
    if (['/news/','/jobs/','/newsletter','/search','/shopping/'].some(p => url.includes(p))) return;
    log.info('Scraping: ' + url);
    let events = [];

    if (isEventbrite(url)) {
      if (url.includes('/e/')) {
        events = extractJsonLdEvents($, url);
        if (events.length === 0) events = extractEventbriteDetail($, url);
      } else {
        const links = extractEventbriteListingLinks($, url);
        if (links.length > 0) {
          await crawler.addRequests(links.map(u => ({ url: u })));
          log.info('Enqueued ' + links.length + ' Eventbrite event pages');
        }
        return;
      }
    } else if (isHkAga(url)) {
      if (isHkAgaListing(url)) {
        // Extract card data and enqueue detail pages for descriptions
        const cards = extractHkAgaCards($, url);
        log.info('Found ' + cards.length + ' HK-AGA exhibition cards');

        if (cards.length > 0) {
          // Enqueue detail pages with card context
          await crawler.addRequests(cards.map(c => ({
            url: c.url,
            userData: { hkAga: c },
          })));
          log.info('Enqueued ' + cards.length + ' HK-AGA detail pages for descriptions');
        }
        return; // Don't save yet — wait for detail pages
      } else {
        // Detail page: merge card data with Wix embedded data
        const card = request.userData?.hkAga || {};
        const wix = extractWixData($, url);
        log.info('HK-AGA detail: desc=' + wix.description.length + ' chars, addr=' + (wix.address || 'none'));

        events = [mapToCultiveSchema({
          title: card.title || '', date: card.date || '',
          venueName: card.venue || '',
          district: detectDistrict((card.district || '') + ' ' + (card.venue || '')) || card.district || '',
          image: card.image || '',
          description: wix.description, address: wix.address,
          lat: wix.lat, lng: wix.lng,
          category: 'Art', modes: ['Indoor'],
          sourceUrl: url,
        }, url)];
      }
    } else {
      events = extractJsonLdEvents($, url);
    }

    const unique = events.filter(e => {
      const key = e.title.toLowerCase().replace(/[^a-z0-9]/g, '');
      if (seen.has(key)) return false;
      seen.add(key); return true;
    });
    if (unique.length > 0) {
      await Dataset.pushData(unique);
      log.info('Saved ' + unique.length + ' event(s)');
    }
  },

  failedRequestHandler({ request, log }) {
    log.error('Failed: ' + request.url);
  },
});

await crawler.run(startUrls);
const info = await (await Dataset.open()).getInfo();
console.log('Done! Total events: ' + (info?.itemCount ?? 0));
await Actor.exit();
