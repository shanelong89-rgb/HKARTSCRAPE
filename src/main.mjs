// CULTIVE HK Events Apify Actor v2.7
// v2.7: Fix boilerplate-only descs (return ""), artist boilerplate leak,
//       word-break spacing in descriptions, address-first district detection,
//       improved artist extraction patterns
// v2.6: Strip boilerplate from descriptions, date/endDate split, district
//       normalization, default Free price, lowercase category, artist extraction
// v2.5: Description extraction from DOM body text + Google Maps coords
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

// ── Helpers ────────────────────────────────────────────────

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

// Parse date ranges like "20 Mar - 20 May, 2026" into { date, endDate } ISO strings
function parseDateRange(text) {
  if (!text) return { date: '', endDate: '' };
  const m = text.match(/(\d{1,2})\s+(\w{3,9})\s*[\-\u2013]\s*(\d{1,2})\s+(\w{3,9}),?\s*(\d{4})/);
  if (m) {
    const s = new Date(m[1]+' '+m[2]+' '+m[5]);
    const e = new Date(m[3]+' '+m[4]+' '+m[5]);
    if (!isNaN(s) && !isNaN(e)) return { date: s.toISOString().split('T')[0], endDate: e.toISOString().split('T')[0] };
  }
  return { date: normalizeDate(text), endDate: '' };
}

// Normalize HK-AGA uppercase district names to CULTIVE standard
const DISTRICT_MAP = {
  'SOUTHERN': 'Wong Chuk Hang',
  'SAI WAN (WESTERN)': 'Kennedy Town',
  'KWAI TSING': 'Kwai Chung',
  'CENTRAL AND WESTERN': 'Central',
  'WAN CHAI': 'Wan Chai',
  'EASTERN': 'North Point',
  'KOWLOON CITY': 'Kowloon',
  'YAU TSIM MONG': 'Tsim Sha Tsui',
  'SHAM SHUI PO': 'Sham Shui Po',
  'SHA TIN': 'Sha Tin',
  'TAI PO': 'Tai Po',
  'TUEN MUN': 'Tuen Mun',
  'YUEN LONG': 'Yuen Long',
  'TSUEN WAN': 'Tsuen Wan',
  'SAI KUNG': 'Sai Kung',
  'ISLANDS': 'Lantau',
  'NORTH': 'North Point',
  'KWUN TONG': 'Kwun Tong',
  'WONG TAI SIN': 'Wong Tai Sin',
};

// v2.7: Address-first detection — real exhibition address beats card district label
function normalizeDistrict(raw, address) {
  // Priority 1: detect from actual address (most accurate for off-site exhibitions)
  const fromAddr = detectDistrict(address || '');
  if (fromAddr) return fromAddr;
  // Priority 2: map HK-AGA uppercase district labels
  const upper = (raw || '').trim().toUpperCase();
  if (DISTRICT_MAP[upper]) return DISTRICT_MAP[upper];
  // Priority 3: detect from raw district text
  const fromRaw = detectDistrict(raw || '');
  if (fromRaw) return fromRaw;
  return raw || '';
}

// Boilerplate markers used in both cleanDescription and extractArtists
const BOILERPLATE_MARKERS = [
  'Founded in 2012, the Hong Kong Art Gallery Association',
  'Founded in 2012',
  '/* real people should not fill this in',
  'TEMPUS FUGIT',
];

// v2.7: Strip HK-AGA boilerplate — idx >= 0 catches boilerplate-only descriptions
function cleanDescription(desc) {
  if (!desc) return '';
  let clean = desc;
  for (const marker of BOILERPLATE_MARKERS) {
    const idx = clean.indexOf(marker);
    if (idx >= 0) clean = clean.slice(0, idx);
  }
  clean = clean.replace(/\s+/g, ' ').trim();
  // If after stripping we have less than 20 chars, it's basically empty
  return clean.length >= 20 ? clean : '';
}

// v2.7: Extract artist names — strip boilerplate first, multiple patterns
function extractArtists(desc) {
  if (!desc) return [];
  // Strip boilerplate before parsing to prevent contamination
  let clean = desc;
  for (const marker of BOILERPLATE_MARKERS) {
    const idx = clean.indexOf(marker);
    if (idx >= 0) clean = clean.slice(0, idx);
  }
  clean = clean.trim();
  if (!clean) return [];

  const artists = [];

  // Pattern 1: "Artists presented:" or "Artists:" or "Artists include:"
  const presented = clean.match(/[Aa]rtists?\s*(?:presented|featured|include|exhibiting)?\s*:\s*([^.\n]+)/);
  if (presented) {
    const names = presented[1].split(/,|\band\b/).map(n => n.trim()).filter(n => n.length > 2 && n.length < 60);
    artists.push(...names);
  }

  // Pattern 2: "solo exhibition of [Name]" or "[Name]'s solo show/exhibition"
  if (artists.length === 0) {
    const solo1 = clean.match(/solo\s+(?:exhibition|show)\s+(?:of|by)\s+([A-Z][\w\s,.-]+?)(?:[,.]|\s+(?:featuring|curated|at|in|from))/i);
    if (solo1) artists.push(solo1[1].trim());
    const solo2 = clean.match(/present(?:s|ed)?\s+(?:the\s+)?solo\s+(?:exhibition|show)\s+of\s+([A-Z][\w\s,.-]+?)(?:[,.]|\s*[\u201c"])/i);
    if (solo2 && artists.length === 0) artists.push(solo2[1].trim());
  }

  // Pattern 3: "presents [Name]'s" or "present [Name],"
  if (artists.length === 0) {
    const presents = clean.match(/present(?:s|ed)?\s+(?:a\s+)?(?:two-person\s+exhibition\s+bringing\s+together\s+)?([A-Z][A-Za-z\s.-]+?)(?:'s|,\s*(?:a|an|the|featuring|curated))/);
    if (presents) artists.push(presents[1].trim());
  }

  // Filter: remove any names that still contain boilerplate fragments or are too generic
  const filtered = artists.filter(a =>
    a.length > 2 && a.length < 60 &&
    !a.includes('Founded') && !a.includes('Hong Kong Art Gallery') &&
    !a.includes('real people') && !a.includes('TEMPUS') &&
    !/^(the|a|an|this|that|our|their)\s/i.test(a)
  );

  return filtered;
}

const HK_DISTRICTS = [
  'Central','Wan Chai','Causeway Bay','Admiralty','Sheung Wan',
  'Tsim Sha Tsui','Mong Kok','Jordan','Sham Shui Po','Kowloon',
  'Kwun Tong','Wong Tai Sin','Sha Tin','Tai Po','Tuen Mun',
  'Yuen Long','Tsuen Wan','Sai Kung','Lantau','Aberdeen',
  'Stanley','Kennedy Town','Sai Ying Pun','North Point',
  'Quarry Bay','Tai Koo','Chai Wan','West Kowloon',
  'Wong Chuk Hang','Hung Hom','Diamond Hill','Tai Kwun','PMQ',
  'Tin Wan','Ap Lei Chau','Repulse Bay','Happy Valley',
];

function detectDistrict(text) {
  const lower = text.toLowerCase();
  return HK_DISTRICTS.find(d => lower.includes(d.toLowerCase())) || '';
}

const CATEGORY_KEYWORDS = {
  music: ['concert','music','dj','live band','gig','jazz','orchestra'],
  art: ['art','exhibition','gallery','museum','sculpture','painting'],
  'food-and-drink': ['food','dining','brunch','cocktail','wine','beer','tasting'],
  nightlife: ['club','nightclub','party','nightlife','rave'],
  wellness: ['yoga','meditation','wellness','fitness','sound bath'],
  film: ['film','movie','cinema','screening'],
  theatre: ['theatre','theater','performance','dance','ballet','comedy'],
  sports: ['sport','run','marathon','hike','hiking','cycling'],
  markets: ['market','flea','bazaar','fair','craft'],
  community: ['community','charity','volunteer','meetup','workshop'],
};

function detectCategory(text) {
  const lower = text.toLowerCase();
  for (const [cat, kws] of Object.entries(CATEGORY_KEYWORDS))
    if (kws.some(k => lower.includes(k))) return cat;
  return 'events';
}

// ── CULTIVE Field Mapping ────────────────────────────────────

function mapToCultiveSchema(raw, url) {
  const r = raw || {};
  const title = r.title || r.name || r.eventTitle || '';
  const dateRaw = r.date || r.startDate || r.eventDate || '';
  const dates = parseDateRange(dateRaw);
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
  const result = {
    title, date: dates.date || normalizeDate(dateRaw), time, venueName, description,
    image: resolveUrl(image, url), price, address,
    lat: lat ? Number(lat) : null, lng: lng ? Number(lng) : null,
    artists,
    district: r.district || detectDistrict(fullText),
    category: r.category || detectCategory(fullText),
    sourceUrl: r.sourceUrl || r.url || url,
    scrapedFrom: url, scrapedAt: new Date().toISOString(),
  };
  if (dates.endDate) result.endDate = dates.endDate;
  return result;
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

  let date = $('time').first().attr('datetime') || '';
  if (!date) {
    const bodyText = $('body').text();
    const dateP = bodyText.match(/((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,\s+\w+\s+\d{1,2}(?:,\s*\d{4})?)/);
    if (dateP) date = dateP[1];
  }

  let venueName = '';
  const venueEl = $('[class*="location-info"]').first();
  if (venueEl.length) {
    venueName = venueEl.find('p').first().text().trim()
      || venueEl.find('strong').first().text().trim() || '';
    venueName = venueName.split(/(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?/)[0].trim();
    venueName = venueName.replace(/,?\s*(?:HKI|KOW|NT|NTW)\s*$/, '').trim();
  }

  let address = '';
  if (venueEl.length) {
    const pEls = venueEl.find('p');
    if (pEls.length > 1) address = $(pEls[1]).text().trim();
    if (!address) address = venueEl.text().trim();
    address = address.split(/(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?/)[0].trim();
    if (venueName && address.startsWith(venueName)) address = address.slice(venueName.length).trim();
  }

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

    const rawText = $(el).text();
    const lines = rawText.split('\n').map(l => l.trim()).filter(l => l.length > 0);
    if (lines.length < 2) return;

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

// v2.7: Inject spaces for <br> and block tags before text extraction
function spacedText($, el) {
  const html = $(el).html() || '';
  // Replace <br>, </p>, </div>, </li>, </h1-6> with space before extracting text
  const spaced = html.replace(/<br\s*\/?>/gi, ' ')
    .replace(/<\/(p|div|li|h[1-6]|span|td|th|blockquote)>/gi, ' ')
    .replace(/<[^>]+>/g, ' ');
  return spaced.replace(/\s+/g, ' ').trim();
}

// Mine HK-AGA detail pages for description, address, coords
function extractWixData($, url) {
  const result = { description: '', address: '', lat: null, lng: null };
  const body = $('body').text() || '';

  // ── Address ──
  const addrMatch = body.match(/Address[:\s]+([^\n]+)/i);
  if (addrMatch) {
    let addr = addrMatch[1].trim();
    addr = addr.split(/\s*(?:Phone|Email|Website|Tel)[:\s]/i)[0].trim();
    if (/\d/.test(addr) && (addr.length < 200)) {
      result.address = addr;
    }
  }

  // ── Description: Strategy A ── v2.7: use spacedText for proper word breaks
  const descParts = [];
  $('p, div, span').each((i, el) => {
    const text = spacedText($, $(el).clone().children().remove().end());
    if (text.length < 80) return;
    if (/^Address|^Phone|^Email|^Website|^Click here|FILTER|CURRENTLY|UPCOMING/i.test(text)) return;
    if (/^\d{1,2}\s+\w{3,9}\s*[\-\u2013]/.test(text)) return;
    if (text === text.toUpperCase() && text.length < 50) return;
    descParts.push(text);
  });

  if (descParts.length > 0) {
    descParts.sort((a, b) => b.length - a.length);
    const unique = descParts.filter((p, i) => !descParts.slice(0, i).some(longer => longer.includes(p)));
    result.description = unique.join(' ').replace(/\s+/g, ' ').trim();
  }

  // Strategy B: fallback to body text between header and "Click here"/"Address:"
  if (!result.description) {
    const clickIdx = body.indexOf('Click here');
    const addrIdx = body.indexOf('Address');
    const endIdx = clickIdx > 0 ? clickIdx : (addrIdx > 0 ? addrIdx : body.length);
    const chunk = body.slice(Math.min(150, endIdx), endIdx).trim();
    if (chunk.length > 80) {
      result.description = chunk.replace(/\s+/g, ' ').trim();
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
    const coordMatch = src.match(/(?:q=|@|ll=|center=)([0-9.]+)[,]([0-9.]+)/);
    if (coordMatch && !result.lat) {
      result.lat = Number(coordMatch[1]);
      result.lng = Number(coordMatch[2]);
    }
  });

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
        const cards = extractHkAgaCards($, url);
        log.info('Found ' + cards.length + ' HK-AGA exhibition cards');

        if (cards.length > 0) {
          await crawler.addRequests(cards.map(c => ({
            url: c.url,
            userData: { hkAga: c },
          })));
          log.info('Enqueued ' + cards.length + ' HK-AGA detail pages for descriptions');
        }
        return;
      } else {
        // Detail page: merge card data with Wix embedded data
        const card = request.userData?.hkAga || {};
        const wix = extractWixData($, url);
        const desc = cleanDescription(wix.description);
        const artists = extractArtists(wix.description);
        const district = normalizeDistrict(card.district || '', wix.address || '');
        log.info('HK-AGA detail: desc=' + desc.length + ' chars, artists=' + artists.length + ', addr=' + (wix.address || 'none'));

        events = [mapToCultiveSchema({
          title: card.title || '', date: card.date || '',
          venueName: card.venue || '',
          district: district,
          image: card.image || '',
          description: desc, address: wix.address,
          lat: wix.lat, lng: wix.lng,
          artists: artists,
          price: 'Free',
          category: 'art',
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
