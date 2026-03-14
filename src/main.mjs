// CULTIVE HK Events Apify Actor v2.3
// v2.3: Fixed HK-AGA detail extraction (og:title, Wix selectors, junk filter)
//       Fixed Eventbrite venue/address concatenation
// v2.2: Removed Time Out, focused on Eventbrite HK + HK-AGA
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
  maxRequestsPerCrawl = 80,
} = (await Actor.getInput()) ?? {};

const proxyConfig = await Actor.createProxyConfiguration({ checkAccess: true });
const seen = new Set();

// ── Helpers ──────────────────────────────────────────────────

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
  const description = (r.description || r.text || r.content || r.summary || '').slice(0, 500);
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

function extractHkAgaLinks($, url) {
  const links = new Set();
  $('a[href]').each((i, el) => {
    const href = $(el).attr('href');
    if (!href) return;
    const full = resolveUrl(href, url);
    if (/hk-aga\.org\/exhibitions\/\d+/.test(full)) {
      links.add(full);
    }
  });
  return [...links];
}

function extractHkAgaCards($, url) {
  const events = [];
  const selectors = ['[class*="exhibition"]','article','.card','.item','.grid-item','.col'];
  for (const sel of selectors) {
    const cards = $(sel);
    if (cards.length < 2) continue;
    cards.each((i, el) => {
      const $c = $(el), text = $c.text();
      let title = '';
      $c.find('h2,h3,h4,strong,[class*="title"]').each((j, h) => {
        const t = $(h).text().trim();
        if (t && t.length > 2 && t !== t.toUpperCase() && !title) title = t;
      });
      if (!title) title = $c.find('h2,h3,h4').first().text().trim();
      if (!title || title.length < 3) return;
      let district = '';
      $c.find('span,div,p').each((j, e2) => {
        const t = $(e2).text().trim();
        if (t && t.length > 2 && t.length < 40 && t === t.toUpperCase() && !district) district = t;
      });
      const dateMatch = text.match(/(\d{1,2}\s+\w{3,9}\s*[-]\s*\d{1,2}\s+\w{3,9},?\s*\d{4})/);
      const image = $c.find('img').first().attr('src') || '';
      const link = $c.find('a').first().attr('href') || '';
      events.push(mapToCultiveSchema({
        title, date: dateMatch ? dateMatch[1] : '', image,
        district: detectDistrict(district) || district,
        category: 'Art', modes: ['Indoor'],
        sourceUrl: link ? resolveUrl(link, url) : url,
      }, url));
    });
    if (events.length > 0) break;
  }
  return events;
}

function extractHkAgaDetail($, url) {
  // HK-AGA uses Wix — titles may be in og:title, h1, h2, h3, or Wix elements
  const JUNK = ['join','mailing list','subscribe','newsletter','login','sign up','contact','cookie'];
  function isJunk(t) { const l = t.toLowerCase(); return JUNK.some(j => l.includes(j)) || t.length > 200; }

  // Try og:title first (most reliable on Wix sites), strip site suffix
  let title = ($('meta[property="og:title"]').attr('content') || '').replace(/\s*[|\-]\s*HK-?AGA.*/i, '').trim();
  // Fallback to heading elements and Wix-specific selectors
  if (!title || title.length < 3 || isJunk(title)) {
    const headings = ['h1','h2','h3','[data-testid="richTextElement"]','.font_2','.font_3','[class*="title"]'];
    for (const sel of headings) {
      $(sel).each((i, el) => {
        if (title && !isJunk(title)) return;
        const t = $(el).text().trim();
        if (t && t.length >= 3 && t.length < 200 && !isJunk(t)) title = t;
      });
      if (title && !isJunk(title)) break;
    }
  }
  if (!title || title.length < 3 || isJunk(title)) return [];

  const body = $('body').text();
  // Date patterns: "1 Mar - 15 Apr, 2026" or "until 30 April 2026" or "March 15, 2026"
  const dateMatch = body.match(/(\d{1,2}\s+\w{3,9}\s*[-\u2013]\s*\d{1,2}\s+\w{3,9},?\s*\d{4})/)
    || body.match(/until\s+(\d{1,2}\s+\w{3,9},?\s*\d{4})/i)
    || body.match(/(\w{3,9}\s+\d{1,2},?\s*\d{4})/);

  // Description: try multiple container selectors (Wix uses divs, not article/main)
  let desc = '';
  const descSels = ['article p','main p','.content p','[data-testid="richTextElement"] p','div[class*="text"] p','section p','p'];
  for (const sel of descSels) {
    $(sel).each((i, el) => {
      if (desc.length >= 500) return;
      const t = $(el).text().trim();
      if (t.length > 30 && !isJunk(t) && t !== title) desc += (desc ? ' ' : '') + t;
    });
    if (desc.length > 50) break;
  }
  if (!desc) desc = $('meta[property="og:description"]').attr('content') || '';

  // Image: og:image first, then content images (skip logos/icons)
  let image = $('meta[property="og:image"]').attr('content') || '';
  if (!image) {
    $('article img, main img, section img, [class*="gallery"] img, img[src*="wix"], img').each((i, el) => {
      if (image) return;
      const src = $(el).attr('src') || '';
      if (src && !src.includes('logo') && !src.includes('icon') && !src.includes('avatar')) image = src;
    });
  }

  // Address and venue
  const addrMatch = body.match(/(?:Address|Location)\s*:?\s*([^\n]{10,80})/i);
  let venue = '';
  $('[class*="gallery"],[class*="venue"],[class*="location"]').each((i, el) => {
    if (!venue) { const t = $(el).text().trim(); if (t.length > 2 && t.length < 60) venue = t; }
  });

  return [mapToCultiveSchema({
    title,
    date: $('time').first().attr('datetime') || (dateMatch ? dateMatch[1] : ''),
    venueName: venue,
    description: desc.slice(0, 500),
    image,
    address: addrMatch ? addrMatch[1].trim() : '',
    category: 'Art', modes: ['Indoor'], sourceUrl: url,
  }, url)];
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
        const links = extractHkAgaLinks($, url);
        if (links.length > 0) {
          await crawler.addRequests(links.map(u => ({ url: u })));
          log.info('Enqueued ' + links.length + ' HK-AGA detail pages');
        }
        const cards = extractHkAgaCards($, url);
        if (cards.length > 0) events.push(...cards);
      } else if (/\/exhibitions\/\d+/.test(url)) {
        events = extractHkAgaDetail($, url);
      }
      // else: skip non-exhibition HK-AGA pages
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
