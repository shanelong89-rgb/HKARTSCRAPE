// CULTIVE HK Events Apify Actor v2.3
// v2.3: HK-AGA is a Wix SPA — extract titles from listing cards,
//       pass via userData to detail pages for gallery info merge.
//       Fixed Eventbrite venue/address concatenation.
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

// ── Helpers ─────────────────────────────────────────���────────

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

// Extract exhibition links WITH their surrounding card context from listing page
function extractHkAgaLinksWithContext($, url) {
  const results = [];
  const seen = new Set();
  $('a[href]').each((i, el) => {
    const href = $(el).attr('href');
    if (!href) return;
    const full = resolveUrl(href, url);
    if (!/hk-aga\.org\/exhibitions\/\d+/.test(full)) return;
    if (seen.has(full)) return;
    seen.add(full);

    // The link text itself might be the exhibition title
    let title = $(el).text().trim();

    // Walk up to find parent card container for more context
    let $card = $(el).closest('[class*="exhibition"], article, .card, .item, .col, div').first();
    if (!$card.length) $card = $(el).parent().parent();
    const cardText = $card.length ? $card.text().trim() : '';

    // If link text is too short, try the card headings
    if (!title || title.length < 3) {
      $card.find('h2,h3,h4,strong,[class*="title"]').each((j, h) => {
        const t = $(h).text().trim();
        if (t && t.length >= 3 && t.length < 150 && !title) title = t;
      });
    }

    // Extract date from card context
    const dateMatch = cardText.match(/(\d{1,2}\s+\w{3,9}\s*[-\u2013]\s*\d{1,2}\s+\w{3,9},?\s*\d{4})/)
      || cardText.match(/(\d{1,2}\s+\w{3,9}\s*[-\u2013]\s*\d{1,2}\s+\w{3,9},?\s*\d{4})/);

    // Extract venue from card context (often a line below the date)
    let venue = '';
    $card.find('span,div,p,small').each((j, e2) => {
      const t = $(e2).text().trim();
      // Look for gallery names — typically mixed case, not too long, not a date
      if (t && t.length > 3 && t.length < 60 && !venue
        && !/\d{1,2}\s+\w{3,9}/.test(t) && t !== title
        && !/^(CURRENTLY|UPCOMING|PAST|FILTER)/.test(t)) {
        venue = t;
      }
    });

    // Extract district from uppercase text in card
    let district = '';
    $card.find('span,div,p,small').each((j, e2) => {
      const t = $(e2).text().trim();
      if (t && t.length > 2 && t.length < 40 && t === t.toUpperCase()
        && !/^(CURRENTLY|UPCOMING|PAST|FILTER|EXHIBITION)/.test(t) && !district) {
        district = t;
      }
    });

    // Extract image from card
    const image = $card.find('img').first().attr('src') || '';

    results.push({
      url: full, title: title || '',
      date: dateMatch ? dateMatch[1] : '',
      venue, district, image: image ? resolveUrl(image, url) : '',
    });
  });
  return results;
}

function extractHkAgaDetail($, url, userData) {
  // HK-AGA is a Wix SPA — exhibition content is JS-rendered.
  // The static HTML only contains the gallery info block.
  // We rely on userData.hkAgaTitle from the listing page.

  const JUNK = ['join','mailing list','subscribe','newsletter','login','sign up','contact','cookie','exhibitions'];
  function isJunk(t) { const l = t.toLowerCase(); return JUNK.some(j => l === j || l.startsWith(j + ' ')) || t.length > 200; }

  // Title: prefer userData from listing page, then try static HTML sources
  let title = (userData && userData.hkAgaTitle) || '';
  if (!title || isJunk(title)) {
    // Try og:title but strip generic suffixes
    title = ($('meta[property="og:title"]').attr('content') || '').replace(/\s*[|\-]\s*HK-?AGA.*/i, '').trim();
  }
  if (!title || isJunk(title)) {
    // Try heading elements
    $('h1,h2,h3').each((i, el) => {
      if (title && !isJunk(title)) return;
      const t = $(el).text().trim();
      if (t && t.length >= 3 && t.length < 200 && !isJunk(t)) title = t;
    });
  }
  if (!title || title.length < 3 || isJunk(title)) return [];

  const body = $('body').text();

  // Date from userData or static HTML
  let date = (userData && userData.hkAgaDate) || '';
  if (!date) {
    const dateMatch = body.match(/(\d{1,2}\s+\w{3,9}\s*[-\u2013]\s*\d{1,2}\s+\w{3,9},?\s*\d{4})/)
      || body.match(/until\s+(\d{1,2}\s+\w{3,9},?\s*\d{4})/i);
    if (dateMatch) date = dateMatch[1];
  }

  // Gallery/venue name — look for it in the static info block
  let venueName = (userData && userData.hkAgaVenue) || '';
  if (!venueName) {
    // HK-AGA detail pages have a gallery section with the name
    $('h2,h3,h4,strong').each((i, el) => {
      const t = $(el).text().trim();
      if (t && t.length > 2 && t.length < 60 && t !== title && !venueName
        && !/exhibitions|filter|currently|upcoming/i.test(t)) {
        venueName = t;
      }
    });
  }

  // Address from static gallery info block
  const addrMatch = body.match(/Address:\s*([^\n]*?)(?=Phone:|Email:|Website:|$)/i);
  let address = addrMatch ? addrMatch[1].trim().slice(0, 120) : '';

  // Description — try og:description, then any paragraph text
  let desc = $('meta[property="og:description"]').attr('content') || '';
  if (!desc || desc.length < 20) {
    $('p').each((i, el) => {
      if (desc.length >= 400) return;
      const t = $(el).text().trim();
      if (t.length > 40 && !/Address:|Phone:|Email:|Website:/i.test(t) && t !== title) {
        desc += (desc ? ' ' : '') + t;
      }
    });
  }

  // Image from userData or og:image (skip btns.svg and other UI assets)
  let image = (userData && userData.hkAgaImage) || '';
  if (!image) {
    const ogImg = $('meta[property="og:image"]').attr('content') || '';
    if (ogImg && !ogImg.includes('btns.svg') && !ogImg.includes('logo') && !ogImg.includes('icon')) {
      image = ogImg;
    }
  }
  if (!image) {
    $('img').each((i, el) => {
      if (image) return;
      const src = $(el).attr('src') || '';
      if (src && !src.includes('btns.svg') && !src.includes('logo') && !src.includes('icon')
        && !src.includes('avatar') && !src.includes('sprite')) {
        image = resolveUrl(src, url);
      }
    });
  }

  const district = (userData && userData.hkAgaDistrict) || detectDistrict(address + ' ' + venueName);

  return [mapToCultiveSchema({
    title, date, venueName,
    description: desc.slice(0, 500),
    image, address,
    district, category: 'Art', modes: ['Indoor'], sourceUrl: url,
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
        // Extract links with card context from listing page
        const linksWithContext = extractHkAgaLinksWithContext($, url);
        log.info('Found ' + linksWithContext.length + ' HK-AGA exhibition links');

        if (linksWithContext.length > 0) {
          // Enqueue detail pages with card context as userData
          await crawler.addRequests(linksWithContext.map(ctx => ({
            url: ctx.url,
            userData: {
              hkAgaTitle: ctx.title,
              hkAgaDate: ctx.date,
              hkAgaVenue: ctx.venue,
              hkAgaDistrict: ctx.district,
              hkAgaImage: ctx.image,
            },
          })));
          log.info('Enqueued ' + linksWithContext.length + ' HK-AGA detail pages with context');

          // Also save card-only events for any cards that have enough data
          for (const ctx of linksWithContext) {
            if (ctx.title && ctx.title.length >= 3) {
              events.push(mapToCultiveSchema({
                title: ctx.title, date: ctx.date,
                venueName: ctx.venue, district: ctx.district,
                image: ctx.image, category: 'Art', modes: ['Indoor'],
                sourceUrl: ctx.url,
              }, url));
            }
          }
          if (events.length > 0) {
            log.info('Extracted ' + events.length + ' events from listing cards');
          }
        }
      } else if (/\/exhibitions\/\d+/.test(url)) {
        // Detail page — merge userData from listing with static HTML data
        events = extractHkAgaDetail($, url, request.userData || {});
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
