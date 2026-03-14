# ── Apify Actor Dockerfile ─────────────────────────────────────
# Based on: https://console.apify.com/actors/templates/js-crawlee-cheerio

FROM apify/actor-node:18

COPY package*.json ./

RUN npm install --omit=dev --omit=optional \
    && echo "Installed NPM packages:" \
    && (npm ls --omit=dev --all || true) \
    && echo "Node.js version:" \
    && node --version \
    && echo "NPM version:" \
    && npm --version

COPY . ./

CMD npm start
