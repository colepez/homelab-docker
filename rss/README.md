FreshRSS. If exposed publicly through a reverse proxy/tunnel, put an
access-control layer (e.g. Cloudflare Access or similar) in front of it
first - FreshRSS's own login is the only other thing standing between it
and the internet.

## First-time setup

1. Visit the site and complete FreshRSS's install wizard - choose SQLite as
   the database, set an admin username/password
2. Settings -> Subscription management -> Import/export -> import
   `feeds.opml` for a pre-organized starting set (Industry News,
   Competitors)
