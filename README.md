# cierne_diery_monitor
Bazos monitoring system for Cierne Diery.

## Scraping

Scraping runs only on the droplet, from `ciernediery-scraper.timer` (systemd, every 30 min,
see `deploy/`). Don't add cron or launchd jobs elsewhere: extra runs hit bazos harder and
local runs write to a database nobody reads. `main.py scrape` holds `data/scrape.lock`, so a
run started while another is still going logs "Another scrape is still running" and exits.
