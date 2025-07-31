# 🎬 Cinema Malaysia Movie Scraper

A lightweight, asynchronous Python scraper that collects movie listings, metadata, and showtimes from cinema.com.my, and stores the data in a Google Sheet. It also downloads movie posters locally.

---

## 📦 Features

- Scrapes:
  - Movie titles, descriptions, metadata (genre, cast, director, etc.)
  - Aggregated showtimes across multiple dates
  - Poster images
- Saves data to a Google Sheet (via `gspread`)
- Handles Google Sheets' 50,000-character cell limit with truncation logic
- Asynchronous HTTP requests using `httpx`
- HTML parsing with `BeautifulSoup`
- Designed for cron job compatibility

---

## 🧰 Requirements

- Python 3.8+
- Google Sheets API credentials (JSON)
- Required Python packages:
  ```bash
  pip install -r requirements.txt
  ```
