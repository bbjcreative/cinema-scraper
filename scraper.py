# Version: 9.5 (Truncation Fix)
# -*- coding: utf-8 -*-
"""
This script uses a lightweight, API-driven approach. This version re-introduces
the truncation logic to prevent the Google Sheets 50,000 character cell limit
error, ensuring the script can always save its data.
"""

import httpx
import gspread
import json
import os
import re
from datetime import datetime, timedelta
import asyncio
from bs4 import BeautifulSoup
import traceback

# --- FIX: Define Absolute Paths for Cron Job Reliability ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Configuration ---
GOOGLE_SHEETS_CREDENTIALS = os.path.join(SCRIPT_DIR, 'cred', 'eighth-jigsaw-464808-s4-370e6105ac3f.json')
GOOGLE_SHEET_NAME = 'My Scraped Data Sheet'
MASTER_SHEET_NAME = 'MasterMovieDatabase'
BASE_IMAGE_DIR = os.path.join(SCRIPT_DIR, 'downloaded_posters')

# --- Scrape Settings (Optimized for API-like requests) ---
MAX_MOVIES_TO_SCRAPE = None # Set to None to scrape all movies
MAX_DAYS_TO_SCRAPE = 5      # Scrape a full week
REQUEST_DELAY = 2           # Seconds to wait between most requests
SHEETS_CELL_CHAR_LIMIT = 49900 # Google Sheets limit is 50k, be safe

# --- URL & Headers ---
MOVIES_NOWSHOWING_URL = 'https://www.cinema.com.my/movies/nowshowing.aspx'
BASE_URL = 'https://www.cinema.com.my'
# Mimic a real browser's headers to avoid being blocked
HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.cinema.com.my/'
}

def get_malaysian_time() -> datetime:
    """Returns the current time in Malaysian timezone (GMT+8)."""
    return datetime.utcnow() + timedelta(hours=8)

async def download_image(client, image_url: str, save_dir: str) -> str | None:
    """Downloads an image asynchronously using the shared httpx client."""
    if not image_url or image_url == 'N/A': return None
    filename = os.path.basename(image_url)
    os.makedirs(save_dir, exist_ok=True)
    full_save_path = os.path.join(save_dir, filename)
    try:
        response = await client.get(image_url, timeout=30.0)
        response.raise_for_status()
        with open(full_save_path, 'wb') as f:
            f.write(response.content)
        return full_save_path
    except Exception as e:
        print(f"        Error downloading image {image_url}: {e}")
    return None

def get_master_worksheet(spreadsheet: gspread.Spreadsheet) -> gspread.Worksheet:
    try:
        worksheet = spreadsheet.worksheet(MASTER_SHEET_NAME)
        print(f"Found master worksheet: '{MASTER_SHEET_NAME}'")
        return worksheet
    except gspread.WorksheetNotFound:
        print(f"Master worksheet '{MASTER_SHEET_NAME}' not found. Creating it.")
        worksheet = spreadsheet.add_worksheet(title=MASTER_SHEET_NAME, rows=1, cols=17)
        headers = ['Movie Title', 'Movie URL', 'Description', 'Running Time (Minutes)', 'Release Date (YYYY-MM-DD)', 'Language', 'Genre', 'Distributor', 'Classification', 'Cast', 'Director', 'Format', 'Cinema Count', 'Poster URL', 'Local Poster Path', 'Aggregated Showtimes Data', 'Scrape Date']
        worksheet.append_row(headers)
        return worksheet

def read_master_sheet(worksheet: gspread.Worksheet) -> dict[str, dict]:
    print("Reading data from master sheet...")
    try:
        records = worksheet.get_all_records()
        return {record['Movie Title']: record for record in records}
    except Exception as e:
        print(f"Could not read master sheet, assuming it's empty. Error: {e}")
        return {}

def merge_data(existing_data: dict[str, dict], fresh_data: list[dict]) -> list[dict]:
    print("Merging fresh data with existing records...")
    updated_data = existing_data.copy()
    new_movies_count = 0
    updated_movies_count = 0
    for movie in fresh_data:
        title = movie['Movie Title']
        if title in updated_data:
            print(f"  Updating existing movie: {title}")
            for key, value in movie.items():
                if value and value != 'N/A': updated_data[title][key] = value
            updated_data[title]['Scrape Date'] = movie['Scrape Date']
            updated_movies_count += 1
        else:
            print(f"  Adding new movie: {title}")
            updated_data[title] = movie
            new_movies_count += 1
    print(f"Merge complete. Updated: {updated_movies_count}, New: {new_movies_count}")
    return sorted(updated_data.values(), key=lambda x: x.get('Movie Title', ''))

def update_master_sheet(worksheet: gspread.Worksheet, data: list[dict]):
    if not data:
        print("No data to write to master sheet.")
        return
    print(f"Updating master worksheet with {len(data)} records...")
    try:
        worksheet.clear()
        headers = ['Movie Title', 'Movie URL', 'Description', 'Running Time (Minutes)', 'Release Date (YYYY-MM-DD)', 'Language', 'Genre', 'Distributor', 'Classification', 'Cast', 'Director', 'Format', 'Cinema Count', 'Poster URL', 'Local Poster Path', 'Aggregated Showtimes Data', 'Scrape Date']
        rows_to_write = [headers] + [[str(d.get(h, '')) for h in headers] for d in data]
        worksheet.update(rows_to_write, value_input_option='USER_ENTERED')
        print("Master worksheet successfully updated.")
    except Exception as e:
        print(f"An error occurred while updating the master sheet: {e}")

def parse_showtimes_from_html(soup: BeautifulSoup) -> dict:
    """Parses the cinema and showtime data from a given HTML soup object."""
    showtimes = {}
    cinema_divs = soup.select('#ShowtimesList > a, #ShowtimesList > div')
    current_cinema = 'N/A'
    for element in cinema_divs:
        if element.name == 'a':
            b_tag = element.find('b')
            if b_tag:
                current_cinema = b_tag.text.strip()
                if current_cinema not in showtimes:
                    showtimes[current_cinema] = []
        elif element.name == 'div':
            times = [t.text.strip() for t in element.select('div.showbox a, div.showbox') if t.text.strip()]
            if times and current_cinema != 'N/A':
                showtimes[current_cinema].extend(times)
    # Remove duplicates
    for cinema in showtimes:
        showtimes[cinema] = sorted(list(set(showtimes[cinema])))
    return showtimes

async def scrape_aggregated_showtimes(client: httpx.AsyncClient, showtimes_url: str) -> str:
    """
    Scrapes showtimes by reverse-engineering the ASP.NET form submissions
    for each date, avoiding the need for a full browser.
    """
    print(f"          Fetching showtimes from: {showtimes_url}")
    all_dates_data = {}
    try:
        # 1. Initial GET request to get the first page and form data
        response = await client.get(showtimes_url, headers=HTTP_HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        # 2. Extract available dates and essential ASP.NET form fields
        date_dropdown = soup.select_one('#ctl00_cphContent_ctl00_ddlShowdate')
        if not date_dropdown: return 'N/A'
        
        date_options = [(opt.get('value'), opt.text) for opt in date_dropdown.find_all('option') if opt.get('value')]
        date_options = date_options[:MAX_DAYS_TO_SCRAPE] if MAX_DAYS_TO_SCRAPE else date_options

        viewstate = soup.select_one("#__VIEWSTATE").get("value") if soup.select_one("#__VIEWSTATE") else ""
        eventvalidation = soup.select_one("#__EVENTVALIDATION").get("value") if soup.select_one("#__EVENTVALIDATION") else ""

        # 3. Process the first date (already loaded)
        if not date_options: return 'N/A'
        print(f"            Processing Date: {date_options[0][1]}")
        first_date_showtimes = parse_showtimes_from_html(soup)
        all_dates_data[date_options[0][1]] = first_date_showtimes

        # 4. Loop through remaining dates, making POST requests
        for i, (date_val, date_txt) in enumerate(date_options[1:]):
            print(f"            Processing Date: {date_txt}")
            await asyncio.sleep(REQUEST_DELAY) # Be respectful to the server

            form_data = {
                '__EVENTTARGET': 'ctl00$cphContent$ctl00$ddlShowdate',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': viewstate,
                '__EVENTVALIDATION': eventvalidation,
                'ctl00$cphContent$ctl00$ddlShowdate': date_val,
            }
            
            post_response = await client.post(showtimes_url, data=form_data, headers=HTTP_HEADERS)
            
            if post_response.status_code != 200:
                print(f"            Request failed with status {post_response.status_code}. Stopping showtime scrape for this movie.")
                break

            post_soup = BeautifulSoup(post_response.text, 'lxml')
            
            date_showtimes = parse_showtimes_from_html(post_soup)
            all_dates_data[date_txt] = date_showtimes

            new_viewstate = post_soup.select_one("#__VIEWSTATE")
            if new_viewstate: viewstate = new_viewstate.get("value")
            new_eventvalidation = post_soup.select_one("#__EVENTVALIDATION")
            if new_eventvalidation: eventvalidation = new_eventvalidation.get("value")

        # 5. Restructure the data into the final JSON format
        final_json = []
        cinema_map = {}
        for date_str, cinemas in all_dates_data.items():
            for cinema_name, times in cinemas.items():
                if cinema_name not in cinema_map:
                    cinema_map[cinema_name] = {"cinemaName": cinema_name, "showings": []}
                cinema_map[cinema_name]["showings"].append({"date": date_str, "times": times})
        
        final_json = list(cinema_map.values())
        return json.dumps(final_json, separators=(',', ':'))

    except Exception as e:
        print(f"          An error occurred in scrape_aggregated_showtimes: {e}")
        return 'N/A'

async def main_scraper():
    """Main function to run the lightweight scraper."""
    print(f"Starting web scraping script v9.5 (Truncation Fix) at {get_malaysian_time()}...")
    
    # Setup Google Sheets
    gc = gspread.service_account(filename=GOOGLE_SHEETS_CREDENTIALS)
    spreadsheet = gc.open(GOOGLE_SHEET_NAME)
    master_worksheet = get_master_worksheet(spreadsheet)
    existing_movie_data = read_master_sheet(master_worksheet)
    
    scraped_records = []
    
    async with httpx.AsyncClient(follow_redirects=True) as client:
        # 1. Get the main movie list
        print(f"Fetching movie list from: {MOVIES_NOWSHOWING_URL}")
        main_page_response = await client.get(MOVIES_NOWSHOWING_URL, headers=HTTP_HEADERS)
        main_page_soup = BeautifulSoup(main_page_response.text, 'lxml')
        movie_listings = main_page_soup.select('div.MovieWrap')
        print(f"Found {len(movie_listings)} movie listings.")

        # 2. Loop through each movie
        for listing in movie_listings[:MAX_MOVIES_TO_SCRAPE]:
            await asyncio.sleep(REQUEST_DELAY)
            title_element = listing.select_one('.mov-lg a, .mov-sm a')
            if not title_element: continue
            
            title = title_element.text.strip()
            movie_url = f"{BASE_URL}{title_element.get('href')}"
            print(f"      Processing movie: {title}")

            # 3. Get movie details
            detail_page_response = await client.get(movie_url, headers=HTTP_HEADERS)
            detail_soup = BeautifulSoup(detail_page_response.text, 'lxml')
            
            container = detail_soup.select_one('.con-lg')
            if not container: continue
            
            description = next((node.strip() for node in container.children if isinstance(node, str) and len(node.strip()) > 50), 'N/A')
            print(f"        Extracted Description: {description[:80]}...")
            
            container_text = container.get_text(separator='\n')
            
            def extract_metadata(p, t, flags=0): return (m.group(1).strip() if (m := re.search(p, t, flags)) else 'N/A')
            
            raw_metadata = {k: extract_metadata(f"^{k}\\s*:\\s*(.+)", container_text, re.MULTILINE | re.I) for k in ['Language', 'Classification', 'Release Date', 'Genre', 'Running Time', 'Distributor', 'Cast', 'Director', 'Format']}
            
            rt_str = raw_metadata.get('Running Time', 'N/A')
            h = int(h.group(1)) * 60 if (h := re.search(r'(\d+)\s*Hours?', rt_str, re.I)) else 0
            m = int(m.group(1)) if (m := re.search(r'(\d+)\s*Minutes?', rt_str, re.I)) else 0
            total_minutes = h + m if h + m > 0 else 'N/A'
            
            formatted_date = 'N/A'
            try:
                formatted_date = datetime.strptime(raw_metadata.get('Release Date', 'N/A'), '%d %b %Y').strftime('%Y-%m-%d')
            except (ValueError, TypeError): pass

            poster_url_element = detail_soup.select_one('#ctl00_cphContent_imgPoster')
            poster_url = poster_url_element.get('src') if poster_url_element else 'N/A'
            img_dir = os.path.join(BASE_IMAGE_DIR, get_malaysian_time().strftime('%Y_%m'))
            local_poster_path = await download_image(client, poster_url, img_dir)

            # 4. Get showtimes
            showtimes_data = 'N/A'
            
            showtimes_link = None
            possible_links = detail_soup.select('#MovieSec .con-lg a')
            for link in possible_links:
                if "showtimes" in link.text.lower():
                    showtimes_link = link
                    break
            
            if showtimes_link:
                showtimes_url = f"{BASE_URL}{showtimes_link.get('href')}"
                showtimes_data = await scrape_aggregated_showtimes(client, showtimes_url)
            
            # FIX: Truncate the data if it exceeds the Google Sheets cell limit
            if len(showtimes_data) > SHEETS_CELL_CHAR_LIMIT:
                print(f"        WARNING: Showtime data for '{title}' is too long ({len(showtimes_data)} chars). Truncating.")
                showtimes_data = showtimes_data[:SHEETS_CELL_CHAR_LIMIT] + "...[TRUNCATED]"

            scraped_records.append({
                'Movie Title': title, 'Movie URL': movie_url, 'Description': description,
                'Running Time (Minutes)': total_minutes, 'Release Date (YYYY-MM-DD)': formatted_date,
                'Language': raw_metadata.get('Language', 'N/A'), 'Genre': raw_metadata.get('Genre', 'N/A'),
                'Distributor': raw_metadata.get('Distributor', 'N/A'), 'Classification': raw_metadata.get('Classification', 'N/A'),
                'Cast': raw_metadata.get('Cast', 'N/A'), 'Director': raw_metadata.get('Director', 'N/A'),
                'Format': raw_metadata.get('Format', 'N/A'), 'Cinema Count': 'N/A',
                'Poster URL': poster_url, 'Local Poster Path': local_poster_path,
                'Aggregated Showtimes Data': showtimes_data,
                'Scrape Date': get_malaysian_time().strftime('%Y-%m-%d %H:%M:%S')
            })

    # 5. Merge and update sheet
    if scraped_records:
        final_data = merge_data(existing_movie_data, scraped_records)
        update_master_sheet(master_worksheet, final_data)
    else:
        print("No fresh data was scraped. Master sheet remains unchanged.")

    print("\nScript finished.")

if __name__ == "__main__":
    try:
        print(f"--- Script invoked at {datetime.now()} ---")
        if not os.path.exists(GOOGLE_SHEETS_CREDENTIALS):
            raise FileNotFoundError(f"CRITICAL ERROR: Credentials file not found at '{GOOGLE_SHEETS_CREDENTIALS}'.")
        asyncio.run(main_scraper())
    except Exception as e:
        print(f"\nCRITICAL TOP-LEVEL ERROR: {e}")
        traceback.print_exc()

