import sqlite3
import threading
import time
import schedule
import logging
import os  # ### NEW ### - For environment variables
import requests  # ### NEW ### - For Unstop API

from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

import telegram

# --- 1. CONFIGURATION ---
# ### UPDATED ###
# Tokens are now read from Render's Environment Variables
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

DB_NAME = "jobs.db"
JOBRAPIDO_URL = "https://in.jobrapido.com/Jobs-in-India?q=job"
UNSTOP_API_URL = "https://unstop.com/api/public/opportunity/search-result"  # ### NEW ###

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- 2. DATABASE SETUP ---
def setup_database():
    """Creates the SQLite database and the 'jobs' table if it doesn't exist."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_link TEXT UNIQUE,  -- UNIQUE prevents duplicate jobs
        job_title TEXT,
        company_name TEXT,
        scraped_at TIMESTAMP,
        posted_to_telegram BOOLEAN DEFAULT 0 
    )
    ''')
    conn.commit()
    conn.close()
    logging.info(f"Database '{DB_NAME}' is ready.")


# --- 3. THE SCRAPERS (PRODUCERS) ---

def scrape_jobrapido_task():
    """
    Scraper for Jobrapido. Uses Selenium.
    """
    logging.info("[SCRAPER-Jobrapido] Starting new job scrape...")
    
    # --- Setup Selenium ---
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    driver = None
    new_jobs_found = 0
    
    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(JOBRAPIDO_URL)

        job_card_selector = (By.CSS_SELECTOR, ".job-card_container__-e_fR")
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located(job_card_selector))
        
        job_cards = driver.find_elements(By.CSS_SELECTOR, ".job-card_container__-e_fR")
        
        if not job_cards:
            logging.info("[SCRAPER-Jobrapido] No job cards found on page.")
            return

        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        for card in job_cards:
            job_title, company, job_link = "N/A", "N/A", "N/A"

            try:
                title_element = card.find_element(By.CSS_SELECTOR, "a.job-card_job-title-url__-1-2G")
                job_title = title_element.text
                job_link = title_element.get_attribute("href")
            except Exception: pass 
            
            try:
                company_element = card.find_element(By.CSS_SELECTOR, "span[data-cy='company-name']")
                company = company_element.text
            except Exception: pass

            if job_link != "N/A":
                try:
                    cursor.execute(
                        "INSERT OR IGNORE INTO jobs (job_link, job_title, company_name, scraped_at) VALUES (?, ?, ?, ?)",
                        (job_link, job_title, company, datetime.now())
                    )
                    if cursor.rowcount > 0: new_jobs_found += 1
                except Exception as e:
                    logging.error(f"[SCRAPER-Jobrapido] DB Error: {e}")

        conn.commit()
        conn.close()
        logging.info(f"[SCRAPER-Jobrapido] Scrape complete. Found {new_jobs_found} new jobs.")

    except Exception as e:
        logging.error(f"[SCRAPER-Jobrapido] Error: {e}")
        
    finally:
        if driver:
            driver.quit()

# ### NEW ###
def scrape_unstop_task():
    """
    Scraper for Unstop. Uses their internal API (no Selenium needed).
    """
    logging.info("[SCRAPER-Unstop] Starting new job scrape...")
    new_jobs_found = 0

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    # This payload tells the API we just want "jobs"
    payload = {
        "opportunity_type": "jobs",
        "page": 1,
        "per_page": 25  # Get 25 jobs
    }
    
    try:
        response = requests.post(UNSTOP_API_URL, headers=headers, json=payload, timeout=10)
        response.raise_for_status()  # Raise error if status is not 200
        data = response.json()
        
        jobs_list = data.get('data', {}).get('data', [])
        
        if not jobs_list:
            logging.info("[SCRAPER-Unstop] No jobs found in API response.")
            return

        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        for job in jobs_list:
            job_title = job.get('title', 'N/A')
            company_name = job.get('organisation', {}).get('name', 'N/A')
            slug = job.get('slug')
            
            if slug:
                job_link = f"https://unstop.com/jobs/{slug}"
                
                try:
                    cursor.execute(
                        "INSERT OR IGNORE INTO jobs (job_link, job_title, company_name, scraped_at) VALUES (?, ?, ?, ?)",
                        (job_link, job_title, company_name, datetime.now())
                    )
                    if cursor.rowcount > 0: new_jobs_found += 1
                except Exception as e:
                    logging.error(f"[SCRAPER-Unstop] DB Error: {e}")

        conn.commit()
        conn.close()
        logging.info(f"[SCRAPER-Unstop] Scrape complete. Found {new_jobs_found} new jobs.")

    except Exception as e:
        logging.error(f"[SCRAPER-Unstop] Error: {e}")


# --- 4. THE TELEGRAM BOT (CONSUMER) ---
def post_job_task():
    """
    This function runs every 2 minutes, fetches ONE unposted job from the DB,
    and posts it to Telegram.
    """
    # ### UPDATED ###
    # Read tokens from environment variables every time
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        logging.warning("[POSTER] Telegram Token/ChatID not set. Skipping post.")
        return

    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM jobs WHERE posted_to_telegram = 0 ORDER BY scraped_at DESC LIMIT 1")
    job = cursor.fetchone()
    
    if not job:
        logging.info("[POSTER] No new jobs in the queue. Waiting...")
        conn.close()
        return

    logging.info(f"[POSTER] Found job to post: {job['job_title']}")

    # Format the message (Title, Company, Link)
    message = f"📢 **New Job Posting!**\n\n"
    message += f"**Title:** {job['job_title']}\n"
    message += f"**Company:** {job['company_name']}\n\n"
    message += f"🔗 **Apply Here:** \n{job['job_link']}"

    try:
        bot = telegram.Bot(token=token)
        bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=telegram.ParseMode.MARKDOWN
        )
        
        cursor.execute("UPDATE jobs SET posted_to_telegram = 1 WHERE id = ?", (job['id'],))
        conn.commit()
        logging.info(f"[POSTER] Successfully posted job ID {job['id']}.")

    except Exception as e:
        logging.error(f"[POSTER] FAILED to post job ID {job['id']}: {e}")
    
    finally:
        conn.close()


# --- 5. THE SCHEDULER & MAIN APP ---
def run_in_parallel(job_func):
    """A helper to run a scheduled job in its own thread."""
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

# ### NEW ###
def run_all_scrapers():
    """Run all scraper functions in parallel threads."""
    logging.info("Starting all scrapers...")
    run_in_parallel(scrape_jobrapido_task)
    run_in_parallel(scrape_unstop_task)

def main():
    # ### UPDATED ###
    # Check for environment variables at startup
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.critical("CRITICAL: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID environment variables not set. Exiting.")
        return
        
    # 1. Setup the database on start
    setup_database()
    
    logging.info("Starting schedulers...")
    
    # 2. Define the schedules
    schedule.every(1).hour.do(run_all_scrapers)  # ### UPDATED ###
    schedule.every(2).minutes.do(run_in_parallel, post_job_task)

    # 3. Run the main loop
    logging.info("Agent is now running. Press Ctrl+C to stop.")
    
    # Run scrapers immediately on start
    run_all_scrapers() 
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Shutting down agent...")
            break
        except Exception as e:
            logging.error(f"Main loop error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
