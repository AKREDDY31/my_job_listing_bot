import sqlite3
import threading
import time
import schedule
import logging
import os
import requests
from datetime import datetime
from threading import Thread

# Web Server Imports
from flask import Flask

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# Telegram Import
import telegram

# --- 1. CONFIGURATION ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Point the database to Render's persistent disk
DB_NAME = "/data/jobs.db"  

# Scraper URLs
UNSTOP_API_URL = "https://unstop.com/api/public/opportunity/search-result"
INTERNSHALA_URL = "https://internshala.com/jobs/work-from-home/" # Example: WFH jobs, can be changed
NCS_URL = "https://www.ncs.gov.in/job-seeker/Pages/Search.aspx?reg=0" # NCS Job Search

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Flask Web Server (for Uptime Robot)
app = Flask(__name__)

# --- 2. DATABASE SETUP ---
def setup_database():
    """Creates the SQLite database and the 'jobs' table if it doesn't exist."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_link TEXT UNIQUE NOT NULL,  -- UNIQUE prevents duplicate jobs, NOT NULL ensures we have a link
        job_title TEXT,
        company_name TEXT,
        source_site TEXT,      -- Added to track where the job came from
        scraped_at TIMESTAMP,
        posted_to_telegram BOOLEAN DEFAULT 0 
    )
    ''')
    conn.commit()
    conn.close()
    logging.info(f"Database '{DB_NAME}' is ready.")

# --- 3. HELPER FUNCTION FOR SELENIUM ---
def setup_selenium_driver():
    """Initializes and returns a Selenium WebDriver instance configured for Render."""
    service = Service(ChromeDriverManager().install())
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu") # Often needed in headless environments
    options.add_argument("--window-size=1920,1080") # Specify window size
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    # Point to Chrome binary from buildpack if env var exists, otherwise use default path
    chrome_bin = os.environ.get("GOOGLE_CHROME_BIN", "/app/.google-chrome/bin/google-chrome")
    options.binary_location = chrome_bin
    
    try:
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except WebDriverException as e:
        logging.error(f"Failed to initialize Selenium driver: {e}")
        # Attempt to find chromedriver path explicitly if webdriver-manager fails in some envs
        try:
             driver_path = ChromeDriverManager().install()
             service = Service(executable_path=driver_path)
             driver = webdriver.Chrome(service=service, options=options)
             logging.info("Successfully initialized driver with explicit path.")
             return driver
        except Exception as inner_e:
             logging.error(f"Failed again to initialize driver with explicit path: {inner_e}")
             return None


# --- 4. THE SCRAPERS (PRODUCERS) ---

def add_job_to_db(job_link, job_title, company_name, source_site):
    """Adds a job to the database if the link doesn't already exist."""
    new_job_added = False
    if not job_link: # Skip if no link
        return new_job_added
        
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """INSERT OR IGNORE INTO jobs 
               (job_link, job_title, company_name, source_site, scraped_at) 
               VALUES (?, ?, ?, ?, ?)""",
            (job_link, job_title or "N/A", company_name or "N/A", source_site, datetime.now())
        )
        if cursor.rowcount > 0:
            new_job_added = True
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"[DB] Error adding job from {source_site}: {e} - Link: {job_link}")
    finally:
        conn.close()
    return new_job_added

def scrape_unstop_task():
    """Scraper for Unstop using their API."""
    logging.info("[SCRAPER-Unstop] Starting...")
    new_jobs_found = 0
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://unstop.com/jobs',
        'Origin': 'https://unstop.com'
    }
    payload = {"opportunity_type": "jobs", "page": 1, "per_page": 50} # Get more jobs
    
    try:
        response = requests.post(UNSTOP_API_URL, headers=headers, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        jobs_list = data.get('data', {}).get('data', [])
        
        if not jobs_list:
            logging.info("[SCRAPER-Unstop] No jobs found in API response.")
            return

        for job in jobs_list:
            job_title = job.get('title')
            company_name = job.get('organisation', {}).get('name')
            slug = job.get('slug')
            if slug:
                job_link = f"https://unstop.com/jobs/{slug}"
                if add_job_to_db(job_link, job_title, company_name, "Unstop"):
                    new_jobs_found += 1
        logging.info(f"[SCRAPER-Unstop] Complete. Found {new_jobs_found} new jobs.")

    except requests.exceptions.RequestException as e:
        logging.error(f"[SCRAPER-Unstop] Request Error: {e}")
    except Exception as e:
        logging.error(f"[SCRAPER-Unstop] Unexpected Error: {e}")

def scrape_internshala_task():
    """Scraper for Internshala using Selenium."""
    logging.info("[SCRAPER-Internshala] Starting...")
    new_jobs_found = 0
    driver = setup_selenium_driver()
    if not driver:
        logging.error("[SCRAPER-Internshala] Failed to start driver. Aborting.")
        return

    try:
        driver.get(INTERNSHALA_URL)
        # Wait for job listings container to be present
        wait = WebDriverWait(driver, 20)
        job_container_selector = (By.ID, "internship_list_container") 
        wait.until(EC.presence_of_element_located(job_container_selector))
        time.sleep(5) # Give extra time for dynamic elements to settle

        # Find individual job cards
        job_cards = driver.find_elements(By.CSS_SELECTOR, ".internship_meta") # Common container for jobs

        if not job_cards:
            logging.info("[SCRAPER-Internshala] No job cards found on page.")
            return

        for card in job_cards:
            job_title, company_name, job_link = None, None, None
            try:
                # Find title and link (often within the same element)
                title_element = card.find_element(By.CSS_SELECTOR, ".job-internship-name")
                job_title = title_element.text.strip()
                
                # Link is usually on a parent div with class 'internship_meta' or similar structure
                link_element = card.find_element(By.CSS_SELECTOR, ".view_detail_button")
                job_link = link_element.get_attribute('href')

                # Find company name
                company_element = card.find_element(By.CSS_SELECTOR, ".company_name a")
                company_name = company_element.text.strip()

                if add_job_to_db(job_link, job_title, company_name, "Internshala"):
                    new_jobs_found += 1

            except NoSuchElementException:
                logging.warning("[SCRAPER-Internshala] Could not find element in a job card. Structure might have changed.")
            except Exception as e:
                logging.error(f"[SCRAPER-Internshala] Error processing card: {e}")
                
        logging.info(f"[SCRAPER-Internshala] Complete. Found {new_jobs_found} new jobs.")

    except TimeoutException:
        logging.error("[SCRAPER-Internshala] Timed out waiting for page elements.")
    except WebDriverException as e:
        logging.error(f"[SCRAPER-Internshala] WebDriver Error: {e}")
    except Exception as e:
        logging.error(f"[SCRAPER-Internshala] Unexpected Error: {e}")
    finally:
        if driver:
            driver.quit()

def scrape_ncs_task():
    """Scraper for NCS.gov.in using Selenium."""
    logging.info("[SCRAPER-NCS] Starting...")
    new_jobs_found = 0
    driver = setup_selenium_driver()
    if not driver:
        logging.error("[SCRAPER-NCS] Failed to start driver. Aborting.")
        return
        
    try:
        driver.get(NCS_URL)
        # Wait for the table containing job results
        wait = WebDriverWait(driver, 30) # NCS can be slow
        job_table_selector = (By.ID, "gvSearch") # The main table ID
        wait.until(EC.presence_of_element_located(job_table_selector))
        time.sleep(5) # Allow dynamic content to load

        job_rows = driver.find_elements(By.CSS_SELECTOR, "#gvSearch > tbody > tr")

        if len(job_rows) <= 1: # Header row only
            logging.info("[SCRAPER-NCS] No job rows found in the table.")
            return
            
        # Skip header row (index 0)
        for row in job_rows[1:]:
            job_title, company_name, job_link = None, None, None
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) > 5: # Need at least enough cells for title, company, link
                    # Adjust indices based on actual table structure (Inspect Element!)
                    job_title = cells[1].text.strip()
                    company_name = cells[2].text.strip()
                    # Link is often in the first cell within an <a> tag
                    link_element = cells[0].find_element(By.TAG_NAME, "a")
                    job_link = link_element.get_attribute('href')

                    if add_job_to_db(job_link, job_title, company_name, "NCS.gov.in"):
                        new_jobs_found += 1
                else:
                    logging.warning("[SCRAPER-NCS] Job row has fewer cells than expected.")

            except NoSuchElementException:
                 logging.warning("[SCRAPER-NCS] Could not find element in a job row. Structure might have changed.")
            except Exception as e:
                 logging.error(f"[SCRAPER-NCS] Error processing row: {e}")

        logging.info(f"[SCRAPER-NCS] Complete. Found {new_jobs_found} new jobs.")

    except TimeoutException:
        logging.error("[SCRAPER-NCS] Timed out waiting for page elements.")
    except WebDriverException as e:
        logging.error(f"[SCRAPER-NCS] WebDriver Error: {e}")
    except Exception as e:
        logging.error(f"[SCRAPER-NCS] Unexpected Error: {e}")
    finally:
        if driver:
            driver.quit()

# --- 5. THE TELEGRAM BOT (CONSUMER) ---
def post_job_task():
    """Posts one unposted job from the DB to Telegram every 2 minutes."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        logging.warning("[POSTER] Telegram Token/ChatID not set. Skipping post.")
        return

    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row # Access columns by name
    cursor = conn.cursor()
    
    job_posted = False
    try:
        cursor.execute("SELECT * FROM jobs WHERE posted_to_telegram = 0 ORDER BY scraped_at ASC LIMIT 1") # Post oldest first
        job = cursor.fetchone()
        
        if not job:
            logging.info("[POSTER] No new jobs in the queue.")
            return

        logging.info(f"[POSTER] Processing job ID {job['id']}: {job['job_title']}")

        # Format message (Title, Company, Source, Link)
        message = f"📢 **New Job Posting!**\n\n"
        message += f"**Title:** {job['job_title']}\n"
        if job['company_name'] and job['company_name'] != "N/A":
             message += f"**Company:** {job['company_name']}\n"
        message += f"**Source:** {job['source_site']}\n\n"
        message += f"🔗 **Details & Apply Here:** \n{job['job_link']}"

        bot = telegram.Bot(token=token)
        bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=telegram.ParseMode.MARKDOWN,
            disable_web_page_preview=True # Optional: disable link previews
        )
        
        # Mark as posted ONLY if send_message was successful
        cursor.execute("UPDATE jobs SET posted_to_telegram = 1 WHERE id = ?", (job['id'],))
        conn.commit()
        job_posted = True
        logging.info(f"[POSTER] Successfully posted job ID {job['id']}.")

    except telegram.error.TelegramError as e:
        logging.error(f"[POSTER] Telegram API Error posting job ID {job['id']}: {e}")
        # Consider adding logic here: should we retry later? Or mark as failed?
        # For now, we just log the error and don't mark it as posted.
    except sqlite3.Error as e:
        logging.error(f"[POSTER] Database Error: {e}")
    except Exception as e:
        logging.error(f"[POSTER] Unexpected Error posting job ID {job['id'] if job else 'N/A'}: {e}")
    
    finally:
        conn.close()


# --- 6. THE SCHEDULER & MAIN APP ---
def run_in_parallel(job_func):
    """Helper to run a scheduled job in its own thread."""
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

def run_all_scrapers():
    """Run all scraper functions in sequence (to avoid overwhelming resources)."""
    logging.info("Starting all scrapers sequentially...")
    # Run sequentially to reduce load, especially on free Render tier
    scrape_unstop_task() 
    time.sleep(10) # Small delay between scrapers
    scrape_internshala_task()
    time.sleep(10)
    scrape_ncs_task()
    logging.info("All scrapers finished.")

def run_agent_tasks():
    """Scheduler loop running in the background thread."""
    logging.info("Starting scheduler loop...")
    
    # Schedule scrapers to run every hour
    schedule.every(1).hour.do(run_all_scrapers) 
    
    # Schedule poster to run every 2 minutes
    schedule.every(2).minutes.do(run_in_parallel, post_job_task)

    # Run scrapers immediately on start, then wait before starting loop
    logging.info("Running initial scrape...")
    run_all_scrapers() 
    logging.info("Initial scrape finished. Starting regular schedule.")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logging.error(f"Scheduler loop error: {e}")
            time.sleep(60) # Wait a bit longer on error

# --- 7. FLASK ROUTE & STARTUP ---
@app.route('/')
def health_check():
    """Endpoint for Uptime Robot."""
    logging.info("Health check endpoint '/' was hit.")
    # You could add checks here, e.g., database connection
    return "Agent is alive and running.", 200

if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.critical("CRITICAL: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID env vars not set. Exiting.")
    else:
        # 1. Setup the database
        setup_database()
        
        # 2. Start the background scheduler thread
        logging.info("Starting background agent thread...")
        agent_thread = Thread(target=run_agent_tasks, daemon=True) # daemon=True allows app to exit even if thread is running
        agent_thread.start()

        # 3. Start the Flask web server (for Uptime Robot)
        logging.info("Starting Flask web server for health checks...")
        port = int(os.environ.get("PORT", 8080)) 
        # Use Gunicorn in production (Render handles this via Start Command)
        # For local testing, Flask's development server is fine:
        # app.run(host='0.0.0.0', port=port) 
        # When run with Gunicorn, this __main__ block doesn't execute app.run(), 
        # Gunicorn imports the 'app' object and serves it.
        logging.info(f"Flask server intended to run on port {port}")
        # Keep the main thread alive if not run by gunicorn (useful for local testing)
        try:
             while True: time.sleep(86400) # Sleep for a day
        except KeyboardInterrupt:
             logging.info("Main thread interrupted. Exiting.")
