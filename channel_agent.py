# --- Required Libraries ---
import feedparser
import json
import os
import time
import telegram # Using python-telegram-bot v20+
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError, RetryAfter
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
import threading
from collections import deque
import re
import requests
import httpx # Required by PTB v20+

# Use waitress for production WSGI server
try:
    from waitress import serve
    USE_WAITRESS = True
except ImportError:
    USE_WAITRESS = False
    print("Waitress not installed, falling back to Flask's development server (NOT recommended for production)", flush=True)

# --- 1. READ SECRETS FROM ENVIRONMENT VARIABLES ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_CHAT_ID = os.environ.get("CHANNEL_CHAT_ID")

if not TELEGRAM_TOKEN or not CHANNEL_CHAT_ID:
    print("FATAL ERROR: TELEGRAM_TOKEN or CHANNEL_CHAT_ID environment variable not set!", flush=True)
    print("Please set these secrets in your Render service environment variables.", flush=True)
    exit()

# --- 2. CONFIGURE YOUR AGENT ---
# --- RENDER FREE TIER PERSISTENCE WARNING ---
SEEN_JOBS_FILE = "all_seen_jobs.json" # THIS WILL BE LOST ON RESTARTS/DEPLOYS ON FREE TIER

# --- DEBUGGING: REDUCED FEED LIST ---
ALL_JOB_FEEDS = [
    # Keep one Indeed feed (often reliable)
    "https://in.indeed.com/rss?q=Software+Developer+fresher&l=India&sort=date",
    # Keep one government feed (often reliable)
    "https://www.sarkarinaukriblog.com/feed",
    # Comment out the rest to isolate potential issues
    # "#https://in.indeed.com/rss?q=B.Tech+CSE+OR+IT+fresher&l=India&sort=date",
    # "#https://in.indeed.com/rss?q=Data+Analyst+fresher&l=India&sort=date",
    # "#https://in.indeed.com/rss?q=B.Tech+ECE+OR+EEE+fresher&l=India&sort=date",
    # "#https://in.indeed.com/rss?q=Mechanical+Engineer+fresher&l=India&sort=date",
    # "#https://in.indeed.com/rss?q=Civil+Engineer+fresher&l=India&sort=date",
    # "#https://in.indeed.com/rss?q=Graduate+Engineer+Trainee&l=India&sort=date",
    # "#http://govtjobsblog.in/feed",
]

UNSTOP_API_URL = "https://unstop.com/api/public/opportunity/search-result"

# --- Shared State ---
pending_jobs_queue = deque()
queue_lock = threading.Lock()
last_check_time = "Never finished initial check"
last_post_time = "Never posted"
seen_job_links_memory = set() # Load into memory at start

# --- 3. HELPER FUNCTIONS (Memory Management) ---
def load_all_seen_jobs():
    global seen_job_links_memory
    if not os.path.exists(SEEN_JOBS_FILE):
        print(f"Memory file '{SEEN_JOBS_FILE}' not found. Starting fresh.", flush=True)
        seen_job_links_memory = set()
        return
    try:
        if os.path.getsize(SEEN_JOBS_FILE) == 0:
             print(f"Memory file '{SEEN_JOBS_FILE}' is empty. Starting fresh.", flush=True)
             seen_job_links_memory = set()
             return
        with open(SEEN_JOBS_FILE, 'r') as f:
            content = f.read()
            if not content:
                 print(f"Memory file '{SEEN_JOBS_FILE}' read as empty. Starting fresh.", flush=True)
                 seen_job_links_memory = set()
                 return
            seen_job_links_memory = set(json.loads(content))
            print(f"Loaded {len(seen_job_links_memory)} seen job links into memory.", flush=True)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error reading memory file '{SEEN_JOBS_FILE}': {e}. Starting fresh.", flush=True)
        seen_job_links_memory = set()
    except Exception as e:
        print(f"Unexpected error loading seen jobs: {e}. Starting fresh.", flush=True)
        seen_job_links_memory = set()

def save_all_seen_jobs():
    global seen_job_links_memory
    try:
        with open(SEEN_JOBS_FILE, 'w') as f:
            json.dump(list(seen_job_links_memory), f, indent=2)
        print(f"Saved {len(seen_job_links_memory)} seen job links to '{SEEN_JOBS_FILE}'.", flush=True)
    except Exception as e:
        print(f"ERROR saving seen jobs to '{SEEN_JOBS_FILE}': {e}", flush=True)

# --- 4. TELEGRAM SENDING FUNCTION (Async) ---
async def send_job_to_channel_async(title, link, summary):
    # (Same async sending function as before)
    global last_post_time
    print(f"Attempting async post: {title}", flush=True)
    timeout = httpx.Timeout(15.0, read=30.0)
    bot = Bot(token=TELEGRAM_TOKEN, request=telegram.request.HTTPXRequest(http_version="1.1", timeout=timeout))
    cleaned_summary = summary
    if summary:
        cleaned_summary = re.sub('<[^<]+?>', '', summary).replace('&nbsp;', ' ').strip()
        if len(cleaned_summary) > 500: cleaned_summary = cleaned_summary[:500] + "..."
    def escape_markdown_v2(text):
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)
    safe_title = escape_markdown_v2(title)
    safe_link_text = link.replace(')', '%29').replace('(', '%28')
    safe_summary = escape_markdown_v2(cleaned_summary if cleaned_summary else "No summary available")
    message = (f"*New Job Alert\\!* 🔔\n\n*Title:* {safe_title}\n\n*Summary/Description:*\n{safe_summary}\n\n*Link:* [Apply Here]({safe_link_text})")
    try:
        await bot.send_message(chat_id=CHANNEL_CHAT_ID, text=message, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=False)
        print(f"  > Successfully sent (MDv2): {title}", flush=True)
        last_post_time = time.ctime()
        return True
    except RetryAfter as e_flood:
        wait_time = e_flood.retry_after
        print(f"  > Flood control hit. Waiting {wait_time}s...", flush=True)
        await asyncio.sleep(wait_time + 1)
        print(f"  > Will retry posting '{title}' later (re-queued).", flush=True)
        return False
    except TelegramError as e:
        print(f"  > FAILED sending '{title}'. Telegram Error: {type(e).__name__} - {e}", flush=True)
        return False
    except Exception as e:
        print(f"  > FAILED (Other) for '{title}'. Error Type: {type(e).__name__}, Details: {e}", flush=True)
        return False

# --- 5. CORE AGENT LOGIC ---
def fetch_unstop_jobs():
    # (Same function as before)
    print("  Fetching jobs from Unstop API...", flush=True)
    unstop_jobs = []
    headers = {'User-Agent': 'Mozilla/5.0 JobAgent/2.0 (compatible; FeedFetcher)', 'Referer': 'https://unstop.com/jobs', 'Origin': 'https://unstop.com'}
    payload = {"opportunity_type": "jobs", "page": 1, "per_page": 50}
    try:
        response = requests.post(UNSTOP_API_URL, headers=headers, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        jobs_list = data.get('data', {}).get('data', [])
        for job in jobs_list:
            title = job.get('title')
            company = job.get('organisation', {}).get('name')
            slug = job.get('slug')
            if title and slug:
                link = f"https://unstop.com/jobs/{slug}"
                summary = f"Company: {company}" if company else "See link for details."
                unstop_jobs.append({"title": title.strip(), "link": link, "summary": summary})
        print(f"  > Found {len(unstop_jobs)} potential jobs from Unstop.", flush=True)
        return unstop_jobs
    except requests.exceptions.RequestException as e:
        print(f"  > ERROR fetching from Unstop API: {e}", flush=True)
    except Exception as e:
        print(f"  > UNEXPECTED ERROR during Unstop fetch: {e}", flush=True)
    return []

def check_sources_for_new_jobs():
    """Fetches jobs, filters vs memory, adds NEW ones to queue."""
    global last_check_time, pending_jobs_queue, queue_lock, seen_job_links_memory
    print(f"[{time.ctime()}] --- Starting Source Check Cycle ---", flush=True)
    # --- RENDER FREE TIER WARNING ---
    print("WARNING: Using temporary file storage on Render Free Tier.", flush=True)
    print("         Seen jobs list WILL BE LOST on service restart/deploy.", flush=True)
    print("         DUPLICATE POSTS ARE LIKELY after restarts.", flush=True)
    # --- END WARNING ---
    original_seen_count = len(seen_job_links_memory)
    all_potential_jobs = []

    # --- 1. Fetch from RSS Feeds (Reduced List) ---
    print("\nChecking RSS Feeds...", flush=True)
    headers = {'User-Agent': 'Mozilla/5.0 JobAgent/2.0 (compatible; FeedFetcher)'}
    for feed_url in ALL_JOB_FEEDS:
        print(f"  Attempting to check feed: {feed_url}", flush=True) # <<< DEBUG LOGGING
        try:
            feed = feedparser.parse(feed_url, request_headers=headers)
            if feed.bozo: print(f"    WARNING: Feed ill-formed: {feed.get('bozo_exception', 'Unknown')}", flush=True)
            if not feed.entries: print("    No entries found.", flush=True); continue
            feed_job_count = 0
            for job in feed.entries:
                if getattr(job, 'link', None) and getattr(job, 'title', None):
                    all_potential_jobs.append({"title": job.title.strip(), "link": job.link, "summary": getattr(job, 'summary', None)})
                    feed_job_count += 1
            print(f"    Found {feed_job_count} potential jobs in this feed.", flush=True)
            print(f"    Successfully processed feed: {feed_url}", flush=True) # <<< DEBUG LOGGING
        except Exception as e:
            print(f"  > ERROR processing feed {feed_url}: {type(e).__name__} - {e}", flush=True)

    # --- 2. Fetch from Unstop API ---
    print("\nChecking Unstop API...", flush=True)
    unstop_jobs = fetch_unstop_jobs()
    all_potential_jobs.extend(unstop_jobs)

    # --- 3. Filter for NEW jobs and add to queue ---
    newly_found_for_queue = []
    added_to_memory_this_cycle = False
    if all_potential_jobs:
        print(f"\nProcessing {len(all_potential_jobs)} total potential jobs found...", flush=True)
        for job_data in all_potential_jobs:
            job_link = job_data['link']
            if job_link not in seen_job_links_memory:
                newly_found_for_queue.append(job_data)
                seen_job_links_memory.add(job_link)
                added_to_memory_this_cycle = True

    if newly_found_for_queue:
        with queue_lock:
            pending_jobs_queue.extend(newly_found_for_queue)
            print(f"\nAdded {len(newly_found_for_queue)} NEW unique jobs to the posting queue. Queue size now: {len(pending_jobs_queue)}", flush=True)
    else:
        print("\n...No new unique jobs found this check cycle to add to the queue.", flush=True)

    # --- 4. Save updated seen list IF it changed ---
    if added_to_memory_this_cycle:
        save_all_seen_jobs()
    else:
         print("No new links added to seen list, skipping save.", flush=True)

    last_check_time = time.ctime() # Update time only AFTER successful completion
    print(f"[{last_check_time}] --- Finished Source Check Cycle ---", flush=True) # This confirms completion

def post_one_job_from_queue():
    # (Same function as before - takes job from queue, calls async send)
    global pending_jobs_queue, queue_lock
    print(f"[{time.ctime()}] --- Running Post Check (Every 2 Mins) ---", flush=True)
    job_to_post = None
    with queue_lock:
        if pending_jobs_queue:
            job_to_post = pending_jobs_queue.popleft()
            print(f"Attempting post: {job_to_post['title']}. Queue size: {len(pending_jobs_queue)}", flush=True)
        else:
            print("No jobs currently in the pending queue.", flush=True)
    if job_to_post:
        try:
            success = asyncio.run(send_job_to_channel_async(job_to_post['title'], job_to_post['link'], job_to_post['summary']))
            if not success:
                print(f"  > Posting failed for '{job_to_post['title']}'. Re-queuing at front.", flush=True)
                with queue_lock: pending_jobs_queue.appendleft(job_to_post)
        except RuntimeError as e:
             print(f"  > RuntimeError during async job post: {e}. Re-queuing job.", flush=True)
             with queue_lock: pending_jobs_queue.appendleft(job_to_post)
        except Exception as e:
            print(f"  > UNEXPECTED ERROR running async post task: {type(e).__name__} - {e}", flush=True)
            print(f"  > Re-queuing job '{job_to_post['title']}' at front due to unexpected error.", flush=True)
            with queue_lock: pending_jobs_queue.appendleft(job_to_post)
    print(f"[{time.ctime()}] --- Finished Post Check ---", flush=True)

# --- 6. FLASK WEB SERVER ---
app = Flask(__name__)
@app.route('/')
def hello_world():
    # (Same function as before)
    global last_check_time, last_post_time, pending_jobs_queue, queue_lock
    q_size = 0
    with queue_lock: q_size = len(pending_jobs_queue)
    return (f'Job Agent is running! (Async Debug Version)<br>'
            f'Last source check finished around: {last_check_time}<br>'
            f'Last successful post attempt around: {last_post_time}<br>'
            f'Jobs currently in posting queue: {q_size}')

def run_flask_app():
    # (Same function as before)
    port = int(os.environ.get('PORT', 10000))
    host = '0.0.0.0'
    print(f"Attempting to start web server on {host}:{port}", flush=True)
    try:
        if USE_WAITRESS:
            print("Using Waitress production server.", flush=True)
            serve(app, host=host, port=port, threads=4)
        else:
            print("Using Flask development server (for testing only).", flush=True)
            app.run(host=host, port=port)
    except Exception as e:
        print(f"FATAL ERROR starting Flask/Waitress server: {e}", flush=True); exit()

# --- 7. SCHEDULER & MAIN EXECUTION ---
if __name__ == "__main__":
    print("--- Initializing Job Agent (Async Telegram - Debug Feeds) ---", flush=True)
    # Load seen jobs ONCE at startup
    load_all_seen_jobs()
    # Initial check
    print("\nRunning initial source check before starting services...", flush=True)
    try: check_sources_for_new_jobs()
    except Exception as e: print(f"ERROR during initial check: {type(e).__name__} - {e}", flush=True)
    print("Initial check complete.\n", flush=True) # This should appear even if check fails internally

    # Scheduler setup
    scheduler = BackgroundScheduler(timezone="UTC")
    try:
        scheduler.add_job(check_sources_for_new_jobs, 'interval', hours=1, id='source_check_task', replace_existing=True, misfire_grace_time=300)
        scheduler.add_job(post_one_job_from_queue, 'interval', minutes=2, id='job_post_task', replace_existing=True, misfire_grace_time=60)
        scheduler.start()
        print("APScheduler started successfully.", flush=True)
        if CHANNEL_CHAT_ID: print(f"Notifications configured for channel ID ending: ...{CHANNEL_CHAT_ID[-6:]}", flush=True)
    except Exception as e:
        print(f"FATAL ERROR starting APScheduler: {e}", flush=True); exit()

    # Start Flask/Waitress (blocks main thread)
    print("Starting Flask/Waitress web server...", flush=True)
    run_flask_app()

    # Graceful Shutdown
    print("Web server stopped. Shutting down scheduler...", flush=True)
    try: scheduler.shutdown()
    except Exception as e: print(f"Error shutting down scheduler: {e}", flush=True)
    print("Job Agent shut down complete.", flush=True)
