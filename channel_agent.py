# --- Required Libraries ---
import feedparser
import json
import os
import time
import telegram # Requires 'python-telegram-bot'
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler # Requires 'APScheduler'
from flask import Flask # Requires 'Flask'
import threading # Requires 'threading' (built-in)
from collections import deque # Efficient queue
import re # For cleaning summary
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
SEEN_JOBS_FILE = "all_seen_jobs.json" # Stores links of jobs already posted

ALL_JOB_FEEDS = [
    # --- B.Tech / Engineering (Freshers & General - India Wide) ---
    "https://in.indeed.com/rss?q=B.Tech+CSE+OR+IT+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=Software+Developer+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=Data+Analyst+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=B.Tech+ECE+OR+EEE+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=Electronics+Engineer+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=Electrical+Engineer+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=B.Tech+Mechanical+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=Mechanical+Engineer+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=B.Tech+Civil+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=Civil+Engineer+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=B.Tech+Aerospace+OR+Aeronautical+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=B.Tech+Biotechnology+OR+Biomedical+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=Graduate+Engineer+Trainee&l=India&sort=date",
    "https://in.indeed.com/rss?q=Entry+Level+Engineer&l=India&sort=date",
    # --- Government Jobs (India Wide - General Feeds) ---
    "http://govtjobsblog.in/feed",
    "http://indiajoblive.com/feed",
    "https://www.sarkarinaukriblog.com/feed",
]

# --- Shared State for New Job Queue ---
pending_jobs_queue = deque()
queue_lock = threading.Lock()

# Global variables for Flask display
last_check_time = "Never finished initial check"
last_post_time = "Never posted"

# --- 3. HELPER FUNCTIONS (Memory Management) ---
def load_all_seen_jobs():
    if not os.path.exists(SEEN_JOBS_FILE):
        print(f"Memory file '{SEEN_JOBS_FILE}' not found. Starting fresh.", flush=True)
        return set()
    try:
        with open(SEEN_JOBS_FILE, 'r') as f:
            content = f.read()
            if not content:
                print(f"Memory file '{SEEN_JOBS_FILE}' is empty. Starting fresh.", flush=True)
                return set()
            return set(json.loads(content))
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error reading memory file '{SEEN_JOBS_FILE}': {e}. Starting fresh.", flush=True)
        return set()
    except Exception as e:
        print(f"Unexpected error loading seen jobs: {e}. Starting fresh.", flush=True)
        return set()

def save_all_seen_jobs(seen_links):
    try:
        with open(SEEN_JOBS_FILE, 'w') as f:
            json.dump(list(seen_links), f, indent=2)
    except Exception as e:
        print(f"ERROR saving seen jobs to '{SEEN_JOBS_FILE}': {e}", flush=True)

# --- 4. TELEGRAM SENDING FUNCTION ---
async def send_job_to_channel(title, link, summary, bot):
    """Sends a single job with summary to the configured Telegram channel."""
    global last_post_time
    print(f"Attempting to post job: {title}", flush=True)

    cleaned_summary = summary
    if summary:
        cleaned_summary = re.sub('<[^<]+?>', '', summary)
        cleaned_summary = cleaned_summary.replace('&nbsp;', ' ').strip()
        if len(cleaned_summary) > 500:
             cleaned_summary = cleaned_summary[:500] + "..."

    safe_title = title
    for char in '_*[]()~`>#+-=|{}.!': safe_title = safe_title.replace(char, f'\\{char}')
    safe_link = link
    for char in '()': safe_link = safe_link.replace(char, f'\\{char}')
    safe_summary = cleaned_summary if cleaned_summary else "No summary available"
    for char in '_*[]()~`>#+-=|{}.!': safe_summary = safe_summary.replace(char, f'\\{char}')

    message = (
        f"*New Job Alert\\!* 🔔\n\n"
        f"*Title:* {safe_title}\n\n"
        f"*Summary/Description:*\n{safe_summary}\n\n"
        f"*Link:* [Apply Here]({safe_link})"
    )

    try:
        await bot.send_message(
            chat_id=CHANNEL_CHAT_ID, text=message, parse_mode='MarkdownV2', disable_web_page_preview=False
        )
        print(f"  > Successfully sent: {title}", flush=True)
        last_post_time = time.ctime()
        return True
    except telegram.error.BadRequest as e_md:
        print(f"  > MarkdownV2 failed for '{title}' (Error: {e_md}). Retrying plain text.", flush=True)
        plain_message = (
            f"New Job Alert! 🔔\n\n"
            f"Title: {title}\n\n"
            f"Summary/Description:\n{cleaned_summary if cleaned_summary else 'No summary available'}\n\n"
            f"Link: {link}"
        )
        try:
            await bot.send_message(
                chat_id=CHANNEL_CHAT_ID, text=plain_message, disable_web_page_preview=False
            )
            print(f"  > Sent (Plain): {title}", flush=True)
            last_post_time = time.ctime()
            return True
        except Exception as e_plain:
             print(f"  > FAILED (Plain Text) for {title}. Error: {type(e_plain).__name__} - {e_plain}", flush=True)
             return False
    except telegram.error.RetryAfter as e_flood:
        wait_time = e_flood.retry_after
        print(f"  > Flood control hit. Waiting {wait_time}s...", flush=True)
        await asyncio.sleep(wait_time + 1)
        print(f"  > Will retry posting '{title}' later.", flush=True)
        return False
    except Exception as e:
        print(f"  > FAILED (Other) for {title}. Error Type: {type(e).__name__}, Details: {e}", flush=True)
        return False

# --- 5. CORE AGENT LOGIC (Separated into Check & Post) ---
def check_feeds_for_new_jobs():
    """Fetches jobs from feeds, filters new ones, and adds them to the pending queue."""
    global last_check_time, pending_jobs_queue, queue_lock
    print(f"[{time.ctime()}] --- Starting Feed Check Cycle ---", flush=True)

    seen_job_links = load_all_seen_jobs()
    original_seen_count = len(seen_job_links)
    print(f"Loaded {original_seen_count} previously seen job links.", flush=True)
    newly_found_jobs = []

    headers = {'User-Agent': 'Mozilla/5.0 JobAgent/2.0'} # Simplified User-Agent

    for feed_url in ALL_JOB_FEEDS:
        print(f"  Checking feed: {feed_url}", flush=True)
        try:
            feed = feedparser.parse(feed_url, request_headers=headers)
            if feed.bozo: print(f"    WARNING: Feed ill-formed. Exception: {feed.get('bozo_exception', 'Unknown')}", flush=True)
            if not feed.entries: print("    No entries found.", flush=True); continue

            processed_count = 0; new_in_feed = 0
            for job in feed.entries:
                if not getattr(job, 'link', None) or not getattr(job, 'title', None): continue
                job_link = job.link; job_title = job.title.strip(); processed_count += 1
                job_summary = getattr(job, 'summary', None)

                if job_link not in seen_job_links:
                    newly_found_jobs.append({"title": job_title, "link": job_link, "summary": job_summary})
                    seen_job_links.add(job_link)
                    new_in_feed += 1

            print(f"    Processed {processed_count} entries. Found {new_in_feed} new jobs in this feed.", flush=True)

        except Exception as e:
            print(f"  > ERROR processing feed {feed_url}. Type: {type(e).__name__}, Details: {e}", flush=True); continue

    if newly_found_jobs:
        with queue_lock:
            # Add jobs to the end of the queue
            pending_jobs_queue.extend(newly_found_jobs)
            print(f"\nAdded {len(newly_found_jobs)} new jobs to the pending queue. Queue size: {len(pending_jobs_queue)}", flush=True)
    else:
        print("\n...No new jobs found this feed check cycle.", flush=True)

    if len(seen_job_links) > original_seen_count:
        print(f"Saving updated seen jobs list ({len(seen_job_links)} total)...", flush=True)
        save_all_seen_jobs(seen_job_links)
        print("Save complete.", flush=True)

    last_check_time = time.ctime()
    print(f"[{last_check_time}] --- Finished Feed Check Cycle ---", flush=True)

def post_one_job_from_queue():
    """Takes one job from the queue (if available) and attempts to post it."""
    global pending_jobs_queue, queue_lock
    print(f"[{time.ctime()}] --- Running 2-Min Post Check ---", flush=True) # Updated timing

    job_to_post = None
    with queue_lock:
        if pending_jobs_queue:
            job_to_post = pending_jobs_queue.popleft()
            print(f"Attempting to post job from queue: {job_to_post['title']}. Jobs remaining: {len(pending_jobs_queue)}", flush=True)
        else:
            print("No jobs currently in the pending queue.", flush=True)

    if job_to_post:
        request_config = telegram.request.HTTPXRequest(connect_timeout=15.0, read_timeout=30.0)
        bot_instance = telegram.Bot(token=TELEGRAM_TOKEN, request=request_config)
        try:
            loop = asyncio.get_event_loop_policy().new_event_loop()
            asyncio.set_event_loop(loop)
            success = loop.run_until_complete(send_job_to_channel(
                job_to_post['title'], job_to_post['link'], job_to_post['summary'], bot_instance
            ))
            loop.close()

            if not success:
                print(f"  > Posting failed for '{job_to_post['title']}'. Re-queuing.", flush=True)
                with queue_lock:
                    pending_jobs_queue.appendleft(job_to_post)

        except Exception as e:
            print(f"  > UNEXPECTED ERROR during job posting: {type(e).__name__} - {e}", flush=True)
            print(f"  > Re-queuing job '{job_to_post['title']}' due to unexpected error.", flush=True)
            with queue_lock:
                pending_jobs_queue.appendleft(job_to_post)

    print(f"[{time.ctime()}] --- Finished 2-Min Post Check ---", flush=True) # Updated timing

# --- 6. FLASK WEB SERVER ---
app = Flask(__name__)

@app.route('/')
def hello_world():
    """Returns a simple message including check/post times and queue size."""
    global last_check_time, last_post_time, pending_jobs_queue, queue_lock
    q_size = 0
    with queue_lock:
        q_size = len(pending_jobs_queue)
    return (
        f'Job Agent is running!<br>'
        f'Last feed check finished around: {last_check_time}<br>'
        f'Last successful post attempt around: {last_post_time}<br>'
        f'Jobs currently in posting queue: {q_size}'
    )

def run_flask_app():
    """Runs the Flask app using Waitress (if available) on Render's port."""
    port = int(os.environ.get('PORT', 10000))
    host = '0.0.0.0'
    print(f"Attempting to start server on {host}:{port}", flush=True)
    try:
        if USE_WAITRESS:
            print("Using Waitress server.", flush=True)
            serve(app, host=host, port=port)
        else:
            print("Using Flask development server.", flush=True)
            app.run(host=host, port=port)
    except Exception as e:
        print(f"ERROR starting Flask server: {e}", flush=True)

# --- 7. SCHEDULER & MAIN EXECUTION ---
if __name__ == "__main__":
    print("--- Initializing Job Agent: Continuous Posting Mode (2min Post/1hr Check) ---", flush=True)

    print("Running initial feed check...", flush=True)
    try: check_feeds_for_new_jobs()
    except Exception as e: print(f"ERROR during initial check: {type(e).__name__} - {e}", flush=True)
    print("Initial check complete.", flush=True)

    scheduler = BackgroundScheduler(timezone="UTC")
    try:
        # Schedule the feed check every 1 HOUR
        scheduler.add_job(check_feeds_for_new_jobs, 'interval', hours=1, id='feed_check_task', replace_existing=True)
        # Schedule the job poster every 2 MINUTES
        scheduler.add_job(post_one_job_from_queue, 'interval', minutes=2, id='job_post_task', replace_existing=True)

        scheduler.start()
        print("Scheduler started. Checks feeds every 1 hour, attempts post every 2 mins.", flush=True)
        print(f"Notifications configured for channel ID ending: ...{CHANNEL_CHAT_ID[-6:]}", flush=True) # Avoid logging full ID
    except Exception as e:
        print(f"ERROR starting APScheduler: {e}", flush=True); exit()

    print("Starting Flask/Waitress web server...", flush=True)
    run_flask_app() # Blocks main thread

    # --- Shutdown ---
    print("Web server has stopped. Shutting down scheduler...", flush=True)
    try: scheduler.shutdown()
    except Exception as e: print(f"Error shutting down scheduler: {e}", flush=True)
    print("Agent shut down.", flush=True)
