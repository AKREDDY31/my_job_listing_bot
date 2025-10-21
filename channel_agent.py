# --- Required Libraries ---
import feedparser
import json
import os
import time
import telegram # Requires 'python-telegram-bot==13.15'
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler # Requires 'APScheduler'
from flask import Flask # Requires 'Flask'
import threading # Built-in
from collections import deque # Efficient queue
import re # For cleaning summary
import requests # Requires 'requests'

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
# --- IMPORTANT: RENDER FREE TIER LIMITATION ---
# On Render's Free tier, this file WILL BE DELETED on restarts/deploys.
# This WILL cause duplicate job postings.
# Use a persistent disk (paid tier) or external DB for true persistence.
SEEN_JOBS_FILE = "all_seen_jobs.json"

# --- Add RSS Feeds Here (Example using Indeed & Govt Blogs like your code) ---
# NOTE: Internshala & NCS usually don't have public RSS feeds for job searches.
ALL_JOB_FEEDS = [
    # --- B.Tech / Engineering (Freshers & General - India Wide - using Indeed RSS examples) ---
    "https://in.indeed.com/rss?q=B.Tech+CSE+OR+IT+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=Software+Developer+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=B.Tech+ECE+OR+EEE+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=Mechanical+Engineer+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=Civil+Engineer+fresher&l=India&sort=date",
    "https://in.indeed.com/rss?q=Graduate+Engineer+Trainee&l=India&sort=date",
    # --- Government Jobs (India Wide - General Feeds from your example) ---
    "http://govtjobsblog.in/feed",
    "https://www.sarkarinaukriblog.com/feed",
    # Add any other valid RSS feeds you find
]

# Unstop API configuration
UNSTOP_API_URL = "https://unstop.com/api/public/opportunity/search-result"

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
        # Check if file is empty first
        if os.path.getsize(SEEN_JOBS_FILE) == 0:
             print(f"Memory file '{SEEN_JOBS_FILE}' is empty. Starting fresh.", flush=True)
             return set()
        with open(SEEN_JOBS_FILE, 'r') as f:
            content = f.read()
            # Handle potential empty content again just in case
            if not content:
                 print(f"Memory file '{SEEN_JOBS_FILE}' read as empty. Starting fresh.", flush=True)
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
        # Basic HTML tag removal
        cleaned_summary = re.sub('<[^<]+?>', '', summary)
        # Replace HTML space and trim
        cleaned_summary = cleaned_summary.replace('&nbsp;', ' ').strip()
        # Truncate if too long
        if len(cleaned_summary) > 500:
             cleaned_summary = cleaned_summary[:500] + "..."

    # Prepare for MarkdownV2 - escape relevant characters
    safe_title = title
    for char in '_*[]()~`>#+-=|{}.!': safe_title = safe_title.replace(char, f'\\{char}')
    safe_link = link
    # Escape parentheses in links for MarkdownV2
    safe_link = safe_link.replace('(', '\\(').replace(')', '\\)')
    safe_summary = cleaned_summary if cleaned_summary else "No summary available"
    for char in '_*[]()~`>#+-=|{}.!': safe_summary = safe_summary.replace(char, f'\\{char}')

    # Construct MarkdownV2 message
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
        print(f"  > Successfully sent (MDv2): {title}", flush=True)
        last_post_time = time.ctime()
        return True
    except telegram.error.BadRequest as e_md:
        print(f"  > MarkdownV2 failed for '{title}' (Error: {e_md}). Retrying plain text.", flush=True)
        # Construct plain text message
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
        return False # Indicate failure so it gets re-queued
    except Exception as e:
        print(f"  > FAILED (Other) for {title}. Error Type: {type(e).__name__}, Details: {e}", flush=True)
        return False # Indicate failure so it gets re-queued


# --- 5. CORE AGENT LOGIC ---

# --- NEW: Function to fetch from Unstop API ---
def fetch_unstop_jobs():
    """Fetches jobs from Unstop API and returns a list of job dicts."""
    print("  Fetching jobs from Unstop API...", flush=True)
    unstop_jobs = []
    headers = {
        'User-Agent': 'Mozilla/5.0 JobAgent/2.0 (compatible; FeedFetcher)', # Identify as feed fetcher
        'Referer': 'https://unstop.com/jobs',
        'Origin': 'https://unstop.com'
    }
    payload = {"opportunity_type": "jobs", "page": 1, "per_page": 50} # Fetch 50 jobs

    try:
        response = requests.post(UNSTOP_API_URL, headers=headers, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        jobs_list = data.get('data', {}).get('data', [])

        for job in jobs_list:
            title = job.get('title')
            company = job.get('organisation', {}).get('name') # Get company name if available
            slug = job.get('slug')
            if title and slug:
                link = f"https://unstop.com/jobs/{slug}"
                # Unstop API doesn't provide a summary, maybe add company?
                summary = f"Company: {company}" if company else "See link for details."
                unstop_jobs.append({"title": title.strip(), "link": link, "summary": summary})
        print(f"  > Found {len(unstop_jobs)} potential jobs from Unstop.", flush=True)
        return unstop_jobs

    except requests.exceptions.RequestException as e:
        print(f"  > ERROR fetching from Unstop API: {e}", flush=True)
        return []
    except Exception as e:
        print(f"  > UNEXPECTED ERROR during Unstop fetch: {e}", flush=True)
        return []

def check_sources_for_new_jobs():
    """Fetches jobs from RSS feeds AND Unstop API, adds new ones to queue."""
    global last_check_time, pending_jobs_queue, queue_lock
    print(f"[{time.ctime()}] --- Starting Source Check Cycle ---", flush=True)

    # --- RENDER FREE TIER WARNING ---
    print("WARNING: Using temporary file storage on Render Free Tier.", flush=True)
    print("         Seen jobs list WILL BE LOST on service restart/deploy.", flush=True)
    print("         DUPLICATE POSTS ARE LIKELY after restarts.", flush=True)
    # --- END WARNING ---

    seen_job_links = load_all_seen_jobs()
    original_seen_count = len(seen_job_links)
    print(f"Loaded {original_seen_count} previously seen job links.", flush=True)
    
    all_potential_jobs = []
    
    # --- 1. Fetch from RSS Feeds ---
    print("\nChecking RSS Feeds...", flush=True)
    headers = {'User-Agent': 'Mozilla/5.0 JobAgent/2.0 (compatible; FeedFetcher)'}
    for feed_url in ALL_JOB_FEEDS:
        print(f"  Checking feed: {feed_url}", flush=True)
        try:
            # Use etag and modified headers for efficiency if feedparser supports them well
            # feed = feedparser.parse(feed_url, request_headers=headers, etag=None, modified=None)
            feed = feedparser.parse(feed_url, request_headers=headers)

            if feed.bozo: print(f"    WARNING: Feed ill-formed. Exception: {feed.get('bozo_exception', 'Unknown')}", flush=True)
            if not feed.entries: print("    No entries found.", flush=True); continue

            feed_job_count = 0
            for job in feed.entries:
                if getattr(job, 'link', None) and getattr(job, 'title', None):
                    all_potential_jobs.append({
                        "title": job.title.strip(),
                        "link": job.link,
                        "summary": getattr(job, 'summary', None) # Get summary if exists
                    })
                    feed_job_count += 1
            print(f"    Found {feed_job_count} potential jobs in this feed.", flush=True)

        except Exception as e:
            print(f"  > ERROR processing feed {feed_url}. Type: {type(e).__name__}, Details: {e}", flush=True); continue

    # --- 2. Fetch from Unstop API ---
    print("\nChecking Unstop API...", flush=True)
    unstop_jobs = fetch_unstop_jobs()
    all_potential_jobs.extend(unstop_jobs)

    # --- 3. Filter for NEW jobs and add to queue ---
    newly_found_for_queue = []
    if all_potential_jobs:
        print(f"\nProcessing {len(all_potential_jobs)} total potential jobs found...", flush=True)
        for job_data in all_potential_jobs:
            job_link = job_data['link']
            # Basic link cleanup (remove tracking params if common patterns are known)
            # job_link = job_link.split('?')[0] # Example: remove query string

            if job_link not in seen_job_links:
                newly_found_for_queue.append(job_data)
                seen_job_links.add(job_link)
    
    if newly_found_for_queue:
        with queue_lock:
            # Add new jobs to the end (FIFO)
            pending_jobs_queue.extend(newly_found_for_queue)
            print(f"\nAdded {len(newly_found_for_queue)} NEW unique jobs to the posting queue. Queue size now: {len(pending_jobs_queue)}", flush=True)
    else:
        print("\n...No new unique jobs found this check cycle to add to the queue.", flush=True)

    # --- 4. Save updated seen list ---
    if len(seen_job_links) > original_seen_count:
        print(f"Saving updated seen jobs list ({len(seen_job_links)} total)...", flush=True)
        save_all_seen_jobs(seen_job_links)
        print("Save complete.", flush=True)
    else:
         print("No new links added to seen list, skipping save.", flush=True)


    last_check_time = time.ctime()
    print(f"[{last_check_time}] --- Finished Source Check Cycle ---", flush=True)

def post_one_job_from_queue():
    """Takes one job from the queue (if available) and attempts to post it."""
    global pending_jobs_queue, queue_lock
    print(f"[{time.ctime()}] --- Running Post Check (Every 2 Mins) ---", flush=True)

    job_to_post = None
    with queue_lock:
        if pending_jobs_queue:
            # Get job from the front of the queue
            job_to_post = pending_jobs_queue.popleft() 
            print(f"Attempting to post job from queue: {job_to_post['title']}. Jobs remaining: {len(pending_jobs_queue)}", flush=True)
        else:
            print("No jobs currently in the pending queue.", flush=True)

    if job_to_post:
        # Configure request timeouts for robustness
        request_config = telegram.request.HTTPXRequest(connect_timeout=15.0, read_timeout=30.0)
        bot_instance = telegram.Bot(token=TELEGRAM_TOKEN, request=request_config)
        
        # Run the async sending function in a managed event loop
        try:
            # Get the current event loop or create a new one if none exists
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Ensure the loop runs until the send task is complete
            success = loop.run_until_complete(send_job_to_channel(
                job_to_post['title'], job_to_post['link'], job_to_post['summary'], bot_instance
            ))

            # If sending failed (e.g., due to flood control), put the job back at the FRONT of the queue
            if not success:
                print(f"  > Posting failed for '{job_to_post['title']}'. Re-queuing at front.", flush=True)
                with queue_lock:
                    pending_jobs_queue.appendleft(job_to_post)

        except Exception as e:
            print(f"  > UNEXPECTED ERROR during job posting setup/execution: {type(e).__name__} - {e}", flush=True)
            print(f"  > Re-queuing job '{job_to_post['title']}' at front due to unexpected error.", flush=True)
            with queue_lock:
                pending_jobs_queue.appendleft(job_to_post) # Requeue at front on unexpected error too

    print(f"[{time.ctime()}] --- Finished Post Check ---", flush=True)


# --- 6. FLASK WEB SERVER ---
app = Flask(__name__)

@app.route('/')
def hello_world():
    """Basic health check endpoint for Uptime Robot."""
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
    """Runs the Flask app using Waitress (recommended) or Flask dev server."""
    port = int(os.environ.get('PORT', 10000))
    host = '0.0.0.0'
    print(f"Attempting to start web server on {host}:{port}", flush=True)
    try:
        if USE_WAITRESS:
            print("Using Waitress production server.", flush=True)
            serve(app, host=host, port=port, threads=4) # Use a few threads
        else:
            print("Using Flask development server (for testing only).", flush=True)
            app.run(host=host, port=port)
    except Exception as e:
        print(f"FATAL ERROR starting Flask/Waitress server: {e}", flush=True)
        # Optionally, try to shut down scheduler gracefully here if possible
        exit() # Exit if server fails to start

# --- 7. SCHEDULER & MAIN EXECUTION ---
if __name__ == "__main__":
    print("--- Initializing Job Agent ---", flush=True)
    print(f"--- Mode: RSS Feed + Unstop API (No Selenium) ---", flush=True)
    print(f"--- Schedule: Check sources every 1 hour, post from queue every 2 minutes ---", flush=True)
    
    # Run initial check before starting scheduler and server
    print("\nRunning initial source check before starting services...", flush=True)
    try: 
        check_sources_for_new_jobs()
    except Exception as e: 
        print(f"ERROR during initial check: {type(e).__name__} - {e}", flush=True)
    print("Initial check complete.\n", flush=True)

    # Initialize and start the background scheduler
    scheduler = BackgroundScheduler(timezone="UTC")
    try:
        # Schedule the source check every 1 HOUR
        scheduler.add_job(check_sources_for_new_jobs, 'interval', hours=1, id='source_check_task', replace_existing=True)
        
        # Schedule the job poster every 2 MINUTES
        scheduler.add_job(post_one_job_from_queue, 'interval', minutes=2, id='job_post_task', replace_existing=True)
        
        scheduler.start()
        print("APScheduler started successfully.", flush=True)
        print(f"Notifications configured for channel ID ending: ...{CHANNEL_CHAT_ID[-6:]}", flush=True)
    except Exception as e:
        print(f"FATAL ERROR starting APScheduler: {e}", flush=True); exit()

    # Start the Flask web server (this will block the main thread)
    print("Starting Flask/Waitress web server...", flush=True)
    run_flask_app() 

    # --- Graceful Shutdown (if web server stops) ---
    print("Web server has stopped. Attempting graceful shutdown of scheduler...", flush=True)
    try: 
        scheduler.shutdown()
        print("Scheduler shut down successfully.", flush=True)
    except Exception as e: 
        print(f"Error shutting down scheduler: {e}", flush=True)
    
    print("Job Agent shut down complete.", flush=True)
