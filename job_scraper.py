"""
JobRadar Bot - Core Scraper Engine
===================================
Scrapes jobs from multiple sources, matches against Adarsh's resume,
sends Gmail alerts, and logs to Google Sheets.

Sources: Naukri RSS, LinkedIn (via Apify free tier), Indeed, Internshala,
         Superset, Cutshort, Wellfound (AngelList), company career pages
"""

import os
import json
import time
import hashlib
import logging
import smtplib
import requests
import feedparser
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────
# CONFIGURATION — Fill these in .env or Railway
# ─────────────────────────────────────────────
CONFIG = {
    "GMAIL_ADDRESS":    os.getenv("GMAIL_ADDRESS",    "pasula.adarshreddy@gmail.com"),
    "GMAIL_APP_PASS":   os.getenv("GMAIL_APP_PASS",   ""),          # Gmail App Password
    "ALERT_EMAIL":      os.getenv("ALERT_EMAIL",      "pasula.adarshreddy@gmail.com"),
    "SHEETS_CREDS":     os.getenv("SHEETS_CREDS",     ""),          # JSON string of service account
    "SHEETS_ID":        os.getenv("SHEETS_ID",        ""),          # Google Sheet ID
    "SCAN_INTERVAL":    int(os.getenv("SCAN_INTERVAL","1800")),      # seconds (30 min default)
    "MIN_MATCH_SCORE":  int(os.getenv("MIN_MATCH_SCORE","55")),      # only alert if match >= this
}

# ─────────────────────────────────────────────
# ADARSH'S RESUME PROFILE
# ─────────────────────────────────────────────
RESUME = {
    "name": "P. Adarsh Reddy",
    "email": "pasula.adarshreddy@gmail.com",
    "degree": "BBA Business Analytics",
    "university": "Christ University, Pune Lavasa (2023-26)",
    "experience_level": "fresher",  # 0-1 year
    "skills": [
        "sql", "power bi", "excel", "advanced excel", "python",
        "data visualization", "pivot tables", "kpi tracking",
        "reporting", "reporting automation", "crm", "tally erp",
        "sas", "stakeholder management", "process improvement",
        "data validation", "business analytics", "financial analytics",
        "mis", "dashboard", "data analysis", "business intelligence",
        "business performance analysis"
    ],
    "target_roles": [
        "business analyst", "data analyst", "reporting analyst",
        "operations analyst", "risk analyst", "finance analyst",
        "associate trainee", "mis analyst", "analytics",
        "management trainee", "process analyst", "graduate trainee",
        "junior analyst", "analyst trainee", "business operations"
    ],
    "priority_locations": ["hyderabad", "bengaluru", "bangalore", "chennai", "kochi", "kerala"],
    "acceptable_locations": [
        "mumbai", "pune", "delhi", "ahmedabad", "gujarat",
        "noida", "gurgaon", "remote", "work from home", "wfh"
    ],
    "avoid_companies": [
        # Already rejected — avoid reapplying
        "deloitte", "citi", "pwc", "infosys", "deutsche bank",
        "tophire", "amazon", "mckinsey", "atica", "exl service",
        "oracle", "wells fargo"
    ],
    "max_experience_years": 2,  # Only fresher/0-2 yr roles
}

# ─────────────────────────────────────────────
# JOB SEARCH QUERIES
# ─────────────────────────────────────────────
SEARCH_QUERIES = [
    "business analyst fresher",
    "data analyst fresher 0-1 year",
    "reporting analyst fresher",
    "operations analyst fresher",
    "risk analyst fresher",
    "finance analyst fresher",
    "MIS analyst fresher",
    "associate trainee business analytics",
    "management trainee analytics",
    "SQL Power BI fresher analyst",
    "excel analyst fresher 2026",
    "business intelligence analyst fresher",
]

LOCATION_QUERIES = [
    "Hyderabad", "Bengaluru", "Chennai", "Kochi",
    "Mumbai", "Pune", "Delhi", "Remote"
]

# ─────────────────────────────────────────────
# RSS FEED SOURCES (legal, no scraping needed)
# ─────────────────────────────────────────────
RSS_FEEDS = [
    # Indeed RSS feeds for each role + location combo
    "https://in.indeed.com/rss?q=business+analyst+fresher&l=Hyderabad&sort=date",
    "https://in.indeed.com/rss?q=business+analyst+fresher&l=Bengaluru&sort=date",
    "https://in.indeed.com/rss?q=data+analyst+fresher&l=Hyderabad&sort=date",
    "https://in.indeed.com/rss?q=reporting+analyst+fresher&l=&sort=date",
    "https://in.indeed.com/rss?q=operations+analyst+fresher&l=&sort=date",
    "https://in.indeed.com/rss?q=MIS+analyst+fresher&l=Hyderabad&sort=date",
    "https://in.indeed.com/rss?q=associate+trainee+analyst&l=&sort=date",
    "https://in.indeed.com/rss?q=risk+analyst+fresher&l=&sort=date",
    "https://in.indeed.com/rss?q=finance+analyst+fresher&l=&sort=date",
    "https://in.indeed.com/rss?q=SQL+power+bi+analyst+fresher&l=&sort=date",
    # TimesJobs RSS
    "https://www.timesjobs.com/candidate/job-search.html?txtKeywords=business+analyst&txtLocation=Hyderabad&rss=1",
    "https://www.timesjobs.com/candidate/job-search.html?txtKeywords=data+analyst+fresher&rss=1",
    # Shine RSS
    "https://www.shine.com/job-search/business-analyst-jobs-in-hyderabad/?rss=1",
    "https://www.shine.com/job-search/data-analyst-fresher/?rss=1",
]

# ─────────────────────────────────────────────
# SCRAPER CLASS
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('jobradar.log')
    ]
)
log = logging.getLogger(__name__)

seen_jobs = set()  # In-memory dedup; persisted to seen_jobs.json

def load_seen_jobs():
    """Load previously seen job IDs to avoid duplicate alerts."""
    global seen_jobs
    try:
        with open("seen_jobs.json", "r") as f:
            seen_jobs = set(json.load(f))
        log.info(f"Loaded {len(seen_jobs)} previously seen jobs")
    except FileNotFoundError:
        seen_jobs = set()

def save_seen_jobs():
    """Persist seen job IDs."""
    with open("seen_jobs.json", "w") as f:
        json.dump(list(seen_jobs), f)

def job_id(job: dict) -> str:
    """Generate unique ID for a job to avoid duplicate alerts."""
    key = f"{job.get('company','')}-{job.get('title','')}-{job.get('location','')}"
    return hashlib.md5(key.lower().encode()).hexdigest()

# ─────────────────────────────────────────────
# MATCH SCORING ENGINE
# ─────────────────────────────────────────────
def compute_match_score(job: dict) -> dict:
    """
    Score how well a job matches Adarsh's resume.
    Returns score (0-100) + breakdown.
    """
    score = 0
    breakdown = {}

    title = (job.get("title", "") + " " + job.get("description", "")).lower()
    location = job.get("location", "").lower()
    company = job.get("company", "").lower()
    exp_text = job.get("experience", "").lower()

    # ── Check if already rejected company ──
    for avoid in RESUME["avoid_companies"]:
        if avoid in company:
            return {"score": 0, "skip": True, "reason": f"Already rejected by {company}"}

    # ── Experience check (only fresher/0-2yr roles) ──
    if any(x in exp_text for x in ["5 year", "6 year", "7 year", "8 year", "10 year",
                                    "5+ year", "senior", "manager", "lead"]):
        return {"score": 0, "skip": True, "reason": "Requires too much experience"}

    # ── Skill matching (40 points) ──
    matched_skills = []
    for skill in RESUME["skills"]:
        if skill in title:
            matched_skills.append(skill)
    skill_score = min(40, len(matched_skills) * 5)
    score += skill_score
    breakdown["skills"] = {"matched": matched_skills, "score": skill_score}

    # ── Role relevance (30 points) ──
    role_score = 0
    matched_roles = []
    for role in RESUME["target_roles"]:
        if role in title:
            matched_roles.append(role)
            role_score = min(30, role_score + 15)
    score += role_score
    breakdown["roles"] = {"matched": matched_roles, "score": role_score}

    # ── Location preference (20 points) ──
    loc_score = 0
    if any(loc in location for loc in RESUME["priority_locations"]):
        loc_score = 20
    elif any(loc in location for loc in RESUME["acceptable_locations"]):
        loc_score = 10
    score += loc_score
    breakdown["location"] = {"score": loc_score}

    # ── Freshness bonus (10 points) ──
    hours_old = job.get("hours_old", 999)
    if hours_old <= 6:
        score += 10
    elif hours_old <= 24:
        score += 7
    elif hours_old <= 48:
        score += 4
    breakdown["freshness"] = {"hours_old": hours_old}

    return {
        "score": min(score, 99),
        "skip": False,
        "breakdown": breakdown,
        "matched_skills": matched_skills
    }

# ─────────────────────────────────────────────
# SCRAPERS
# ─────────────────────────────────────────────
def scrape_rss_feeds() -> list:
    """Scrape all RSS feeds for new job postings."""
    jobs = []
    headers = {"User-Agent": "Mozilla/5.0 (compatible; JobRadarBot/1.0)"}

    for feed_url in RSS_FEEDS:
        try:
            log.info(f"Fetching RSS: {feed_url[:60]}...")
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:20]:  # Max 20 per feed
                job = {
                    "title":       entry.get("title", "Unknown Role"),
                    "company":     entry.get("author", entry.get("source", {}).get("title", "Unknown")),
                    "location":    entry.get("location", "India"),
                    "description": BeautifulSoup(entry.get("summary", ""), "html.parser").get_text()[:800],
                    "url":         entry.get("link", ""),
                    "source":      "Indeed/RSS",
                    "posted_date": entry.get("published", ""),
                    "experience":  "",
                    "hours_old":   _hours_since(entry.get("published", "")),
                }
                jobs.append(job)
            time.sleep(0.5)  # Be respectful
        except Exception as e:
            log.warning(f"RSS feed failed: {feed_url[:40]} — {e}")

    log.info(f"RSS scraper found {len(jobs)} raw jobs")
    return jobs

def scrape_naukri() -> list:
    """
    Scrape Naukri.com jobs via their public search.
    Uses lightweight requests — no Selenium needed.
    """
    jobs = []
    base = "https://www.naukri.com/jobapi/v3/search"
    queries = [
        ("business analyst", "fresher"),
        ("data analyst", "fresher"),
        ("reporting analyst", "0"),
        ("operations analyst", "0"),
        ("mis analyst", "0"),
    ]
    headers = {
        "User-Agent":    "Mozilla/5.0",
        "appid":         "109",
        "systemid":      "109",
        "Content-Type":  "application/json",
    }
    for role, exp in queries:
        try:
            params = {
                "noOfResults": 20,
                "urlType": "search_by_keyword",
                "searchType": "adv",
                "keyword": role,
                "jobAge": 3,       # Last 3 days
                "experience": exp,
                "location": "Hyderabad,Bengaluru,Chennai,Mumbai,Pune,Delhi",
                "k": role,
                "l": "Hyderabad",
                "sort": "1",       # Sort by date
            }
            r = requests.get(base, headers=headers, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                for j in data.get("jobDetails", []):
                    jobs.append({
                        "title":       j.get("title", ""),
                        "company":     j.get("companyName", ""),
                        "location":    ", ".join(j.get("placeholders", [{}])[0].get("label", "").split(",")[:2]),
                        "description": j.get("jobDescription", "")[:800],
                        "url":         f"https://www.naukri.com{j.get('jdURL','')}",
                        "source":      "Naukri",
                        "experience":  j.get("experienceText", ""),
                        "hours_old":   int(j.get("footerPlaceholderLabel", "48 days").split()[0] if j.get("footerPlaceholderLabel","").split()[0].isdigit() else 48),
                        "posted_date": "",
                        "applicants":  j.get("totalApplicants", 0),
                    })
            time.sleep(1)
        except Exception as e:
            log.warning(f"Naukri scrape failed for '{role}': {e}")

    log.info(f"Naukri scraper found {len(jobs)} raw jobs")
    return jobs

def scrape_internshala() -> list:
    """Scrape Internshala for fresher/entry-level jobs."""
    jobs = []
    urls = [
        "https://internshala.com/jobs/business-analyst-jobs/",
        "https://internshala.com/jobs/data-analyst-jobs/",
        "https://internshala.com/jobs/operations-jobs/",
        "https://internshala.com/jobs/finance-jobs/",
    ]
    headers = {"User-Agent": "Mozilla/5.0"}
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(r.content, "html.parser")
            cards = soup.find_all("div", class_="individual_internship")[:10]
            for card in cards:
                title    = card.find("h3")
                company  = card.find("p", class_="company-name")
                location = card.find("p", class_="location_link")
                link_tag = card.find("a", class_="view_detail_button")
                desc_tag = card.find("div", class_="internship_other_details_container")
                jobs.append({
                    "title":       title.text.strip() if title else "",
                    "company":     company.text.strip() if company else "",
                    "location":    location.text.strip() if location else "India",
                    "description": desc_tag.text.strip()[:500] if desc_tag else "",
                    "url":         "https://internshala.com" + link_tag["href"] if link_tag else url,
                    "source":      "Internshala",
                    "experience":  "fresher",
                    "hours_old":   24,
                    "posted_date": "",
                })
            time.sleep(1)
        except Exception as e:
            log.warning(f"Internshala failed: {e}")

    log.info(f"Internshala found {len(jobs)} raw jobs")
    return jobs

def scrape_wellfound() -> list:
    """Scrape Wellfound (AngelList) for startup jobs."""
    jobs = []
    # Wellfound has a public GraphQL API
    url = "https://wellfound.com/role/l/business-analyst/india"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.content, "html.parser")
        listings = soup.find_all("div", attrs={"data-test": "JobListing"})[:15]
        for item in listings:
            title   = item.find("a", attrs={"data-test": "JobTitle"})
            company = item.find("a", attrs={"data-test": "StartupName"})
            loc     = item.find("span", attrs={"data-test": "Location"})
            jobs.append({
                "title":       title.text.strip() if title else "",
                "company":     company.text.strip() if company else "",
                "location":    loc.text.strip() if loc else "Remote / India",
                "description": "",
                "url":         "https://wellfound.com" + title["href"] if title and title.get("href") else url,
                "source":      "Wellfound",
                "experience":  "0-2 years",
                "hours_old":   48,
                "posted_date": "",
                "type":        "Startup",
            })
    except Exception as e:
        log.warning(f"Wellfound scrape failed: {e}")

    log.info(f"Wellfound found {len(jobs)} raw jobs")
    return jobs

def scrape_cutshort() -> list:
    """Scrape Cutshort startup jobs via their public API."""
    jobs = []
    try:
        url = "https://cutshort.io/api/jobs/search"
        payload = {
            "keywords": ["business analyst", "data analyst", "reporting analyst"],
            "minExperience": 0,
            "maxExperience": 2,
            "locations": ["Hyderabad", "Bengaluru", "Chennai", "Mumbai", "Remote"],
            "size": 20,
        }
        r = requests.post(url, json=payload, timeout=10,
                          headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"})
        if r.status_code == 200:
            for j in r.json().get("data", []):
                jobs.append({
                    "title":       j.get("title", ""),
                    "company":     j.get("company", {}).get("name", ""),
                    "location":    ", ".join(j.get("locations", ["Remote"])),
                    "description": j.get("description", "")[:500],
                    "url":         f"https://cutshort.io/job/{j.get('slug','')}",
                    "source":      "Cutshort",
                    "experience":  f"{j.get('minExp',0)}-{j.get('maxExp',2)} years",
                    "hours_old":   24,
                    "posted_date": j.get("postedAt", ""),
                    "type":        "Startup",
                })
    except Exception as e:
        log.warning(f"Cutshort API failed: {e}")

    log.info(f"Cutshort found {len(jobs)} raw jobs")
    return jobs

# ─────────────────────────────────────────────
# EMAIL ALERT SYSTEM
# ─────────────────────────────────────────────
def send_job_alert(job: dict, match: dict):
    """Send a beautifully formatted job alert email to Adarsh."""
    if not CONFIG["GMAIL_APP_PASS"]:
        log.warning("No Gmail App Password set — skipping email send")
        return False

    match_score   = match["score"]
    matched_skills = match.get("matched_skills", [])
    is_priority   = any(loc in job.get("location","").lower() for loc in RESUME["priority_locations"])
    applicants    = job.get("applicants", "N/A")
    low_comp      = isinstance(applicants, int) and applicants < 100

    subject = f"🎯 [{match_score}% Match] {job['company']} — {job['title']}"
    if low_comp:
        subject = f"🔥 LOW COMPETITION · " + subject

    html_body = f"""
<!DOCTYPE html>
<html>
<head>
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #f4f4f8; margin: 0; padding: 20px; }}
  .container {{ max-width: 600px; margin: 0 auto; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.08); }}
  .header {{ background: linear-gradient(135deg, #7c6dfa, #9c5cfa); padding: 28px 30px; }}
  .header h1 {{ color: white; margin: 0; font-size: 20px; }}
  .header p {{ color: rgba(255,255,255,0.8); margin: 6px 0 0; font-size: 13px; }}
  .score-badge {{ display: inline-block; background: rgba(255,255,255,0.25); color: white; padding: 6px 14px; border-radius: 20px; font-size: 14px; font-weight: bold; margin-top: 10px; }}
  .body {{ padding: 28px 30px; }}
  .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin: 20px 0; }}
  .info-item {{ background: #f8f8fc; border-radius: 8px; padding: 12px 16px; }}
  .info-label {{ font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }}
  .info-value {{ font-size: 14px; font-weight: 600; color: #333; }}
  .skills-section {{ margin: 20px 0; }}
  .skill-chip {{ display: inline-block; padding: 4px 10px; border-radius: 12px; font-size: 12px; margin: 3px; background: #ede9fe; color: #7c6dfa; border: 1px solid #c4b5fd; }}
  .desc-box {{ background: #fafafa; border-left: 3px solid #7c6dfa; padding: 14px 18px; border-radius: 0 8px 8px 0; margin: 20px 0; font-size: 13px; color: #555; line-height: 1.7; }}
  .alert-box {{ background: #fef3c7; border: 1px solid #fbbf24; border-radius: 8px; padding: 12px 16px; margin: 14px 0; font-size: 13px; color: #92400e; }}
  .cta-btn {{ display: block; text-align: center; background: linear-gradient(135deg, #7c6dfa, #9c5cfa); color: white; text-decoration: none; padding: 14px 28px; border-radius: 8px; font-size: 16px; font-weight: 700; margin: 24px 0 10px; }}
  .footer {{ background: #f8f8fc; padding: 16px 30px; font-size: 12px; color: #999; text-align: center; }}
  .priority-tag {{ background: #dcfce7; color: #166534; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; margin-left: 6px; }}
  .source-tag {{ background: #e0e7ff; color: #3730a3; padding: 3px 10px; border-radius: 12px; font-size: 12px; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <p style="margin:0;font-size:11px;color:rgba(255,255,255,0.7);text-transform:uppercase;letter-spacing:1px">JobRadar Alert · {datetime.now().strftime("%d %b %Y, %I:%M %p")}</p>
    <h1 style="margin:8px 0 4px">{job['company']}</h1>
    <p style="font-size:16px;color:white;margin:0">{job['title']}</p>
    <div class="score-badge">🎯 {match_score}% Resume Match</div>
    {'<div class="score-badge" style="background:rgba(251,191,36,0.4);margin-left:8px">🔥 Low Competition</div>' if low_comp else ''}
  </div>

  <div class="body">
    {"<div class='alert-box'>⚡ <strong>APPLY NOW!</strong> Only " + str(applicants) + " applicants so far — early applicants get 3x more visibility!</div>" if low_comp else ""}
    {'<div style="background:#dcfce7;border:1px solid #86efac;border-radius:8px;padding:10px 14px;font-size:13px;color:#166534;margin-bottom:14px">📍 <strong>South India Priority Location</strong> — High preference match!</div>' if is_priority else ''}

    <div class="info-grid">
      <div class="info-item">
        <div class="info-label">Company</div>
        <div class="info-value">{job['company']}</div>
      </div>
      <div class="info-item">
        <div class="info-label">Role</div>
        <div class="info-value">{job['title']}</div>
      </div>
      <div class="info-item">
        <div class="info-label">Location</div>
        <div class="info-value">{job.get('location','Not specified')}</div>
      </div>
      <div class="info-item">
        <div class="info-label">Experience Required</div>
        <div class="info-value">{job.get('experience','Fresher / 0-2 yrs')}</div>
      </div>
      <div class="info-item">
        <div class="info-label">Applicants So Far</div>
        <div class="info-value">{applicants if applicants != 'N/A' else 'Not available'}</div>
      </div>
      <div class="info-item">
        <div class="info-label">Posted</div>
        <div class="info-value">{job.get('hours_old','?')}h ago</div>
      </div>
    </div>

    <div class="skills-section">
      <p style="font-size:13px;font-weight:600;color:#333;margin-bottom:8px">✅ Your Matching Skills:</p>
      {''.join(f'<span class="skill-chip">✓ {s}</span>' for s in matched_skills) if matched_skills else '<span style="font-size:13px;color:#888">General match based on role keywords</span>'}
    </div>

    <div class="desc-box">
      <strong style="display:block;margin-bottom:6px;color:#333">Job Description:</strong>
      {job.get('description','No description available.')[:600]}{'...' if len(job.get('description',''))>600 else ''}
    </div>

    <div style="margin:16px 0;font-size:13px;color:#666">
      <span class="source-tag">📡 Source: {job.get('source','Unknown')}</span>
    </div>

    <a href="{job.get('url','#')}" class="cta-btn">🚀 Apply Now →</a>

    <p style="font-size:12px;color:#999;text-align:center">
      Quick tip: Tailor your resume summary to mention "{job['company']}" and highlight your Power BI/SQL experience for this role.
    </p>
  </div>
  <div class="footer">
    JobRadar Bot · Running 24/7 for Adarsh · pasula.adarshreddy@gmail.com<br>
    Match score based on: skills ({match.get('breakdown',{}).get('skills',{}).get('score',0)}/40) · role ({match.get('breakdown',{}).get('roles',{}).get('score',0)}/30) · location ({match.get('breakdown',{}).get('location',{}).get('score',0)}/20) · freshness (bonus)
  </div>
</div>
</body>
</html>
"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = CONFIG["GMAIL_ADDRESS"]
        msg["To"]      = CONFIG["ALERT_EMAIL"]
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(CONFIG["GMAIL_ADDRESS"], CONFIG["GMAIL_APP_PASS"])
            server.sendmail(CONFIG["GMAIL_ADDRESS"], CONFIG["ALERT_EMAIL"], msg.as_string())

        log.info(f"✅ Alert sent: {job['company']} — {job['title']} ({match_score}% match)")
        return True
    except Exception as e:
        log.error(f"❌ Email send failed: {e}")
        return False

# ─────────────────────────────────────────────
# GOOGLE SHEETS TRACKER
# ─────────────────────────────────────────────
def log_to_sheets(job: dict, match: dict):
    """Log job alert to Google Sheets for tracking."""
    if not CONFIG["SHEETS_CREDS"] or not CONFIG["SHEETS_ID"]:
        log.info("Google Sheets not configured — skipping sheet log")
        return

    try:
        creds_dict = json.loads(CONFIG["SHEETS_CREDS"])
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(CONFIG["SHEETS_ID"]).sheet1

        # Add header if sheet is empty
        if sheet.row_count == 0 or sheet.cell(1, 1).value is None:
            sheet.append_row([
                "Date Alerted", "Company", "Role", "Industry/Type",
                "Location", "Experience", "Match Score", "Matched Skills",
                "Applicants", "Source", "Apply URL", "Status"
            ])

        sheet.append_row([
            datetime.now().strftime("%d-%b-%Y %H:%M"),
            job.get("company", ""),
            job.get("title", ""),
            job.get("type", job.get("industry", "N/A")),
            job.get("location", ""),
            job.get("experience", "Fresher"),
            f"{match['score']}%",
            ", ".join(match.get("matched_skills", [])),
            str(job.get("applicants", "N/A")),
            job.get("source", ""),
            job.get("url", ""),
            "Alerted",
        ])
        log.info(f"📊 Logged to Google Sheets: {job['company']}")
    except Exception as e:
        log.warning(f"Sheets log failed: {e}")

# ─────────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────────
def _hours_since(date_str: str) -> int:
    """Convert a date string to hours since posted."""
    if not date_str:
        return 999
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        delta = datetime.now(dt.tzinfo) - dt
        return int(delta.total_seconds() / 3600)
    except:
        return 999

# ─────────────────────────────────────────────
# MAIN SCAN LOOP
# ─────────────────────────────────────────────
def run_single_scan():
    """Run one full scan across all sources."""
    log.info("=" * 60)
    log.info(f"🔍 Starting scan at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    all_jobs = []

    # Collect from all sources
    all_jobs += scrape_rss_feeds()
    all_jobs += scrape_naukri()
    all_jobs += scrape_internshala()
    all_jobs += scrape_wellfound()
    all_jobs += scrape_cutshort()

    log.info(f"📦 Total raw jobs collected: {len(all_jobs)}")

    alerts_sent = 0
    for job in all_jobs:
        if not job.get("title") or not job.get("company"):
            continue

        jid = job_id(job)
        if jid in seen_jobs:
            continue  # Already alerted

        match = compute_match_score(job)

        if match.get("skip"):
            log.debug(f"Skipped: {job['company']} — {match.get('reason','')}")
            continue

        if match["score"] < CONFIG["MIN_MATCH_SCORE"]:
            log.debug(f"Low match ({match['score']}%): {job['company']} — {job['title']}")
            continue

        # New job with good match — alert!
        log.info(f"🎯 Match found: {job['company']} — {job['title']} ({match['score']}%)")
        sent = send_job_alert(job, match)
        log_to_sheets(job, match)

        seen_jobs.add(jid)
        alerts_sent += 1

        time.sleep(0.5)  # Don't spam emails

    save_seen_jobs()
    log.info(f"✅ Scan complete. Alerts sent: {alerts_sent} | Total seen: {len(seen_jobs)}")
    return alerts_sent

def run_forever():
    """Main loop — runs continuously."""
    load_seen_jobs()
    log.info(f"🤖 JobRadar Bot started for {RESUME['name']}")
    log.info(f"📧 Alerts → {CONFIG['ALERT_EMAIL']}")
    log.info(f"⏱ Scan interval: {CONFIG['SCAN_INTERVAL']}s ({CONFIG['SCAN_INTERVAL']//60} min)")
    log.info(f"🎯 Min match score: {CONFIG['MIN_MATCH_SCORE']}%")

    while True:
        try:
            run_single_scan()
        except KeyboardInterrupt:
            log.info("Bot stopped by user.")
            break
        except Exception as e:
            log.error(f"Scan error: {e}")

        log.info(f"💤 Sleeping {CONFIG['SCAN_INTERVAL']//60} minutes until next scan...")
        time.sleep(CONFIG["SCAN_INTERVAL"])

if __name__ == "__main__":
    run_forever()
