import os
import json
import gspread
import pandas as pd
import re
import datetime
import time
from urllib.parse import urlparse
from google import genai
from google.genai import types
from openai import OpenAI
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

try:
    from pyvirtualdisplay import Display
    HAS_VIRTUAL_DISPLAY = True
except ImportError:
    HAS_VIRTUAL_DISPLAY = False

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

# -------------------- CONFIG --------------------
STOP_REQUESTED = False  # Global flag for API-based stopping

load_dotenv()

LOG_FILE = f"research_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
SHEET_ID = "1D7g1d0ayXFABI2RA_N3_vMB7UaqH6j_vuLeGnMhGYZ8"
SHEET_NAME = "Expired LEI's"
CREDS_FILE = "service_account.json"

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "GEMINI").upper()
google_client = None
openai_client = None

if LLM_PROVIDER == "GEMINI":
    GENAI_API_KEY = os.getenv("GOOGLE_API_KEY")
    google_client = genai.Client(api_key=GENAI_API_KEY)
    MODEL_ID = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
else:
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    openai_client = OpenAI(api_key=OPENAI_API_KEY)
    MODEL_ID = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

BLOCKED_DOMAINS = [
    "indiamart.com",
    "justdial.com",
    "sulekha.com",
    "facebook.com",
    "instagram.com",
    "maps.google.com",
    "wisebooks.com",
    "falconebiz.com",
    "google.com/maps",
    "leikart.com",
    "economictimes.com",
    "economictimes.indiatimes.com",
    "linkedin.com",
    "indialei.in",
    "credhive.in",
    "tracxn.com",
    "legalentityidentifier.in",
]
DIRECTORY_DOMAINS = [
    "zaubacorp.com",
    "tofler.in",
    "indiafilings.com",
    "thecompanycheck.com",
    "falconebiz.com",
    "company-profile",
    "onrender.com",
]
FORBIDDEN_PHONE = "8800973322"
# Serper keys will be initialized after the log function is defined
SERPER_KEYS = []
current_serper_key_index = 0

def get_current_serper_key():
    global current_serper_key_index
    if current_serper_key_index < len(SERPER_KEYS):
        return SERPER_KEYS[current_serper_key_index]
    return None

def rotate_serper_key(is_manual=False):
    global current_serper_key_index
    if not SERPER_KEYS:
        return False
    
    if is_manual:
        # Circular for manual calls (back to 1 after last)
        current_serper_key_index = (current_serper_key_index + 1) % len(SERPER_KEYS)
        log(f"🔄 Manually rotating to Serper Key #{current_serper_key_index + 1}...")
        return True
    else:
        # Linear for automatic calls (stop after last key)
        if current_serper_key_index + 1 < len(SERPER_KEYS):
            current_serper_key_index += 1
            log(f"🔄 Serper quota exceeded. Rotating to Key #{current_serper_key_index + 1}...")
            return True
        else:
            return False

FILTERS = {
    "Enrichment Status": "pending",
    "Status": "LAPSED",
}

# -------------------- LOGGING --------------------

def log(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_msg = f"[{timestamp}] {message}"
    print(formatted_msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(formatted_msg + "\n")

# -------------------- INITIALIZE SERPER KEYS --------------------
def initialize_serper_keys():
    global SERPER_KEYS
    found_keys_dict = {}
    
    # Scan all environment variables for Serper keys
    for env_key, value in os.environ.items():
        # Check for primary key
        if env_key == "SERPER_API_KEY":
            if value and value.strip():
                found_keys_dict[0] = value.strip()
            else:
                log("Serper Key #1 (Primary) is defined but EMPTY in .env")
        
        # Check for numbered keys (e.g., SERPER_API_KEY_1)
        elif env_key.startswith("SERPER_API_KEY_"):
            suffix = env_key.replace("SERPER_API_KEY_", "")
            try:
                num = int(suffix)
                if value and value.strip():
                    found_keys_dict[num] = value.strip()
                else:
                    log(f"Serper Key #{num+1} ({env_key}) is defined but EMPTY in .env")
            except ValueError:
                # Handle non-numeric suffixes if any (e.g., SERPER_API_KEY_TEST)
                if value and value.strip():
                    found_keys_dict[env_key] = value.strip()
                else:
                    log(f"Serper Key ({env_key}) is defined but EMPTY in .env")
            
    # Sort numeric keys in order (0, 1, 2...)
    sorted_indices = sorted([k for k in found_keys_dict.keys() if isinstance(k, int)])
    # Add any non-numeric keys at the end
    non_numeric = sorted([k for k in found_keys_dict.keys() if not isinstance(k, int)])
    
    SERPER_KEYS = [found_keys_dict[i] for i in sorted_indices] + [found_keys_dict[k] for k in non_numeric]
    
    if not SERPER_KEYS:
        log("❌ CRITICAL: No valid Serper API keys found in environment! Search functionality will fail.")
    else:
        log(f"✅ Dynamically loaded {len(SERPER_KEYS)} valid Serper API keys from environment.")

initialize_serper_keys()

def fetch_zaubacorp(url, p):
    if STOP_REQUESTED:
        log("🛑 Stop signal received. Aborting Zauba fetch...")
        return None
    log(f"🚀 ZaubaCorp detected. Switching to HEADFUL mode with Virtual Display for {url}...")
    log(f"Note: The main headless browser will remain idle during this specialized fetch.")
    vdisplay = None
    browser = None
    try:
        # The virtual display is now started globally in main() to ensure stability
        browser = p.chromium.launch(
            headless=False,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
            ]
        )
        context = browser.new_context(
            viewport={'width': 1080, 'height': 720},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        Stealth().apply_stealth_sync(page)
        
        # Block unnecessary resources to save memory/bandwidth
        def route_intercept(route):
            if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
                route.abort()
            else:
                route.continue_()
        page.route("**/*", route_intercept)

        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        # Check for Cloudflare
        title = page.title().lower()
        if "just a moment" in title or "attention required" in title or "cloudflare" in title:
            log("Cloudflare detected on ZaubaCorp. Waiting 10s for auto-bypass...")
            time.sleep(10)
        
        html = page.content()
        return html
    except Exception as e:
        log(f"ZaubaCorp fetch error: {e}")
        return None
    finally:
        if browser:
            try:
                browser.close()
            except:
                pass
        # Only closing the browser; the virtual display remains running in the background

# -------------------- HELPERS --------------------

def safe_clean_text(text):
    return re.sub(r"\s+", " ", text).strip()

def setup_sheets():
    if not os.path.exists(CREDS_FILE):
        raise Exception(f"Missing {CREDS_FILE}.")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

def setup_playwright_page(p):
    log("Initializing Playwright (Sync) with stealth mode...")
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ]
    )
    context = browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    )
    page = context.new_page()
    
    # Extra stealth headers
    page.set_extra_http_headers({
        "Accept-Language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Chromium";v="123", "Google Chrome";v="123", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    })
    
    Stealth().apply_stealth_sync(page)
    
    blocked_hosts = [
        "google-analytics.com",
        "analytics.google.com",
        "googletagmanager.com",
        "connect.facebook.net",
        "www.googleadservices.com",
        "googleads.g.doubleclick.net",
        "bat.bing.com",
        "cdn.krxd.net",
    ]
    
    def route_intercept(route):
        if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
            route.abort()
        elif any(domain in route.request.url for domain in blocked_hosts):
            route.abort()
        else:
            route.continue_()
            
    page.route("**/*", route_intercept)
    page.set_default_timeout(60000)
    return browser, context, page

def call_llm(prompt, text_content="", use_search=False, page=None):
    full_prompt = f"{prompt}\n\nCONTENT:\n{text_content}"
    try:
        if LLM_PROVIDER == "GEMINI":
            config = None
            if use_search:
                config = types.GenerateContentConfig(
                    tools=[
                        types.Tool(
                            google_search_retrieval=types.GoogleSearchRetrieval()
                        )
                    ]
                )
            # genai client is sync-compatible
            try:
                response = google_client.models.generate_content(
                    model=MODEL_ID,
                    contents=full_prompt,
                    config=config,
                )
                return response.text
            except Exception as e:
                error_msg = str(e).lower()
                log(f"Gemini Error: {e}")
                if "quota" in error_msg or "429" in error_msg:
                    log("CRITICAL: Gemini API Quota reached! Stopping.")
                    raise Exception("Gemini API Quota reached")
                return None
        else:
            # OpenAI Implementation
            retries = 3
            for attempt in range(retries):
                try:
                    current_messages = [{"role": "user", "content": full_prompt}]
                    tools = None
                    if use_search and page:
                        tools = [
                            {
                                "type": "function",
                                "function": {
                                    "name": "google_search",
                                    "description": "Perform a Google search to find relevant URLs or information.",
                                    "parameters": {
                                        "type": "object",
                                        "properties": {
                                            "query": {
                                                "type": "string",
                                                "description": "The search query.",
                                            }
                                        },
                                        "required": ["query"],
                                    },
                                },
                            }
                        ]

                    response = openai_client.chat.completions.create(
                        model=MODEL_ID,
                        messages=current_messages,
                        tools=tools,
                        max_tokens=800,
                        response_format=(
                            {"type": "json_object"} if "JSON" in prompt else None
                        ),
                    )
                    msg = response.choices[0].message
                    
                    if msg.tool_calls:
                        for tool_call in msg.tool_calls:
                            if tool_call.function.name == "google_search":
                                args = json.loads(tool_call.function.arguments)
                                search_results = serper_search(args["query"])
                                current_messages.append(msg)
                                current_messages.append(
                                    {
                                        "role": "tool",
                                        "tool_call_id": tool_call.id,
                                        "content": json.dumps({"results": search_results}),
                                    }
                                )

                        response = openai_client.chat.completions.create(
                            model=MODEL_ID,
                            messages=current_messages,
                            max_tokens=800,
                            response_format=(
                                {"type": "json_object"} if "JSON" in prompt else None
                            ),
                        )
                        msg = response.choices[0].message

                    content = msg.content
                    if content and content.strip():
                        return content
                    else:
                        log(f"Attempt {attempt+1}: Empty response from OpenAI.")

                except Exception as e:
                    error_msg = str(e).lower()
                    log(f"Attempt {attempt+1}: OpenAI Error: {e}")
                    if "quota" in error_msg or "429" in error_msg or "rate limit" in error_msg:
                        log("CRITICAL: API Quota reached or Rate Limited! Stopping.")
                        raise Exception("OpenAI API Quota or Rate Limit reached")
                    if attempt < retries - 1:
                        wait_time = (attempt + 1) * 5
                        log(f"Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                    else:
                        return None
            return None
    except Exception as e:
        log(f"Global LLM Error: {e}")
        return None

def agent_process_page(html, company_details, source_url, page, is_official=False):
    soup = BeautifulSoup(html, "lxml")
    page_title = soup.title.string if soup.title else "No Title"
    top_headers = " | ".join([h.get_text(strip=True) for h in soup.find_all(["h1", "h2"])[:3]])
    
    emails_from_links = [
        a["href"].replace("mailto:", "").split("?")[0]
        for a in soup.find_all("a", href=True)
        if a["href"].startswith("mailto:")
    ]
    phones_from_links = [
        "".join(filter(lambda x: x.isdigit() or x == '+', a["href"].replace("tel:", "")))
        for a in soup.find_all("a", href=True)
        if a["href"].startswith("tel:")
    ]
    phones_from_links = [p for p in phones_from_links if sum(c.isdigit() for c in p) >= 10]
    
    text_content_raw = soup.get_text(separator=" ", strip=True)
    phones_from_text = re.findall(r"\+?[\d\s\-\(\)]{10,20}", text_content_raw)
    phones_from_text = [p.strip() for p in phones_from_text if sum(c.isdigit() for c in p) >= 10]
    
    contact_links = list(
        set(
            [
                a["href"]
                for a in soup.find_all("a", href=True)
                if any(
                    word in a.get_text(strip=True).lower() or word in a["href"].lower()
                    for word in ["contact", "about", "reach"]
                )
                and a["href"].rstrip("/") != source_url.rstrip("/")
            ]
        )
    )

    special_info = f"SOURCE URL: {source_url}\nPAGE TITLE: {page_title}\nHEADERS: {top_headers}\n"
    if emails_from_links:
        special_info += f"EMAILS IN LINKS: {', '.join(emails_from_links)}\n"
    if phones_from_links:
        special_info += f"PHONES IN LINKS: {', '.join(phones_from_links)}\n"
    if phones_from_text:
        special_info += f"POTENTIAL PHONES IN TEXT: {', '.join(list(set(phones_from_text))[:5])}\n"
    if contact_links:
        log(f"Found potential subpages on {source_url}: {contact_links[:3]}")

    about_tag = soup.find(id="about")
    if about_tag:
        special_info += f"ZAUBA/ABOUT DATA: {about_tag.get_text(strip=True)}\n"

    zauba_contact = soup.find(id=re.compile(r"contact-details", re.I))
    if zauba_contact:
        special_info += f"ZAUBA CONTACT DATA: {zauba_contact.get_text(separator=' ', strip=True)}\n"

    tofler_box = soup.find(class_="registered_box_wrapper")
    if tofler_box:
        special_info += f"TOFLER REGISTERED DATA: {tofler_box.get_text(separator=' ', strip=True)}\n"

    is_directory = not is_official
    tags_to_remove = ["head", "script", "style", "nav", "aside"]
    if is_directory:
        tags_to_remove.append("footer")
    for s in soup(tags_to_remove):
        s.decompose()

    if is_directory:
        for tag in soup.find_all(attrs={"id": re.compile(r"footer", re.I)}):
            tag.decompose()
        for tag in soup.find_all(attrs={"class": re.compile(r"footer", re.I)}):
            tag.decompose()

    if "zaubacorp.com" in source_url.lower():
        text_content = "" 
    else:
        text_content = safe_clean_text(soup.get_text(separator=" ", strip=True))[:30000]

    final_text = f"{special_info}\n{text_content}"

    prompt = f"""
### TASK
You are a Business Research Agent. Your task is to find VERIFIED contact details for: {company_details.get('Company Name')}
Location: {company_details.get('City')}
GST/Udyam: {company_details.get('GST/Udayam Number')}

### BUSINESS IDENTITY VERIFICATION (MANDATORY)
- You are verifying the company based ONLY on these provided details:
  1. Company Name: "{company_details.get('Company Name')}"
  2. Target City: "{company_details.get('City')}"
  3. Target GST/Udyam: "{company_details.get('GST/Udayam Number')}"

- MATCHING RULES:
  - Name Match: Is the company name on the page a strong match for "{company_details.get('Company Name')}"?
  - GST Match: Is the EXACT number "{company_details.get('GST/Udayam Number')}" present in the text? (Set false if missing or different).
  - City Match: Does the page mention "{company_details.get('City')}" anywhere as a location for this company? 
    - IMPORTANT: Check the ENTIRE page. Companies often have multiple offices. If ANY listed office matches the target city, "city_matched" must be TRUE.
  - Address Match: ALWAYS set "address_matched": false (unless a specific street address was provided in the task above).

- VERIFICATION CRITERIA (STRICT):
  - If Name matches AND City matches, you MUST set "verified": true, EVEN IF GST is missing or not found.
  - If Name matches AND GST matches, you MUST set "verified": true.
  - If Name matches but BOTH GST and City are missing or mismatched, you MUST set "verified": false.

### CONTACT EXTRACTION RULES
- Extract Email and Phone ONLY if directly visible on the page.
- PHONE VALIDATION:
  - The phone number MUST be at least 10 digits long.
  - PRIORITIZE +91 NUMBERS: If a number starts with +91 or is a 10-digit Indian mobile, it is HIGH TRUST.
  - USE TEL: LINKS: If "PHONES IN LINKS" are provided, they are very high trust.
  - IGNORE LEI NUMBERS: Numbers starting with 984500 are often part of an LEI ID. DO NOT extract them.
  - IGNORE 6-digit PIN codes/Postal codes.
  - IGNORE masked info (e.g., 98******10).
- IGNORE placeholders (test@test.com, 1234567890).
- If no valid number is found, return "NOT_FOUND".

### OUTPUT FORMAT
JSON ONLY:
{{
  "business_category": "Industry/Category",
  "phone": "Exact Phone or NOT_FOUND",
  "email": "Exact Email or NOT_FOUND",
  "verified": bool,
  "name_matched": bool,
  "gst_matched": bool,
  "city_matched": bool,
  "address_matched": bool,
  "reason": "Format: [Name: Yes/No, GST: Yes/No, City: Yes/No, Address: No]. Explain logic.",
  "source_url": "{source_url}"
}}
"""
    res_text = call_llm(prompt, final_text, page=page)
    if not res_text:
        return {"verified": False, "reason": "No response from LLM"}
    try:
        match = re.search(r"\{.*\}", res_text, re.DOTALL)
        if not match:
            return {"verified": False, "reason": "LLM response did not contain JSON"}
        data = json.loads(match.group(0))
        
        if data.get("verified"):
            name_matched = data.get("name_matched", False)
            city_matched = data.get("city_matched", False)
            gst_matched = data.get("gst_matched", False)
            
            if not name_matched or (not city_matched and not gst_matched):
                data["verified"] = False
                data["reason"] = f"Script Override: LLM hallucinated 'verified=true'. {data.get('reason', '')}"
                return data

            if data.get("phone") == FORBIDDEN_PHONE:
                data["phone"] = "NOT_FOUND"
            return data
        return data
    except Exception as e:
        log(f"JSON Parse Error: {e}")
        return {"verified": False, "reason": f"JSON Parse Error: {e}"}

def serper_search(query):
    log(f"🔍 Serper Search: {query}")
    url = "https://google.serper.dev/search"
    while True:
        api_key = get_current_serper_key()
        if not api_key:
            if not SERPER_KEYS:
                raise Exception("No Serper API keys were configured in .env.")
            else:
                raise Exception("ALL configured Serper API keys have been exhausted!")
        payload = json.dumps({"q": query, "num": 10})
        headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
        try:
            response = requests.post(url, headers=headers, data=payload)
            error_body = response.text.lower()

            if response.status_code == 200:
                # Successfully got results
                data = response.json()
                links = []
                for result in data.get("organic", []):
                    link = result.get("link")
                    if link and not any(d in link for d in BLOCKED_DOMAINS):
                        links.append(link)
                log(f"Serper found {len(links)} organic results.")
                return links
            
            # If not 200, check for quota issues
            if response.status_code in [403, 429] or "credits" in error_body or "quota" in error_body:
                if rotate_serper_key():
                    continue
                else:
                    raise Exception("Final Serper API key quota exceeded.")
            
            # Other errors
            log(f"CRITICAL: Serper API Error {response.status_code}: {response.text}")
            raise Exception(f"Serper API Error {response.status_code}")
        except Exception as e:
            if "Final Serper API key quota exceeded" in str(e):
                raise e
            log(f"Serper Search Error: {e}")
            return []

def get_candidate_urls(company, page=None):
    company_name = company.get("Company Name")
    city = company.get("City")
    gst_cin = company.get("GST/Udayam Number") or company.get("GST Number") or company.get("CIN") or ""

    links = serper_search(f"{company_name} {city}")
    if len(links) < 3 and gst_cin:
        log("Insufficient results. Trying fallback search with GST/CIN...")
        links += serper_search(f"{company_name} {gst_cin}")
        links = list(dict.fromkeys(links))

    if not links:
        log(f"❌ No URLs found for {company_name}")
        return []

    log(f"Initial URLs found by Serper:\n" + "\n".join(f"  - {u}" for u in links[:20]))

    links_text = "\n".join(links[:20])
    prompt = f"""
Select the top 5 URLs that definitely belong to: {company_name}
Target City: {city}

PRIORITY: Official Website, Google Maps, IndiaFilings, Tofler, ZaubaCorp.
STRICT RULES:
1. The company name ({company_name}) MUST be a strong match in the title or URL.
2. DO NOT select URLs for different companies that just happen to be in the same city.
3. STICTLY FORBIDDEN: LinkedIn, IndiaMART, Tracxn (tracxn.com).
4. If no clear matches are found, return empty array.

For each selected URL, you must determine if it is the official company website (`"is_official": true`) or a third-party directory/aggregator (`"is_official": false`).

Return JSON ONLY: {{ "results": [{{"url": "url1", "is_official": true}}, {{"url": "url2", "is_official": false}}] }}
"""
    res_text = call_llm(prompt, text_content=links_text)
    default_results = [{"url": u, "is_official": not any(d in u.lower() for d in DIRECTORY_DOMAINS)} for u in links[:5]]
    
    try:
        match = re.search(r"\{.*\}", res_text, re.DOTALL)
        if not match:
            log("LLM URL selection failed or returned no JSON. Using fallback logic.")
            return default_results

        data = json.loads(match.group(0))
        final_results = data.get("results", [])

        if final_results:
            final_results.sort(key=lambda x: x.get("is_official", False), reverse=True)
            log("Final candidate URLs selected by LLM (sorted by priority):")
            for item in final_results[:5]:
                cat = "Official Website" if item.get("is_official") else "Directory/Other"
                log(f"  - [{cat}] {item.get('url')}")
        else:
            log("LLM returned an empty list of candidate URLs.")

        final_list = final_results[:5] if final_results else default_results
        final_list.sort(key=lambda x: x.get("is_official", False), reverse=True)
        return final_list
    except Exception as e:
        log(f"Error parsing LLM candidate URLs: {e}. Using fallback logic.")
        default_results.sort(key=lambda x: x.get("is_official", False), reverse=True)
        return default_results

def validate_contact_info(res, company_details=None):
    if not res:
        return None
    email = res.get("email", "NOT_FOUND")
    phone = res.get("phone", "NOT_FOUND")

    placeholders = ["example.com", "test@test", "1234567890", "0123456789", "domain.com", "email@email", FORBIDDEN_PHONE]
    if any(p in email.lower() for p in placeholders):
        email = "NOT_FOUND"
    
    # Clean phone for comparison
    clean_phone = "".join(filter(str.isdigit, str(phone)))
    
    if any(p in str(phone) for p in placeholders):
        phone = "NOT_FOUND"
    
    # Reject LEI prefixes if not a full 10-digit number
    if str(clean_phone).startswith("984500") and len(clean_phone) < 10:
        phone = "NOT_FOUND"

    # Reject if it matches part of the GST/CIN
    if company_details is not None:
        gst_cin = str(company_details.get('GST/Udayam Number') or company_details.get('CIN') or "").strip()
        if clean_phone and len(clean_phone) > 5 and clean_phone in gst_cin:
            phone = "NOT_FOUND"

    phone_digits = sum(c.isdigit() for c in str(phone))
    if phone != "NOT_FOUND" and phone_digits < 10:
        phone = "NOT_FOUND"
    
    # Strip '+' to prevent Google Sheets formula issues
    if phone != "NOT_FOUND" and str(phone).startswith("+"):
        phone = str(phone).lstrip("+")

    res["email"] = email
    res["phone"] = phone
    return res

def main():
    log(f"Starting Multi-Verification Cycle (Provider: {LLM_PROVIDER})")
    
    vdisplay = None
    import sys
    if HAS_VIRTUAL_DISPLAY and sys.platform.startswith("linux"):
        try:
            log("Starting background Virtual Display (Xvfb) for headful tasks...")
            vdisplay = Display(visible=0, size=(1920, 1080))
            vdisplay.start()
            log(f"Background Virtual Display started (DISPLAY={os.environ.get('DISPLAY')})")
        except Exception as e:
            log(f"Could not start Virtual Display: {e}")

    try:
        sheet = setup_sheets()
        log(f"Using Google Sheet: {SHEET_ID}")
        log(f"Active Worksheet: {SHEET_NAME}")

        all_vals = sheet.get_all_values()
        raw_headers = all_vals[0]
        clean_headers = [h.strip() if h.strip() else f"Empty_{i}" for i, h in enumerate(raw_headers)]
        df = pd.DataFrame(all_vals[1:], columns=clean_headers)

        log(f"Total records fetched from sheet: {len(df)}")

        log("Applying the following filters:")
        for col, val in FILTERS.items():
            log(f"  - {col} == '{val}'")

        mask = pd.Series(True, index=df.index)
        for col, val in FILTERS.items():
            if col in df.columns:
                mask &= df[col].astype(str).str.strip().str.lower() == str(val).lower()

        filtered_df = df[mask]
        log(f"Records remaining after applying all filters: {len(filtered_df)}")

        if filtered_df.empty:
            log("No entries matching filters.")
            return

        all_sheet_updates = []
        processed_count = 0

        def emergency_flush():
            if all_sheet_updates:
                log(f"⚠️  Interrupt detected! Flushing {len(all_sheet_updates)} pending updates...")
                try:
                    sheet.batch_update(all_sheet_updates)
                    log("✅ Emergency flush successful.")
                except Exception as e:
                    log(f"❌ Emergency flush failed: {e}")

        with sync_playwright() as p:
            log("Initializing Playwright (Sync) with stealth mode...")
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ]
            )
    
            try:
                processed_since_refresh = 0
                context = None
                page = None
                
                def create_fresh_page(browser):
                    nonlocal context, page
                    if context:
                        try:
                            page.close()
                            context.close()
                        except:
                            pass
                    
                    log("Refreshing Browser Context (Every 10 companies)...")
                    context = browser.new_context(
                        viewport={'width': 1920, 'height': 1080},
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
                    )
                    
                    context.set_extra_http_headers({
                        "Accept-Language": "en-US,en;q=0.9",
                        "sec-ch-ua": '"Chromium";v="123", "Google Chrome";v="123", "Not-A.Brand";v="99"',
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": '"Windows"',
                    })
                    
                    new_page = context.new_page()
                    Stealth().apply_stealth_sync(new_page)
                    
                    blocked_hosts = [
                        "google-analytics.com",
                        "analytics.google.com",
                        "googletagmanager.com",
                        "connect.facebook.net",
                        "www.googleadservices.com",
                        "googleads.g.doubleclick.net",
                        "bat.bing.com",
                        "cdn.krxd.net",
                    ]
                    
                    def route_intercept(route):
                        if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
                            route.abort()
                        elif any(domain in route.request.url for domain in blocked_hosts):
                            route.abort()
                        else:
                            route.continue_()
                            
                    new_page.route("**/*", route_intercept)
                    new_page.set_default_timeout(60000)
                    return new_page
    
                for index, row in filtered_df.iterrows():
                    if STOP_REQUESTED:
                        log("🛑 Stop signal received. Stopping enrichment cycle...")
                        break
                    
                    # Refresh browser context every 10 companies to prevent memory bloat
                    if context is None or processed_since_refresh >= 10:
                        page = create_fresh_page(browser)
                        processed_since_refresh = 0
                    
                    company_name = row["Company Name"]
                    log(f"--- Investigating: {company_name} ---")
    
                    urls_data = get_candidate_urls(row, page)
                    final_res = {"phone": "NOT_FOUND", "email": "NOT_FOUND", "source": "NOT_FOUND"}
    
                    if not urls_data:
                        log(f"No candidate URLs found for {company_name}. Marking as 'not found'.")
    
                    for url_info in urls_data:
                        if STOP_REQUESTED:
                            log("🛑 Stop signal received. Breaking URL loop...")
                            break
                        url = url_info.get("url")
                        if not url:
                            continue
                        is_official = url_info.get("is_official", False)
    
                        if is_official:
                            parsed_url = urlparse(url)
                            path = parsed_url.path.lower()
                            if path not in ["", "/", "/index.html", "/index.php", "/home"]:
                                if "contact" not in path and "about" not in path:
                                    log(f"Skipping non-relevant official subpage: {url}")
                                    continue
    
                        html_content = None
                        is_zauba = "zaubacorp.com" in url.lower()
    
                        if is_zauba:
                            html_content = fetch_zaubacorp(url, p)
                            if not html_content:
                                log(f"Skipping {url} (Zauba fetch failed)")
                                continue
                        else:
                            try:
                                max_retries = 3
                                page_loaded = False
                                for attempt in range(max_retries):
                                    if STOP_REQUESTED:
                                        break
                                    try:
                                        page.goto(url, wait_until="domcontentloaded", timeout=60000)
                                        page_loaded = True
                                        break
                                    except Exception as e:
                                        err_msg = str(e).lower()
                                        log(f"Attempt {attempt+1} failed to load {url}: {e}")
    
                                        if "target closed" in err_msg or "browser has been closed" in err_msg:
                                            log("Browser appears to have crashed. Restarting Playwright...")
                                            try:
                                                browser.close()
                                            except:
                                                pass
                                            browser = p.chromium.launch(
                                                headless=True,
                                                args=[
                                                    "--no-sandbox",
                                                    "--disable-dev-shm-usage",
                                                    "--disable-blink-features=AutomationControlled",
                                                ]
                                            )
                                            context = None # Force recreation
                                            page = create_fresh_page(browser)
                                            processed_since_refresh = 0
    
                                        if attempt < max_retries - 1:
                                            log("Retrying in 5 seconds...")
                                            time.sleep(5)
    
                                if not page_loaded:
                                    log(f"Skipping {url} after {max_retries} attempts.")
                                    continue
    
                                log(f"Scanning for contact info on {url}...")
                                time.sleep(3)
    
                                title = page.title()
                                title_lower = title.lower()
                                if "404 not found" in title_lower or "page not found" in title_lower:
                                    log(f"Skipping 404: {url}")
                                    continue
    
                                if "just a moment" in title_lower or "attention required" in title_lower or "cloudflare" in title_lower:
                                    log(f"Cloudflare challenge detected on {url}. Waiting for auto-bypass...")
                                    for _ in range(5):
                                        time.sleep(1)
                                        title = page.title()
                                        title_lower = title.lower()
                                        if not ("just a moment" in title_lower or "attention required" in title_lower or "cloudflare" in title_lower):
                                            break
    
                                    if "just a moment" in title_lower or "attention required" in title_lower or "cloudflare" in title_lower:
                                        log(f"Skipping Cloudflare block on: {url} (Bypass failed)")
                                        continue
                                    else:
                                        log(f"Successfully bypassed Cloudflare on {url}")
    
                                html_content = page.content()
                            except Exception as e:
                                log(f"Error scanning {url}: {e}")
                                continue
                        res = agent_process_page(html_content, row, url, page, is_official=is_official)
                        res = validate_contact_info(res, company_details=row)
    
                        if res and res.get("verified"):
                            email = res.get("email", "NOT_FOUND")
                            phone = res.get("phone", "NOT_FOUND")
    
                            found_sth = False
                            if email != "NOT_FOUND":
                                final_res["email"] = email
                                found_sth = True
                            if phone != "NOT_FOUND":
                                final_res["phone"] = phone
                                found_sth = True
    
                            if found_sth:
                                final_res["source"] = url
                                final_res["category"] = res.get("business_category", "NOT_FOUND")
                                log(f"Verified match found at {url}. Reason: {res.get('reason')}")
                                break
                            else:
                                if is_official:
                                    try:
                                        log(f"Verified identity but no contact info on homepage of {url}. Searching for Contact Page...")
                                        contact_keywords = ["contact", "reach", "get in touch", "support"]
                                        about_keywords = ["about", "profile", "who we are"]
                                        contact_element = None
    
                                        links = page.locator("a").element_handles()
                                        
                                        # Pass 1: Try to find Contact page
                                        for link in links:
                                            text = link.inner_text().lower()
                                            href = (link.get_attribute("href") or "").lower()
                                            if any(k in text or k in href for k in contact_keywords):
                                                contact_element = link
                                                break
                                        
                                        # Pass 2: Try to find About page if Contact not found
                                        if not contact_element:
                                            for link in links:
                                                text = link.inner_text().lower()
                                                href = (link.get_attribute("href") or "").lower()
                                                if any(k in text or k in href for k in about_keywords):
                                                    contact_element = link
                                                    break
    
                                        if contact_element:
                                            target_url = contact_element.get_attribute("href")
                                            if target_url:
                                                if not target_url.startswith("http"):
                                                    from urllib.parse import urljoin
                                                    target_url = urljoin(page.url, target_url)
                                                
                                                log(f"Navigating to contact page: {target_url}")
                                                page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
                                                time.sleep(2)
                                                
                                                current_url = page.url
                                                log(f"Got the html for contact page: {current_url}")
    
                                                html_content_sub = page.content()
                                                res_sub = agent_process_page(html_content_sub, row, current_url, page, is_official=True)
                                                res_sub = validate_contact_info(res_sub, company_details=row)
    
                                                if res_sub and res_sub.get("verified"):
                                                    email_sub = res_sub.get("email", "NOT_FOUND")
                                                    phone_sub = res_sub.get("phone", "NOT_FOUND")
    
                                                    if email_sub != "NOT_FOUND" or phone_sub != "NOT_FOUND":
                                                        final_res["email"] = email_sub
                                                        final_res["phone"] = phone_sub
                                                        final_res["source"] = current_url
                                                        final_res["category"] = res_sub.get("business_category", "NOT_FOUND")
                                                        log(f"Success! Found contact info on subpage: {current_url}")
                                                        break
                                    except Exception as e:
                                        log(f"Deep scan failed for {url}: {e}")
    
                                log(f"Verified identity but no contact info found on {url}. Reason: {res.get('reason') if res else 'No response'}")
                        else:
                            log(f"No identity match on {url}. Reason: {res.get('reason', 'Unknown mismatch') if res else 'No response'}")
    
    
                    # Update Sheet
                    row_num = index + 2
                    cols = {col: i + 1 for i, col in enumerate(clean_headers)}
    
                    found_any = (final_res["email"] != "NOT_FOUND" or final_res["phone"] != "NOT_FOUND")
                    enrich_status = "success" if found_any else "not found"
    
                    def get_safe_range(r, c):
                        return gspread.utils.rowcol_to_a1(r, c)
    
                    updates = [
                        {"range": get_safe_range(row_num, cols["Email"]), "values": [[final_res["email"]]]},
                        {"range": get_safe_range(row_num, cols["Phone"]), "values": [[final_res["phone"]]]},
                        {"range": get_safe_range(row_num, cols["Source URL"]), "values": [[final_res["source"]]]},
                        {"range": get_safe_range(row_num, cols["Enrichment Status"]), "values": [[enrich_status]]},
                    ]
    
                    if found_any:
                        updates.append({"range": get_safe_range(row_num, cols["Outreach Status"]), "values": [["pending"]]})
    
                    if "Business Category" in cols:
                        updates.append({"range": get_safe_range(row_num, cols["Business Category"]), "values": [[final_res.get("category", "NOT_FOUND")]]})
    
                    all_sheet_updates.extend(updates)
                    processed_count += 1
    
                    log(f"✅ Evaluated {company_name} (Status: {enrich_status})")
    
                    if processed_count % 10 == 0:
                        log(f"Pushing batched updates for the last 10 records to Google Sheets...")
                        try:
                            safe_name = SHEET_NAME.replace("'", "''")
                            data_to_push = []
                            for u in all_sheet_updates:
                                full_range = f"'{safe_name}'!{u['range']}"
                                data_to_push.append({"range": full_range, "values": u['values']})
                            
                            body = {
                                "valueInputOption": "USER_ENTERED",
                                "data": data_to_push
                            }
                            sheet.spreadsheet.values_batch_update(body)
                            all_sheet_updates = []
                        except Exception as e:
                            log(f"Batch update failed: {e}")
    
                    if STOP_REQUESTED:
                        log("🛑 Stop signal received. Gracefully exiting after saving current company data...")
                        break
    
                    processed_since_refresh += 1
    
            except KeyboardInterrupt:
                log("\nCtrl+C received. Performing emergency flush...")
                emergency_flush()
            finally:
                if all_sheet_updates:
                    log(f"Pushing final batched updates...")
                    try:
                        safe_name = SHEET_NAME.replace("'", "''")
                        data_to_push = []
                        for u in all_sheet_updates:
                            full_range = f"'{safe_name}'!{u['range']}"
                            data_to_push.append({"range": full_range, "values": u['values']})
                        
                        body = {
                            "valueInputOption": "USER_ENTERED",
                            "data": data_to_push
                        }
                        sheet.spreadsheet.values_batch_update(body)
                    except Exception as e:
                        log(f"Final batch update failed: {e}")

                browser.close()
                log("Cycle Complete.")

    except Exception as e:
        log(f"Fatal error in main: {e}")
    finally:
        if vdisplay:
            vdisplay.stop()
            log("Background Virtual Display stopped.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
