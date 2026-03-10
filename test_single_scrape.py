import requests
from bs4 import BeautifulSoup
import logging
import re
import random
import string

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_random_bid():
    return "".join(random.sample(string.ascii_letters + string.digits, 11))

# User Agent similar to spider
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

def test_single_movie(douban_id):
    url = f"https://movie.douban.com/subject/{douban_id}/"
    logger.info(f"Fetching {url}...")
    
    try:
        # Create a session to maintain cookies if needed
        session = requests.Session()
        session.headers.update(HEADERS)
        
        # Add BID cookie
        bid = get_random_bid()
        logger.info(f"Using random BID: {bid}")
        session.cookies.set("bid", bid, domain=".douban.com")
        
        response = session.get(url, timeout=10)
        logger.info(f"Status Code: {response.status_code}")
        
        # Check for redirects
        if response.history:
            print("Redirect History:")
            for resp in response.history:
                print(f" - {resp.status_code} -> {resp.url}")
        
        # Print first 500 chars of response text to see what we got
        print("\n=== Response Text Preview ===")
        print(response.text[:1000])

        if response.status_code != 200:
            print("Failed to fetch page.")
            return

        soup = BeautifulSoup(response.text, "html.parser")
        print("\n=== Page Title ===")
        print(soup.title.string if soup.title else "No Title")
        
        # 2. Inspect Raw #info Block (where metadata lives)
        print("\n=== Raw #info Block ===")
        info = soup.find("div", id="info")
        if info:
            print(info.prettify())
        else:
            print("No #info div found!")
            return

        # 3. Inspect Release Date Tags
        print("\n=== Looking for Release Date ===")
        
        # Method A: Meta tag
        date_tags = soup.find_all("span", property="v:initialReleaseDate")
        print(f"Found {len(date_tags)} tags with property='v:initialReleaseDate':")
        for tag in date_tags:
            print(f" - Raw HTML: {tag}")
            print(f" - Text: {tag.get_text()}")

        # Method B: Text Search in Info
        print("\n=== Text Search in Info Block ===")
        info_text = info.get_text()
        print(f"Full Info Text (first 500 chars):\n{info_text[:500]}...")
        
        # Check extraction logic
        print("\n=== Testing Extraction Logic ===")
        release_date = ""
        
        # Priority 1: Tag
        release_date_tag = soup.find("span", property="v:initialReleaseDate")
        
        # Priority 2: Text fallback
        if not release_date_tag:
             date_lines = [line for line in info_text.split('\n') if "上映日期" in line]
             if date_lines:
                 print(f"Fallback found text line: {date_lines[0]}")
                 release_date_tag = type('obj', (object,), {'text': date_lines[0]})

        if release_date_tag:
            raw_date = release_date_tag.text.strip()
            print(f"Raw date string to parse: '{raw_date}'")
            match = re.search(r'\d{4}-\d{2}-\d{2}', raw_date)
            if match:
                release_date = match.group(0)
                print(f"Regex (YYYY-MM-DD) matched: {release_date}")
            else:
                match = re.search(r'\d{4}-\d{2}', raw_date)
                if match:
                    release_date = match.group(0)
                    print(f"Regex (YYYY-MM) matched: {release_date}")
                else:
                    print("Regex did not match.")
        
        if not release_date:
             print("Tag extraction failed. Trying regex on full text...")
             match = re.search(r'上映日期:.*?(\d{4}-\d{2}-\d{2})', info_text.replace('\n', ' '))
             if match:
                 release_date = match.group(1)
                 print(f"Full text regex matched: {release_date}")

        print(f"\n>>> FINAL EXTRACTED DATE: {release_date}")

    except Exception as e:
        logger.error(f"Error: {e}")

def test_abstract_api(douban_id):
    url = f"https://movie.douban.com/j/subject_abstract?subject_id={douban_id}"
    logger.info(f"Fetching Abstract API {url}...")
    
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        session.headers["X-Requested-With"] = "XMLHttpRequest"
        
        # Add BID cookie
        bid = get_random_bid()
        session.cookies.set("bid", bid, domain=".douban.com")
        
        response = session.get(url, timeout=10)
        logger.info(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("\n=== Abstract API Data ===")
            try:
                print(response.json())
            except Exception as json_err:
                print(f"Failed to parse JSON: {json_err}")
                print(f"Response content preview:\n{response.text[:500]}")
        else:
            print(f"Abstract API failed: {response.text}")

    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    # Test with the ID from the screenshot: 37825206 (超时空辉夜姬！)
    target_id = "37825206" 
    
    print(">>> Testing Detail Page Extraction...")
    test_single_movie(target_id)
    
    print("\n>>> Testing Abstract API...")
    test_abstract_api(target_id)
