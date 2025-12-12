import os
import time
import json
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from gemini_webapi import GeminiClient
from gemini_webapi.constants import Model

# Load environment variables
load_dotenv()

# Database Configuration
# Database Configuration - MUST be set via environment variable (GitHub Secret)
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is required. Set it in GitHub Secrets.")

# Gemini Configuration - cookies loaded from GEMINI_COOKIES env var
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COOKIES_FILE = os.path.join(SCRIPT_DIR, "cookies.txt")

VEO3_PROMPT_TEMPLATE = """You are an Elite AI Commercial Director.
Your task is to generate a 2-part video prompt must be **22 to 28 words** per part to JSON Only for Google Veo 3 based on the raw product title. DO NOT PRINT TO CHAT PROCESSING LOGIC FOR EVERY STEPS.

**Input Product Title:** "{title}"
**START INTERNAL LOGIC (APPLY SILENTLY - DON'T PRINT TO CHAT):**
**STEP 0: CREATE "SPOKEN SHORT NAME" (INTERNAL PROCESSING)**
1.  Analyze the long title.
2.  Extract ONLY the **Category + Brand/Key Feature** to create a natural "Short Name" (Max 3-5 words).
    - Example Input: "Quần Bò Jean Nữ Ống Loe đứng CANA Jeans Cạp Cao MS21"
    - Example Output Short Name: **"Quần Jean Loe CANA"**

**STEP 1: TIMING & SCRIPT RULES (UPDATED)**
- **Duration:** Exactly 8 seconds per part.
- **Word Count:** Vietnamese script must be **22 to 28 words** per part.
- **Pacing:** Very fast, high-density energetic delivery (Livestream style).
- **Naming Rule:** ONLY use the **Short Name** generated in Step 0.
- **Content:** Fill the time with benefits, do not leave silence.

**STEP 2: VISUAL LOGIC**
- **Category Awareness:** Apply correct camera angles (Footwear=Low angle, Fashion=Medium shot, Cosmetics=Close up).
- **Visual Fidelity:** Describe the product using details from the full title (color, material), ensuring 4K photorealism.

**PART 1 (0s-8s): The Hook & Pain Point**
- **Visual:** Dramatic reveal, problem visualization, or high-end product showcase.
- **Script:**
  1. Start INSTANTLY with a question or strong statement.
  2. Mention the [Short Name].
  3. Pack the script with adjectives and energetic filler words ("cực đỉnh", "siêu mê", "ngay đi").
- **Example Structure:** "Bà nào đùi to chân ngắn mà chưa biết đến em [Short Name] này là tiếc hùi hụi nha! Thiết kế cạp cao hack dáng siêu đỉnh, mặc vào là chân dài miên man, che khuyết điểm cực tốt luôn ạ."

**PART 2 (8s-16s): Feature & CTA**
- **Visual:** Product in use/motion showing results. Dynamic movement.
- **Script:**
  1. Focus on the best feature (material, durability, effect).
  2. End with a breathless, urgent CTA.
- **Example Structure:** "Chất vải co giãn bốn chiều, bao giặt máy không lo bai xù. Số lượng trong kho còn cực ít, các bác nhanh tay bấm ngay vào giỏ hàng góc trái chốt đơn liền kẻo hết size đẹp nhé!"
**END INTERNAL LOGIC (APPLY SILENTLY - DON'T PRINT TO CHAT):**

**Output Format (JSON Only):**
{{
  "part1_prompt": "cinematic 4k shot, [Camera Logic], [Detailed Visual of Product using Full Title details], professional lighting --text-input \\"[Vietnamese Script Part 1 (~35 words)]\\"",
  "part2_prompt": "cinematic 4k shot, [Camera Logic], [Detailed Visual of Product in action], aesthetic style --text-input \\"[Vietnamese Script Part 2 (~35 words)]\\""
}}
"""


def load_cookies_from_file(filepath):
    """Load cookies from text file in format: COOKIE1=value1;COOKIE2=value2;..."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Cookies file not found: {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        cookie_string = f.read().strip()
    
    # Parse cookies
    cookies = {}
    for cookie in cookie_string.split(';'):
        cookie = cookie.strip()
        if '=' in cookie:
            key, value = cookie.split('=', 1)
            cookies[key] = value
    
    return cookies


def load_cookies_from_env():
    """Load cookies from environment variable GEMINI_COOKIES."""
    cookie_string = os.getenv("GEMINI_COOKIES", "")
    if not cookie_string:
        return None
    
    cookies = {}
    for cookie in cookie_string.split(';'):
        cookie = cookie.strip()
        if '=' in cookie:
            key, value = cookie.split('=', 1)
            cookies[key] = value
    
    return cookies if cookies else None


def get_db_connection():
    """Establish a connection to the database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def fetch_pending_records(conn, limit=30):
    """
    Fetch pending records and mark them with crawl_status=TRUE to prevent
    other jobs from processing the same records.
    
    Uses UPDATE ... RETURNING to atomically claim records.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Atomically claim records by setting crawl_status = TRUE
            # Only select records where crawl_status IS NULL (not yet claimed)
            cur.execute("""
                UPDATE products_ai
                SET crawl_status = TRUE
                WHERE id IN (
                    SELECT id FROM products_ai
                    WHERE prompt_veo3 IS NULL 
                      AND image_status = TRUE
                      AND crawl_status IS NULL
                    ORDER BY id ASC
                    LIMIT %s
                )
                RETURNING id, title, image_data
            """, (limit,))
            records = cur.fetchall()
            conn.commit()
            return records
    except Exception as e:
        print(f"Error fetching records: {e}")
        conn.rollback()
        return []


def update_record(conn, record_id, prompt_veo3):
    """Update the record with the generated Veo3 prompt."""
    try:
        with conn.cursor() as cur:
            print(f"Attempting to update record {record_id} with prompt length: {len(prompt_veo3) if prompt_veo3 else 0}")
            cur.execute("""
                UPDATE products_ai
                SET prompt_veo3 = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (prompt_veo3, record_id))
            conn.commit()
            print(f"Successfully updated record {record_id} with Veo3 prompt.")
    except Exception as e:
        print(f"Error updating record {record_id}: {e}")
        conn.rollback()


def generate_prompt_text(record):
    """Format the prompt using the template and record data."""
    try:
        return VEO3_PROMPT_TEMPLATE.format(title=record['title'])
    except Exception as e:
        print(f"Error formatting prompt for record {record['id']}: {e}")
        return None


def clean_json_response(text):
    """Clean markdown formatting and extract JSON object from response."""
    text = text.strip()
    
    # Remove markdown code blocks if present
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    
    # Extract JSON object
    try:
        start_index = text.find('{')
        end_index = text.rfind('}')
        if start_index != -1 and end_index != -1 and end_index > start_index:
            text = text[start_index:end_index+1]
    except Exception as e:
        print(f"Warning: Could not extract JSON object: {e}")
    
    return text.strip()


async def process_records_with_api(client, conn, limit_records=None):
    """Process pending records using Gemini API."""
    print("\n" + "="*80)
    print("STARTING RECORD PROCESSING")
    print("="*80)
    
    print("\n[STEP 1] Fetching pending records from database...")
    records = fetch_pending_records(conn)
    
    if not records:
        print("❌ No pending records found.")
        return
    
    print(f"✅ Found {len(records)} records to process.")
    
    # Limit to specified number of records for testing
    if limit_records:
        records = records[:limit_records]
        print(f"⚠️  Processing limited to {limit_records} record(s) for testing\n")
    
    for idx, record in enumerate(records, 1):
            print(f"\n{'='*80}")
            print(f"[RECORD {idx}/{len(records)}] Processing ID: {record['id']}")
            print(f"{'='*80}")
            print(f"\n[STEP 2.{idx}] Product Title: {record['title'][:100]}..." if len(record['title']) > 100 else f"\n[STEP 2.{idx}] Product Title: {record['title']}")
            
            prompt_text = generate_prompt_text(record)
            print(f"\n[STEP 3.{idx}] Generated prompt template (length: {len(prompt_text) if prompt_text else 0} chars)")
            
            if not prompt_text:
                continue
            
            try:
                # Retry logic for prompt submission
                max_prompt_retries = 4
                valid_response_received = False
                cleaned_response = ""
                
                print(f"\n[STEP 4.{idx}] Starting API communication (max {max_prompt_retries} attempts)")
                
                for retry_attempt in range(max_prompt_retries):
                    if retry_attempt > 0:
                        print(f"\n⚠️  [RETRY {retry_attempt + 1}/{max_prompt_retries}] Previous attempt failed, retrying...")
                        await asyncio.sleep(3)
                    else:
                        print(f"\n[ATTEMPT {retry_attempt + 1}] Sending prompt to Gemini API...")
                    
                    try:
                        # Send prompt via API
                        print(f"   → Using model: Gemini 2.5 Flash")
                        print(f"   → Request timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                        
                        response = await client.generate_content(
                            prompt=prompt_text,
                            model=Model.G_2_5_FLASH,
                        )
                        
                        # Get response text
                        response_text = response.text
                        print(f"\n✅ Response received!")
                        print(f"   → Response length: {len(response_text)} characters")
                        print(f"   → First 200 chars: {response_text[:200]}...")
                        
                        # Clean and validate JSON
                        print(f"\n[STEP 5.{idx}] Cleaning and validating JSON response...")
                        cleaned_response = clean_json_response(response_text)
                        print(f"   → Cleaned JSON length: {len(cleaned_response)} chars")
                        
                        # Check if response contains the original prompt (means Gemini didn't generate properly)
                        if "You are an Elite AI" in response_text:
                            print(f"\n❌ VALIDATION FAILED: Response contains original prompt template")
                            print(f"   → This usually means Gemini echoed the prompt instead of generating")
                            continue
                        
                        # Try to parse JSON to validate
                        try:
                            parsed_json = json.loads(cleaned_response)
                            if "part1_prompt" in parsed_json and "part2_prompt" in parsed_json:
                                valid_response_received = True
                                print(f"\n✅ VALIDATION PASSED: Valid JSON with required fields")
                                print(f"   → part1_prompt: {parsed_json['part1_prompt'][:80]}...")
                                print(f"   → part2_prompt: {parsed_json['part2_prompt'][:80]}...")
                                break
                            else:
                                print(f"\n❌ VALIDATION FAILED: JSON missing required fields")
                                print(f"   → Found keys: {list(parsed_json.keys())}")
                                continue
                        except json.JSONDecodeError as je:
                            print(f"\n❌ VALIDATION FAILED: Invalid JSON format")
                            print(f"   → Error: {je}")
                            print(f"   → Attempted to parse: {cleaned_response[:200]}...")
                            continue
                    
                    except Exception as e:
                        print(f"Error during API call: {e}")
                        
                        # Check if error is related to invalid response (cookie expiration)
                        str_error = str(e)
                        if "Invalid response" in str_error or "406" in str_error:
                            print("\n⚠️  Possible cookie expiration detected.")
                            print("   → On GitHub Actions, you need to update the GEMINI_COOKIES secret")
                            print("   → Get fresh cookies from https://gemini.google.com")

                        if retry_attempt < max_prompt_retries - 1:
                            continue
                        else:
                            raise
                
                if not valid_response_received:
                    print(f"\n❌ FAILED: Could not get valid response after {max_prompt_retries} attempts")
                    print(f"   → Skipping record ID {record['id']}\n")
                    continue
                
                print(f"\n[STEP 6.{idx}] Saving to database...")
                # Update database
                print(f"   → Updating record ID: {record['id']}")
                update_record(conn, record['id'], cleaned_response)
                
            except Exception as e:
                print(f"Error processing record {record['id']}: {e}")
            
            # Wait before next request to avoid rate limits
            if idx < len(records):
                print(f"\n⏳ Waiting 15 seconds before next record...\n")
                await asyncio.sleep(15)


async def main():
    """Main function to run the script."""
    print("="*80)
    print("GEMINI VEO3 PROMPT GENERATOR - GITHUB ACTIONS VERSION")
    print("="*80)
    
    # Load cookies
    print(f"\nLoading cookies...")
    cookies = None
    
    # Try loading from environment variable first (for GitHub Actions)
    print(f"  → Trying GEMINI_COOKIES environment variable...")
    cookies = load_cookies_from_env()
    if cookies:
        print(f"  ✅ Loaded {len(cookies)} cookies from environment variable")
    
    # Fallback to file if env var not set
    if not cookies:
        try:
            print(f"  → Trying cookies.txt at {COOKIES_FILE}...")
            cookies = load_cookies_from_file(COOKIES_FILE)
            print(f"  ✅ Loaded {len(cookies)} cookies from file")
        except Exception as e:
            print(f"  ⚠️ Failed to load from file: {e}")
    
    if not cookies:
        print(f"\n❌ ERROR: Could not load cookies")
        print(f"   On GitHub Actions:")
        print(f"   1. Add GEMINI_COOKIES secret with format: __Secure-1PSID=xxx;__Secure-1PSIDTS=yyy")
        print(f"   Or locally:")
        print(f"   1. Create cookies.txt with the same format")
        return
    
    # Check if required cookies exist
    if '__Secure-1PSID' not in cookies:
        print(f"\n❌ ERROR: __Secure-1PSID cookie not found")
        return
    
    # Extract required cookies
    secure_1psid = cookies.get('__Secure-1PSID')
    secure_1psidts = cookies.get('__Secure-1PSIDTS')
    
    print(f"\n✅ Found required cookies:")
    print(f"   __Secure-1PSID: {secure_1psid[:50]}...")
    print(f"   __Secure-1PSIDTS: {secure_1psidts[:50] if secure_1psidts else 'NOT FOUND'}...")
    
    # Connect to database
    print(f"\nConnecting to database...")
    conn = get_db_connection()
    if not conn:
        print("ERROR: Failed to connect to database")
        return
    print("Database connected successfully")
    
    # Initialize Gemini client
    print(f"\nInitializing Gemini API client...")
    print(f"   Using __Secure-1PSID: {secure_1psid[:30]}...")
    print(f"   Using __Secure-1PSIDTS: {secure_1psidts[:30] if secure_1psidts else 'None'}...")
    try:
        client = GeminiClient(
            secure_1psid=secure_1psid,
            secure_1psidts=secure_1psidts,
            proxy=None
        )
        
        await client.init(
            timeout=60,
            auto_close=False,
            auto_refresh=True,
            verbose=True
        )
        
        print("✅ Gemini client initialized successfully")
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"❌ ERROR: Failed to initialize Gemini client")
        print(f"{'='*80}")
        print(f"\nError message: {e}")
        print(f"\n⚠️  COOKIES HAVE EXPIRED!")
        print(f"\nOn GitHub Actions:")
        print(f"1. Go to Repository Settings → Secrets and variables → Actions")
        print(f"2. Update GEMINI_COOKIES secret with fresh cookies")
        print(f"\nTo get fresh cookies:")
        print(f"1. Open https://gemini.google.com in your browser")
        print(f"2. Press F12 (Developer Tools)")
        print(f"3. Go to 'Application' tab → 'Cookies' → 'https://gemini.google.com'")
        print(f"4. Copy the values of __Secure-1PSID and __Secure-1PSIDTS")
        print(f"5. Format: __Secure-1PSID=xxx;__Secure-1PSIDTS=yyy")
        conn.close()
        return
    
    # Process all pending records
    print(f"\n" + "="*80)
    print("READY TO PROCESS RECORDS")
    print("="*80)
    
    try:
        await process_records_with_api(client, conn)
    except KeyboardInterrupt:
        print("\n\nScript interrupted by user")
    except Exception as e:
        print(f"\n\nERROR: Unexpected error: {e}")
    finally:
        # Cleanup
        print("\nClosing connections...")
        await client.close()
        conn.close()
        print("Script finished.")


if __name__ == "__main__":
    asyncio.run(main())
