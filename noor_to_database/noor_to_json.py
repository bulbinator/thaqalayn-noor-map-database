import time
from hadithids import get_hadith_ids
import requests
import json
from tqdm import tqdm

API_URL = "https://hadith.inoor.ir/service/api/elastic/ElasticHadithById"
BATCH_SIZE = 1000
OUTPUT_FILE = "noor_raw_data.json"

hadith_ids = get_hadith_ids()
all_hadiths = []

print(f"📦 Total hadith IDs to fetch: {len(hadith_ids)}")

for i in tqdm(range(0, len(hadith_ids), BATCH_SIZE), desc="Fetching batches"):
    batch = hadith_ids[i:i+BATCH_SIZE]
    payload = {"hadithId": batch, "searchPhrase": ""}

    try:
        response = requests.post(API_URL, json=payload)
        response.raise_for_status()
        data = response.json()
        hadiths = data.get("data") or []

        all_hadiths.extend(hadiths)

    except Exception as e:
        print(f"⚠ Error fetching batch starting at index {i}: {e}")
    
    time.sleep(3)

print(f"✅ Fetched {len(all_hadiths)} hadiths. Saving to file...")

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(all_hadiths, f, ensure_ascii=False, indent=2)

print(f"📄 Done! Saved to '{OUTPUT_FILE}'")
