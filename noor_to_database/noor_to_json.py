import time
from hadithids import get_hadith_ids
import requests
import json
from tqdm import tqdm

API_URL = "https://hadith.inoor.ir/service/api/elastic/ElasticHadithById"
BATCH_SIZE = 1000
TOTAL_PARTS = 4
PART = 1        # CHANGE this to 2, 3, or 4 when you run again

# ✅ Get all IDs
hadith_ids = get_hadith_ids()
total_ids = len(hadith_ids)
ids_per_part = total_ids // TOTAL_PARTS

# Figure out start and end for this part
start_idx = (PART - 1) * ids_per_part
end_idx = start_idx + ids_per_part if PART < TOTAL_PARTS else total_ids

print(f"📦 Total hadith IDs: {total_ids}")
print(f"⚙ Running PART {PART}/{TOTAL_PARTS}: fetching IDs from index {start_idx} to {end_idx-1}")

# Subset of IDs for this part
part_hadith_ids = hadith_ids[start_idx:end_idx]
all_hadiths = []

for i in tqdm(range(0, len(part_hadith_ids), BATCH_SIZE), desc=f"Fetching batches for part {PART}"):
    batch = part_hadith_ids[i:i+BATCH_SIZE]
    payload = {"hadithId": batch, "searchPhrase": ""}

    try:
        response = requests.post(API_URL, json=payload)
        response.raise_for_status()
        data = response.json()
        hadiths = data.get("data") or []

        all_hadiths.extend(hadiths)

    except Exception as e:
        print(f"⚠ Error fetching batch starting at index {start_idx + i}: {e}")

print(f"✅ Fetched {len(all_hadiths)} hadiths in PART {PART}. Saving to file...")

output_file = f"noor_part{PART}.json"
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(all_hadiths, f, ensure_ascii=False, indent=2)

print(f"📄 Done! Saved to '{output_file}'")
