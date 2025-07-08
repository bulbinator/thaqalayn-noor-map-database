from hadithids import get_hadith_ids
import pymongo
from embedding import get_embedding
import requests
import json
import os
from dotenv import find_dotenv, load_dotenv
from text_utils import normalize
from tqdm import tqdm

API_URL = "https://hadith.inoor.ir/service/api/elastic/ElasticHadithById"
BATCH_SIZE = 1000
TOTAL_PARTS = 4
PART = 1        # CHANGE this to 2, 3, or 4 when you run again

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

print(f"✅ Fetched {len(all_hadiths)} hadiths in PART {PART}.")


# Here we are connectting to our noor database.
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
DB_PASSWORD = os.getenv("DB_PASSWORD")
client = pymongo.MongoClient(f"mongodb+srv://smmaisum:{DB_PASSWORD}@thaqalayn-noor-map-data.arnvlro.mongodb.net/?retryWrites=true&w=majority&appName=thaqalayn-noor-map-database")
db = client['thaqalayn-noor']
HADITH_BOOKS = ['الکافي', 'الخصال', "عيون أخبار الرضا عليه السلام", 'الأمالي (للمفید)', 'الأمالی (للصدوق)', 'التوحيد', 'الغيبة (للنعمانی)', 'الغيبة (للطوسی)', 'ثواب الأعمال و عقاب الأعمال', 'کامل الزيارات', "فضائل الشیعة", 'صفات الشیعة', 'معاني الأخبار', 'المؤمن', 'الزهد', "نهج البلاغة"]

hadiths_to_insert = []

for hadith in tqdm(all_hadiths, desc="Processing hadiths"):
    hadith_id = hadith.get("hadithId")
    book_title = hadith.get("bookTitle", "").strip()
    text = hadith.get("text", "")

    if book_title in HADITH_BOOKS:
        text = normalize(text)
        embedding = get_embedding(text)

        doc = {
            "noorId": hadith_id,
            "noorBookTitle": book_title,
            "noorText": text,
            "noorEmbedding": embedding
        }

        hadiths_to_insert.append(doc)

db['data'].insert_many(hadiths_to_insert)
print(f"✅ Inserted {len(hadiths_to_insert)} hadiths into MongoDB.")