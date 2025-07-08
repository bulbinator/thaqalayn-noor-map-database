from hadithids import get_hadith_ids
from embedding import get_embedding
import pymongo
import requests
import os
from dotenv import load_dotenv, find_dotenv
from text_utils import normalize
from tqdm import tqdm

# -------------------
# ⚙️ Config
# -------------------
API_URL = "https://hadith.inoor.ir/service/api/elastic/ElasticHadithById"
BATCH_SIZE = 1000
TOTAL_PARTS = 4
PART = 1   # ← change to 2, 3, 4 when running again

HADITH_BOOKS = [
    'الکافي', 'الخصال', "عيون أخبار الرضا عليه السلام", 'الأمالي (للمفید)',
    'الأمالی (للصدوق)', 'التوحيد', 'الغيبة (للنعمانی)', 'الغيبة (للطوسی)',
    'ثواب الأعمال و عقاب الأعمال', 'کامل الزيارات', "فضائل الشیعة",
    'صفات الشیعة', 'معاني الأخبار', 'المؤمن', 'الزهد', "نهج البلاغة"
]

# -------------------
# 📦 Load hadith IDs & partition
# -------------------
hadith_ids = get_hadith_ids()
total_ids = len(hadith_ids)
ids_per_part = total_ids // TOTAL_PARTS

start_idx = (PART - 1) * ids_per_part
end_idx = start_idx + ids_per_part if PART < TOTAL_PARTS else total_ids

part_hadith_ids = hadith_ids[start_idx:end_idx]

print(f"📦 Total IDs: {total_ids}")
print(f"⚙️ Running PART {PART}/{TOTAL_PARTS}: IDs {start_idx} → {end_idx - 1}")

# -------------------
# 📡 Fetch hadith data
# -------------------
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
        print(f"⚠️ Error fetching batch starting at index {start_idx + i}: {e}")

print(f"✅ Fetched {len(all_hadiths)} hadiths in PART {PART}.")

# -------------------
# 🧠 Process & embed
# -------------------
# Filter only valid hadiths & normalize text
filtered_hadiths = [
    {
        "id": h.get("hadithId"),
        "book": h.get("bookTitle", "").strip(),
        "text": normalize(h.get("text", ""))
    }
    for h in all_hadiths
    if h.get("bookTitle", "").strip() in HADITH_BOOKS
]

print(f"🧠 Generating embeddings for {len(filtered_hadiths)} hadiths...")
texts = [h["text"] for h in filtered_hadiths]
embeddings = get_embedding(texts)   # batch embed!

# -------------------
# 📂 Build docs
# -------------------
hadiths_to_insert = []
for hadith, embedding in zip(filtered_hadiths, embeddings):
    doc = {
        "noorId": hadith["id"],
        "noorBookTitle": hadith["book"],
        "noorText": hadith["text"],
        "noorEmbedding": embedding
    }
    hadiths_to_insert.append(doc)

# -------------------
# 🛢 Insert into MongoDB
# -------------------
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
DB_PASSWORD = os.getenv("DB_PASSWORD")

client = pymongo.MongoClient(
    f"mongodb+srv://smmaisum:{DB_PASSWORD}"
    "@thaqalayn-noor-map-data.arnvlro.mongodb.net/"
    "?retryWrites=true&w=majority&appName=thaqalayn-noor-map-database"
)
db = client['thaqalayn-noor']

db['data'].insert_many(hadiths_to_insert)
print(f"✅ Inserted {len(hadiths_to_insert)} hadiths into MongoDB.")
