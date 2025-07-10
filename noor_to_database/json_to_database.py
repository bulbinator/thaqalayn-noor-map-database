from embedding import get_embedding
import pymongo
import json
import os
from dotenv import load_dotenv, find_dotenv
from text_utils import normalize
from tqdm import tqdm

INPUT_FILE = "noor_data.json"
EMBED_BATCH_SIZE = 1000
HADITH_BOOKS = [
    'الکافي', 'الخصال', "عيون أخبار الرضا عليه السلام", 'الأمالي (للمفید)',
    'الأمالی (للصدوق)', 'التوحيد', 'الغيبة (للنعمانی)', 'الغيبة (للطوسی)',
    'ثواب الأعمال و عقاب الأعمال', 'کامل الزيارات', "فضائل الشیعة",
    'صفات الشیعة', 'معاني الأخبار', 'المؤمن', 'الزهد', "نهج البلاغة"
]

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    all_hadiths = json.load(f)


filtered_hadiths = []
for h in tqdm(all_hadiths):
    book = h.get("bookTitle", "").strip()
    if book in HADITH_BOOKS:
        text = normalize(h.get("text", ""))
        filtered_hadiths.append({
            "id": h.get("hadithId"),
            "book": book,
            "text": text
        })


all_embeddings = []
for i in tqdm(range(0, len(filtered_hadiths), EMBED_BATCH_SIZE)):
    batch_texts = [h["text"] for h in filtered_hadiths[i:i+EMBED_BATCH_SIZE]]
    batch_embeddings = get_embedding(batch_texts)
    all_embeddings.extend(batch_embeddings)


hadiths_to_insert = []
for hadith, embedding in zip(filtered_hadiths, all_embeddings):
    doc = {
        "noorId": hadith["id"],
        "noorBookTitle": hadith["book"],
        "noorText": hadith["text"],
        "noorEmbedding": embedding
    }
    hadiths_to_insert.append(doc)

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
NOOR_DB_PASSWORD = os.getenv("NOOR_DB_PASSWORD")

client = pymongo.MongoClient(
    f"mongodb+srv://smmaisum:{NOOR_DB_PASSWORD}"
    "@thaqalayn-noor-map-data.arnvlro.mongodb.net/"
    "?retryWrites=true&w=majority&appName=thaqalayn-noor-map-database"
)
db = client['thaqalayn-noor']
db['data'].insert_many(hadiths_to_insert)
