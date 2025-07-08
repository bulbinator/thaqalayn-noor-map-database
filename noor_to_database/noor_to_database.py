from hadithids import get_hadith_ids
import pymongo
from embedding import get_embedding
import requests
import json
import os
from dotenv import find_dotenv, load_dotenv
from text_utils import normalize
from tqdm import tqdm

# Here we are connectting to our noor database.
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
DB_PASSWORD = os.getenv("DB_PASSWORD")
client = pymongo.MongoClient(f"mongodb+srv://smmaisum:{DB_PASSWORD}@thaqalayn-noor-map-data.arnvlro.mongodb.net/?retryWrites=true&w=majority&appName=thaqalayn-noor-map-database")
db = client['thaqalayn-noor']
HADITH_BOOKS = ['الکافي', 'الخصال', "عيون أخبار الرضا عليه السلام", 'الأمالي (للمفید)', 'الأمالی (للصدوق)', 'التوحيد', 'الغيبة (للنعمانی)', 'الغيبة (للطوسی)', 'ثواب الأعمال و عقاب الأعمال', 'کامل الزيارات', "فضائل الشیعة", 'صفات الشیعة', 'معاني الأخبار', 'المؤمن', 'الزهد', "نهج البلاغة"]
API_URL = "https://hadith.inoor.ir/service/api/elastic/ElasticHadithById"
BATCH_SIZE = 1000

hadith_ids = get_hadith_ids()
for i in tqdm(range(0, len(hadith_ids), BATCH_SIZE), desc="Fetching batches"):
    batch = hadith_ids[i:i+BATCH_SIZE]
    payload = {"hadithId": batch, "searchPhrase": ""}

    try:
        response = requests.post(API_URL, json=payload)
        response.raise_for_status()
        data = response.json()

    except Exception as e:
        print(f"⚠ Error fetching batch starting at index {i}: {e}")
    

    hadiths_to_insert = []
    hadiths = data.get("data")
    for hadith in hadiths:
        hadith_id = hadith.get("hadithId")
        book_title = hadith.get("bookTitle").strip()
        text = normalize(hadith.get("text"))

        if book_title in HADITH_BOOKS:
            embedding = get_embedding(text)

            doc = {
                "noorId": hadith_id,
                "noorBookTitle": book_title,
                "noorText": text,
                "noorEmbedding": embedding
            }

            hadiths_to_insert.append(doc)  
    if hadiths_to_insert:
        db['data'].insert_many(hadiths_to_insert)