import pymongo
from noor_to_database.embedding import get_embedding
from noor_to_database.text_utils import normalize
from tqdm import tqdm
from dotenv import find_dotenv, load_dotenv
import os
from BOOK_MAP import BOOK_MAP

# -------------------------
# ⚙️ Connect to MongoDB
# -------------------------
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
THAQ_DB_PASSWORD = os.getenv("THAQ_DB_PASSWORD")
NOOR_DB_PASSWORD = os.getenv("NOOR_DB_PASSWORD")


client = pymongo.MongoClient(f"mongodb+srv://syedmaisum1:{THAQ_DB_PASSWORD}@thaqalayn-noor.rri8t2j.mongodb.net/?retryWrites=true&w=majority&appName=thaqalayn-noor")
thaq_db = client['thaqalayn-noor']
thaq_collection = thaq_db['data']

client = pymongo.MongoClient(
    f"mongodb+srv://smmaisum:{NOOR_DB_PASSWORD}"
    "@thaqalayn-noor-map-data.arnvlro.mongodb.net/"
    "?retryWrites=true&w=majority&appName=thaqalayn-noor-map-database"
)
noor_db = client['thaqalayn-noor']
noor_collection = noor_db["data"]       # collection with Noor ahadith + embeddings

# -------------------------
# 🔍 Loop and vector search
# -------------------------
# Find all Thaqalayn ahadith (you can add filters if you want)
docs = list(thaq_collection.find(
    {}, 
    {"_id": 1, "thaqText": 1, "thaqBookId": 1}
))  # keep _id so we can update

for doc in tqdm(docs, desc="Matching Thaqalayn hadiths"):
    thaq_id = doc["_id"]
    text = doc.get("thaqText", "")
    book = doc.get("thaqBookId", "")

    if not text:
        continue

    text = normalize(text)
    words = text.split()
    text = " ".join(words[-100:])

    # Vector search on Noor
    results = list(noor_collection.aggregate([
        {
            "$search": {
                "compound": {
                    "must": [
                        {   # full-text search in "noorText"
                            "text": {
                                "query": text,
                                "path": "noorText"
                            }
                        },
                        {   # filter documents where "noorBookTitle" == title_to_match
                            "text": {
                                "query": BOOK_MAP.get(book),
                                "path": "noorBookTitle"
                            }
                        }
                    ]
                }
            }
        },
        {"$limit": 1},
        {
            "$project": {
                "_id": 0,
                "noorId": 1,
                "noorBookTitle": 1,
                "noorText": 1
            }
        }
    ]))




    # If we got a match, update the original Thaqalayn doc
    if results:
        best_match = results[0]
        thaq_collection.update_one(
            {"_id": thaq_id},
            {"$set": {
                "noorId": best_match["noorId"],
                "noorBookTitle": best_match["noorBookTitle"],
                "noorText": best_match["noorText"],
            }}
        )
