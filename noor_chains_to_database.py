import pymongo
import requests
from dotenv import load_dotenv, find_dotenv
import os
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Create a session with retry logic
session = requests.Session()
retries = Retry(
    total=5,                # Max retries
    backoff_factor=1,       # Wait 1s, 2s, 4s, etc.
    status_forcelist=[500, 502, 503, 504],
    raise_on_status=False
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("http://", adapter)
session.mount("https://", adapter)

load_dotenv(find_dotenv())
THAQ_DB_PASSWORD = os.getenv("THAQ_DB_PASSWORD")

client = pymongo.MongoClient(f"mongodb+srv://syedmaisum1:{THAQ_DB_PASSWORD}@thaqalayn-noor.rri8t2j.mongodb.net/?retryWrites=true&w=majority&appName=thaqalayn-noor")

db = client["thaqalayn-noor"]
collection = db["data"]

def get_chain(noor_id):
    url = f"https://hadith.inoor.ir/service/api/hadith/HadithRejalList/v2?hadithId={noor_id}"
    try:
        response = session.get(url)
        if response.status_code != 200:
            print(f"Failed to call Noor API: {response.status_code}")
            return None
        return response.json()
    except requests.RequestException as e:
        print(f"Request error for hadithId={noor_id}: {e}")
        return None


docs = list(collection.find({
    "noorId": {"$exists": True},
    "chains": {"$exists": False}
}))

for doc in tqdm(docs, desc="Processing Hadiths"):
    noor_id = doc["noorId"]
    _id = doc["_id"]

    chains = get_chain(noor_id)

    if chains is not None:
        collection.update_one({"_id": _id}, {"$set": {"chains": chains}})