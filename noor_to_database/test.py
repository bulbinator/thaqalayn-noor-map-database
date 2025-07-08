from embedding import get_embedding
from text_utils import normalize
import os
from dotenv import find_dotenv, load_dotenv
import pymongo
from hadithids import get_hadith_ids
import requests
"""
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
DB_PASSWORD = os.getenv("DB_PASSWORD")
client = pymongo.MongoClient(f"mongodb+srv://smmaisum:{DB_PASSWORD}@thaqalayn-noor-map-data.arnvlro.mongodb.net/?retryWrites=true&w=majority&appName=thaqalayn-noor-map-database")
db = client['thaqalayn-noor']
collection = db['data']

def find_hadith(text, book):
    text = normalize(text)
    vector = get_embedding(text)

    results = collection.aggregate([
        {
            "$vectorSearch": {
                "queryVector": vector,
                "path": "noorEmbedding",
                "numCandidates": 100,
                "limit": 3,
                "filter": {"noorBookTitle": book},
                "index": "vectorSearch"
            }
        }
    ])


    print("Thaqalayn Text: " + text)

    for doc in results:
        print("Noor Hadith: " + doc['noorText'] + " " + str(doc['noorId']))


find_hadith('2351 - وَ سَأَلَ سُلَيْمَانُ بْنُ خَالِدٍ أَبَا عَبْدِ اَللَّهِ عَلَيْهِ اَلسَّلاَمُ : عَنْ رَجُلٍ أَغْلَقَ بَابَهُ عَلَى طَيْرٍ فَمَاتَ فَقَالَ «إِنْ كَانَ أَغْلَقَ اَلْبَابَ عَلَيْهِ بَعْدَ مَا أَحْرَمَ فَعَلَيْهِ دَمٌ وَ إِنْ كَانَ أَغْلَقَهُ قَبْلَ أَنْ يُحْرِمَ وَ هُوَ حَلاَلٌ فَعَلَيْهِ ثَمَنُهُ».', 'من لا يحضره الفقيه')
"""

import requests

API_URL = "https://hadith.inoor.ir/service/api/elastic/ElasticHadithById"

payload = {
    "hadithId": ["103902"],
    "searchPhrase": ""
}

proxies = {
   'https': 'http://200.25.254.193:54240',
}

try:
    response = requests.post(API_URL, json=payload, proxies=proxies)
    response.raise_for_status()
    data = response.json()
    print(data)
except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")
    print(f"Response content: {response.text}")
except Exception as err:
    print(f"Other error occurred: {err}")

