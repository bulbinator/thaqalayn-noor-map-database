from embedding import get_embedding
from text_utils import normalize
import os
from dotenv import find_dotenv, load_dotenv
import pymongo
from hadithids import get_hadith_ids
import requests

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
NOOR_DB_PASSWORD = os.getenv("NOOR_DB_PASSWORD")
client = pymongo.MongoClient(f"mongodb+srv://smmaisum:{NOOR_DB_PASSWORD}@thaqalayn-noor-map-data.arnvlro.mongodb.net/?retryWrites=true&w=majority&appName=thaqalayn-noor-map-database")
db = client['thaqalayn-noor']
collection = db['data']

def find_hadith(text, book):
    text = normalize(text)
    vector = get_embedding(text)

    results = collection.aggregate([
        {
            "$search": {
                "text": vector,
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


find_hadith("3ـ أَحْمَدُ بْنُ إِدْرِيسَ عَنْ مُحَمَّدِ بْنِ عَبْدِ الْجَبَّارِ عَنْ بَعْضِ أَصْحَابِنَا رَفَعَهُ إِلَى أَبِي عَبْدِ الله (عَلَيْهِ السَّلام)قَالَ قُلْتُ لَهُ مَا الْعَقْلُ قَالَ مَا عُبِدَ بِهِ الرَّحْمَنُ وَاكْتُسِبَ بِهِ الْجِنَانُ قَالَ قُلْتُ فَالَّذِي كَانَ فِي مُعَاوِيَةَ فَقَالَ تِلْكَ النَّكْرَاءُ تِلْكَ الشَّيْطَنَةُ وَهِيَ شَبِيهَةٌ بِالْعَقْلِ وَلَيْسَتْ بِالْعَقْلِ.", 'الکافي')

