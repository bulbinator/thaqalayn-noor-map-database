import pymongo
from pymongo.errors import BulkWriteError
import os
import re
import requests
from dotenv import find_dotenv, load_dotenv
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
THAQ_DB_PASSWORD = os.getenv("THAQ_DB_PASSWORD")
client = pymongo.MongoClient(f"mongodb+srv://syedmaisum1:{THAQ_DB_PASSWORD}@thaqalayn-noor.rri8t2j.mongodb.net/?retryWrites=true&w=majority&appName=thaqalayn-noor")
thaq_db = client['thaqalayn-noor']
thaq_collection = thaq_db['data']

response = requests.get('https://www.thaqalayn-api.net/api/v2/allbooks')
books = response.json()
books_to_not_include = ['Kitab-al-Duafa-Ghadairi', 'Mujam-al-Ahadith-al-Mutabara-Muhsini', 'Risalat-al-Huquq-Abidin']
book_ids = [book['bookId'] for book in books if book['bookId'] not in books_to_not_include]

all_hadiths = []

for id in tqdm(book_ids):
    url = f"https://www.thaqalayn-api.net/api/v2/{id}"
    response = requests.get(url)
    hadiths = response.json()

    for hadith in tqdm(hadiths, desc=f"Uploading hadiths from '{id}'"):
        all_hadiths.append(
            {
                'thaqId': hadith['id'],
                'thaqBookId': hadith['bookId'],
                'thaqURL': hadith['URL'],
                'thaqText': hadith['arabicText']
            }
        )

thaq_collection.insert_many(all_hadiths)