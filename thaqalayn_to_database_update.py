import pymongo
import os
import requests
from dotenv import find_dotenv, load_dotenv
from tqdm import tqdm

# Load .env
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
THAQ_DB_PASSWORD = os.getenv("THAQ_DB_PASSWORD")

# MongoDB setup
client = pymongo.MongoClient(
    f"mongodb+srv://syedmaisum1:{THAQ_DB_PASSWORD}@thaqalayn-noor.rri8t2j.mongodb.net/?retryWrites=true&w=majority&appName=thaqalayn-noor"
)
thaq_db = client['thaqalayn-noor']
thaq_collection = thaq_db['data']

response = requests.get('https://www.thaqalayn-api.net/api/v2/allbooks')
books = response.json()
books_to_skip = ['Kitab-al-Duafa-Ghadairi', 'Mujam-al-Ahadith-al-Mutabara-Muhsini', 'Risalat-al-Huquq-Abidin']
book_ids = [book['bookId'] for book in books if book['bookId'] not in books_to_skip]

for book_id in tqdm(book_ids, desc="Processing books"):
    url = f"https://www.thaqalayn-api.net/api/v2/{book_id}"
    response = requests.get(url)
    hadiths = response.json()

    for hadith in tqdm(hadiths, desc=f"Updating hadiths from '{book_id}'", leave=False):
        thaq_id = hadith['id']
        sanad = hadith.get('thaqalaynSanad')
        matn = hadith.get('thaqalaynMatn')

        update_fields = {}
        if sanad:
            update_fields['thaqEngSanad'] = sanad
        if matn:
            update_fields['thaqEngMatn'] = matn

        if update_fields:
            result = thaq_collection.update_one(
                {"thaqId": thaq_id, "thaqBookId": book_id},
                {"$set": update_fields}
            )
