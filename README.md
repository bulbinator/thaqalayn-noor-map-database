# thaqalayn-noor-map-database
A program to map most hadith in thaqalayn.net to its corresponding hadith from hadith.inoor using embeddings and upload them to a databse.

# Uploading and Embedding hadith.noor data to MongoDB
The first step of the process will be to upload and embedd the hadith data from hadith.inoor to MongoDB. We can access this data by sending `POST` requests to https://hadith.inoor.ir/service/api/elastic/ElasticHadithById, with a payload containing a list of `hadithIds` and `searchPhrases`. All `hadithIds` can be found on hadith.inoor's site map: https://hadith.inoor.ir/assets/download/sitemap/sitemap-index.xml. Total number of ahadith in inoor collection is 389,577.

# `noor_to_json.py`
The hadith.inoor API lets you send a request with a maximum of 1,000 hadithIds, and rate limits you after 100 requests. So to fetch 25,000 ahadthis, the API must be called about ~400 times, which is not possible to do in one program. So this file splits all hadithIds into 4 parts, and the file must be run manually over each part by changing the `PART = 1` variable in the file. Once the ahadith have been fetched they are then stored into a `.json` file which we will use to later upload the ahadith to the database. The 4 `.json` files were not included in this repo since they are each too big, so the files were stored locally and merged together into a `.json` file containing all hadith data from inoor, which also has been removed for the aforementioned reason.

# `json_to_database.py`
In this file we will begin the process of filtering and embedding the hadiths to the database. We will first read all the hadith data from the `.json` file we created earlier and then we will filter through all the hadiths from inoor and take only the ones that are in books found on thaqalayn.net (~25,000). From each valid hadith we will take it's `hadithId`, `bookTitle`, and it's `text`. From then on we will apply our custom `normalize` function on the text to strip it of any i'rab and XML tags so that the similarity process is accurate. Then we will encode each `text` using our custom `get_embedding` function in batches of 1,000 at a time (soley for the purpose of displaying a progress bar). Once all hadith are processed, they get uploaded to the database.

# `hadithids.py`
This file is used to find every hadithId on inoor's site map.

# `embedding.py`
This file is used to perform the embedding process, we have tried the following sentence-transformers: `all-mpnet-base-v2`, `paraphrase-multilingual-MiniLM-L12-v2`, `all-MiniLM-L6-v2`, `Arabic-KW-Mdel`, `multilingual-e5-small`. However, we found `all-mpnet-base-v2` to be the best and most accurate which is surpising considering that it was not made for a language other than English.

# `text_utils.py`
This file is used to normalize arabic text by striping it of i'rab and XML tags (which come from inoor's site). By doing this the vector search will be more accurate.
