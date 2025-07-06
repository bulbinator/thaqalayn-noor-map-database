# thaqalayn-noor-map-database
A program to map most hadith in thaqalayn.net to its corresponding hadith from hadith.inoor using embeddings and upload them to a databse.

# Uploading and Embedding hadith.noor data to MongoDB
The first step of the process will be to upload and embedd the hadith data from hadith.noor to MongoDB. We can access this data by sending `POST` requests to https://hadith.inoor.ir/service/api/elastic/ElasticHadithById, with a payload containing a list of hadithIds and searchPhrases. All hadithIds can be found on hadith.noor's site map: https://hadith.inoor.ir/assets/download/sitemap/sitemap-index.xml
