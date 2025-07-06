from sentence_transformers import SentenceTransformer

model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')

def get_embedding(text):
    return model.encode(text).tolist()
