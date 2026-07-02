from pymongo import MongoClient
db = MongoClient('mongodb://localhost:27017')['bce']
doc = db['enterprise_silver'].find_one({"denomination_principale": {"$exists": True}})
print(doc.get('denomination_principale') if doc else 'champ absent')

# Compter combien en ont un
count = db['enterprise_silver'].count_documents({"denomination_principale": {"$exists": True, "$ne": None}})
print(f"Docs avec denomination_principale : {count}")