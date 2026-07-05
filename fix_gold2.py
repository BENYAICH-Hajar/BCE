from pymongo import MongoClient

db = MongoClient('mongodb://localhost:27017')['bce']
coll = db['hotel_gold']

# Supprimer les docs avec points si la version sans points existe déjà
fixed = 0
deleted = 0

for doc in list(coll.find({}, {'_id': 1, 'enterprise_number': 1})):
    old = doc['enterprise_number']
    new = old.replace('.', '')
    if old == new:
        continue  # déjà bon
    # Vérifier si la version sans points existe
    existing = coll.find_one({'enterprise_number': new})
    if existing:
        # Supprimer le doublon avec points
        coll.delete_one({'_id': doc['_id']})
        deleted += 1
    else:
        # Renommer
        coll.update_one({'_id': doc['_id']}, {'$set': {'enterprise_number': new}})
        fixed += 1

print(f'OK : {fixed} corrigés, {deleted} doublons supprimés')
print(f'Total hotel_gold : {coll.count_documents({})}')