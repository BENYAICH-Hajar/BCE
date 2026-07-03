from pymongo import MongoClient
db = MongoClient('mongodb://localhost:27017')['bce']
fixed = 0
for doc in db['hotel_gold'].find({}, {'_id': 1, 'enterprise_number': 1}):
    old = doc['enterprise_number']
    new = old.replace('.', '')
    if old != new:
        db['hotel_gold'].update_one({'_id': doc['_id']}, {'$set': {'enterprise_number': new}})
        fixed += 1
print('OK', fixed, 'corriges')
