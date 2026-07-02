"""
Enrichit enterprise_silver avec denomination_principale et address
depuis les collections denominations et addresses.
"""
from pymongo import MongoClient, UpdateOne
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

db = MongoClient('mongodb://localhost:27017')['bce']

# 1. Charger toutes les dénominations (TypeOfDenomination=001 = nom officiel)
log.info("Chargement des dénominations...")
denoms = {}
for d in db['denominations'].find({}, {'_id': 0, 'EntityNumber': 1, 'Denomination': 1, 'TypeOfDenomination': 1, 'Language': 1}):
    bce = d.get('EntityNumber')
    if not bce:
        continue
    # Priorité : TypeOfDenomination=001
    if bce not in denoms or str(d.get('TypeOfDenomination', '')) == '001':
        denoms[bce] = d.get('Denomination', '')

log.info(f"  {len(denoms):,} dénominations chargées")

# 2. Charger les adresses REGO
log.info("Chargement des adresses REGO...")
addresses = {}
for a in db['addresses'].find({'TypeOfAddress': 'REGO'}, {
    '_id': 0, 'EntityNumber': 1,
    'MunicipalityFR': 1, 'Municipality': 1,
    'Zipcode': 1, 'StreetFR': 1, 'Street': 1,
    'HouseNumber': 1, 'TypeOfAddress': 1
}):
    bce = a.get('EntityNumber')
    if bce:
        addresses[bce] = a

log.info(f"  {len(addresses):,} adresses chargées")

# 3. Mettre à jour enterprise_silver en batch
log.info("Mise à jour enterprise_silver...")
ops = []
total = db['enterprise_silver'].count_documents({})
done = 0

for doc in db['enterprise_silver'].find({}, {'_id': 0, 'EnterpriseNumber': 1}):
    bce = doc['EnterpriseNumber']
    update = {}

    denom = denoms.get(bce)
    if denom:
        update['denomination_principale'] = denom

    addr = addresses.get(bce)
    if addr:
        update['address'] = addr

    if update:
        ops.append(UpdateOne({'EnterpriseNumber': bce}, {'$set': update}))

    if len(ops) >= 2000:
        db['enterprise_silver'].bulk_write(ops, ordered=False)
        done += len(ops)
        ops = []
        log.info(f"  {done:,} / {total:,}")

if ops:
    db['enterprise_silver'].bulk_write(ops, ordered=False)
    done += len(ops)

log.info(f"✅ Terminé — {done:,} documents enrichis")

# Vérification
sample = db['enterprise_silver'].find_one({'denomination_principale': {'$exists': True}})
print(f"\nExemple : {sample.get('EnterpriseNumber')} → {sample.get('denomination_principale')}")