import pymongo

def delete() -> None:
    client = pymongo.MongoClient('localhost', 27017)
    db = client['UniversityDB']
    collection = db['university']
    collection.drop()