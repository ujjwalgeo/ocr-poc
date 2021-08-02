from pymongo import MongoClient, TEXT, GEO2D
from common.config import ASBUILTS_COLLECTION, OCR_LINE_COLLECTION, AZURE_ANALYSIS_COLLECTION,  MONGO_DB
from common.mongodb_helper import MongoHelper
import common.logger


source_mongo = MONGO_DB
target_mongo = 'mongodb://localhost:27017'
source_db = 'chicago_big1'
target_db = 'chicago_big1'
batch_size = 100

source_client = MongoClient(source_mongo)
source_db = source_client[source_db]

target_client = MongoClient(target_mongo)
target_db = target_client[target_db]

collections = [ASBUILTS_COLLECTION, OCR_LINE_COLLECTION, AZURE_ANALYSIS_COLLECTION]


def idlimit(collection, page_size, last_id=None):
    """Function returns `page_size` number of documents after last_id
    and the new last_id.
    """
    if last_id is None:
        # When it is first page
        cursor = collection.find().limit(page_size)
    else:
        cursor = collection.find({'_id': {'$gt': last_id}}).limit(page_size)

    # Get the data
    data = [x for x in cursor]

    if not data:
        # No documents left
        return None, None

    # Since documents are naturally ordered with _id, last document will
    # have max id.
    last_id = data[-1]['_id']

    # Return data and last_id
    return data, last_id


def copy_collection(collection):
    source_docs = source_db[collection]
    target_docs = target_db[collection]

    data, last_id = idlimit(source_docs)
    while not (last_id is None):
        target_docs.insert_many(data, ordering=False)
        data, last_id = idlimit(source_docs, last_id)
        log.info(last_id)


if __name__ == "__main__":
    common.logger.setup()
    log = common.logger.logger

    for c in collections:
        copy_collection(c)

