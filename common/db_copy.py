from pymongo import MongoClient, TEXT, GEO2D
from common.config import ASBUILTS_COLLECTION, OCR_LINE_COLLECTION, AZURE_ANALYSIS_COLLECTION,  MONGO_DB
from common.mongodb_helper import MongoHelper
import common.logger


source_mongo = MONGO_DB
target_mongo = 'mongodb://localhost:27017'
source_db = 'chicago_big1'
target_db = 'chicago_big1'
page_size = 100

source_client = MongoClient(source_mongo)
source_db = source_client[source_db]

target_client = MongoClient(target_mongo)
target_db = target_client[target_db]

collections = [ASBUILTS_COLLECTION, OCR_LINE_COLLECTION, AZURE_ANALYSIS_COLLECTION]


def skiplimit(collection, page_num):
    """returns a set of documents belonging to page number `page_num`
    where size of each page is `page_size`.
    """
    # Calculate number of documents to skip
    skips = page_size * (page_num - 1)

    # Skip and limit
    cursor = collection.find().skip(skips).limit(page_size)

    # Return documents
    return [x for x in cursor]


def idlimit(collection, last_id=None):
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
        target_docs.insert_many(data, ordered=False)
        data, last_id = idlimit(source_docs, last_id)
        log.info(last_id)


def copy_collection1(collection):
    source_docs = source_db[collection]
    target_docs = target_db[collection]
    num_source_docs = source_docs.count()
    num_pages = 1 + int(num_source_docs / page_size)

    page_num = 1
    while page_num <= num_pages:
        data = skiplimit(source_docs, page_num)
        target_docs.insert_many(data)
        page_num += 1


if __name__ == "__main__":
    common.logger.setup()
    log = common.logger.logger

    for c in collections:
        copy_collection(c)

