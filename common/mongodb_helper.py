from pymongo import MongoClient, TEXT, GEO2D
from datetime import datetime
from bson import ObjectId
import re
from common.config import MONGO_DB, OCR_LINE_COLLECTION, AZURE_ANALYSIS_COLLECTION, ASBUILTS_COLLECTION


# start mongo server
# mongod --config /usr/local/etc/mongod.conf

class MongoHelper(object):

    def __init__(self, dbname):
        client = MongoClient(MONGO_DB)
        try:
            client.server_info()
            self.db = client[dbname]
        except Exception as ex:
            raise ex

    def query(self, coll_name, query):
        coll = self.db[coll_name]
        return coll.find(query)

    def get_document(self, coll_name, id):
        if isinstance(id, str):
            id = ObjectId(id)

        query = {"_id": id}
        coll = self.db[coll_name]
        return coll.find_one(query)

    def update_document(self, coll_name, id, values):
        if isinstance(id, str):
            id = ObjectId(id)

        query = {"_id": id}
        coll = self.db[coll_name]
        coll.update_one(query, {"$set": values})

    def insert_one(self, coll_name, doc):
        if not 'created_dt' in doc:
            doc['created_dt'] = datetime.now()
        doc_id = self.db[coll_name].insert_one(doc).inserted_id
        return doc_id

    def insert_many(self, coll_name, docs):
        for doc in docs:
            if not 'created_dt' in doc:
                doc['created_dt'] = datetime.now()
        self.db[coll_name].insert_many(docs, ordered=False)

    def create_indexes(self, coll_name='ocr_line'):
        line_collection = self.db[coll_name]
        line_collection.create_index([('text', TEXT)], name='%s_text_index' % coll_name)
        line_collection.create_index([('centroid', GEO2D)], min=-10000, max=10000, name='%s_centroid_index' % coll_name)

    def search_text(self, coll_name, text, analysis_id, page_num):
        coll = self.db[coll_name]
        if isinstance(analysis_id, str):
            analysis_id = ObjectId(analysis_id)
        regx = re.compile(text, re.IGNORECASE)
        query = {'text': regx, 'page': page_num, 'analysis_id': analysis_id}
        docs = coll.find(query)
        return list(docs)

    def get_site_info_bbox(self, analysis_id, text='site information'):
        docs = self.search_text(OCR_LINE_COLLECTION, text=text, analysis_id=analysis_id, page_num=1)
        if len(docs):
            doc = docs[0]
            bbox = doc['boundingBox']
            return bbox

    def get_totals_bbox(self, analysis_id, page_num=1):
        docs = self.search_text('ocr_line', text='totals', analysis_id=analysis_id, page_num=page_num)
        doc = docs[0]
        bbox = doc['boundingBox']
        return bbox

    def get_equipment_key_bbox(self, analysis_id, page_num=2):
        docs = self.search_text('ocr_line', text='Equipment Key', analysis_id=analysis_id, page_num=page_num)
        try:
            doc = docs[0]
            bbox = doc['boundingBox']
            return bbox
        except:
            # print('no eq key bbox for %s, %d' % (analysis_id, page_num) )
            pass
