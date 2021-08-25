"""
db.getCollection('asbuilts').aggregate([

 { $match: { 'pages.raw_text': { $regex: /POLE ELEVATION/ } } },

 { $project: {
     pages: {
         $filter: {
             input: '$pages',
             as: 'pages',
             cond: { $regexMatch: {input:'$$pages.raw_text', regex: /POLE ELEVATION/ } }
         }
     }
 }}

])
"""

from pymongo import MongoClient
from common.config import MONGO_DB, ASBUILTS_COLLECTION, DBNAME, OCR_LINE_COLLECTION
import re
from bson import ObjectId
from common.mongodb_helper import MongoHelper

client = MongoClient(MONGO_DB)
db = client[DBNAME]
coll = db[ASBUILTS_COLLECTION]
asbuilt_id = "6109a413abba9c3dbd230d51"


def annotate_elevation_page(asbuilt_id, search_text='pole elevation', annotation='POLE ELEVATION'):

    asbuilt = coll.find_one({"_id": asbuilt_id})

    asbuilt_pages = asbuilt['pages']
    regx = re.compile(search_text, re.IGNORECASE)
    idx = 0
    for abp in asbuilt_pages:
        if "ocr_analysis_id" in abp:
            ocr_analysis_id = abp["ocr_analysis_id"]
            ocr_lines_query = {"analysis_id": ocr_analysis_id, "text": regx}
            ocr_lines_count = db[OCR_LINE_COLLECTION].find(ocr_lines_query).count()
            if ocr_lines_count > 0:
                coll.update_one({"_id": asbuilt["_id"]}, {'$set': {'pages.%d.page_type' % idx: annotation}})
                print('Updated page type for %s, page %d' % (str(asbuilt["_id"]), idx))
        else:
            print('No ocr analysis for %s, page %d' % (str(asbuilt["_id"]), idx))
        idx += 1


def annotate_elevation_page_using_rawtext(asbuilt, search_text='pole elevation', annotation='POLE ELEVATION'):
    asbuilt_id = asbuilt["_id"]
    regx = re.compile(r'^%s$' % search_text, re.IGNORECASE)
    # regx = re.compile(search_text, re.IGNORECASE)
    if isinstance(asbuilt_id, str):
        asbuilt_id = ObjectId(asbuilt_id)

    cursor = coll.aggregate(
        [
            {'$match': {'pages.raw_text': regx, '_id': asbuilt_id}},
            {'$project': {
                'pages': {
                    '$filter': {
                        'input': '$pages',
                        'as': 'pages',
                        'cond': {'$regexMatch': {'input': '$$pages.raw_text', 'regex': search_text, 'options': 'i'}}
                    }
                }
            }}
        ]
    )
    docs = list(cursor)
    for doc in docs:
        # print("%s, %d" % (doc['_id'], len(doc['pages'])))
        pages = doc['pages']

        if len(pages) >= 1:
            page_number = pages[0]['page']
            page_type = annotation

            # update asbuilt
            asbuilt_pages = asbuilt['pages']
            idx = 0
            found_page = -1
            for abp in asbuilt_pages:
                if abp['page'] == page_number:
                    found_page = idx
                    break
                idx += 1
            if found_page >= 0:
                coll.update_one({"_id": asbuilt_id}, {'$set': {'pages.%d.page_type' % found_page: page_type}})
                print('Updated page type for %s, page %d' % (str(asbuilt_id), found_page))


asbuilts = coll.find()
asbuilt_ids = [o["_id"] for o in asbuilts]

for asbuilt_id in asbuilt_ids:

    annotate_elevation_page(asbuilt_id, 'pole elevation', 'proposed_pole_elevation')
    annotate_elevation_page(asbuilt_id, 'existing pole elevation', 'existing_pole_elevation')

    annotate_elevation_page(asbuilt_id, 'pole detail', 'proposed_pole_elevation')
    annotate_elevation_page(asbuilt_id, 'proposed wood pole detail', 'proposed_pole_elevation')

