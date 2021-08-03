import os
from glob import glob
from bson import ObjectId
from common.pdf_helper import PDFDocument
from common.mongodb_helper import MongoHelper
from common.azure_ocr_helper import run_ocr_restapi
from common.config import ASBUILTS_COLLECTION, OCR_LINE_COLLECTION, AZURE_ANALYSIS_COLLECTION


def get_dbname_from_project_name(project_name):
    project_name = "".join([p for p in project_name if str(p).isalnum()])
    project_name = project_name.replace(" ", "-")
    db_name = project_name
    return db_name


def process_folder(input_folder, project_name, db_name, num_files=None, output_folder=None, overwrite=False):
    if output_folder is None:
        output_folder = os.path.join(os.path.dirname(__file__), 'asbuilts')

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    pdf_files = glob(os.path.join(input_folder, '*.pdf'))
    if not num_files is None:
        n_files = min(len(pdf_files), num_files)
        pdf_files = pdf_files[: n_files]

    pdf_files = sorted(pdf_files)
    inserted_asbuilts = []
    pages = [1, 2, 3, 4]
    idx = 0
    n_files = len(pdf_files)
    for pdf_file in pdf_files:

        log.info('Extracting pages for %s (%d of %d)' % (pdf_file, idx+1, n_files))
        pdf_doc = PDFDocument(file_path=pdf_file, output_dir=output_folder)
        asbuilt_oid = ObjectId()
        try:
            extracted = pdf_doc.extract_pages(pages, overwrite=overwrite)
            asbuilt = {
                "_id": asbuilt_oid,
                "source_file": pdf_file,
                "num_pages": len(pdf_doc.pages),
                "pages": extracted,
                "project": project_name
            }
            inserted_asbuilts.append(asbuilt)
        except Exception as ex:
            log.debug('Error while extracting pages for %s' % pdf_file)
            print(str(ex))
        idx += 1

    if len(inserted_asbuilts):
        mongo_helper = MongoHelper(dbname=db_name)
        mongo_helper.insert_many(ASBUILTS_COLLECTION, inserted_asbuilts)


def ocr_red_images(project_name, db_name, overwrite=True):
    mongo_helper = MongoHelper(dbname=db_name)
    as_builts = mongo_helper.query(ASBUILTS_COLLECTION, { 'project': project_name } )
    as_built_ids = [ab['_id'] for ab in as_builts] # save ids and get docs again in for loop to prevent cursor timeout
    log.info('Will OCR %d as-builts' % as_builts.count())

    for as_built_id in as_built_ids:
        as_built = mongo_helper.get_document(ASBUILTS_COLLECTION, as_built_id)
        # ocr each page for as built
        # log.info("as-built-id %s" % as_built["_id"])
        if as_built.get('pages') is None:
            log.info('skip  as built %s since not extracted' % as_built['_id'])
            continue

        extracted_pages = as_built['pages']
        ep_dirty = False
        for ep in extracted_pages:
            if overwrite or (ep.get('red_analysis_id') is None):
                try:
                    # red image ocr
                    if ep['has_red_pixels']:
                        log.info('ocr red: %s' % ep['red_image'])
                        red_ocr_doc, red_ocr_lines = run_ocr_restapi(ep['red_image'], project_name,
                                                                     page_number=ep['page'],
                                                                     category='redline')
                        if len(red_ocr_lines):
                            inserted_red_analysis_id = mongo_helper.insert_one(AZURE_ANALYSIS_COLLECTION, red_ocr_doc)
                            mongo_helper.insert_many(OCR_LINE_COLLECTION, red_ocr_lines)
                            ep['red_ocr_analysis_id'] = red_ocr_doc['_id']
                        else:
                            ep['red_ocr_analysis_id'] = None
                    else:
                        ep['red_ocr_analysis_id'] = None
                    ep_dirty = True

                except Exception as ex:
                    log.debug("ocr error %s" % str(ex))

        if ep_dirty:
            mongo_helper.update_document(ASBUILTS_COLLECTION, as_built['_id'], {'pages': extracted_pages})


def ocr_asbuilts(project_name, db_name, overwrite=False):
    mongo_helper = MongoHelper(dbname=db_name)
    as_builts = mongo_helper.query(ASBUILTS_COLLECTION, { 'project': project_name } )
    as_built_ids = [ab['_id'] for ab in as_builts] # save ids and get docs again in for loop to prevent cursor timeout
    log.info('Will OCR %d as-builts' % as_builts.count())
    n_docs = len(as_built_ids)
    idx = 0
    for as_built_id in as_built_ids:
        idx += 1
        as_built = mongo_helper.get_document(ASBUILTS_COLLECTION, as_built_id)

        # ocr each page for as built
        log.info("as-built-id %s, %d of %d" % (as_built["_id"], idx, n_docs))
        if as_built.get('pages') is None:
            log.info('skip  as built %s since not extracted' % as_built['_id'])
            continue

        extracted_pages = as_built['pages']
        # {
        #     "pdf": pdf_out_file,
        #     "image": img_out_file,
        #     "image_width": img_w,
        #     "image_height": img_h,
        #     "red_image": red_image_path,
        #     "has_red_pixels": has_red_pixels,
        #     "page": i,
        #     "raw_text": text,
        #     "annotations": annotation
        # }
        ep_dirty = False
        for ep in extracted_pages:
            if overwrite or (ep.get('ocr_analysis_id') is None):
                log.info('ocr: %s' % ep['image'])
                try:
                    ocr_doc, ocr_lines = run_ocr_restapi(ep['image'], project_name, page_number=ep['page'],
                                                         category='as-built')
                    inserted_analysis_id = mongo_helper.insert_one(AZURE_ANALYSIS_COLLECTION, ocr_doc)
                    mongo_helper.insert_many(OCR_LINE_COLLECTION, ocr_lines)
                    ep['ocr_analysis_id'] = ocr_doc['_id']

                    # red image ocr
                    if ep['has_red_pixels']:
                        log.info('ocr: %s' % ep['red_image'])
                        red_ocr_doc, red_ocr_lines = run_ocr_restapi(ep['red_image'], project_name,
                                                                     page_number=ep['page'],
                                                                     category='redline')
                        inserted_red_analysis_id = mongo_helper.insert_one(AZURE_ANALYSIS_COLLECTION, red_ocr_doc)
                        mongo_helper.insert_many(OCR_LINE_COLLECTION, red_ocr_lines)

                        ep['red_ocr_analysis_id'] = red_ocr_doc['_id']
                    else:
                        ep['red_ocr_analysis_id'] = None
                    ep_dirty = True
                except Exception as ex:
                    log.debug("ocr error %s" % str(ex))
            else:
                log.info('skip ocr: %s' % ep['image'])

        if ep_dirty:
            mongo_helper.update_document(ASBUILTS_COLLECTION, as_built['_id'], {'pages': extracted_pages})


if __name__ == '__main__':
    from common import logger

    # folder = r'/Users/ujjwal/projects/cci/data/as-builts/chicago_test'
    # folder  = r"/home/unarayan@us.crowncastle.com/ocrpoc/data/chicago/"
    # project_id = 'chicago_big'
    # dbname = 'chicago_big1'

    # folder = r'/Users/ujjwal/projects/cci/data/as-builts/new_batch_demo'
    # project_id = 'new_batch_demo'
    # dbname = 'new_batch_demo'

    folder = r'/home/unarayan@us.crowncastle.com/ocrpoc/data/100_test_set_asbuilts'
    project_id = 'colo_test_set'
    dbname = 'colo_test_set'

    logger.setup()
    log = logger.logger

    process_folder(folder, project_id, dbname, num_files=None)
    ocr_asbuilts(project_id, dbname, True)
    ocr_red_images(project_id, dbname)
