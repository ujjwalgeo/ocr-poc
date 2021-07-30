import os
from glob import glob
from bson import ObjectId
from common.pdf_helper import PDFDocument
from common.logger import logger as log
from common.mongodb_helper import MongoHelper
from common.azure_ocr_helper import run_ocr_restapi



ASBUILTS_COLLECTION = 'asbuilts'


def get_dbname_from_project_name(project_name):
    project_name = "".join([p for p in project_name if str(p).isalnum()])
    project_name = project_name.replace(" ", "-")
    db_name = project_name
    return db_name


def process_folder(input_folder, project_name, output_folder=None):
    db_name = get_dbname_from_project_name(project_name)
    mongo_helper = MongoHelper(dbname=db_name)

    if output_folder is None:
        output_folder = os.path.join(os.path.dirname(__file__), 'asbuilts')

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    pdf_files = glob(os.path.join(input_folder, '*.pdf'))

    inserted_asbuilts = []
    pages = [1, 2, 3, 4]
    for pdf_file in pdf_files:
        log.info('Extracting pages for %s' % pdf_file)
        pdf_doc = PDFDocument(file_path=pdf_file, output_dir=output_folder)
        asbuilt_oid = ObjectId()
        try:
            extracted = pdf_doc.extract_pages(pages)
            asbuilt = {
                "_id": asbuilt_oid,
                "source_file": pdf_file,
                "num_pages": len(pdf_doc.pages),
                "pages": extracted,
                "project": project_name
            }
            inserted_asbuilts.append(asbuilt)
        except Exception as ex:
            log.debug('Error extracted pages for %s' % pdf_file)

    mongo_helper.insert_many(ASBUILTS_COLLECTION, inserted_asbuilts)


def ocr_asbuilts(project_name):
    db_name = get_dbname_from_project_name(project_name)
    mongo_helper = MongoHelper(dbname=db_name)
    as_builts = mongo_helper.query(ASBUILTS_COLLECTION, { 'project': project_name } )
    log.info('Will OCR %d as-builts' % as_builts.count())

    for as_built in as_builts[:2]:
        # ocr each page for as built
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
        for ep in extracted_pages:
            log.info('ocr: %s' % ep['image'])
            ocr_doc, ocr_lines = run_ocr_restapi(ep['image'], project_name, category='as-built')
            mongo_helper.insert_one('azure_analysis', ocr_doc)
            mongo_helper.insert_many('ocr_lines', ocr_lines)
            ep['ocr_analysis_id'] = ocr_doc['_id']

            # red image ocr
            if ep['has_red_pixels']:
                log.info('ocr: %s' % ep['red_image'])
                red_ocr_doc, red_ocr_lines = run_ocr_restapi(ep['red_image'], project_name, category='as-built')
                ep['red_ocr_analysis_id'] = red_ocr_doc['_id']
            else:
                ep['red_ocr_analysis_id'] = None

        mongo_helper.update_document(ASBUILTS_COLLECTION, as_built['_id'], {'extracted': extracted_pages})


if __name__ == '__main__':

    folder = r'/Users/ujjwal/projects/cci/data/as-builts/chicago_test'
    project_id = 'chicago_test_7'
    # process_folder(folder, project_id)
    ocr_asbuilts(project_id)
