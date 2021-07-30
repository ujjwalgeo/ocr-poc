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


def process_folder(input_folder, project_name, num_files=None, output_folder=None):
    db_name = get_dbname_from_project_name(project_name)
    mongo_helper = MongoHelper(dbname=db_name)

    if output_folder is None:
        output_folder = os.path.join(os.path.dirname(__file__), 'asbuilts')

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    pdf_files = glob(os.path.join(input_folder, '*.pdf'))
    if not num_files is None:
        n_files = min(len(pdf_files), num_files)
        pdf_files = pdf_files[: n_files]
    
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
            log.debug('Error while extracting pages for %s' % pdf_file)
            print(str(ex))

    if len(inserted_asbuilts):
        mongo_helper.insert_many(ASBUILTS_COLLECTION, inserted_asbuilts)


def ocr_asbuilts(project_name, overwrite=False):
    db_name = get_dbname_from_project_name(project_name)
    mongo_helper = MongoHelper(dbname=db_name)
    as_builts = mongo_helper.query(ASBUILTS_COLLECTION, { 'project': project_name } )
    log.info('Will OCR %d as-builts' % as_builts.count())

    for as_built in as_builts:
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
        ep_dirty = False
        for ep in extracted_pages:
            if overwrite or (ep.get('ocr_analysis_id') is None):
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

                ep_dirty = True

        if ep_dirty:
            mongo_helper.update_document(ASBUILTS_COLLECTION, as_built['_id'], {'extracted': extracted_pages})


if __name__ == '__main__':

    # folder = r'/Users/ujjwal/projects/cci/data/as-builts/chicago_test'
    folder  = r"/home/unarayan@us.crowncastle.com/ocrpoc/data/chicago/"
    project_id = 'chicago_big'

    # process_folder(folder, project_id, num_files=None)
    ocr_asbuilts(project_id)
