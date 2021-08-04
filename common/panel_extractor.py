import pandas as pd
from common.mongodb_helper import MongoHelper
from common import simple_line_detector
from common import table_maker
from common import azure_ocr_helper
from common.config import ASBUILTS_COLLECTION, AZURE_ANALYSIS_COLLECTION, OCR_LINE_COLLECTION
from shapely.geometry import Point, Polygon, LineString


def _construct_site_info_tables(mongo_hlpr, asbuilt_id):
    asbuilt = mongo_hlpr.get_document(ASBUILTS_COLLECTION, asbuilt_id)
    site_info_kvps = {}

    if (not 'site_info' in asbuilt ) or (asbuilt['site_info'] is None):
        return site_info_kvps

    site_info = asbuilt['site_info']
    if not 'cells' in site_info:
        return site_info_kvps

    table_cells = site_info['cells']
    site_info_analysis = mongo_hlpr.get_document(AZURE_ANALYSIS_COLLECTION, site_info['analysis_id'])
    site_info_lines = site_info_analysis['analysis']['analyzeResult']['readResults'][0]['lines']

    for tc in table_cells:
        ul = tc['ul']
        br = tc['br']
        tc['poly'] = Polygon([
            (ul[0], ul[1]), (br[0], ul[1]), (br[0], br[1]), (ul[0], br[1])
        ])

    lines_data = {
        'cols': [],
        'rows': [],
        'text': []
    }
    for line in site_info_lines:
        bbox = line['boundingBox']
        bbox_poly = Polygon([
            (bbox[0], bbox[1]),
            (bbox[2], bbox[3]),
            (bbox[4], bbox[5]),
            (bbox[6], bbox[7]),
        ])
        max_coverage = 0
        intersect_tc = None
        for tc in table_cells:
            intersection = bbox_poly.intersection(tc['poly'])
            coverage = intersection.area / bbox_poly.area
            if coverage > max_coverage:
                max_coverage = coverage
                intersect_tc = tc

        if intersect_tc:
            lines_data['cols'].append(intersect_tc['j'])
            lines_data['rows'].append(intersect_tc['i'])
            lines_data['text'].append(line['text'])

    # find 2 columns with the highest number of text/lines. then lower value column is left column and higher value
    # column is right column

    lines_df = pd.DataFrame(lines_data)
    col_counts = lines_df.cols.value_counts()
    col_counts = col_counts.sort_values(ascending=False)
    col1_idx = col_counts.index[0]
    col2_idx = col_counts.index[1]

    if col1_idx <= col2_idx:
        left_col = col1_idx
        right_col = col2_idx
    else:
        left_col = col2_idx
        right_col = col1_idx

    unique_rows = lines_df.rows.unique()

    for unique_row in unique_rows:
        row_df = lines_df.where(lines_df['rows'] == unique_row)
        key_text = row_df[row_df['cols'] == left_col]['text']
        value_text = row_df[row_df['cols'] == right_col]['text']
        if len(key_text.values) and len(value_text.values):
            print("%s --> %s" % (key_text.values[0], value_text.values[0]))
            site_info_kvps[key_text.values[0]] = value_text.values[0]

    return site_info_kvps


def _extract_site_info_data(dbname, asbuilt_id, text_size_percent=2, line_distance_percent=2,
                            rerun_ocr=False, debug_mode=False):

    mongo_helper = MongoHelper(dbname)
    asbuilt = mongo_helper.get_document(ASBUILTS_COLLECTION, asbuilt_id)
    asbuilt_pages = asbuilt['pages']
    site_info_panel_file = None
    mongo_helper.close()

    for abp in asbuilt_pages:

        mongo_helper = MongoHelper(dbname)
        analysis_id = abp.get('ocr_analysis_id')
        if analysis_id is None:
            continue

        analysis_doc = mongo_helper.get_document(AZURE_ANALYSIS_COLLECTION, analysis_id)
        site_info_panel_file = None

        bbox = mongo_helper.get_site_info_bbox(analysis_id, text='site information')

        # if site information header is absent, try project summary
        if bbox is None:
            bbox = mongo_helper.get_site_info_bbox(analysis_id, text='project summary')

        if bbox:
            page = abp
            # dpi = page['image_width'] / page['pdf_width'] # 3400.0 / 17
            page_image = page['image']
            mongo_helper.close()

            try:
                site_info_panel_file = simple_line_detector.detect_panel(page_image, bbox,
                                                  panel_name="site_info_panel", debug_mode=debug_mode,
                                                                         overwrite=rerun_ocr)

                table_file, cells = table_maker.detect_table(site_info_panel_file, text_size_percent,
                                                             line_distance_percent, debug_mode)

                mongo_helper = MongoHelper(dbname)

                site_info = {
                    "page": abp['page'],
                    "image_file": site_info_panel_file,
                    "table_file": table_file,
                    "cells": cells,
                }

                run_ocr = False
                if not 'site_info' in asbuilt:
                    rerun_ocr = True
                elif analysis_doc['site_info'] is None:
                    rerun_ocr = True
                else:
                    if not 'analysis_id' in asbuilt['site_info']:
                        rerun_ocr = True

                if rerun_ocr:
                    run_ocr = True
                else:
                    if not 'analysis_id' in asbuilt['site_info']:
                        run_ocr = True
                    else:
                        site_info["analysis_id"] = asbuilt['site_info']['analysis_id']
                if run_ocr:
                    mongo_helper.close()
                    site_info_analysis_doc, site_info_analysis_lines = \
                        azure_ocr_helper.run_ocr_restapi(site_info_panel_file, project_name=asbuilt['project'],
                                            page_number=abp['page'], category="site-info")
                    site_info["analysis_id"] = site_info_analysis_doc['_id']

                    # re open mongo connection after ocr
                    mongo_helper = MongoHelper(dbname)

                    mongo_helper.insert_one(AZURE_ANALYSIS_COLLECTION, site_info_analysis_doc)
                    mongo_helper.insert_many(OCR_LINE_COLLECTION, site_info_analysis_lines)

                mongo_helper.update_document(ASBUILTS_COLLECTION, asbuilt_id, {"site_info": site_info})

                site_info_kvps = _construct_site_info_tables(mongo_helper, asbuilt_id)
                site_info["kvps"] = site_info_kvps
                print(site_info_kvps)

                mongo_helper.update_document(ASBUILTS_COLLECTION, asbuilt_id, {"site_info": site_info})
                log.info("Success extracting site info asbuilt: %s,  %s" % (asbuilt_id, asbuilt["source_file"]))

                break # return after first success finding panel
            except Exception as ex:
                log.info("Error extracting panels for %s,  %s" % (analysis_id, analysis_doc["source_file"]))
                log.info(str(ex))
                break
        else:
            log.info("No site info for %s,  %s" % (analysis_id, analysis_doc["source_file"]))
            mongo_helper.update_document(AZURE_ANALYSIS_COLLECTION, analysis_id, {"site_info": None})

    return site_info_panel_file


def process_panels(dbname, project_name):
    # detect and write panel data
    mongo_helper = MongoHelper(dbname)
    _docs = mongo_helper.query(ASBUILTS_COLLECTION, {'project': project_name})
    ids = [t['_id'] for t in _docs]

    for id in ids:
        log.info("Extracting site info for %s " % id)
        site_info_panel = _extract_site_info_data(dbname, id)
        # print(site_info_panel)


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

    process_panels(dbname, project_id)
