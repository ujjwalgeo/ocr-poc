import pandas as pd
from common.mongodb_helper import MongoHelper
from common import simple_line_detector
from common import table_maker
from common import azure_ocr_helper
from common.config import ASBUILTS_COLLECTION, AZURE_ANALYSIS_COLLECTION, OCR_LINE_COLLECTION
from shapely.geometry import Point, Polygon, LineString


def _construct_site_info_tables(mongo_hlpr, analysis_id):
    az_analysis = mongo_hlpr.get_document('azure_analysis', analysis_id)
    site_info_kvps = {}

    if (not 'site_info' in az_analysis) or (az_analysis['site_info'] is None):
        return site_info_kvps

    site_info = az_analysis['site_info']
    if not 'cells' in site_info:
        return site_info_kvps

    table_cells = site_info['cells']
    site_info_analysis = mongo_hlpr.get_document('azure_analysis', site_info['analysis_id'])
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
            # print("%s --> %s" % (key_text.values[0], value_text.values[0]))
            site_info_kvps[key_text.values[0]] = value_text.values[0]

    return site_info_kvps


def _extract_site_info_data(mongo_helper, asbuilt_id, text_size_percent=3, line_distance_percent=5,
                            rerun_ocr=False, debug_mode=False):

    asbuilt = mongo_helper.get_document(ASBUILTS_COLLECTION, asbuilt_id)
    asbuilt_pages = asbuilt['pages']

    for abp in asbuilt_pages:

        analysis_id = abp['ocr_analysis_id']
        analysis_doc = mongo_helper.get_document(AZURE_ANALYSIS_COLLECTION, analysis_id)
        site_info_panel_file = None
        page_num = 0
        bbox = mongo_helper.get_site_info_bbox(analysis_id, text='site information')

        # if site information header is absent, try project summary
        if bbox is None:
            bbox = mongo_helper.get_site_info_bbox(analysis_id, text='project summary')

        if bbox:
            page = abp
            # dpi = page['image_width'] / page['pdf_width'] # 3400.0 / 17
            page_image = page['image']
            site_info_panel_file = simple_line_detector.detect_panel(page_image, bbox, dpi,
                                              panel_name="site_info_panel", debug_mode=debug_mode)

            table_file, cells = table_maker.detect_table(site_info_panel_file, text_size_percent, line_distance_percent,
                                                         debug_mode)
            site_info = {
                "page": page_num,
                "image_file": site_info_panel_file,
                "table_file": table_file,
                "cells": cells,
            }

            run_ocr = False
            if not 'site_info' in analysis_doc:
                rerun_ocr = True
            elif analysis_doc['site_info'] is None:
                rerun_ocr = True
            else:
                if not 'analysis_id' in analysis_doc['site_info']:
                    rerun_ocr = True

            if rerun_ocr:
                run_ocr = True
            else:
                if not 'analysis_id' in analysis_doc['site_info']:
                    run_ocr = True
                else:
                    site_info["analysis_id"] = analysis_doc['site_info']['analysis_id']
            if run_ocr:
                image_analysis_id = azure_ocr_helper.run_ocr_restapi(site_info_panel_file,
                                        project_name=analysis_doc['project_id'], category="site-info")
                site_info["analysis_id"] = image_analysis_id

            mongo_helper.update_document('azure_analysis', analysis_id, {"site_info": site_info})

            site_info_kvps = _construct_site_info_tables(mongo_helper, analysis_id)
            site_info["kvps"] = site_info_kvps

            mongo_helper.update_document('azure_analysis', analysis_id, {"site_info": site_info})

            log.info("Success extracting site info %s,  %s" % (analysis_id, analysis_doc["source_file"]))
        else:
            log.info("No site info for %s,  %s" % (analysis_id, analysis_doc["source_file"]))
            mongo_helper.update_document('azure_analysis', analysis_id, {"site_info": None})

    return site_info_panel_file


def process_panels(dbname, project_name):
    # detect and write panel data
    mongo_helper = MongoHelper(dbname)
    category = 'as-built'
    analysis_docs = mongo_helper.query('azure_analysis', {'project_id': project_id, 'category': category})
    ids = []
    for ad in analysis_docs:
        ids.append(ad["_id"])
    return ids

    site_info_panel = _extract_site_info_data(analysis_id, text_size_percent=2, line_distance_percent=2, rerun_ocr=False)
    # eq_key_panel = _extract_equipment_key(analysis_id)



if __name__ == '__main__':
    from common import logger

    folder = r'/Users/ujjwal/projects/cci/data/as-builts/chicago_test'
    # folder  = r"/home/unarayan@us.crowncastle.com/ocrpoc/data/chicago/"
    project_id = 'chicago_big'
    dbname = 'chicago_big'

    logger.setup()
    log = logger.logger

    process_folder(folder, project_id, dbname, num_files=None)
    ocr_asbuilts(project_id, dbname)
