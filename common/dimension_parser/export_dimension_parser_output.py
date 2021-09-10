import pandas as pd
import os
from common import mongodb_helper, logger
from common.config import ASBUILTS_COLLECTION, AZURE_ANALYSIS_COLLECTION, OCR_LINE_COLLECTION
from common.dimension_parser.dimension_parser import DimensionParser, EntityParserTemplate


def get_page_types(asbuilt):
    # create dictionary of page_number: page_type
    page_types = {}
    for page in asbuilt['pages']:
        page_types[page['page']] = page.get('page_type', None)
    return page_types


def extract_from_kvp_parser_object(obj, property, prev_value=None):
    items = obj.get(property, None)
    if items and isinstance(items, list):
        if len(items):
            return items[0]['value']
    return prev_value


def get_dim_value(entity, ocr_dims):
    assert (isinstance(entity, EntityParserTemplate))
    top_value = "X"
    bottom_value = "X"
    center_value = "X"
    has_entity = False
    for ocr_dim in ocr_dims:
        if entity.entity == ocr_dim['entity']:
            has_entity = True
            if entity.category == 'box':
                if ocr_dim['position'] == 'top':
                    top_value = ocr_dim['value']
                if ocr_dim['position'] == 'bottom':
                    bottom_value = ocr_dim['value']
                if ocr_dim['position'] == 'center':
                    center_value = ocr_dim['value']
            else:
                center_value = ocr_dim['value']

    if has_entity:
        if entity.category == 'box':
            if (bottom_value != 'X') or (top_value != 'X'):
                return "%s-%s" % (bottom_value, top_value)
            if (center_value != 'X'):
                return "%s (center)" % center_value
            return "X-X"
        return center_value
    else:
        return None


def export_output_csv(dbname, project_id, dimension_parser_template, page_type='proposed_pole_elevation'):

    mongo_hlpr = mongodb_helper.MongoHelper(dbname)
    asbuilt_docs = mongo_hlpr.query(ASBUILTS_COLLECTION, {'project': project_id})
    asbuilt_doc_ids = [ d["_id"] for d in asbuilt_docs ]
    # asbuilt_doc_ids = [ '6104b3ce7ca78bc7866ee8a0' ]

    dimension_parser = DimensionParser(dbname, dimension_parser_template)

    _data = {
        'scu': [],
        'source_file': [],
        'filename': [],
        'jurisdiction': [],
        'file': [],
        'owner': [],
        'lat': [],
        'lng': [],
        'address': [],
        'county': [],
        'analysis_id': []
    }

    # create expected output dataframe
    entity_parsers = dimension_parser.entity_parsers
    for k, ep in entity_parsers.items():
        _data[k] = []
        _data["%s_comment" % k] = []
        _data["%s_redline" % k] = []

    n_docs = len(asbuilt_doc_ids)
    idx = 0
    for doc_id in asbuilt_doc_ids:
        idx += 1
        ad = mongo_hlpr.get_document(ASBUILTS_COLLECTION, doc_id)
        log.info('export_output - %s, %d of %d' % (doc_id, idx, n_docs))

        scu = None
        source_file = None
        latitude = None
        longitude = None
        address = None
        jurisdiction = None
        owner = None
        county = None
        jurisdiction = None

        # first look in kvp_parser
        if ad.get('kvp_parser'):
            kvp_parser = ad['kvp_parser']
            if kvp_parser.get('site_info'):
                si = kvp_parser['site_info']
                scu = extract_from_kvp_parser_object(si, 'scu')
                jurisdiction = extract_from_kvp_parser_object(si, 'jurisdiction')
                # owner = kvps.get('UTILITIES:', 'XXXX')
                latitude = extract_from_kvp_parser_object(si, 'latitude')
                longitude = extract_from_kvp_parser_object(si, 'longitude')
                address = extract_from_kvp_parser_object(si, 'address')
                # county = kvps.get('COUNTY', 'XXXX')

        if ad.get('site_info'):
            if 'kvps' in ad['site_info']:
                kvps = ad['site_info']['kvps']
                scu = kvps.get('SCU:', scu)
                jurisdiction = kvps.get('JURISDICTION:', None)
                owner = kvps.get('UTILITIES:', None)
                latitude = kvps.get('LATITUDE:', latitude)
                longitude = kvps.get('LONGITUDE:', longitude)
                address = kvps.get('SITE ADDRESS:', address)
                county = kvps.get('COUNTY', None)

                # look in kvp_redline_parser
                if ad.get('kvp_parser_redline'):
                    kvp_parser = ad['kvp_parser_redline']
                    if kvp_parser.get('site_info'):
                        si = kvp_parser['site_info']
                        scu = extract_from_kvp_parser_object(si, 'scu', scu)
                        latitude = extract_from_kvp_parser_object(si, 'latitude', latitude)
                        longitude = extract_from_kvp_parser_object(si, 'longitude', longitude)
                        address = extract_from_kvp_parser_object(si, 'address', address)
                        jurisdiction = extract_from_kvp_parser_object(si, 'jurisdiction', jurisdiction)

        file = os.path.basename(ad['source_file'])

        _data['scu'].append(scu)
        _data['source_file'].append(ad['source_file'])
        _data['filename'].append(os.path.basename(ad['source_file']))
        _data['jurisdiction'].append(jurisdiction)
        _data['owner'].append(owner)
        _data['file'].append(file)
        _data['lat'].append(latitude)
        _data['lng'].append(longitude)
        _data['county'].append(county)
        _data['address'].append(address)
        _data['analysis_id'].append(str(ad['_id']))

        page_dims = []
        page_red_dims = []
        page_n = 0
        for page in ad['pages']:
            pg_type = page.get('page_type', None)
            if pg_type == page_type:
                page_dims = page.get('ocr_detections', [])
                page_red_dims = page.get('red_ocr_detections', [])
                page_n = page['page']

        for k, v in entity_parsers.items():

            # if (v.entity == 'latitude') and (latitude is not None):
            #     continue
            # if (v.entity == 'longitude') and (longitude is not None):
            #     continue
            # if k == 'power_meter':
            #     print(k)

            dim_value = get_dim_value(v, page_dims)
            _data[k].append(dim_value)
            comment = "page %d" % page_n
            if (dim_value == "X") or (dim_value == "X-X") or (dim_value is None):
                comment = ""
            _data["%s_comment" % k].append(comment)

            dim_red_value = get_dim_value(v, page_red_dims)
            _data["%s_redline" % k].append(dim_red_value)


    df = pd.DataFrame(_data)
    df = df.sort_values('scu')
    # print (df.head())
    ofile = '%s_proposed_output.csv' % project_id
    df.to_csv(ofile, index=False)
    log.info('Exported output %s' % ofile)


if __name__ == '__main__':
    from common.config import DBNAME, PROJECT

    project_id = PROJECT
    dbname = DBNAME

    logger.setup('dims_standardization')
    log = logger.logger

    # mongo_helper = mongodb_helper.MongoHelper(dbname=dbname)
    # match_dimensional_lines(dbname, project_id)
    # identify_labels(dbname, project_id)
    dimension_parser_template = './dimension_parser_templates_chicago.json'
    export_output_csv(dbname, project_id, dimension_parser_template)

    # analysis_id = ObjectId ("60ff65991e23b73c6053a8b3")
    # n_pages = 3
    # _get_page_dims(analysis_id, n_pages, category='as-built')
