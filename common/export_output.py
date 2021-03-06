import pandas as pd
import numpy as np
import os
from common import mongodb_helper
from common.config import ASBUILTS_COLLECTION, AZURE_ANALYSIS_COLLECTION, OCR_LINE_COLLECTION
from common import logger


# def search_redline_dims(doc, entity, pages, page_types, position=None):
#     value = None
#     try:
#         for page in pages:
#             redline_dims = doc['redline_dims']
#             if len(redline_dims) > page - 1:
#                 page_dims = redline_dims[page - 1]['dims']
#                 entity = entity.lower()
#                 found_dims = []
#                 if position:
#                     position = position.lower()
#
#                 for dim in page_dims:
#                     if position:
#                         if ((dim['entity']).lower() == entity) and (position == dim['position'].lower()):
#                             found_dims.append(dim)
#                     elif (dim['entity']).lower() == entity:
#                         found_dims.append(dim)
#                     else:
#                         pass
#
#                 if len(found_dims) == 1:
#                     value = np.around((found_dims[0]['feet'] + found_dims[0]['inches'] / 12), 2)
#
#                 if len(found_dims) > 1:
#                     value = np.around(0.5 * (
#                             (found_dims[0]['feet'] + found_dims[0]['inches'] / 12) +
#                             (found_dims[1]['feet'] + found_dims[1]['inches'] / 12)), 2)
#     except Exception as ex:
#         log.info(str(ex))
#
#     return value


dimensional_entity_mappings = {
    "AC LOAD PANEL": "BOX",
    "AC PANEL": "BOX",
    "BANNER": "BOX",
    "COMMUNICATION LINE": "POINT",
    "CROSS ARM": "BOX",
    "FIBER DIST PANEL": "BOX",
    "GUY WIRES": "POINT",
    "LIGHT ARM": "BOX",
    "OTHER": "BOX",
    "POWER METER": "BOX",
    "POWER RISER": "BOX",
    "PRIMARY POWER": "BOX",
    "SECONDARY POWER": "BOX",
    "SHROUD": "BOX",
    "SIGN": "BOX",
    "SURVEILLANCE EQUIPMENT": "BOX",
    "TRAFFIC SIGNAL": "BOX",
    "TRANSFORMER": "BOX"
}


core_dimensional_entities = [
    "COMM CONNECTION",
    "GRADE REFERENCE",
    "ANTENNA",
    "SHROUD",
    "POLE",
    "PRIMARY ELECTRICAL",
    "NEUTRAL ELECTRICAL", # also secondary eletcrical, power and electrical are same
    "ELECTRIC BOX",
    "NEUTRAL ELECTRICAL SERVICE",
    "SECONDARY SERVICE",
    "SECONDARY POWER",
    "FIBER DIST PANEL",
    # "STREET SIGN",
    "AC LOAD PANEL",
    "AC PANEL",
    "OVERHEAD CATV",
    "TELCO CONNECTION",
    "RADIO LTE",

    "BANNER",
    "BOX",
    "COMMUNICATION LINE",
    "CROSS ARM",
    "GUY WIRE",
    "LIGHT ARM",
    "OTHER",
    "POWER METER",
    "POWER RISER",
    "PRIMARY POWER",
    "SIGN",
    "TRAFFIC SIGNAL",
    "TRANSFORMER",
    "SURVEILLANCE EQUIPMENT" # camera is
]


def search_dims(doc, entity, pages, page_types, position=None):
    proposed_dims = []
    existing_dims = []
    try:
        for page in pages:
            _dims = doc['dims']
            if len(_dims) <= (page - 1):
                continue
            page_dims = _dims[page - 1]['dims']
            page_type = page_types.get(page, "")

            entity = entity.lower()
            found_dims = []
            if position:
                position = position.lower()

            for dim in page_dims:
                if position:
                    if ((dim['entity']).lower() == entity) and (position == dim['position'].lower()):
                        found_dims.append(dim)
                elif (dim['entity']).lower() == entity:
                    found_dims.append(dim)
                else:
                    pass

            if 'existing' in page_type:
                existing_dims = found_dims
            elif 'proposed' in page_type:
                proposed_dims = found_dims
            else:
                pass

    except Exception as ex:
        log.info(str(ex))

    return proposed_dims, existing_dims


def search_redline_dims(doc, entity, pages, page_types, position=None):
    proposed_dims = []
    existing_dims = []
    try:
        for page in pages:
            _dims = doc['redline_dims']
            if len(_dims) <= (page - 1):
                continue
            page_dims = _dims[page - 1]['dims']
            page_type = page_types.get(page, "")

            entity = entity.lower()
            found_dims = []
            if position:
                position = position.lower()

            for dim in page_dims:
                if position:
                    if ((dim['entity']).lower() == entity) and (position == dim['position'].lower()):
                        found_dims.append(dim)
                elif (dim['entity']).lower() == entity:
                    found_dims.append(dim)
                else:
                    pass

            if 'existing' in page_type:
                existing_dims = found_dims
            elif 'proposed' in page_type:
                proposed_dims = found_dims
            else:
                pass

    except Exception as ex:
        log.info(str(ex))

    return proposed_dims, existing_dims


def extract_from_kvp_parser_object(obj, property, prev_value=None):
    items = obj.get(property, None)
    if items and isinstance(items, list):
        if len(items):
            return items[0]['value']
    return prev_value


def get_page_types(asbuilt):
    # create dictionary of page_number: page_type
    page_types = {}
    for page in asbuilt['pages']:
        page_types[page['page']] = page.get('page_type', None)
    return page_types


def get_dim_value(dim):
    dv = -1
    try:
       dv = np.around((float(dim['feet']) + float(dim['inches']) / 12), 2)
    except:
        pass
    return dv


def get_entity_dims(ad, pages, page_types, entity, entity_class, redline=False):

    proposed_dim_value = None
    existing_dim_value = None

    if entity_class == 'BOX':

        # we will look for TOP and BOTTOM position separately
        if redline:
            proposed_top_dims, existing_top_dims = search_redline_dims(ad, entity, pages, page_types, position='TOP')
            proposed_bottom_dims, existing_bottom_dims = search_redline_dims(ad, entity, pages, page_types,
                                                                 position='BOTTOM')
        else:
            proposed_top_dims, existing_top_dims = search_dims(ad, entity, pages, page_types, position='TOP')
            proposed_bottom_dims, existing_bottom_dims = search_dims(ad, entity, pages, page_types,
                                                                 position='BOTTOM')

        proposed_top_dim_value = -1
        existing_top_dim_value = -1
        proposed_bottom_dim_value = -1
        existing_bottom_dim_value = -1

        if len(proposed_top_dims):
            proposed_top_dim = proposed_top_dims[0]
            proposed_top_dim_value = get_dim_value(proposed_top_dim)

        if len(existing_top_dims):
            existing_top_dim = existing_top_dims[0]
            existing_top_dim_value = get_dim_value(existing_top_dim)

        if len(proposed_bottom_dims):
            proposed_bottom_dim = proposed_bottom_dims[0]
            proposed_bottom_dim_value = get_dim_value(proposed_bottom_dim)

        if len(existing_bottom_dims):
            existing_bottom_dim = existing_bottom_dims[0]
            existing_bottom_dim_value = get_dim_value(existing_bottom_dim)

        # if we didn't find any top and bottom dim values, check whether 2 entries are present
        # without TOP and BOTTOM labels if yes use the lower one as bottom and higher one as top
        if redline:
            proposed_dims, existing_dims = search_redline_dims(ad, entity, pages, page_types, position=None)
        else:
            proposed_dims, existing_dims = search_dims(ad, entity, pages, page_types, position=None)

        if len(proposed_dims) > 1:
            proposed_top_dim_value1 = get_dim_value(proposed_dims[0])
            proposed_top_dim_value2 = get_dim_value(proposed_dims[1])

            if proposed_top_dim_value1 > proposed_top_dim_value2:
                proposed_top_dim_value = proposed_top_dim_value1
                proposed_bottom_dim_value = proposed_top_dim_value2
            else:
                proposed_top_dim_value = proposed_top_dim_value2
                proposed_bottom_dim_value = proposed_top_dim_value1

        if len(existing_dims) > 1:
            existing_top_dim_value1 = get_dim_value(existing_dims[0])
            existing_top_dim_value2 = get_dim_value(existing_dims[1])

            if existing_top_dim_value1 > existing_top_dim_value2:
                existing_top_dim_value = existing_top_dim_value1
                existing_bottom_dim_value = existing_top_dim_value2
            else:
                existing_top_dim_value = existing_top_dim_value2
                existing_bottom_dim_value = existing_top_dim_value1

        if (proposed_bottom_dim_value != -1) or (proposed_top_dim_value != -1):
            proposed_dim_value = "%.2f-%.2f" % (proposed_bottom_dim_value, proposed_top_dim_value)

        if (existing_bottom_dim_value != -1) or (existing_top_dim_value != -1):
            existing_dim_value = "%.2f-%.2f" % (existing_bottom_dim_value, existing_top_dim_value)

    else:
        if redline:
            proposed_entity_dims, existing_entity_dims = search_redline_dims(ad, entity, pages, page_types)
        else:
            proposed_entity_dims, existing_entity_dims = search_dims(ad, entity, pages, page_types)

        if len(proposed_entity_dims):
            proposed_dim_value = "-".join("%.2f" % get_dim_value(d) for d in proposed_entity_dims)
        if len(existing_entity_dims):
            existing_dim_value = "-".join("%.2f" % get_dim_value(d) for d in existing_entity_dims)

    return existing_dim_value, proposed_dim_value


def export_output_csv(dbname, project_id):

    mongo_hlpr = mongodb_helper.MongoHelper(dbname)
    asbuilt_docs = mongo_hlpr.query(ASBUILTS_COLLECTION, {'project': project_id})
    asbuilt_doc_ids = [ d["_id"] for d in asbuilt_docs ]
    # asbuilt_doc_ids = [ '6104b3bc7ca78bc7866ee89f' ]

    proposed_data = {
        'scu': [],
        'source_file': [],
        'filename': [],
        'jurisdiction': [],
        'file': [],
        'owner': [],
        'latitude': [],
        'longitude': [],
        'address': [],
        'county': [],
        'analysis_id': []
    }

    existing_data = {
        'scu': [],
        'source_file': [],
        'filename': [],
        'analysis_id': []
    }

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

        pages = [1, 2, 3, 4]
        page_types = get_page_types(ad)

        proposed_data['scu'].append(scu)
        proposed_data['source_file'].append(ad['source_file'])
        proposed_data['filename'].append(os.path.basename(ad['source_file']))
        proposed_data['jurisdiction'].append(jurisdiction)
        proposed_data['owner'].append(owner)
        proposed_data['file'].append(file)
        proposed_data['latitude'].append(latitude)
        proposed_data['longitude'].append(longitude)
        proposed_data['county'].append(county)
        proposed_data['address'].append(address)
        proposed_data['analysis_id'].append(str(ad['_id']))

        existing_data['scu'].append(scu)
        existing_data['source_file'].append(ad['source_file'])
        existing_data['filename'].append(os.path.basename(ad['source_file']))
        existing_data['analysis_id'].append(str(ad['_id']))

        for entity in core_dimensional_entities:
            entity_class = dimensional_entity_mappings.get(entity, None)

            if entity not in proposed_data:
                proposed_data[entity] = []
            if entity not in existing_data:
                existing_data[entity] = []

            existing_dim_value, proposed_dim_value = get_entity_dims(ad, pages, page_types, entity, entity_class,
                                                                     redline=False)
            existing_dim_red_value, proposed_dim_red_value = get_entity_dims(ad, pages, page_types, entity,
                                                                             entity_class, redline=True)

            if proposed_dim_red_value:
                proposed_data[entity].append(proposed_dim_red_value)
            elif proposed_dim_value:
                proposed_data[entity].append(proposed_dim_value)
            else:
                proposed_data[entity].append("X")

            if existing_dim_red_value:
                existing_data[entity].append(existing_dim_red_value)
            elif existing_dim_value:
                existing_data[entity].append(existing_dim_value)
            else:
                existing_data[entity].append("X")

    df = pd.DataFrame(proposed_data)
    df = df.sort_values('scu')
    # print (df.head())
    ofile = '%s_proposed_output.csv' % project_id
    df.to_csv(ofile, index=False)
    log.info('Exported output %s' % ofile)

    df1 = pd.DataFrame(existing_data)
    df1 = df1.sort_values('scu')
    # print (df.head())
    ofile = '%s_existing_output.csv' % project_id
    df1.to_csv(ofile, index=False)
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
    export_output_csv(dbname, project_id)

    # analysis_id = ObjectId ("60ff65991e23b73c6053a8b3")
    # n_pages = 3
    # _get_page_dims(analysis_id, n_pages, category='as-built')
