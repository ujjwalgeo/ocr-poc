import pandas as pd
import numpy as np
import os
import re
from common import mongodb_helper
from common.config import ASBUILTS_COLLECTION, AZURE_ANALYSIS_COLLECTION, OCR_LINE_COLLECTION
from common import logger


data =\
"""
EXISTING OVERHEAD NEUTRAL ELECTRICAL LINE - # ,-
EXISTING OVERHEAD COMM CONNECTION = = ,-
EXISTING GRADE REFERENCE = 1
1OP OF PROPOSED ANTENNA = 2 ,-
€ OF PROPOSED ANTENNA = =
BURIED , BELOW GRADE & TIED TOGETHER
EXISTING OVERHEAD NEUTRAL ELECTRICAL LINE = & ,-
EXISTING OVERHEAD COMM CONNECTION = =
TOP OF PROPOSED SHROUD = #
TOP OF PROPOSED AC LOAD PANEL = +
TOP OF PROPOSED FIBER DIST PANEL = + , -
BOTTOM OF PROPOSED FIBER DIST PANEL = +
EXISTING GRADE REFERENCE = 1:
TOP OF EXISTING PRIMARY ELECTRICAL LINE = 2
EXISTING OVERHEAD SECONDARY SERVICE LINE = +
DISTING OVERHEAD CATV = 1
EXISTING OVERHEAD COMM CONNECTION = #
€ OF EXISTING STREET SIGN = =
EXISTING GRADE REFERENCE = 1
TOP OF PROPOSED ANTEMNA = 2
€ OF PROPOSED ANTENNA = #
1OP OF RELOCATED PRIMARY ELECTRICAL LINE = 4
BURIED , BELOW GRADE & TIED TOGETHER
EXISTING OVERHEAD SECONDARY SERVICE LINE - +
DOSTING OVERHEAD CATV = 1
EXISTING OVERHEAD COMM CONNECTION = #
TOP OF PROPOSED SHROUD = # ,.
TOP OF PROPOSED AC LOAD PANEL = +
TOP OF PROPOSED FIBER DIST PANEL = + , -
BOTTOM OF PROPOSED FIBER DIST PANEL = +
€ OF EXISTING STREET SIGN = =
EXISTING GRADE REFERENCE = 1
TOP OF DOSTING POLE = + ,-
EXISTING GUY WIRE CONNECTION = #
DUSTING OVERHEAD NEUTRAL ELECTRICAL SERVICE UNE = =
EXISTING OVERHEAD CATV/COMM CONNECTION = 2
E OF EXISTING CITY STREET SIGN = 1
€ OF EXISTING CITY STREET SIGN = +
EXISTING GRADE REFERENCE = 1
€ OF PROPOSED ANTENNA = #
TOP OF RELOCATED EXISTING OVERHEAD PRAIACY ELECTRICAL LINE - #
BURIED , BELOW GRADE & TIED TOGETHER
DUSTING OVERHEAD NEUTRAL ELECTRICAL SERVICE UNE = +
BOTTOM OF FIBER DIST PANEL = #
E OF EXISTING CITY STREET SIGN = 1
€ OF EXISTING CITY STREET SIGN = +
EXISTING GRADE REFERENCE = 1:
EXISTING OVERHEAD TELCO COMMECTION = 1
DUSTING GRADE REFERENCE = #
TOP OF NEW ANTENNA = + ,-
€ OF PROPOSED ANTENNA = 1 ,-
MAX
TOP OF PROPOSED POLE= + ,-
EXISTING OVERHEAD TELCO CONNECTION = +
EXISTING OVERHEAD CATV COMMECTION = 1
TOP OF PROPOSED FIER DIST PANEL - + ,-
TOP OF PROPOSED SHROUD - #
BOTTOM OF FIBER DIST PANEL = # , -
BURIED , BELOW GRADE & TIED TOGETHER
EXISTING GRACE REFERENCE = #
-TOP OF EXISTING POLE= =
EXISTING STREET LIGHT - #
DUSTING GRADE REFERENCE = #
TOP OF MEW ANTENNA = +
€ OF PROPOSED ANTENNA = =
MAX
TOP OF EXISTING PRIMARY ELECTRICAL LINE - #
TOP OF EXISTING POLE= 1:
EXISTING STREET LICHT = #
DISTING STREET LIGHT = # ,-
TOP OF PROPOSED AC LOAD PANEL = 1
DISTING GRADE REFERENCE = 1
TOP OF EXISTING PRIMARY ELECTRICAL LINE = +
-TOP OF EXISTING POLE= 1 ,-
EXISTING OVERHEAD COMM, CATV & SERVICE DROP CONNECTION = #
DISTING ELECTRIC BOX = # ,-
{ OF EXISTING STREET SIGN = # ,-
DOSTING GRADE REFERENCE = #
TOP OF NEW ANTENNA = 1
{ OF PROPOSED ANTENNA = +
MAX
TOP OF EXISTING PRIMARY ELECTRICAL UNE = + ,-
TOP OF EXISTING POLE = + ,-
EXISTING OVERHEAD COMM, CATV & SERVICE DROP CONNECTION - # ,-
-TOP OF PROPOSED SHROUD = # ,-
TOP OF PROPOSED FIBER DIST PANEL = $
BOTTOM OF FIBER DIST PANEL = $
{ OF EXISTING STREET SIGN = # ,-
DOSTING GRADE REFERENCE = #
EXISTING OVERHEAD NEUTRAL SERVICE UNE = %
EXISTING OVERHEAD COMM CONNECTION = +
EXISTING OVERHEAD COMM CONNECTION = +
€ OF COSTING COUM BOX = =
EXISTING GRACE REFERENCE = #
TOP OF NEW ANTENNA = +
€ OF PROPOSED ANTENNA = =
TOP OF PROPOSED POLE = =
BELOW GRADE & TED TOGETHER W/ 16 AWG VERTICAL
EXISTING OVERHEAD COMM CONNECTION = +
EXISTING OVERHEAD COUM CONNECTION = +
OF COSTING COMM BOX = $
EXISTING GRACE REFERENCE = #


 OF COSTING COUM BOX   --- OF COSTING COMM BOX   (0.000000) --  OF PROPOSED ANTENNA   (0.125000)
 OF EXISTING CITY STREET SIGN   ---  OF EXISTING STREET SIGN   (0.125000) --  OF EXISTING STREET SIGN    (0.000000)
 OF EXISTING STREET SIGN   ---  OF EXISTING STREET SIGN    (0.142857) --  OF EXISTING CITY STREET SIGN   (0.800000)
 OF EXISTING STREET SIGN    ---  OF EXISTING STREET SIGN   (0.142857) --  OF EXISTING CITY STREET SIGN   (0.800000)
 OF PROPOSED ANTENNA   ---  OF PROPOSED ANTENNA  1  (0.166667) -- 1OP OF PROPOSED ANTENNA  2  (0.142857)
 OF PROPOSED ANTENNA  1  ---  OF PROPOSED ANTENNA   (0.142857) -- TOP OF NEW ANTENNA  1 (0.125000)
1OP OF PROPOSED ANTENNA  2  ---  OF PROPOSED ANTENNA   (0.125000) --  OF PROPOSED ANTENNA  1  (0.111111)
1OP OF RELOCATED PRIMARY ELECTRICAL LINE  4 --- TOP OF EXISTING PRIMARY ELECTRICAL LINE   (0.100000) -- TOP OF EXISTING PRIMARY ELECTRICAL LINE  2 (0.090909)
BELOW GRADE  TED TOGETHER W 16 AWG VERTICAL --- BURIED  BELOW GRADE  TIED TOGETHER (0.000000) -- DOSTING GRADE REFERENCE   (0.000000)
BOTTOM OF FIBER DIST PANEL   --- BOTTOM OF FIBER DIST PANEL     (0.125000) -- BOTTOM OF PROPOSED FIBER DIST PANEL   (0.111111)
BOTTOM OF FIBER DIST PANEL     --- BOTTOM OF FIBER DIST PANEL   (0.125000) -- BOTTOM OF PROPOSED FIBER DIST PANEL   (0.111111)
BOTTOM OF PROPOSED FIBER DIST PANEL   --- BOTTOM OF FIBER DIST PANEL   (0.111111) -- BOTTOM OF FIBER DIST PANEL     (0.100000)
BURIED  BELOW GRADE  TIED TOGETHER --- BELOW GRADE  TED TOGETHER W 16 AWG VERTICAL (0.000000) -- DOSTING GRADE REFERENCE   (0.000000)
DISTING ELECTRIC BOX    --- DISTING STREET LIGHT    (0.166667) -- DISTING GRADE REFERENCE  1 (0.000000)
DISTING GRADE REFERENCE  1 --- EXISTING GRADE REFERENCE  1 (0.000000) -- DUSTING GRADE REFERENCE   (0.000000)
DISTING OVERHEAD CATV  1 --- DOSTING OVERHEAD CATV  1 (0.000000) -- EXISTING OVERHEAD CATV COMMECTION  1 (0.000000)
DISTING STREET LIGHT    --- EXISTING STREET LIGHT   (0.000000) -- DISTING ELECTRIC BOX    (0.142857)
DOSTING GRADE REFERENCE   --- DUSTING GRADE REFERENCE   (0.000000) -- EXISTING GRADE REFERENCE  1 (0.000000)
DOSTING OVERHEAD CATV  1 --- DISTING OVERHEAD CATV  1 (0.000000) -- EXISTING OVERHEAD CATV COMMECTION  1 (0.000000)
DUSTING GRADE REFERENCE   --- DOSTING GRADE REFERENCE   (0.000000) -- EXISTING GRADE REFERENCE  1 (0.000000)
DUSTING OVERHEAD NEUTRAL ELECTRICAL SERVICE UNE   --- EXISTING OVERHEAD NEUTRAL SERVICE UNE   (0.000000) -- EXISTING OVERHEAD NEUTRAL ELECTRICAL LINE    (0.000000)
E OF EXISTING CITY STREET SIGN  1 ---  OF EXISTING CITY STREET SIGN   (0.100000) --  OF EXISTING STREET SIGN   (0.714286)
EXISTING GRACE REFERENCE   --- EXISTING GRADE REFERENCE  1 (0.000000) -- EXISTING STREET LIGHT   (0.142857)
EXISTING GRADE REFERENCE  1 --- DISTING GRADE REFERENCE  1 (0.000000) -- EXISTING GRACE REFERENCE   (0.125000)
EXISTING GUY WIRE CONNECTION   --- EXISTING OVERHEAD COUM CONNECTION   (0.000000) -- EXISTING OVERHEAD COMM CONNECTION    (0.125000)
EXISTING OVERHEAD CATV COMMECTION  1 --- EXISTING OVERHEAD TELCO COMMECTION  1 (0.000000) -- DISTING OVERHEAD CATV  1 (0.111111)
EXISTING OVERHEAD CATVCOMM CONNECTION  2 --- EXISTING OVERHEAD COUM CONNECTION   (0.000000) -- EXISTING OVERHEAD TELCO CONNECTION   (0.111111)
EXISTING OVERHEAD COMM CATV  SERVICE DROP CONNECTION   --- EXISTING OVERHEAD COMM CATV  SERVICE DROP CONNECTION    (0.000000) -- EXISTING OVERHEAD COMM CONNECTION    (0.090909)
EXISTING OVERHEAD COMM CATV  SERVICE DROP CONNECTION    --- EXISTING OVERHEAD COMM CATV  SERVICE DROP CONNECTION   (0.000000) -- EXISTING OVERHEAD COMM CONNECTION    (0.090909)
EXISTING OVERHEAD COMM CONNECTION   --- EXISTING OVERHEAD COMM CONNECTION    (0.000000) -- EXISTING OVERHEAD COUM CONNECTION   (0.125000)
EXISTING OVERHEAD COMM CONNECTION    --- EXISTING OVERHEAD COMM CONNECTION   (0.000000) -- EXISTING OVERHEAD COUM CONNECTION   (0.125000)
EXISTING OVERHEAD COUM CONNECTION   --- EXISTING OVERHEAD COMM CONNECTION   (0.142857) -- EXISTING OVERHEAD TELCO CONNECTION   (0.125000)
EXISTING OVERHEAD NEUTRAL ELECTRICAL LINE    --- TOP OF RELOCATED EXISTING OVERHEAD PRAIACY ELECTRICAL LINE   (0.000000) -- EXISTING OVERHEAD SECONDARY SERVICE LINE   (0.111111)
EXISTING OVERHEAD NEUTRAL SERVICE UNE   --- DUSTING OVERHEAD NEUTRAL ELECTRICAL SERVICE UNE   (0.000000) -- EXISTING OVERHEAD SECONDARY SERVICE LINE   (0.111111)
EXISTING OVERHEAD SECONDARY SERVICE LINE   --- EXISTING OVERHEAD NEUTRAL SERVICE UNE   (0.000000) -- EXISTING OVERHEAD NEUTRAL ELECTRICAL LINE    (0.111111)
EXISTING OVERHEAD TELCO COMMECTION  1 --- EXISTING OVERHEAD CATV COMMECTION  1 (0.000000) -- EXISTING OVERHEAD TELCO CONNECTION   (0.111111)
EXISTING OVERHEAD TELCO CONNECTION   --- EXISTING OVERHEAD COUM CONNECTION   (0.000000) -- EXISTING OVERHEAD COMM CONNECTION    (0.125000)
EXISTING STREET LICHT   --- EXISTING STREET LIGHT   (0.000000) --  OF EXISTING STREET SIGN   (0.333333)
EXISTING STREET LIGHT   --- DISTING STREET LIGHT    (0.000000) -- EXISTING STREET LICHT   (0.333333)
MAX ---  OF COSTING COUM BOX   (0.000000) -- TOP OF EXISTING PRIMARY ELECTRICAL LINE  2 (0.000000)
OF COSTING COMM BOX   ---  OF COSTING COUM BOX   (0.600000) --  OF PROPOSED ANTENNA   (0.125000)
TOP OF DOSTING POLE    --- TOP OF EXISTING POLE  (0.142857) -- TOP OF EXISTING POLE    (0.125000)
TOP OF EXISTING POLE  --- TOP OF EXISTING POLE    (0.142857) -- TOP OF EXISTING POLE 1 (0.285714)
TOP OF EXISTING POLE    --- TOP OF EXISTING POLE  (0.142857) -- TOP OF EXISTING POLE 1 (0.285714)
TOP OF EXISTING POLE 1 --- TOP OF EXISTING POLE 1  (0.125000) -- TOP OF EXISTING POLE  (0.250000)
TOP OF EXISTING POLE 1  --- TOP OF EXISTING POLE 1 (0.125000) -- TOP OF EXISTING POLE  (0.250000)
TOP OF EXISTING PRIMARY ELECTRICAL LINE   --- TOP OF EXISTING PRIMARY ELECTRICAL LINE  2 (0.111111) -- TOP OF EXISTING PRIMARY ELECTRICAL UNE    (0.222222)
TOP OF EXISTING PRIMARY ELECTRICAL LINE  2 --- TOP OF EXISTING PRIMARY ELECTRICAL LINE   (0.100000) -- TOP OF EXISTING PRIMARY ELECTRICAL UNE    (0.200000)
TOP OF EXISTING PRIMARY ELECTRICAL UNE    --- TOP OF EXISTING PRIMARY ELECTRICAL LINE   (0.111111) -- TOP OF EXISTING PRIMARY ELECTRICAL LINE  2 (0.222222)
TOP OF MEW ANTENNA   --- TOP OF NEW ANTENNA   (0.142857) -- TOP OF NEW ANTENNA    (0.125000)
TOP OF NEW ANTENNA   --- TOP OF NEW ANTENNA    (0.142857) -- TOP OF NEW ANTENNA  1 (0.125000)
TOP OF NEW ANTENNA    --- TOP OF NEW ANTENNA   (0.142857) -- TOP OF NEW ANTENNA  1 (0.125000)
TOP OF NEW ANTENNA  1 --- TOP OF NEW ANTENNA   (0.125000) -- TOP OF NEW ANTENNA    (0.111111)
TOP OF PROPOSED AC LOAD PANEL   --- TOP OF PROPOSED AC LOAD PANEL  1 (0.111111) -- TOP OF PROPOSED FIER DIST PANEL    (0.100000)
TOP OF PROPOSED AC LOAD PANEL  1 --- TOP OF PROPOSED AC LOAD PANEL   (0.100000) -- TOP OF PROPOSED FIER DIST PANEL    (0.090909)
TOP OF PROPOSED ANTEMNA  2 --- TOP OF PROPOSED SHROUD    (0.125000) -- TOP OF PROPOSED SHROUD   (0.111111)
TOP OF PROPOSED FIBER DIST PANEL   --- TOP OF PROPOSED FIBER DIST PANEL     (0.111111) -- TOP OF PROPOSED FIER DIST PANEL    (0.100000)
TOP OF PROPOSED FIBER DIST PANEL     --- TOP OF PROPOSED FIBER DIST PANEL   (0.111111) -- TOP OF PROPOSED FIER DIST PANEL    (0.100000)
TOP OF PROPOSED FIER DIST PANEL    --- TOP OF PROPOSED FIBER DIST PANEL     (0.111111) -- TOP OF PROPOSED FIBER DIST PANEL   (0.100000)
TOP OF PROPOSED POLE   --- TOP OF DOSTING POLE    (0.142857) -- TOP OF PROPOSED SHROUD    (0.125000)
TOP OF PROPOSED SHROUD   --- TOP OF PROPOSED SHROUD    (0.142857) -- TOP OF PROPOSED POLE   (0.125000)
TOP OF PROPOSED SHROUD    --- TOP OF PROPOSED SHROUD   (0.142857) -- TOP OF PROPOSED POLE   (0.125000)
TOP OF RELOCATED EXISTING OVERHEAD PRAIACY ELECTRICAL LINE   --- TOP OF EXISTING PRIMARY ELECTRICAL LINE   (0.090909) -- TOP OF EXISTING PRIMARY ELECTRICAL LINE  2 (0.181818)
"""

known_errors = {
    'FIRER': 'FIBER',
    'FIER': 'FIBER',
    'DOSTING': 'EXISTING',
    'ANTEMNA': 'ANTENNA',
    '1OP': 'TOP',
    'NOP': 'TOP',
    'DISTING': 'EXISTING',
    'LICHT': 'LIGHT',
    'MEW': 'NEW',
}

core_entities = [
    "COMM CONNECTION",
    "GRADE REFERENCE",
    "ANTENNA",
    "SHROUD",
    "PROPOSED POLE",
    "POLE",
    "PRIMARY ELECTRICAL",
    "NEUTRAL ELECTRICAL",
    "ELECTRIC BOX",
    "NEUTRAL ELECTRICAL SERVICE",
    "SECONDARY SERVICE",
    "FIBER DIST PANEL",
    "STREET SIGN",
    "AC LOAD PANEL",
    "OVERHEAD CATV",
    "TELCO CONNECTION",
]

known_labels = [
    "COMM CONNECTION",
    "GRADE REFERENCE",
    "ANTENNA",
    "ANTEMNA",
    "PROPOSED ANTENNA",
    "TOP OF ANTENNA",
    "SHROUD",
    "TOP OF PROPOSED SHROUD",
    "TOP OF PROPOSED POLE",
    "TOP OF POLE",
    "PRIMARY ELECTRICAL LINE",
    "NEUTRAL ELECTRICAL LINE",
    "NEUTRAL ELECTRICAL SERVICE LINE",
    "ELECTRIC BOX",
    "SECONDARY SERVICE LINE",
    "TOP OF FIBER DIST PANEL",
    "BOTTOM OF FIBER DIST PANEL",
    "CITY STREET SIGN",
    "STREET SIGN",
    "GUY WIRE CONNECTION",
    "STREET LIGHT",
    "AC LOAD PANEL",
    "OVERHEAD CATV",
    "BURIED  BELOW GRADE  TIED TOGETHER",
    "MAX",
    "TELCO CONNECTION",
    "TOP OF NID",
]


def get_jaccard_sim(str1, str2):
    str1 = str1.lower()
    str2 = str2.lower()
    a = set(str1.split())
    b = set(str2.split())
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))


def remove_special_chars(txt):
    txt = str(txt).strip()
    return "".join([t for t in txt if (t.isalnum() or t == ' ')])


def get_self_similarity():
    items = data.split("\n")
    items = [remove_special_chars(i) for i in items if len(i)]
    items = list(set(items))
    items = sorted(items)
    sim_arr = np.zeros((len(items), len(items)))

    for i in range(len(items)):
        for j in range(len(items)):
            if i == j:
                continue
            sim = get_jaccard_sim(items[i], items[j])
            sim_arr[i][j] = sim

    sim_df = pd.DataFrame(sim_arr)
    for idx, row in sim_df.iterrows():
        sims = row.sort_values(ascending=False)
        match1 = sims.index[0]
        match2 = sims.index[1]
        print("%s --- %s (%f) -- %s (%f)" % (items[idx], items[match1], sims[0], items[match2], sims[1]))


def _standardize_label(page_dim):
    label = page_dim['label']
    value = page_dim['value']
    feet = page_dim['feet']
    inches = page_dim['inches']
    # print(label)
    label = remove_special_chars(label)
    label = label.upper()

    # max_similarity = -1
    # known_label = None
    # for ko in known_labels:
    #     sim = get_jaccard_sim(ko, label)
    #     if sim > max_similarity:
    #         max_similarity = sim
    #         known_label = ko
    entity = ""
    position = ""
    for ci in core_entities:
        if ci in label:
            entity = ci
            if 'TOP' in label:
                position = 'TOP'
            if 'BOTTOM' in label:
                position = 'BOTTOM'

    page_dim['entity'] = entity
    page_dim['position'] = position

    return page_dim


def identify_labels(dbname, project_id):

    mongo_hlpr = mongodb_helper.MongoHelper(dbname)
    asbuilts = mongo_hlpr.query(ASBUILTS_COLLECTION, {'project': project_id })
    asbuilt_ids = [ad['_id'] for ad in asbuilts]

    for doc_id in asbuilt_ids:
        doc = mongo_hlpr.get_document(ASBUILTS_COLLECTION, doc_id)
        all_dims = doc.get('dims')
        if all_dims:
            idx = 0
            for ad in all_dims:
                dims = ad['dims']
                count = 0
                for pd in dims:
                    page_dim = _standardize_label(pd)
                    label_key = "dims.%d.dims.%d" % (idx, count)
                    mongo_hlpr.update_document(ASBUILTS_COLLECTION, doc_id, {label_key: page_dim})
                    count += 1

                idx += 1

        all_dims = doc.get('redline_dims')
        if all_dims:
            idx = 0
            for ad in all_dims:
                dims = ad['dims']
                count = 0
                for pd in dims:
                    page_dim = _standardize_label(pd)
                    label_key = "redline_dims.%d.dims.%d" % (idx, count)
                    mongo_hlpr.update_document(ASBUILTS_COLLECTION, doc_id, {label_key: page_dim})
                    count += 1

                idx += 1


def export_output_csv(dbname, project_id):

    mongo_hlpr = mongodb_helper.MongoHelper(dbname)
    asbuilt_docs = mongo_hlpr.query(ASBUILTS_COLLECTION, {'project': project_id})
    asbuilt_doc_ids = [ d["_id"] for d in asbuilt_docs ]

    data = {
        'scu': [],
        'jurisdiction': [],
        'file': [],
        'owner': [],
        'latitude': [],
        'longitude': [],
        'address': [],
        'county': [],
        'pole height': [],
        'pole height comment': [],
        'primary power': [],
        'primary power comment': [],
        'secondary power': [],
        'secondary power comment': [],
        'fiber dist panel': [],
        'fiber dist panel comment': [],
        'ac load panel': [],
        'ac load panel comment': [],
        'street sign': [],
        'street sign comment': [],
        'shroud': [],
        'shroud comment': [],
        'antenna': [],
        'antenna comment': [],
        'analysis_id': []
    }

    def search_redline_dims(doc, entity, pages):
        try:
            for page in pages:
                redline_dims = doc['redline_dims']
                if len(redline_dims) > page -1:
                    page_dims = redline_dims[page-1]['dims']
                    entity = entity.lower()
                    found_dims = []
                    for dim in page_dims:
                        if (dim['entity']).lower() == entity:
                            found_dims.append(dim)

                    if len(found_dims) == 1:
                        return np.around((found_dims[0]['feet'] + found_dims[0]['inches'] / 12), 2)

                    if len(found_dims) > 1:
                        return np.around(0.5 * (
                                (found_dims[0]['feet'] + found_dims[0]['inches'] / 12) +
                                (found_dims[1]['feet'] + found_dims[1]['inches'] / 12)), 2)
        except Exception as ex:
            log.info(str(ex))

        return None

    def search_dims(doc, entity, pages):
        try:
            for page in pages:
                all_dims = doc['dims']
                if len(all_dims) <= (page-1):
                    continue
                page_dims = all_dims[page-1]['dims']
                entity = entity.lower()
                found_dims = []
                for dim in page_dims:
                    if (dim['entity']).lower() == entity:
                        found_dims.append(dim)

                if len(found_dims) == 1:
                    return np.around((found_dims[0]['feet'] + float(found_dims[0]['inches']) / 12), 2)

                if len(found_dims) > 1:
                    # if entity == 'fiber dist panel':
                    #     print(entity)
                    return np.around(0.5 * (
                            (float(found_dims[0]['feet']) + float(found_dims[0]['inches']) / 12) +
                            (float(found_dims[1]['feet']) + float(found_dims[1]['inches']) / 12)), 2)
        except Exception as ex:
            log.info(str(ex))

        return None

    for doc_id in asbuilt_doc_ids:
        ad = mongo_hlpr.get_document(ASBUILTS_COLLECTION, doc_id)

        if not 'site_info' in ad:
            log.info('no site info - %s' % ad['source_file'] )
            continue

        if not 'kvps' in ad['site_info']:
            log.info('no site info kvps - %s' % ad['source_file'] )
            continue

        kvps = ad['site_info']['kvps']
        scu = kvps.get('SCU:', 'XXXX')
        jurisdiction = kvps.get('JURISDICTION:', 'XXXX')
        owner = kvps.get('UTILITIES:', 'XXXX')
        latitude = kvps.get('LATITUDE:', 9999)
        longitude = kvps.get('LONGITUDE:', 9999)
        address = kvps.get('SITE ADDRESS:', 'XXXX')
        county = kvps.get('COUNTY', 'XXXX')

        file = os.path.basename(ad['source_file'])

        pole_height = search_dims(ad, "POLE", pages=[3, 2, 4, 1])
        pole_height_comment = ""
        if pole_height is None:
            pole_height = search_dims(ad, "PROPOSED POLE", pages=[3, 2, 4, 1])

        redline_pole_height = search_redline_dims(ad, 'POLE', pages=[3, 2, 1])
        if redline_pole_height:
            pole_height = redline_pole_height
            pole_height_comment = 'redline'

        primary_power = search_dims(ad, 'PRIMARY ELECTRICAL', pages=[3, 2, 4, 1])
        primary_power_comment = ""

        secondary_power = search_dims(ad, 'SECONDARY SERVICE', pages=[3, 2, 4, 1])
        secondary_power_comment = ""

        fiber_dist = search_dims(ad, 'FIBER DIST PANEL', pages=[3, 2, 4, 1])
        fiber_dist_comment = ""

        ac_load = search_dims(ad, 'AC LOAD PANEL', pages=[3, 2, 4, 1])
        ac_load_comment = ""

        sign = search_dims(ad, 'STREET SIGN', pages=[3, 2, 4, 1])
        sign_comment = ""

        shroud = search_dims(ad, 'SHROUD', pages=[3, 2, 4, 1])
        shroud_comment = ""

        antenna = search_dims(ad, 'ANTENNA', pages=[3, 2, 4, 1])
        antenna_comment = ""

        data['scu'].append(scu)
        data['jurisdiction'].append(jurisdiction)
        data['owner'].append(owner)
        data['file'].append(file)
        data['latitude'].append(latitude)
        data['longitude'].append(longitude)
        data['county'].append(county)
        data['address'].append(address)

        data['pole height'].append(pole_height)
        data['pole height comment'].append(pole_height_comment)

        data['primary power'].append(primary_power)
        data['primary power comment'].append(primary_power_comment)

        data['secondary power'].append(secondary_power)
        data['secondary power comment'].append(secondary_power_comment)

        data['fiber dist panel'].append(fiber_dist)
        data['fiber dist panel comment'].append(fiber_dist_comment)

        data['ac load panel'].append(ac_load)
        data['ac load panel comment'].append(ac_load_comment)

        data['street sign'].append(sign)
        data['street sign comment'].append(sign_comment)

        data['shroud'].append(shroud)
        data['shroud comment'].append(shroud_comment)

        data['antenna'].append(antenna)
        data['antenna comment'].append(antenna_comment)

        data['analysis_id'].append(str(ad['_id']))
        # data['ac']

    df = pd.DataFrame(data)

    df = df.sort_values('scu')
    # print (df.head())
    ofile = '%s_output.csv' % project_id
    df.to_csv(ofile, index=False)
    log.info('Exported output %s' % ofile)


def _replace_words_with_errors(t):
    words = t.split(" ")
    replaced = []
    for w in words:
        if known_errors.get(w):
            replaced.append(known_errors[w])
        else:
            replaced.append(w)
    return  " ".join(replaced)


def _get_page_dims(dbname, analysis_id, category='as-built'):

    all_dims = []
    # regx = re.compile("[0-9]+'-[0-9]+\"", re.IGNORECASE) # match with inches symbol
    regx1 = re.compile("[0-9]+'-[0-9\s\/0-9]+", re.IGNORECASE)  # separated by -
    regx2 = re.compile("[0-9]+' [0-9]+", re.IGNORECASE)  # separated by space
    regx3 = re.compile("[0-9]+-[0-9]+\"", re.IGNORECASE)  # no feet symbol, must have inch symbol separated by -
    regx4 = re.compile('[0-9]+ [0-9]+"', re.IGNORECASE)  # no feet symbol, must have inch separated by space
    regx5 = re.compile("[0-9]+'(?!')$", re.IGNORECASE)  # label followed by - and number with only feet symbol

    mongo_hlpr = mongodb_helper.MongoHelper(dbname)

    dims = []
    for regx in [regx1, regx2, regx3, regx4, regx5]:
    # for regx in [regx5]:
        ocr_lines = mongo_hlpr.query(OCR_LINE_COLLECTION, {"analysis_id": analysis_id, "text": regx})
        for line in ocr_lines:
            t = line["text"]
            t = _replace_words_with_errors(t)

            feet_symbol_count = len(str(t).split("\'"))
            if feet_symbol_count > 2:
                print('cant parse %s' % t)
                continue

            label = ""
            dim_feet = 0
            dim_inches = 0
            value = ""

            if regx.pattern != regx5.pattern:
                mats = re.findall(regx, t)
                if mats:
                    mat = mats[0]

                    if mat:
                        parts = re.split(regx, t)
                        parts = [p.strip() for p in parts if len(p.strip())]
                        label = ",".join(parts)

                        # split mat into feet and inches

                        if "'" in mat:
                            tokens = mat.split("'")
                        elif "-" in mat:
                            tokens = mat.split("-")
                        else:
                            tokens = mat.split(" ")

                        dim_feet = "".join([t for t in tokens[0] if t.isalnum()])
                        if len(tokens) < 2:
                            continue

                        if r'/' in tokens[1]:
                            # print('parsing %s' % tokens[1])
                            try:
                                inches_tokens = tokens[1].split()
                                whole_part = "".join([t for t in inches_tokens[0] if t.isalnum()])
                                whole_part = int(whole_part)
                                fraction = inches_tokens[1].split(r'/')
                                inches_num = "".join([t for t in fraction[0] if t.isalnum()])
                                inches_deno = "".join([t for t in fraction[1] if t.isalnum()])
                                dim_inches = whole_part + (float(inches_num) / float(inches_deno))
                                dim_inches = np.around(dim_inches, decimals=2)
                            except Exception as ex:
                                log.info('Error calculating dims %s' % t)
                                log.info(str(ex))
                        else:
                            dim_inches = "".join([t for t in tokens[1] if t.isalnum()])
                            if len(dim_inches):
                                dim_inches = int(dim_inches)
                            else:
                                dim_inches = 0
                        try:
                            dim_feet = float(dim_feet)
                        except Exception as  ex:
                            print ("Error parsing dims from %s" % t)
                            continue

                        value = mat

            else:
                # regx is regx5
                # print(t)
                tokens = t.split('-')
                if tokens and (len(tokens) > 1):
                    try:
                        label = tokens[0]
                        dim_feet = str(tokens[1]).replace("'", "").replace(" ", "") # second token, remove feet symbol at end
                        dim_feet = float(dim_feet)
                        dim_inches = 0
                        value = tokens[1]
                    except Exception as ex:
                        log.info("Error parsing dimensions for %s" % t)
                        log.info(str(ex))

            dims.append({
                "label": label,
                "value": value,
                "feet": dim_feet,
                "inches": dim_inches,
                "line": line,
                "analysis_category": category
            })

    return dims


def match_dimensional_lines(dbname, project_id):
    mongo_hlpr = mongodb_helper.MongoHelper(dbname)
    asbuilts = mongo_hlpr.query(ASBUILTS_COLLECTION, {'project': project_id})
    asbuilt_ids = [ad['_id'] for ad in asbuilts]

    for doc_id in asbuilt_ids:
        asbuilt = mongo_hlpr.get_document(ASBUILTS_COLLECTION, doc_id)
        asbuilt_pages = asbuilt.get('pages', [])

        all_page_dims = []
        all_redline_dims = []

        for asbuilt_page in asbuilt_pages:
            ocr_analysis_id = asbuilt_page.get('ocr_analysis_id')
            if ocr_analysis_id:
                page_ocr_dims = _get_page_dims(dbname, ocr_analysis_id, category='as-built')
                all_page_dims.append({"page": asbuilt_page['page'], "dims": page_ocr_dims})

            red_analysis_id = asbuilt_page.get('red_ocr_analysis_id')
            if red_analysis_id:
                page_redline_dims = _get_page_dims(dbname, red_analysis_id, category='redline')
                all_redline_dims.append({"page": asbuilt_page['page'], "dims": page_redline_dims})

        mongo_hlpr.update_document(ASBUILTS_COLLECTION, doc_id, {"dims": all_page_dims})
        mongo_hlpr.update_document(ASBUILTS_COLLECTION, doc_id, {"redline_dims": all_redline_dims})


if __name__ == '__main__':
    from bson import ObjectId
    # folder = r'/Users/ujjwal/projects/cci/data/as-builts/chicago_test'
    project_id = 'chicago_big'
    dbname = 'chicago_big1'
    category = "as-built"
    logger.setup()
    log = logger.logger

    # mongo_helper = mongodb_helper.MongoHelper(dbname=dbname)
    match_dimensional_lines(dbname, project_id)
    identify_labels(dbname, project_id)
    export_output_csv(dbname, project_id)

    # analysis_id = ObjectId ("60ff65991e23b73c6053a8b3")
    # n_pages = 3
    # _get_page_dims(analysis_id, n_pages, category='as-built')
