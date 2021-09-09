import os, json, re
from common import mongodb_helper
from common.config import ASBUILTS_COLLECTION, AZURE_ANALYSIS_COLLECTION, OCR_LINE_COLLECTION, DBNAME, PROJECT
from common import logger
from shapely.geometry import Polygon, mapping
from shapely.affinity import scale
import geopandas
import pandas
import numpy as np


SCALE_ORIGIN_CENTER = 'center'
SCALE_ORIGIN_TOPLEFT = 'top-left'


def _poly_from_bbox(bbox, scale_x=1., scale_y=1., origin=SCALE_ORIGIN_CENTER):
    tl = (bbox[0], bbox[1])
    tr = (bbox[2], bbox[3])
    br = (bbox[4], bbox[5])
    bl = (bbox[6], bbox[7])
    p = Polygon([tl, tr, br, bl])
    origin_point = p.centroid
    if origin == SCALE_ORIGIN_TOPLEFT:
        origin_point = tl
    p = scale(p, xfact=scale_x, yfact=scale_y, origin=origin_point)
    return p


class RegexParser(object):
    def __init__(self, template):
        self.patterns = template.get('patterns', [])
        case_sensitive = template.get('case_sensitive', False)
        self.regexes = []
        for pattern in self.patterns:
            if case_sensitive:
                regx = re.compile(pattern)
            else:
                regx = re.compile(pattern, re.IGNORECASE)
            self.regexes.append(regx)

    def search(self, text):
        results = []
        for regx in self.regexes:
            res = re.search(regx, text)
            if res:
                regs = list(res.regs)
                if len(regs):
                    start = regs[0][0]
                    end = regs[0][1]
                    match = text[start: end]
                    results.append({
                        'match': match,
                        'regx': regx,
                        'start': start,
                        'end': end
                    })
        return results

    def find_all(self, text):
        for regx in self.regexes:
            matches = re.findall(regx, text)
            if matches is not None:
                if len(matches):
                    return matches

        return None

    def split(self, text):
        for regx in self.regexes:
            parts = re.split(regx, text)
            if len(parts) > 1:
                return parts
        return None


class Detection(object):
    def __init__(self, text, entity, page_number, label_line_ids, value_line_ids,
                 asbuilt_id, analysis_id):

        assert (isinstance(entity, EntityParserTemplate))

        self.text = text
        self.label_part = ""
        self.value_part = ""
        self.value_feet = -1
        self.value_inches = -1
        self.category = entity.category
        self.value_type = entity.value_type
        self.entity = entity
        self.page_number = page_number
        self.label_line_ids = label_line_ids
        self.value_line_ids = value_line_ids
        self.analysis_id = analysis_id
        self.asbuilt_id = asbuilt_id
        self.position = "center"
        self.parsed = False

    def parse(self):
        matches = self.entity.value_parser.find_all(self.text)
        if matches is None:
            return

        parts = self.entity.value_parser.split(self.text)
        parts = [p.strip() for p in parts if len(p.strip())]
        label = ",".join(parts)
        if self.entity.remove_special_chars_in_label:
            label = "".join([l for l in label if (str(l).isalnum() or l == " ")])
            label = label.strip()
        self.label_part = label

        if self.value_type == EntityParserTemplate.VALUE_TYPE_GEO:
            if len(matches) == 2:
                self.value_part = ",".join([str(float(m)) for m in matches])
            if len(matches) == 1:
                self.value_part = str(float(matches[0]))
            self.parsed = True

        elif self.value_type == EntityParserTemplate.VALUE_TYPE_DIMENSION:
            mat = matches[0]
            if mat:
                # split mat into feet and inches
                if "'" in mat:
                    tokens = mat.split("'")
                elif "-" in mat:
                    tokens = mat.split("-")
                else:
                    tokens = mat.split(" ")

                dim_inches = 0
                dim_feet = "".join([t for t in tokens[0] if t.isalnum()])
                if len(tokens) < 2:
                    return

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
                        log.info('Error calculating dims %s' % self.text)
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
                    print("Error parsing dims from %s" % self.text)

                self.value_part = "%.0f' %.0f\"" % (dim_feet, dim_inches)
                self.value_inches = dim_inches
                self.value_feet = dim_feet
                self.parsed = True

        else:
            raise Exception("Incorrect value_type for entity %s" % self.entity.entity)

            # value_results = self.entity.value_parser.search(self.text)
            #
            # if len(value_results):
            #     value_search = value_results[0]
            #     value_part = value_search['match']
            #     if self.entity.remove_spaces_in_values:
            #         value_part = "".join(value_part.split())
            #     value_start = value_search['start']
            #     value_end = value_search['end']
            #
            #     # sometimes value is to the left of the label
            #     if value_start < len(self.text) / 5:
            #         label_part = self.text[value_end:]
            #     else:
            #         label_part = self.text[:value_start]
            #
            #     self.label_part = str(label_part).strip()
            #     self.value_part = str(value_part).strip()
            #
            #     self.parsed = True

        if self.category == "box":
            label_upper = self.label_part.upper()
            label_upper = label_upper.strip()
            label_words = label_upper.split()
            label_words = [ l.strip() for l in label_words ]
            position_options = ['TOP', 'BOTTOM', 'CENTER', 'OP', '₡']
            position_indicators = [ w for w in label_words if w in position_options ]
            if len(position_indicators):
                if 'TOP' in position_indicators:
                    self.position = "TOP"
                if 'BOTTOM' in position_indicators:
                    self.position = "BOTTOM"
                if ('₡' in position_indicators) or ('CENTER' in position_indicators):
                    self.position = "CENTER"
                # some times 'TOP' is detected as 'OP'
                if 'OP' == position_indicators[0]:
                    self.position = "TOP"

    def toJson(self):
        return {
            "text": self.text,
            "label": self.label_part,
            "value": self.value_part,
            "feet": self.value_feet,
            "inches": self.value_inches,
            "value_type": self.value_type,
            "category": self.category,
            "entity": self.entity.entity,
            "position": self.position.lower(),
            "page_number": self.page_number,
            "analysis_id": self.analysis_id,
            "asbuilt_id": self.asbuilt_id,
            "label_line_ids": [str(l) for l in self.label_line_ids],
            "value_line_ids": [str(l) for l in self.value_line_ids]
        }


class EntityParserTemplate(object):

    VALUE_TYPE_DIMENSION = 'dimension'
    VALUE_TYPE_GEO = 'geo'
    VALUE_TYPE_INTEGER = 'int'
    VALUE_TYPE_STRING = 'string'

    MODE_SINGLELINE = "singleline"
    MODE_MULTILINE  = "multiline"

    def __init__(self, template):
        self.entity = template['entity']
        self.category = template['category']
        self.label_parser = RegexParser(template['label_parser'])
        self.value_parser = RegexParser(template['value_parser'])
        self.remove_spaces_in_values = template.get('remove_spaces_in_value', False)
        self.remove_special_chars_in_label = template.get('remove_special_chars_in_label', False)

        self.value_type = template.get('value_type', None)
        self.mode = template.get('mode', self.MODE_SINGLELINE)
        self.xfact = template.get('scale_x', 1)
        self.yfact = template.get('scale_y', 1)
        self.origin = template.get('scale_origin', SCALE_ORIGIN_TOPLEFT)

    def parse_dims(self, mongo_helper, asbuilt_id, page_number, analysis_id, gdf=None):
        detections = []

        # if self.entity == 'sign':
        #     print(self.entity)

        for label_regx in self.label_parser.regexes:
            cursor = mongo_helper.query(OCR_LINE_COLLECTION,
                                        {"analysis_id": analysis_id, "text": label_regx})
            label_line_ids = []
            value_line_ids = []
            ocr_lines = [l for l in cursor]
            for ocr_line in ocr_lines:
                text = ocr_line['text']
                label_line_ids.append(ocr_line['_id'])
                ocr_line_id = str(ocr_line['_id'])

                # if mode is multiline, get more text
                if self.mode == self.MODE_MULTILINE:
                    bbox = ocr_line['boundingBox']
                    line_bbox = _poly_from_bbox(bbox, self.xfact, self.yfact)
                    if not gdf.empty:
                        near_mask = gdf['geom'].apply(lambda x: x.intersects(line_bbox))
                        near_lines = gdf[near_mask]
                        for indx, near_line in near_lines.iterrows():
                            near_line_id = str(near_line['line_id'])
                            if near_line_id != ocr_line_id:
                                if len(near_line['text']) > 1:
                                    text = text + " " + near_line['text']
                                    value_line_ids.append(near_line['line_id'])
                else:
                    value_line_ids.append(ocr_line['_id'])

                detection = Detection(text, self, page_number,
                                      label_line_ids, value_line_ids, asbuilt_id, analysis_id)
                detection.parse()
                if detection.parsed:
                    detections.append(detection.toJson())
                    # log.info(detection.toJson())

        return detections


class DimensionParser(object):

    def __init__(self, dbname, template_file):
        self.dbname = dbname
        self.entity_parsers = {}
        self.gdf = None
        entity_parsers = {}
        with open(template_file, 'r') as tf:
            contents = tf.read()
            templates = json.loads(contents)
            for template in templates:
                parser = EntityParserTemplate(template)
                entity_parsers[template['entity']] = parser
        self.entity_parsers = entity_parsers

    def load_analysis_gdf(self, analysis_id):
        mongo_helper = mongodb_helper.MongoHelper(self.dbname)
        ocr_lines_count = mongo_helper.count(OCR_LINE_COLLECTION, { "analysis_id": analysis_id })
        if ocr_lines_count:
            cursor = mongo_helper.query(OCR_LINE_COLLECTION, { "analysis_id": analysis_id })
            text = []
            geom = []
            line_id = []
            page = []
            for ol in cursor:
                text.append(ol['text'])
                page.append(ol['page'])
                geom.append(_poly_from_bbox(ol['boundingBox']))
                line_id.append(ol['_id'])

            df = pandas.DataFrame(data={'line_id': line_id, 'text': text, 'page': page, 'geom': geom})
            ocr_lines_gdf = geopandas.GeoDataFrame(df, geometry='geom')
            return ocr_lines_gdf
        return None

    def parse_dims(self, asbuilt_id, page_number, analysis_id):
        gdf = self.load_analysis_gdf(analysis_id)
        mongo_helper = mongodb_helper.MongoHelper(dbname)
        all_detections = []
        for entity_parser in self.entity_parsers.keys():
            # if entity_parser != 'sign':
            #     continue
            detections = self.entity_parsers[entity_parser].parse_dims(mongo_helper, asbuilt_id, page_number, analysis_id, gdf)
            if len(detections):
                all_detections = all_detections + detections
        return all_detections


def detect_dimensions(dbname, project_id, template_file):
    from bson import ObjectId
    mongo_hlpr = mongodb_helper.MongoHelper(dbname)
    asbuilts = mongo_hlpr.query(ASBUILTS_COLLECTION, {'project': project_id})
    asbuilt_ids = [ad['_id'] for ad in asbuilts]
    # asbuilt_ids = [ObjectId("6109a4baabba9c3dbd230d57")]
    mongo_hlpr.close()
    dimension_parser = DimensionParser(dbname, template_file)

    n_docs = len(asbuilt_ids)
    idx = 0
    for doc_id in asbuilt_ids:
        idx += 1
        mongo_hlpr = mongodb_helper.MongoHelper(dbname)
        asbuilt = mongo_hlpr.get_document(ASBUILTS_COLLECTION, doc_id)
        asbuilt_pages = asbuilt.get('pages', [])
        log.debug('detect dimension - %s, %s: %d of %d' % (doc_id, asbuilt['source_file'], idx, n_docs))
        page_num = 0
        for asbuilt_page in asbuilt_pages:
            ocr_analysis_id = asbuilt_page.get('ocr_analysis_id')
            page_number = asbuilt_page['page']
            if ocr_analysis_id:
                ocr_detections = dimension_parser.parse_dims(doc_id, page_number, ocr_analysis_id)
                mongo_hlpr.update_document(ASBUILTS_COLLECTION, doc_id,
                                           {"pages.%d.ocr_detections" % page_num: ocr_detections})

            red_analysis_id = asbuilt_page.get('red_ocr_analysis_id')
            if red_analysis_id:
                red_ocr_detections = dimension_parser.parse_dims(doc_id, page_number, red_analysis_id)
                mongo_hlpr.update_document(ASBUILTS_COLLECTION, doc_id,
                                           {"pages.%d.red_ocr_detections" % page_num: red_ocr_detections })
            page_num += 1

        mongo_hlpr.close()


if __name__ == '__main__':

    project_id = PROJECT
    dbname = DBNAME

    logger.setup('dimension_parser')
    log = logger.logger

    template_file = './dimension_parser_templates.json'
    detect_dimensions(dbname, project_id, template_file)
