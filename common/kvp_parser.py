from bson import ObjectId
import re, os, json
from common.config import ASBUILTS_COLLECTION, AZURE_ANALYSIS_COLLECTION, OCR_LINE_COLLECTION
from common.mongodb_helper import MongoHelper
import geopandas
import pandas
from shapely.geometry import Polygon, mapping
from shapely.affinity import scale


class TargetNotFoundException(Exception):
    pass


def poly_from_bbox(bbox, scale_x=1., scale_y=1.):
    tl = (bbox[0], bbox[1])
    tr = (bbox[2], bbox[3])
    br = (bbox[4], bbox[5])
    bl = (bbox[6], bbox[7])
    p = Polygon([tl, tr, br, bl])
    p = scale(p, xfact=scale_x, yfact=scale_y)
    return p


class Detection(object):
    def __init__(self, target, near_lines):
        self.target = target
        self.near_lines = near_lines

    def to_mongo_dict(self):
        label = self.target.label
        sameline = self.target.ocr_line['text']
        nearlines = "\n".join(nl['text'] for nl in self.near_lines)
        value = nearlines

        tokens = str(sameline).split(self.target.delimiter)
        if len(tokens) > 1:
            label = tokens[0]
            if len(tokens[1]):
                value = tokens[1]

        return {
            "label": label,
            "value": value,
            "sameline": sameline,
            "nearlines": nearlines,
            "target": {
                "label": self.target.label,
                "proximity_mode": self.target.proximity_mode,
                "match_type": self.target.match_type,
                "page": self.target.page,
                "line_id": self.target.ocr_line["_id"]
            }
        }

    def __str__(self):
        return """
            %s -
            -------------------------
            %s
            
            %s
            =========================
            
        """ % (self.target.label, self.target.ocr_line['text'], "\n".join(nl['text'] for nl in self.near_lines))


class Target(object):

    SAMELINE = 'sameline'
    MULTILINE = 'multiline'

    ORIGIN_CENTER = 'center'
    ORIGIN_TOPLEFT = 'top-left'

    def __init__(self, label, proximity_mode, match_type, page=0, priority=0, case_sensitive=False, delimiter=":",
                 xfact=1., yfact=1., origin=ORIGIN_CENTER):
        self.label = label
        self.proximity_mode = proximity_mode
        self.xfact = xfact
        self.yfact = yfact
        self.origin = origin
        self.ocr_line = None
        self.match_type = match_type
        self.case_sensitive = case_sensitive
        self.page = page
        self.priority = priority
        self.delimiter = delimiter

    def load_target_line(self, mongo_helper, analysis_ids):
        target_regx = re.compile(self.label, re.IGNORECASE)  # regex to look for exact label
        target_ocr_lines_cursor = mongo_helper.query(OCR_LINE_COLLECTION, {'text': target_regx,
                                                                           'analysis_id': {'$in': analysis_ids}
                                                    })
        target_ocr_lines = [d for d in target_ocr_lines_cursor]
        if len(target_ocr_lines) == 0:
            raise TargetNotFoundException('Could not find target text %s' % self.label)
        return target_ocr_lines[0]


class Parser(object):

    def __init__(self, dbname, asbuilt_id, mode='asbuilt'):
        self.asbuilt_id = asbuilt_id
        self.asbuilt_doc = None
        self.dbname = dbname
        self.mode = mode
        self.ocr_lines_gdf = None
        self.analysis_ids = []

        self.gdf = self._load_asbuilt()

    def _load_asbuilt(self):
        mongo_helper = MongoHelper(self.dbname)
        try:
            as_built_doc = mongo_helper.get_document(ASBUILTS_COLLECTION, self.asbuilt_id)
            self.asbuilt_doc = as_built_doc
            analysis_ids = []
            pages = as_built_doc.get('pages')
            if pages is None:
                raise Exception('asbuilt %s, %s has no pages' % (self.asbuilt_id, self.asbuilt_doc['source_file']))

            for page in as_built_doc['pages']:
                if self.mode == 'asbuilt':
                    analysis_id = page.get('ocr_analysis_id', None)
                    if analysis_id is None:
                        log.debug('No OCR Analysis: asbuilt %s, page %d' % (self.asbuilt_id, page['page']))
                        continue

                elif self.mode == 'redline':
                    analysis_id = page.get('red_ocr_analysis_id', None)
                    if analysis_id is None:
                        log.debug('No RED OCR Analysis: asbuilt %s, page %d' % (self.asbuilt_id, page['page']))
                        continue
                else:
                    raise Exception('mode should be asbuilt or redline')
                analysis_ids.append(analysis_id)

            ocr_lines_cursor = mongo_helper.query(OCR_LINE_COLLECTION, {'analysis_id': {'$in': analysis_ids} })
            text = []
            geom = []
            line_id = []
            page = []
            anal_ids = []
            for ol in ocr_lines_cursor:
                text.append(ol['text'])
                page.append(ol['page'])
                geom.append(poly_from_bbox(ol['boundingBox']))
                anal_ids.append(ol['analysis_id'])
                line_id.append(ol['_id'])

            df = pandas.DataFrame(data={'line_id': line_id, 'text': text, 'page': page, 'geom': geom})
            ocr_lines_gdf = geopandas.GeoDataFrame(df, geometry='geom')
            ocr_lines_gdf_analysis_ids = list(set(anal_ids))
            self.analysis_ids = ocr_lines_gdf_analysis_ids
            log.debug('GDF analysis ids: %s' % ",".join(str(o) for o in ocr_lines_gdf_analysis_ids))
        except Exception as ex:
            mongo_helper.close()
            raise ex

        mongo_helper.close()
        return ocr_lines_gdf

    def parse(self, targets):
        # load target lines
        # geodataframe based location matching is needed because cosmosdb mongo does not support 2d index

        mongo_helper = MongoHelper(self.dbname)
        matched_targets = []

        for target in targets:
            assert (isinstance(target, Target))
            try:
                ocr_line = target.load_target_line(mongo_helper, self.analysis_ids)
                target.ocr_line = ocr_line
                matched_targets.append(target)
            except TargetNotFoundException:
                pass

        detections = []

        # order targets by priority so that highest priority overrides
        sorted(matched_targets, key=lambda x: x.priority, reverse=True)

        for target in matched_targets:
            label_text = target.ocr_line['text']
            target_geom = poly_from_bbox(target.ocr_line['boundingBox'])
            scale_origin = target_geom.centroid

            if target.origin == Target.ORIGIN_TOPLEFT:
                scale_origin = tuple(target_geom.bounds[:2])

            target_geom = scale(target_geom, xfact=target.xfact, yfact=target.yfact, origin=scale_origin)

            gdf = self.gdf
            gdf = gdf[gdf['page'] == target.ocr_line['page']].copy()
            near_mask = gdf['geom'].apply(lambda x: x.intersects(target_geom))
            near_lines = gdf[near_mask]
            near_lines_output = []
            for indx, near_line in near_lines.iterrows():
                if near_line['line_id'] != target.ocr_line['_id']:
                    if len(near_line['text']) > 1:
                        near_lines_output.append({
                            'line_id': near_line['line_id'],
                            'text': near_line['text'],
                            'page': near_line['page']
                        })

            if len(near_lines_output):
                detection = Detection(target, near_lines_output)
                detections.append(detection)

        return detections


def parse_asbuilt(dbname, parser_file, asbuilt_id, container_field, mode='asbuilt'):
    if not os.path.exists(parser_file):
        raise Exception('Could not open file %s' % parser_file)

    with open(parser_file, 'r') as pfh:
        data = pfh.read()
        parser_configs = json.loads(data)

    entity_targets = {}
    for parser_config in parser_configs:
        entity = parser_config['entity']
        collection = parser_config['collection']

        for target in parser_config['targets']:
            label = target['label']
            match_type = target['match']
            case_sensitive = target['case_sensitive']
            match_mode = target['mode']
            scale_x = target['scale_x']
            scale_y = target['scale_y']
            scale_origin = target['scale_origin']
            delimiter = target['delimiter']
            page = target['page']
            priority = target['priority']

            tgt = Target(label=label, proximity_mode=match_mode, match_type=match_type, page=page, xfact=scale_x,
                         yfact=scale_y, origin=scale_origin, case_sensitive=case_sensitive, priority=priority,
                         delimiter=delimiter)

            if entity_targets.get(entity):
                entity_targets[entity]["targets"].append(tgt)
            else:
                entity_targets[entity] = {
                    "entity": entity,
                    "collection": collection,
                    "targets": [tgt]
                }

    parser = Parser(dbname, asbuilt_id, mode)
    for entity in entity_targets:
        entity_target = entity_targets[entity]
        try:
            targets = entity_target.get('targets', None)
            if targets:
                detections = parser.parse(targets)
                mongo_helper = MongoHelper(dbname)
                detections_mongo = [ d.to_mongo_dict() for d in detections ]
                mongo_helper.update_document(ASBUILTS_COLLECTION, asbuilt_id,
                                { "%s.%s.%s" % (container_field, entity_target['collection'], entity): detections_mongo })
                mongo_helper.close()
            else:
                log.debug('Entity %s had no matched targets' % entity)
        except Exception as ex:
            # raise ex
            log.debug(str(ex))


if __name__ == "__main__":

    from common import logger
    from common.config import DBNAME, PROJECT

    logger.setup("kvp_parser")
    log = logger.logger

    dbname = DBNAME
    project = PROJECT

    mongo_helper = MongoHelper(dbname)
    asbuilts_cursor = mongo_helper.query(ASBUILTS_COLLECTION, {"project": project})
    asbuilt_ids = [ ab["_id"] for ab in asbuilts_cursor ]
    mongo_helper.close()

    # asbuilt_ids = [ ObjectId("6109a413abba9c3dbd230d51") ]
    count = len(asbuilt_ids)
    idx = 0

    for asbuilt_id in asbuilt_ids:
        idx += 1
        log.info('processing %s - %d of %d' % (asbuilt_id, idx, count))
        parse_asbuilt(dbname, './kvp_parser_targets.json', asbuilt_id, "kvp_parser", 'asbuilt')
        parse_asbuilt(dbname, './kvp_parser_targets.json', asbuilt_id, "kvp_parser_redline", 'redline')
