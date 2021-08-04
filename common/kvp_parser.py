from bson import ObjectId
import re
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
    ORIGIN_TOPLEFT = 'topleft'

    def __init__(self, label, proximity_mode, xfact=1., yfact=1., origin=ORIGIN_CENTER):
        self.label = label
        self.proximity_mode = proximity_mode
        self.xfact = xfact
        self.yfact = yfact
        self.origin = origin
        self.ocr_line = None

    def load_target_line(self, mongo_helper):
        target_regx = re.compile(self.label, re.IGNORECASE)  # regex to look for exact label
        target_ocr_lines_cursor = mongo_helper.query(OCR_LINE_COLLECTION, {'text': target_regx})
        target_ocr_lines = [d for d in target_ocr_lines_cursor]
        if len(target_ocr_lines) == 0:
            raise TargetNotFoundException('Could not find target text %s' % self.label)
        return target_ocr_lines[0]


class Parser(object):

    def __init__(self, dbname, asbuilt_id, mode='asbuilt'):
        self.asbuilt_id = asbuilt_id
        self.dbname = dbname
        self.mode = mode
        self.ocr_lines_gdf = None

    def load_asbuilt(self):
        mongo_helper = MongoHelper(self.dbname)
        try:
            as_built_doc = mongo_helper.get_document(ASBUILTS_COLLECTION, self.asbuilt_id)
            analysis_ids = []
            for page in as_built_doc['pages']:
                if self.mode == 'asbuilt':
                    analysis_id = page['ocr_analysis_id']
                elif self.mode == 'redline':
                    analysis_id = page['red_ocr_analysis_id']
                else:
                    raise Exception('mode should be asbuilt or redline')
                analysis_ids.append(analysis_id)

            ocr_lines_cursor = mongo_helper.query(OCR_LINE_COLLECTION, {'analysis_id': {'$in': analysis_ids} })
            text = []
            geom = []
            line_id = []
            page = []
            for ol in ocr_lines_cursor:
                text.append(ol['text'])
                page.append(ol['page'])
                geom.append(poly_from_bbox(ol['boundingBox']))
                line_id.append(ol['_id'])

            df = pandas.DataFrame(data={'line_id': line_id, 'text': text, 'page': page, 'geom': geom})
            ocr_lines_gdf = geopandas.GeoDataFrame(df, geometry='geom')
        except Exception as ex:
            mongo_helper.close()
            raise ex

        return ocr_lines_gdf

    def parse(self, targets):
        # load target lines
        # geodataframe based location matching is needed because cosmosdb mongo does not support 2d index

        mongo_helper = MongoHelper(self.dbname)
        matched_targets = []
        for target in targets:
            assert (isinstance(target, Target))
            try:
                ocr_line = target.load_target_line(mongo_helper)
                target.ocr_line = ocr_line
                matched_targets.append(target)
            except TargetNotFoundException:
                pass

        gdf = self.load_asbuilt()
        detections = []
        for target in matched_targets:

            label_text = target.ocr_line['text']

            target_geom = poly_from_bbox(target.ocr_line['boundingBox'])
            scale_origin = target_geom.centroid
            if target.origin == Target.ORIGIN_TOPLEFT:
                scale_origin = tuple(target_geom.bounds[:2])

            if target.proximity_mode == Target.SAMELINE:
                target_geom = scale(target_geom, xfact=target.xfact, yfact=target.yfact, origin=scale_origin)
            elif target.proximity_mode == Target.MULTILINE:
                target_geom = scale(target_geom, xfact=target.xfact, yfact=target.yfact, origin=scale_origin)

            gdf = gdf[gdf['page'] == target.ocr_line['page']].copy()
            near_mask = gdf['geom'].apply(lambda x: x.intersects(target_geom))
            near_lines = gdf[near_mask]
            near_lines_output = []
            for indx, near_line in near_lines.iterrows():
                if near_line['line_id'] != target.ocr_line['_id']:
                    near_lines_output.append({
                        'line_id': near_line['line_id'],
                        'text': near_line['text'],
                        'page': near_line['page']
                    })
            detection = Detection(target, near_lines_output)
            detections.append(detection)

        return detections


if __name__ == "__main__":
    asbuilt_id = ObjectId("6109a413abba9c3dbd230d51")
    target1 = Target("BU #:", proximity_mode=Target.SAMELINE)
    target2 = Target("SITE INFORMATION", proximity_mode=Target.MULTILINE, xfact=1, yfact=2,
                     origin=Target.ORIGIN_TOPLEFT)
    target3 = Target("SITE LAT:", proximity_mode=Target.SAMELINE, xfact=3, yfact=1,
                     origin=Target.ORIGIN_CENTER)

    dbname = 'new_batch_demo'
    parser = Parser(dbname, asbuilt_id)
    detections = parser.parse([target1, target3])

    for d in detections:
        print(d)


