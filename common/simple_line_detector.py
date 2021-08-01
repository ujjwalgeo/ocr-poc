import cv2
from shapely.geometry import LineString, Polygon, Point
import geopandas
import os
import pandas as pd
import math


def detect_panel(image_file, bbox, panel_name="panel", debug_mode=False, overwrite=False):

    ofile_name = os.path.join(os.path.dirname(image_file), "%s_%s" % (panel_name, os.path.basename(image_file)))
    if os.path.exists(ofile_name):
        if overwrite:
            os.remove(ofile_name)
        else:
            return ofile_name

    centroid = [0.25 * (bbox[0] + bbox[2] + bbox[4] + bbox[6]),
                0.25 * (bbox[1] + bbox[3] + bbox[5] + bbox[7])]

    bbox_xs = [bbox[0], bbox[2], bbox[4], bbox[6]]
    bbox_width = max(bbox_xs) - min(bbox_xs)
    min_line_length = int(bbox_width * 1.5)

    img = cv2.imread(image_file)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    threshold = 50
    max_val = 255
    max_buffer_pixels = 25

    ret, thresh = cv2.threshold(gray, threshold, max_val, cv2.THRESH_BINARY_INV)

    arr = thresh
    n_rows = arr.shape[0]
    n_cols = arr.shape[1]
    plot_on = img

    if debug_mode:
        # cv2.imshow('img', plot_on)
        # cv2.imshow('gray', gray)
        # cv2.imshow('thresh', thresh)
        thresh_image = os.path.join(os.path.dirname(image_file), "thresh_%s" % os.path.basename(image_file))
        cv2.imwrite(thresh_image, thresh)
        # cv2.waitKey(0)

    tolerance = 1

    def fix_coord(val):
        if tolerance == 1:
            return val
        return (math.floor(val/tolerance)) * tolerance

    # create a padding of zeros
    padding = 2
    arr[0:padding, :] = 0
    # arr[padding:-1, :] = 0
    arr[:, 0:padding] = 0
    # arr[:, padding:-1] = 0

    # vertical lines
    vlines = []
    for j in range(n_cols):
        col = arr[:, j]
        if col.min() == col.max():
            continue

        col_series = pd.Series(col)
        col_series_shift = col_series.shift(1)
        col_diff = col_series_shift - col_series

        col_df = pd.DataFrame(data={'col': col_series, 'col_shift': col_series_shift, 'col_diff': col_diff})
        line_start_indxs = col_df.index[col_df['col_diff'] == -max_val].tolist()
        line_end_indxs = col_df.index[(col_df['col_diff'] == max_val)].tolist()

        vlines += [LineString([(fix_coord(j), fix_coord(l[0])), ( fix_coord(j), fix_coord(l[1]))])
                   for l in zip(line_start_indxs, line_end_indxs) if ((l[1] - l[0]) > min_line_length)]

    hlines = []
    for k in range(n_rows):
        row = arr[k, :]
        if row.min() == row.max():
            continue

        row_series = pd.Series(row)
        row_series_shift = row_series.shift(1)
        row_diff = row_series_shift - row_series

        row_df = pd.DataFrame(data={'row': row_series, 'row_shift': row_series_shift, 'row_diff': row_diff})
        line_start_indxs = row_df.index[row_df['row_diff'] == -max_val].tolist()
        line_end_indxs = row_df.index[(row_df['row_diff'] == max_val)].tolist()

        hlines += [ LineString([( fix_coord(l[0]), fix_coord(k)), ( fix_coord(l[1]), fix_coord(k) )])
                    for l in zip(line_start_indxs, line_end_indxs) if ((l[1] - l[0]) > min_line_length)]

    # unary_union(hlines)
    # unary_union(vlines)

    g_centroid = Point(centroid)

    hgdf = geopandas.GeoDataFrame(geometry=geopandas.GeoSeries(hlines))
    hgdf['wkt'] = hgdf.geometry.apply(lambda g: g.wkt)
    hgdf = hgdf.drop_duplicates(subset='wkt', keep='first')

    hgdf['distance'] = hgdf.geometry.apply(lambda g: g_centroid.distance(g))
    hgdf['offset'] = hgdf.geometry.apply(lambda g: 'above' if (g_centroid.y >= g.centroid.y) else 'below')
    hgdf_above = hgdf[hgdf['offset'] == 'above']
    hgdf_below = hgdf[hgdf['offset'] == 'below']

    hgdf_above = hgdf_above.sort_values('distance')
    hgdf_below = hgdf_below.sort_values('distance')
    hline1 = hgdf_above.iloc[0]
    hline2 = hgdf_below.iloc[0]

    vgdf = geopandas.GeoDataFrame(geometry=geopandas.GeoSeries(vlines))
    vgdf['wkt'] = vgdf.geometry.apply(lambda g: g.wkt)
    vgdf = vgdf.drop_duplicates(subset='wkt', keep='first')

    buffer_pixels = 0
    while buffer_pixels < max_buffer_pixels:
        vgdf['intersects'] = vgdf.geometry.apply(lambda g: g.buffer(buffer_pixels).intersects(hline1.geometry) or
                                                       g.buffer(buffer_pixels).intersects(hline2.geometry))
        num_true_list = [1 for val in vgdf['intersects'].tolist() if val]
        num_true = len(num_true_list)
        if num_true >= 2:
            break
        buffer_pixels += 1

    ivgdf = (vgdf[vgdf['intersects']]).copy()
    ivgdf['distance'] = ivgdf.geometry.apply(lambda g: g_centroid.distance(g))
    ivgdf['offset'] = ivgdf.geometry.apply(lambda g: 'left' if (g_centroid.x >= g.centroid.x) else 'right')
    ivgdf_left = ivgdf[ivgdf['offset'] == 'left'].sort_values('distance')
    ivgdf_right = ivgdf[ivgdf['offset'] == 'right'].sort_values('distance')

    vline1 = ivgdf_left.iloc[0]
    vline2 = ivgdf_right.iloc[0]

    hgdf_bottom = (hgdf_below[hgdf_below['distance'] > min_line_length]).copy()
    hgdf_bottom['bottom_intersects'] = hgdf_bottom.geometry.apply(
        lambda g: g.buffer(buffer_pixels).intersects(vline1.geometry) or
                  g.buffer(buffer_pixels).intersects(vline2.geometry))
    iihgdf = (hgdf_bottom[hgdf_bottom['bottom_intersects']]).copy()
    bottom_hline = iihgdf.iloc[0]
    all_lines = hlines + vlines
    if debug_mode:
        # cv2.imshow('gray', gray)
        # cv2.imshow('thresh', thresh)

        for line_string in all_lines:
            x1 = line_string.xy[0][0]
            x2 = line_string.xy[0][1]

            y1 = line_string.xy[1][0]
            y2 = line_string.xy[1][1]

            x1 = math.floor(x1)
            y1 = math.floor(y1)
            x2 = math.floor(x2)
            y2 = math.floor(y2)

            cv2.line(img, (x1, y1), (x2, y2), (255, 0, 255), 2)

        lines = [ vline1.geometry, vline2.geometry, hline1.geometry, hline2.geometry, bottom_hline.geometry ]
        for line_string in lines:
            x1 = line_string.xy[0][0]
            x2 = line_string.xy[0][1]

            y1 = line_string.xy[1][0]
            y2 = line_string.xy[1][1]

            x1 = math.floor(x1)
            y1 = math.floor(y1)
            x2 = math.floor(x2)
            y2 = math.floor(y2)

            cv2.line(plot_on, (x1, y1), (x2, y2), (255, 0, 0), 6)

        cv2.imshow('img', img)
        cv2.waitKey(0)
        cv2.destroyAllWindows()

    # poly, dangles, cuts, invalids = polygonize_full(lines)
    # print(list(poly))

    vline1_x = vline1.geometry.centroid.x
    vline2_x = vline2.geometry.centroid.x
    if vline1_x < vline2_x:
        left_vline = vline1
        right_vline = vline2
    else:
        left_vline = vline2
        right_vline = vline1

    hline1_y = hline1.geometry.centroid.y
    hline2_y = hline2.geometry.centroid.y
    if hline1_y > hline2_y:
        top_hline = hline1
    else:
        top_hline = hline2

    ul = math.floor(left_vline.geometry.centroid.x), math.floor(top_hline.geometry.centroid.y)
    clip_width = math.floor(right_vline.geometry.centroid.x - left_vline.geometry.centroid.x)
    clip_height = math.floor(bottom_hline.geometry.centroid.y - top_hline.geometry.centroid.y)

    clip_img = img[ul[1]:ul[1]+clip_height, ul[0]:ul[0]+clip_width]

    if debug_mode:
        cv2.imshow('img', plot_on)
        cv2.imshow('gray', gray)
        cv2.imshow('thresh', thresh)
        cv2.imshow('cropped', clip_img)
        cv2.waitKey(0)
        cv2.destroyAllWindows()

    cv2.imwrite(ofile_name, clip_img)

    json_ofile_name = os.path.join(os.path.dirname(image_file), "lines_%s.json" % (os.path.basename(image_file)))
    gjson = geopandas.GeoSeries(all_lines).to_json()
    with open(json_ofile_name, 'w') as of:
        of.write(gjson)

    return ofile_name


if __name__ == "__main__":
    from common.mongodb_helper import MongoHelper

    project_id = 'chicago_big'
    dbname = 'chicago_big1'
    mongo_helper = MongoHelper(dbname)

    # works:
    analysis_id = "6104b8ae0ca5df61eb7a1aa6"
    image_file = '/Users/ujjwal/projects/cci/github/ocr-poc/common/asbuilts/pdf_images/CH1509BA_91LAB_Elevation_As_Built-Node/CH1509BA_91LAB_Elevation_As_Built-Node_page-1.png'

    # doesn't work:
    # analysis_id = "6104ba620ca5df61eb7a2bef"
    # image_file = '/Users/ujjwal/projects/cci/github/ocr-poc/common/asbuilts/pdf_images/CH2262BA_21LAB_Elevation_As_Built/CH2262BA_21LAB_Elevation_As_Built_page-1.png'

    bbox = mongo_helper.get_site_info_bbox(analysis_id=analysis_id)
    detect_panel(image_file, bbox, panel_name="site_info_panel", debug_mode=True)
