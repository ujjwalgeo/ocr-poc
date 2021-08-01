import numpy as np
import skimage.io
import skimage.color
import skimage.morphology
import scipy.signal
import cv2
import os


def detect_table(img_file, text_size_percent=3, line_distance_percent=5, debug_mode=False):

    img = skimage.io.imread(img_file)
    h, w, _ = img.shape
    padding = 2
    img = img[padding:h-padding, padding:w-padding]
    h, w, _ = img.shape

    gray = skimage.color.rgb2gray(img)

    # Create some large dark area with the text, 10 is quite big!
    eroded = skimage.morphology.erosion(gray, skimage.morphology.square(5))

    # Compute mean values along axis 0 or 1
    hist_v = np.mean(eroded, axis=0)
    width_v = w * text_size_percent // 100
    dist_v = w * line_distance_percent // 100
    peaks_cols, proms_cols = scipy.signal.find_peaks(hist_v, width=width_v, distance=dist_v)

    hist_w = np.mean(eroded, axis=1)
    width_w = h * text_size_percent // 100
    dist_w = h * line_distance_percent // 100
    peaks_rows, proms_rows = scipy.signal.find_peaks(hist_w, width=width_w, distance=dist_w)

    # column should have atleast 5 character width
    min_col_width = 5 * w * text_size_percent / 100
    min_row_height = 1.2 * h * text_size_percent / 100
    cells = []
    # if len(peaks_cols > 3):
    #     peaks_cols = peaks_cols[:-1]
    col_xs = [0] + [int(p) for p in peaks_cols] + [w] # remove last line from peaks_cols to take column to the end
    row_ys = [0] + [int(p) for p in peaks_rows] + [h]
    for i in range(1, len(row_ys)):
        if abs(row_ys[i-1] - row_ys[i]) < min_row_height:
            continue

        for j in range(1, len(col_xs)):
            cell = {
                "i": i-1,
                "j": j-1,
                "ul": (col_xs[j-1], row_ys[i-1]),
                "br": (col_xs[j], row_ys[i])
            }
            if (cell["br"][0] - cell["ul"][0]) > min_col_width:
                cells.append(cell)
                cv2.rectangle(img, cell["ul"], cell["br"], (0, 0, 0), 2)

    # for p0 in peaks_cols:
    #     cv2.line(img, (p0, 0), (p0, h), (0, 255, 0), 2)
    #
    # for p1 in peaks_rows:
    #     cv2.line(img, (0, p1), (w, p1), (255, 0, 0), 2)

    if debug_mode:
        cv2.imshow('img', img)
        cv2.waitKey(0)
        cv2.destroyAllWindows()

    ofile_name = os.path.join(os.path.dirname(img_file),
                              "table_%s.png" % (os.path.splitext(os.path.basename(img_file)))[0])
    if os.path.exists(ofile_name):
        os.remove(ofile_name)
    cv2.imwrite(ofile_name, img)

    return ofile_name, cells


if __name__ == '__main__':
    img_file = r'/Users/ujjwal/projects/cci/github/ocr-poc/common/asbuilts/pdf_images/CH1509BA_91LAB_Elevation_As_Built-Node/site_info_panel_CH1509BA_91LAB_Elevation_As_Built-Node_page-1.png'
    # img_file = r'/Users/ujjwal/projects/cci/data/as-builts/demo/pdf_images/CH1361BA_71LAB_Elevation_As_Built/site_info_panel_CH1361BA_71LAB_Elevation_As_Built_page-1.png'
    detect_table(img_file, text_size_percent=2, line_distance_percent=2, debug_mode=True)
