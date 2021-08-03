import os
import cv2
import time
import PyPDF2
import pdf2image
import numpy as np
from common.config import RED_HUE_RANGE1, RED_HUE_RANGE2, POPPLER_INSTALL_PATH, PDF_2_IMAGE_DPI

"""
extract pages with with qpdf
qpdf ~/projects/cci/data/as-builts/demo/CH1362BA_21LAB_Elevation_As_Built.pdf --pages . 2 -- ~/projects/cci/data/as-builts/demo/pdf_pages/CH1362BA_21LAB_Elevation_As_Built/pg2.pdf
"""


def create_red_image(image_file, output_file, overwrite=False):
    # https://stackoverflow.com/questions/30331944/finding-red-color-in-image-using-python-opencv
    img = cv2.imread(image_file)
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

    lower_val = np.array([RED_HUE_RANGE1[0], 50, 50])
    upper_val = np.array([RED_HUE_RANGE1[1], 255, 255])
    mask1 = cv2.inRange(hsv, lower_val, upper_val)

    lower_val = np.array([RED_HUE_RANGE2[0], 50, 50])
    upper_val = np.array([RED_HUE_RANGE2[1], 255, 255])
    mask2 = cv2.inRange(hsv, lower_val, upper_val)

    mask = mask1 + mask2
    out = img.copy()
    out[np.where(mask == 0)] = 0

    if overwrite and os.path.exists(output_file):
        os.remove(output_file)

    if not os.path.exists(output_file):
        cv2.imwrite(output_file, out)
        cnt = 0
        while True:
            if os.path.isfile(output_file):
                break
            cnt += 1
            time.sleep(.1)
            if cnt > 100:
                raise Exception("Error while saving red file %s" % output_file)

    # return width and height here since we don't want to use opencv again later just to retrieve image dims
    width, height = img.shape[1], img.shape[2]
    has_red_pixels = np.asscalar((out.min() > 0) or (out.max() > 0))
    return width, height, has_red_pixels


class PDFDocument(object):

    """
    PDFDocument encapsulates an as-built pdf file and supports extraction of pages,
    conversion of single page to image, extraction of PDF properties, annotations and raw text
    """

    def __init__(self, file_path, output_dir):
        if not os.path.exists(file_path):
            raise Exception('Could not access %s' % file_path)

        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
            except Exception as ex:
                raise ex

        self.file_path = file_path
        self.output_dir = output_dir
        self.pages = []

    def extract_pages(self, pages=None, overwrite=True):
        from PIL import Image

        if pages is None:
            pages = []

        pdf_in = PyPDF2.PdfFileReader(open(self.file_path, 'rb'), strict=False)
        pdf_in_name = os.path.splitext(os.path.basename(self.file_path))[0]

        pdf_out_dir = self.output_dir
        pdf_pages_out_dir = os.path.join(pdf_out_dir, 'pdf_pages', pdf_in_name)
        pdf_images_out_dir = os.path.join(pdf_out_dir, 'pdf_images', pdf_in_name)

        if not os.path.exists(pdf_pages_out_dir):
            os.makedirs(pdf_pages_out_dir)

        if not os.path.exists(pdf_images_out_dir):
            os.makedirs(pdf_images_out_dir)

        extracted = []
        for i in range(1, pdf_in.numPages + 1):

            if len(pages) and (i not in pages):
                continue

            pdf_out_name = "%s_page-%d.pdf" % (pdf_in_name, i)
            img_out_name = "%s_page-%d.png" % (pdf_in_name, i)
            img_out_file = os.path.join(pdf_images_out_dir, img_out_name)
            pdf_out_file = os.path.join(pdf_pages_out_dir, pdf_out_name)

            if overwrite and os.path.exists(pdf_out_file):
                os.remove(pdf_out_file)

            if not os.path.exists(pdf_out_file):
                pdf_page = pdf_in.getPage(i-1) #getPage is 0 index
                pdf_out = PyPDF2.PdfFileWriter()
                pdf_out.addPage(pdf_page)
                with open(pdf_out_file, 'wb') as pdf_of:
                    pdf_out.write(pdf_of)

            if overwrite and os.path.exists(img_out_file):
                os.remove(img_out_file)

            if not os.path.exists(img_out_file):
                images = pdf2image.convert_from_path(pdf_out_file, dpi=PDF_2_IMAGE_DPI,
                                                     strict=False, thread_count=4,
                                                     poppler_path=POPPLER_INSTALL_PATH)
                img = images[0]
                img.thumbnail((10000, 10000), Image.ANTIALIAS) # max image size for azure vision
                img.save(img_out_file)

            red_image_path = os.path.join(os.path.dirname(img_out_file), "red_%s" % os.path.basename(img_out_file))
            if overwrite and os.path.exists(red_image_path):
                os.remove(red_image_path)

            img_w, img_h, has_red_pixels = create_red_image(img_out_file, red_image_path, overwrite)

            text = pdf_page.extractText()
            annotation = []
            if r'/Annots' in pdf_page:
                for annot in pdf_page['/Annots']:
                    ann = annot.getObject()
                    contents = ""
                    ann_style = ""
                    if r'/Contents' in ann:
                        contents = ann[r'/Contents']
                    if r'/DS' in ann:
                        ann_style = ann[r'/DS']  # font: Helvetica,sans-serif 12.0pt; text-align:left; color:#E52237

                    if len(contents) or len(ann_style):
                        annotation.append({'content': contents, 'style': ann_style})

            extracted.append({
                "pdf": pdf_out_file,
                "image": img_out_file,
                "image_width": img_w,
                "image_height": img_h,
                "red_image": red_image_path,
                "has_red_pixels": has_red_pixels,
                "page": i,
                "raw_text": text,
                "annotations": annotation
            })

            self.pages.append(i)

        return extracted


if __name__ == '__main__':

    fl = '/Users/ujjwal/projects/cci/data/as-builts/chicago_test/CH1424BA_81LAB_Elevation_As_Built.pdf'
    pdf_doc = PDFDocument(file_path=fl, output_dir='./')
    extracted_pages = pdf_doc.extract_pages(pages=[1, 2])
    print(extracted_pages)
