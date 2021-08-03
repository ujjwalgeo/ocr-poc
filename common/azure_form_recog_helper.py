import requests
import time
import json
from bson import ObjectId
from common.config import AZURE_FORM_RECOGNIZER_APIKEY, AZURE_FORM_RECOGNIZER_ENDPOINT
import os


def run_layout_restapi(input_file, project_name, page_number, category="as-built"):

    if not os.path.exists(input_file):
        raise Exception("Could not access %s" % input_file)

    endpoint = AZURE_FORM_RECOGNIZER_ENDPOINT
    subscription_key = AZURE_FORM_RECOGNIZER_APIKEY

    url = endpoint + "/formrecognizer/v2.1/layout/analyze"
    headers = {"Content-Type": "application/octet-stream", "Ocp-Apim-Subscription-Key": subscription_key}
    with open(input_file, 'rb') as f:
        data = f.read()

    resp = requests.post(url, data=data, headers=headers)
    if resp.status_code == 400:
        raise Exception(resp.text)

    operation_location = resp.headers['Operation-Location']
    while True:
        resp = requests.get(operation_location, headers=headers)
        obj = json.loads(resp.text)
        status = obj['status']
        if status in ['failed', 'succeeded']:
            break
        # print(resp.text)
        time.sleep(2)

    obj = json.loads(resp.text)
    analysis_id = ObjectId()
    analysis_doc = {
        "_id": analysis_id,
        "source_file": input_file,
        "project_id": project_name,
        "category": category,
        "analysis": obj
    }

    readResults = obj['analyzeResult']['readResults']
    lines = []
    for readResultCount in range(len(readResults)):
        readResult = readResults[readResultCount]
        page = readResult["page"]
        angle = readResult["angle"]
        width = readResult["width"]
        height = readResult["height"]
        unit = readResult["unit"]

        for line in readResult["lines"]:
            doc = line
            doc["page"] = page_number
            doc["angle"] = angle
            doc["width"] = width
            doc["height"] = height
            doc["unit"] = unit
            doc["read_result_id"] = readResultCount
            doc["analysis_id"] = analysis_id
            lines.append(doc)

    pageResults = obj['analyzeResult']['pageResults']
    tables = []
    for pageResult in pageResults:
        if len(pageResult['tables']):
            tables.append(pageResult['tables'])

    return analysis_doc, lines, tables


if __name__ == '__main__':
    input_file = "./asbuilts/pdf_images/CH1424BA_81LAB_Elevation_As_Built/CH1424BA_81LAB_Elevation_As_Built_page-1.png"
    project_name = "Test"
    page_number = 1

    doc, lines, tables = run_layout_restapi(input_file, project_name, page_number)
    for l in lines:
        print (l['text'])

    for t in tables:
        print(t)


