import requests
import time
import json
import os
from bson import ObjectId
from common.config import AZURE_COGNITIVE_SERVICES_APIKEY, AZURE_COGNITIVE_SERVICES_ENDPOINT

# https://southcentralus.dev.cognitive.microsoft.com/docs/services/Custom_Vision_Training_3.3/operations/5eb0bcc6548b571998fddeb9

def create_project():
    endpoint = 'https://cci-coloocr-customvision-poc-cv.cognitiveservices.azure.com'


def download_image(url, id):
    ofile_name = os.path.join(output_dir, '%s.png' % id)
    resp = requests.get(url)
    with open(ofile_name, 'wb') as imgf:
        imgf.write(resp.content)


def export_images(project_dump_json_file):
    with open(project_dump_json_file, 'r') as pdf:
        data = pdf.read()
    images = json.loads(data)
    for image in images:
        id = image['id']
        regions = image['regions']
        resizedImageUri = image['resizedImageUri']
        time.sleep(1)
        download_image(resizedImageUri, id)
        print(resizedImageUri)


def export_project():
    project_id = '35d32dd8-47b9-48fe-b176-ecd0fca63ed4'
    endpoint = r'https://westus2.api.cognitive.microsoft.com'
    subscription_key = r'f7966a1ec75442bc8bec0d824c47eeff'
    iteration_id = r'a9117442-10e8-40e5-851b-ac3cafc3d639'
    url = 'https://{endpoint}/customvision/v3.3/Training/projects/{projectId}/images/tagged[?iterationId][&tagIds][&orderBy][&take][&skip]'
    url = endpoint + "/customvision/v3.3/Training/projects/" + project_id + "/images/tagged?skip=0&take=200"
    headers = {"Training-Key": subscription_key}
    resp = requests.get(url, headers=headers)
    obj = json.loads(resp.text)
    print(resp.text)


projct_dump_json_file = r'./custom_vision/custom_vision_personal_project.json'
output_dir = r'./custom_vision/images/'

# export_project()
export_images(projct_dump_json_file)

