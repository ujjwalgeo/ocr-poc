from eve import Eve
from flask import jsonify, send_file, Response
from common.api.schema.azure_analysis import resource_azure_analysis
from common.api.schema.asbuilt import resource_asbuilt
from bson import ObjectId
import os


DOMAIN = {
    'azure_analysis': resource_azure_analysis,
    'asbuilts': resource_asbuilt
}

dev_settings = {
    'MONGO_HOST': 'localhost',
    'MONGO_PORT': 27017,
    'MONGO_DBNAME': 'new_batch_demo',
    'DOMAIN': DOMAIN,
    'X_DOMAINS': '*',
    'X_HEADERS': 'Authorization'
}

app = Eve(settings=dev_settings)


@app.route('/projects', methods=['GET'])
def get_projects():
    db = app.data.driver.db
    coll = db['asbuilts']
    results = coll.find({}, {"project": 1})
    projects = [ r["project"] for r in results ]
    projects = list(set(projects))
    response = jsonify({"projects": projects})
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response


@app.route('/asbuilt-pdf/<asbuilt_id>', methods=['GET'])
def get_asbuilt_pdf(asbuilt_id):
    db = app.data.driver.db
    coll = db['asbuilts']
    oid = ObjectId(asbuilt_id)
    results = coll.find({'_id': oid}, {"source_file": 1})
    pdf_file_path = None
    for result in results:
        pdf_file_path = result['source_file']
        break
    if pdf_file_path:
        response = send_file(pdf_file_path, mimetype='application/pdf', download_name=os.path.basename(pdf_file_path))
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response

    return jsonify()

app.run()
