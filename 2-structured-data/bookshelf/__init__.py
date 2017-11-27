# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import http.client, urllib.request, urllib.parse, urllib.error, base64, json
from pprint import pprint
from flask import current_app, Flask, redirect, url_for

def get_fundamentals(iModel):
    headers = {
        # Request headers
        'Ocp-Apim-Subscription-Key': '4137e8075de949d49d3cd644cd81b884',
    }

    params = urllib.parse.urlencode({
        # Request parameters
        'formType': '10-K',
        'filingOrder': '0',
    })

    conn = http.client.HTTPSConnection('services.last10k.com')
    conn.request("GET", "/v1/company/TTWO/balancesheet?%s" % params, "{body}", headers)
    response = conn.getresponse()
    print(response.status)
    data = response.read().decode('utf-8')
    jsonData = json.loads(data)
    add_to_mongo(iModel,jsonData)
    conn.close()

def create_app(config, debug=False, testing=False, config_overrides=None):
    app = Flask(__name__)
    app.config.from_object(config)

    app.debug = debug
    app.testing = testing

    if config_overrides:
        app.config.update(config_overrides)

    # Configure logging
    if not app.testing:
        logging.basicConfig(level=logging.INFO)

    # Setup the data model.
    with app.app_context():
        model = get_model()
        model.init_app(app)
        get_fundamentals(model)

    # Register the Bookshelf CRUD blueprint.
    from .crud import crud

    # a blueprint is a set of operations which can be registered on an application
    # Flask associates view functions with blueprints when dispatching requests and
    # generating URLs from one endpoint to another
    app.register_blueprint(crud, url_prefix='/books')

    # Add a default root route.
    @app.route("/")
    def index():
        return redirect(url_for('crud.list'))

    # Add an error handler. This is useful for debugging the live application,
    # however, you should disable the output of the exception for production
    # applications.
    @app.errorhandler(500)
    def server_error(e):
        return """
        An internal error occurred: <pre>{}</pre>
        See logs for full stacktrace.
        """.format(e), 500



    return app


def get_model():
    model_backend = current_app.config['DATA_BACKEND']
    if  model_backend == 'mongodb':
        from . import model_mongodb
        model = model_mongodb
    else:
        raise ValueError(
            "No appropriate databackend configured. "
            "Please specify datastore, cloudsql, or mongodb")

    return model

def add_to_mongo(iModel,iData):
    #data = iData.to_dict(flat=True)
    del iData['Content']
    from . import model_mongodb
    iModel.create(iData)
    print("added to mongo")
