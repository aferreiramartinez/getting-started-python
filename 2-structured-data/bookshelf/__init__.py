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
import http.client, urllib.request, urllib.parse, urllib.error, base64, json, time
from pprint import pprint
from flask import current_app, Flask, redirect, url_for

def retrieve_sp_500_tickers():
    # load json with S&P 500 companies#
    data = json.load(open('./bookshelf/SP500.json','r'))
    tickers = []
    for company in data["SP500"]:
        tickers.append(company["Ticker"])
    return tickers

def get_message_headers(iIndex):

    # Request headers
    #ant1bball
    #'Ocp-Apim-Subscription-Key': '4137e8075de949d49d3cd644cd81b884'
    #leo.fercom1
    #'Ocp-Apim-Subscription-Key': '999bcdb357474cf8bb325e75ef19666d'

    pool_api_keys=['4137e8075de949d49d3cd644cd81b884', '999bcdb357474cf8bb325e75ef19666d']
    headers = {
        # Request headers
        'Ocp-Apim-Subscription-Key': pool_api_keys[iIndex]
    }
    return headers

def get_requesst_params(iFormType,iFilingOrder):
    params = urllib.parse.urlencode({
        # Request parameters
        'formType': iFormType,
        'filingOrder': iFilingOrder,
    })

def get_balance_sheet(iModel):
    #Prepare request
    get_message_headers(0)
    get_requesst_params('10-K','0')
    
    #tickers = retrieve_sp_500_tickers()
    dataType = {"Balance Sheet":{}}
    dataYear = {"2016-10K":{}}
    errorList = []
    companies = json.load(open('./bookshelf/SP500.json','r'))
    conn = http.client.HTTPSConnection('services.last10k.com')
    for company in companies["SP500"]:
        conn.request("GET", "/v1/company/"+company["Ticker"]+"/balancesheet?%s" % params, "{body}", headers)
        response = conn.getresponse()
        print(str(response.status) + company["Ticker"])
        if (response.status != 200):
            conn.close()
            conn = http.client.HTTPSConnection('services.last10k.com')
            print("second attempt "+str(response.status))
        if (response.status == 200):
            data = response.read().decode('utf-8')
            jsonData = json.loads(data)
            del jsonData['Content'] #deletes HTML content and URL to save space
            del jsonData['Url']
            dataType["Balance Sheet"]=jsonData
            dataYear["2016-10K"]=dataType
            company.update(dataYear)
            add_to_mongo(iModel,company)
            time.sleep(12.01)
        else:
            print("error, skip "+str(company["Ticker"]))
            errorList.append(company["Ticker"])
            time.sleep(12)
            continue
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
        get_balance_sheet(model)

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
    #del iData['Content']
    from . import model_mongodb
    iModel.create(iData)
    print("added to mongo")
