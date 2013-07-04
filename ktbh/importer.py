
#  KTBH, a framework for pipelined data handling, by Martin Keegan
#
#  Copyright (C) 2012  Martin Keegan
#
#  This programme is free software; you may redistribute and/or modify
#  it under the terms of the GNU Affero General Public License

import json
import subprocess
import urllib
import tempfile
import requests
import subprocess
import os
import ConfigParser

def get_config():
    config_file = os.path.join(os.path.dirname(__file__), 
                               os.path.pardir,
                               "etc", "ktbh.conf")
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    return config

def setup_import(model, url, date_formats):
    dataset_name = model["dataset"]["name"]

    cfg = get_config()
    base_url = cfg.get("proxy", "url")

    filename = tempfile.mktemp()
    with file(filename, 'w') as f:
        f.write(json.dumps(model, indent=2))

    date_col = model["mapping"]["time"]["column"]

    url_args = {
        'url': url,
        'uniq_col': 'unique_rowid',
        'date_col': date_col,
        'date_fmt': date_formats[date_col]
        }
    csv_url = base_url + '?' + urllib.urlencode(url_args)

    return (dataset_name, filename, csv_url)

def import_ds_callback(body):
    args = json.loads(body)

    cfg = get_config()
    loadds = cfg.get("openspending", "load_dataset_script")

    model = args["model"]
    url = args["url"]
    date_formats = args["date_format"]

    dataset_name, model_filename, csv_url = setup_import(model, 
                                                         url, date_formats)

    model_url = 'file://' + model_filename
    loadds_args = [loadds,
                   dataset_name,
                   model_url,
                   csv_url]
    print loadds_args
    subprocess.check_call(loadds_args)

    os.unlink(model_filename)

    return []

def validate_csv_callback(body):
    args = json.loads(body)

    assert args["model_validated"]
    model = args["model"]
    url = args["url"]
    date_formats = args["date_format"]

    dataset_name, model_filename, csv_url = setup_import(model, 
                                                         url, date_formats)

    csv_file = tempfile.mktemp()
    with file(csv_file, 'w') as f:
        r = requests.get(csv_url)
        f.write(r.content)

    validate_args = ["osvalidate",
                     "data",
                     "--model",
                     model_filename,
                     csv_file]

    print validate_args
    subprocess.check_call(validate_args)

    os.unlink(model_filename)
    os.unlink(csv_file)

    args["csv_validated"] = True
    return [ ("ready", args) ]
