import json
import subprocess
import urllib
import tempfile
import requests
import subprocess
import os

base_url = 'http://localhost:1237/hack_csv/'
loadds = "/home/mk270/bin/loadds"

def setup_import(model, url, date_formats):
    dataset_name = model["dataset"]["name"]

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
