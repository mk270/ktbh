import json
import subprocess
import urllib
import tempfile

base_url = 'http://localhost:1237/hack_csv/'
loadds = "/home/mk270/bin/loadds"

def import_ds_callback(body):
    args = json.loads(body)

    model = args["model"]
    url = args["url"]

    dataset_name = model["dataset"]["name"]

    filename = tempfile.mktemp()
    with file(filename, 'w') as f:
        f.write(json.dumps(model, indent=2))

    model_url = 'file://' + filename

    date_col = model["mapping"]["time"]["column"]

    url_args = {
        'url': url,
        'uniq_col': 'unique_rowid',
        'date_col': date_col,
        'date_fmt': args["date_format"][date_col]
        }
    csv_url = base_url + '?' + urllib.urlencode(url_args)

    loadds_args = [loadds,
                   dataset_name,
                   model_url,
                   csv_url]
    print loadds_args
    subprocess.check_call(loadds_args)

    os.unlink(filename)

    import sys
    sys.exit(1)

    return []

    
