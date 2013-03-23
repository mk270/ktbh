import json
import subprocess

import tempfile

def import_ds_callback(body):
    args = json.loads(body)

    model = args["model"]
    url = args["url"]

    dataset_name = model["dataset"]["name"]

    filename = tempfile.mktemp()
    with file(filename, 'w') as f:
        f.write(json.dumps(model, indent=2))

    model_url = 'file://' + filename
    uniqued_url = 'http://localhost:1237/add_uniq/unique_rowid/' + url

    loadds_args = ["/home/mk270/bin/loadds",
                   dataset_name,
                   model_url,
                   uniqued_url]
    print loadds_args
    subprocess.check_call(loadds_args)

    os.unlink(filename)

    import sys
    sys.exit(1)

    return []

    
