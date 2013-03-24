
import json

class AutoModellingException(Exception): pass

def timestamp():
    import time
    return str(int(time.time()))

def make_model(pubname, pubtitle, amount_field, date_field, fields):
    currency = "GBP"
    ts = timestamp()
    dataset_name = pubname + "-" + ts
    description = pubtitle
    label = pubtitle + "-" + ts

    dataset = {
        "description": description,
        "temporal_granularity": "day", 
        "schema_version": "2011-12-07", 
        "name": dataset_name,
        "category": "other",
        "currency": currency,
        "label": label
        }

    def dimension(name, column_id, dim_type, data_type):
        assert dim_type in ["date", "attribute", "measure"]
        assert data_type in ["float", "string", "date"]
        return (name, {
            "default_value":  "", 
            "description": name.title(), 
            "column": column_id, 
            "label": name.title(), 
            "datatype": data_type, 
            "type": dim_type
            })

    def compound_dimension(name, column_id):
        return (name, 
                {
                "attributes": {
                    "name": {
                        "datatype": "id",
                        "column": column_id,
                        "default_value": "missing"
                        },
                    "label": {
                        "column": column_id,
                        "datatype": "string",
                        "default_value": "Missing"
                        }
                    },
                "type": "compound",
                "description": column_id,
                "label": column_id
                })

    def as_os_type(t):
        if t in ["integer", "number"]:
            return "float"
        else:
            return "string"

    dimensions_list = [
        dimension("amount", amount_field["label"], "measure", "float"),
        dimension("time", date_field["label"], "date", "date"),
        ]

    for f in fields:
        #dim = dimension(f["id"], f["label"], 
        #                "attribute", as_os_type(f["type"]))
        dim = compound_dimension(f["id"], f["label"])
        dimensions_list.append(dim)

    dimensions_list.append(("unique_rowid",
                            {"default_value": "", 
                             "description": "Nonce Row ID", 
                             "column": "unique_rowid", 
                             "label": "RowID", 
                             "datatype": "string", 
                             "key": True, 
                             "type": "attribute"
                             }))
    
    return {
        "dataset": dataset,
        "mapping": dict(dimensions_list)
        }

def infer_model_callback(body):
    args = json.loads(body)

    fields = args["schema"]["fields"]

    # we need a date
    # an amount
    types = [ field["type"] for field in fields ]

    numbers = filter(lambda s: s == "number", types)
    if len(numbers) != 1:
        raise AutoModellingException("Found more than one numerical field")

    dates = filter(lambda s: s == "date", types)
    if len(dates) != 1:
        raise AutoModellingException("Found more than one date field")

    other_fields = filter(lambda s: s not in ["number", "date"], types)

    model = make_model(
        args["publisher_name"]["code"],
        args["publisher_name"]["title"],
        [ f for f in fields if f["type"] == "number"][0], 
        [ f for f in fields if f["type"] == "date" ][0], 
        [ f for f in fields if f["type"] not in ["number", "date"]]
        )

    args["model"] = model
    return [ ("modelled", args) ]

def infer_spender_callback(body):
    args = json.loads(body)

    model = args["model"]

    def get_col(dimension):
        if "column" in dimension:
            return dimension["column"]
        else:
            return dimension["attributes"]["label"]["column"]

    cols = dict([ (get_col(v), k) for k, v in model["mapping"].iteritems() ])

    if "Body Name" in cols:
        orig_key = cols["Body Name"]
        args["model"]["mapping"]["from"] = model["mapping"][orig_key]
        del args["model"]["mapping"][orig_key]

    return [ ("modelled_from", args) ]

def infer_supplier_callback(body):
    args = json.loads(body)

    model = args["model"]

    def get_col(dimension):
        if "column" in dimension:
            return dimension["column"]
        else:
            return dimension["attributes"]["label"]["column"]

    cols = dict([ (get_col(v), k) for k, v in model["mapping"].iteritems() ])

    if "Supplier Name" in cols:
        orig_key = cols["Supplier Name"]
        args["model"]["mapping"]["to"] = model["mapping"][orig_key]
        del args["model"]["mapping"][orig_key]

    return [ ("import", args) ]

def infer_date_range_callback(body):
    args = json.loads(body)

    date_formats = args["date_format"]
    date_col = model["mapping"]["time"]["column"]
    fmt = date_formats[date_col]

    import requests
    r = requests.get(url, stream=True)
    import unicodecsv

    rd = unicodecsv.DictCursor(r.iter_content(chunk_size=4096))
    dates = [ row[date_col] for row in rd ]
    

def validate_model_callback(body):
    import tempfile
    import os
    import subprocess

    args = json.loads(body)
    model = args["model"]

    try:
        fn = tempfile.mktemp()

        with file(fn, 'w') as f:
            json.dump(model, f, indent=2)

        cmd = [
            "osvalidate",
            "model",
            fn
            ]

        output = subprocess.check_output(cmd)

        args["model_validated"] = True

        return [ ( "try_import", args ) ]
        
    finally:
        os.unlink(fn)
    
