
import json

class AutoModellingException(Exception): pass

def make_model(amount_field, date_field, fields):
    currency = "GBP"
    dataset_name = "new-dataset"
    description = "Dataset description"
    label = "Dataset label"

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
                        "column": column_id
                        },
                    "label": {
                        "column": column_id,
                        "datatype": "string"
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
        [ f for f in fields if f["type"] == "number"][0], 
        [ f for f in fields if f["type"] == "date" ][0], 
        [ f for f in fields if f["type"] not in ["number", "date"]]
        )

    args["model"] = model
    return [ ("import", args) ]
