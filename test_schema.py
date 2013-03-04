
import ktbh.schema
import csvddf
import unicodecsv
import json

def test_inference():
    data = file('data/generic.csv').read()
    dialect = csvddf.CSVDDF(dialect=unicodecsv.Sniffer().sniff(data)).as_dict()['dialect']
    sch = ktbh.schema.infer_schema(data, dialect)
    sch = json.loads(sch)
    fields = sch['fields']
    
    types = dict([ (i["label"], i["type"]) for i in fields ])
    assert types["Expenditure Code"] == "integer"

if __name__ == '__main__':
    test_inference()
