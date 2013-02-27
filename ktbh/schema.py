import unicodecsv
from cStringIO import StringIO
import messytables
import itertools
import slugify
import jsontableschema

from messytables.types import *

classes = {
    StringType: "string",
    IntegerType: "integer",
    FloatType: "number",
    DecimalType: "number",
    DateType: "date",
    DateUtilType: "date"
}

def censor(dialect):
    tmp = dict(dialect)
    censored = [
        "doublequote",
        "lineterminator",
        "skipinitialspace"
        ]
    
    [ tmp.pop(i) for i in censored ]
    return tmp

def infer_schema(data, dialect):
    f = StringIO(data)

    d = unicodecsv.reader(f, **dialect)
    field_names = d.next()
    f.seek(0)

    dialect = censor(dialect)
    
    t = messytables.CSVTableSet(f, **dialect).tables[0]
    sample = itertools.islice(t, 0, 9)
    types = messytables.type_guess(sample)

    json_table_schema_types = [
        classes.get(t.__class__, "any") for t in types
        ]

    slugs = [ slugify.slugify(i) for i in field_names ]

    metadata = zip(slugs, field_names, json_table_schema_types)

    sch = jsontableschema.JSONTableSchema()
    for field_id, label, field_type in metadata:
        sch.add_field(field_id=field_id,
                      label=label,
                      field_type=field_type)
    return sch.as_json()
