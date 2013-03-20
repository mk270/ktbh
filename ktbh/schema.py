import unicodecsv
from cStringIO import StringIO
import messytables
import itertools
import slugify
import jsontableschema

from messytables.types import *
from messytables_jts import rowset_as_schema

def censor(dialect):
    tmp = dict(dialect)
    censored = [
        "doublequote",
        "lineterminator",
        "skipinitialspace"
        ]
    
    [ tmp.pop(i) for i in censored ]
    return tmp

def sabotage(d):
    [ d.__setitem__(k, d[k].encode('utf-8')) for k in d
      if isinstance(d[k], unicode) ]

def get_type_of_column(col):
    try:
        return rowset_as_schema(col)
    except:
        return "any"

def infer_schema(data, _dialect):
    f = StringIO(data)

    sabotage(_dialect)
    d = unicodecsv.reader(f, dialect=None, **_dialect)
        
    field_names = d.next()
    f.seek(0)

    dialect = censor(_dialect)
    
    t = messytables.CSVTableSet(f, **dialect).tables[0]
    sample = itertools.islice(t, 0, 9)
    types = messytables.type_guess(sample)

    json_table_schema_types = map(get_type_of_column(t),
                                  types)

    slugs = [ slugify.slugify(i) for i in field_names ]

    metadata = zip(slugs, field_names, json_table_schema_types)

    sch = jsontableschema.JSONTableSchema()
    for field_id, label, field_type in metadata:
        sch.add_field(field_id=field_id,
                      label=label,
                      field_type=field_type)
    return sch.as_json()
