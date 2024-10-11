from dataclasses import dataclass, asdict

import pyarrow as pa


def _data_type_from_arrow(_t):
    if isinstance(_t, pa.lib.TimestampType):
        if getattr(_t, "tz", None) is None:
            return "timestamp_ntz"
        else:
            return "timestamp"
    elif _t not in ARROW_TO_DELTA_TYPE:
        err = f"Cannot handle arrow type '{_t}', type={type(_t)}"
        raise TypeError(err)
    return ARROW_TO_DELTA_TYPE[_t]

ARROW_TO_DELTA_TYPE = {
    pa.bool_(): "boolean",
    pa.int8(): "byte",
    pa.int16(): "short",
    pa.int32(): "integer",
    pa.int64(): "long",
    pa.uint8(): "byte",
    pa.uint16(): "short",
    pa.uint32(): "integer",
    pa.uint64(): "long",
    pa.date32(): "date",
    pa.date64(): "date",
    pa.timestamp("us"): "timestamp",
    pa.float32(): "float",
    pa.float64(): "double",
    pa.binary(): "binary",
    pa.string(): "string",
    pa.utf8(): "string",
    pa.large_binary(): "binary",
    pa.large_string(): "string",
    pa.large_utf8(): "string",
}

DELTA_TO_ARROW_TYPE = {
    "boolean": pa.bool_(),
    "byte": pa.int8(),
    "short": pa.int16(),
    "integer": pa.int32(),
    "long": pa.int64(),
    "date": pa.date64(),
    "timestamp": pa.timestamp("us", tz="utc"),
    "timestamp_ntz": pa.timestamp("us"),
    "float": pa.float64(),
    "double": pa.float64(),
    "binary": pa.binary(),
    "string": pa.string(),
}

@dataclass
class SchemaField:
    name: str
    type: str
    nullable: bool
    metadata: dict

@dataclass
class Schema:
    fields: list[dict]
    type: str = "struct"

    @classmethod
    def from_pyarrow_schema(cls, schema: pa.Schema) -> "Schema":
        """Create a Schema object from a pyarrow.Schema object

        Args:
            schema (pa.Schema): A pyarrow schema object

        Returns:
            Schema: A new schema object
        """
        fields = [
            asdict(SchemaField(f.name, _data_type_from_arrow(f.type), f.nullable, f.metadata or {}))
            for f in schema
        ]
        return cls(fields=fields)

    def to_pyarrow_schema(self) -> pa.Schema:
        """Convert the Schema object to a pyarrow.Schema object.

        Returns:
            pa.Schema
        """
        pairs = [(f["name"], DELTA_TO_ARROW_TYPE[f["type"]]) for f in self.fields]
        return pa.schema(pairs)

    def merge(self, other) -> "Schema":
        """Merge two schemas

        Args:
            other (Schema): Another schema object

        Returns:
            Schema: A new schema object
        """
        a = self.to_pyarrow_schema()
        b = other.to_pyarrow_schema()
        merged_schema = pa.unify_schemas([a, b])
        return type(self).from_pyarrow_schema(merged_schema)

    def __eq__(self, o):
        a_fields = sorted(self.fields, key=lambda x: x["name"])
        b_fields = sorted(o.fields, key=lambda x: x["name"])
        return a_fields == b_fields
