import json
import datetime
from enum import Enum
from uuid import uuid4
from collections import defaultdict
from dataclasses import dataclass, asdict, field, replace
from collections.abc import ValuesView
from typing import IO, Iterable, Sequence

import pyarrow as pa

from xdlake import utils


CLIENT_VERSION = "xdlake-0.0.0"


class WriteMode(Enum):
    append = "Append"
    overwrite = "Overwrite"
    error = "Error"
    ignore = "Ignore"


class Type(Enum):
    commitInfo = "commitInfo"
    metaData = "metaData"
    protocol = "protocol"
    add = "add"
    remove = "remove"

class _JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        elif isinstance(o, bytes):
            return repr(o.decode("raw_unicode_escape", "backslashreplace"))
        return super().default(o)

class _DeltaLogAction:
    def asdict(self):
        return asdict(self)

    def json(self):
        return json.dumps(self.asdict(), cls=_JSONEncoder)

    def replace(self, **kwargs):
        return replace(self, **kwargs)

    def to_action_dict(self) -> dict:
        raise NotADirectoryError()

@dataclass
class Protocol(_DeltaLogAction):
    minReaderVersion: int = 1
    minWriterVersion: int = 2

    def to_action_dict(self) -> dict:
        return {Type.protocol.name: self.asdict()}

@dataclass
class TableFormat(_DeltaLogAction):
    provider: str = "parquet"
    options: dict = field(default_factory=lambda: dict())

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
    "float": pa.float64(),
    "double": pa.float64(),
    "binary": pa.binary(),
    "string": pa.string(),
}

@dataclass
class SchemaField(_DeltaLogAction):
    name: str
    type: str
    nullable: bool
    metadata: dict

@dataclass
class Schema(_DeltaLogAction):
    fields: list[dict]
    type: str = "struct"

    @classmethod
    def from_pyarrow_schema(cls, schema: pa.Schema) -> "Schema":
        fields = [
            SchemaField(f.name, _data_type_from_arrow(f.type), f.nullable, f.metadata or {}).asdict()
            for f in schema
        ]
        return cls(fields=fields)

    def to_pyarrow_schema(self):
        pairs = [(f["name"], DELTA_TO_ARROW_TYPE[f["type"]]) for f in self.fields]
        return pa.schema(pairs)

    def merge(self, other) -> "Schema":
        a = self.to_pyarrow_schema()
        b = other.to_pyarrow_schema()
        merged_schema = pa.unify_schemas([a, b])
        return type(self).from_pyarrow_schema(merged_schema)

    def __eq__(self, o):
        a_fields = sorted(self.fields, key=lambda x: x["name"])
        b_fields = sorted(o.fields, key=lambda x: x["name"])
        return a_fields == b_fields

@dataclass
class TableMetadata(_DeltaLogAction):
    schemaString: str
    createdTime: int = field(default_factory=lambda: utils.timestamp())
    id: str = field(default_factory=lambda: f"{uuid4()}")
    name: str | None = None
    description: str | None = None
    format: dict = field(default_factory=lambda: TableFormat().asdict())
    partitionColumns: list[str] = field(default_factory=lambda: list())
    configuration: dict = field(default_factory=lambda: dict())

    def to_action_dict(self) -> dict:
        return {Type.metaData.name: self.asdict()}

    @property
    def schema(self) -> Schema:
        return Schema(**json.loads(self.schemaString))

class TableOperationParm:
    METADATA = "metadata"
    PROTOCOL = "protocol"
    LOCATION = "location"
    MODE = "mode"
    PARTITION_BY = "partitionBy"

class TableCommitOperation:
    CREATE = "CREATE TABLE"
    WRITE = "WRITE"
    DELETE = "DELETE"

@dataclass
class TableCommit(_DeltaLogAction):
    timestamp: int
    operationParameters: dict
    operationMetrics: dict | None = field(default_factory=dict)
    operation: str = TableCommitOperation.CREATE
    clientVersion: str = CLIENT_VERSION
    readVersion: int | None = None

    def to_action_dict(self) -> dict:
        info = {k: v for k, v in self.asdict().items()
                if v}
        return {Type.commitInfo.name: info}

    @property
    def metadata(self):
        if TableOperationParm.METADATA in self.operationParameters:
            return json.loads(self.operationParameters[TableOperationParm.METADATA])
        else:
            return {}

    @classmethod
    def create_with_parms(cls, location: str, timestamp: int, metadata: TableMetadata, protocol: Protocol):
        op_parms = {
            TableOperationParm.METADATA: metadata.json(),
            TableOperationParm.PROTOCOL: protocol.json(),
            TableOperationParm.LOCATION: location,
            TableOperationParm.MODE: "ErrorIfExists",
        }
        return cls(timestamp=timestamp, operationParameters=op_parms, operation=TableCommitOperation.CREATE)

    @classmethod
    def write_with_parms(cls, timestamp: int, mode: str, partition_by: list | None = None):
        op_parms = {
            TableOperationParm.PARTITION_BY: partition_by or list(),
            TableOperationParm.MODE: mode,
        }
        return cls(timestamp=timestamp, operationParameters=op_parms, operation=TableCommitOperation.WRITE)

    @classmethod
    def delete_with_parms(
        cls,
        timestamp: int,
        predicate: str,
        read_version: int,
        operation_metrics: dict,
    ):
        op_parms = {
            "predicate": predicate,
        }
        return cls(
            timestamp=timestamp,
            operationParameters=op_parms,
            operationMetrics=operation_metrics,
            operation=TableCommitOperation.DELETE,
            readVersion=read_version,
        )

def _data_type_from_arrow(_t):
    if isinstance(_t, pa.lib.TimestampType):
        assert _t.unit == "us"
        return "timestamp"
    elif _t not in ARROW_TO_DELTA_TYPE:
        err = f"Cannot handle arrow type '{_t}', type={type(_t)}"
        raise TypeError(err)
    return ARROW_TO_DELTA_TYPE[_t]

@dataclass
class Statistics(_DeltaLogAction):
    numRecords: int
    minValues: dict
    maxValues: dict
    nullCount: dict

    @classmethod
    def from_parquet_file_metadata(cls, md: pa.parquet.FileMetaData) -> "Statistics":
        md = md.to_dict()
        min_values: dict = defaultdict(dict)
        max_values: dict = defaultdict(dict)
        nullcounts: dict = defaultdict(int)
        for rg_info in md["row_groups"]:
            for col_info in rg_info["columns"]:
                column = col_info["path_in_schema"]
                if col_info["statistics"]["has_min_max"]:
                    if not min_values.get(column):
                        min_values[column] = col_info["statistics"]["min"]
                    else:
                        min_values[column] = min(min_values[column], col_info["statistics"]["min"])
                    if not max_values.get(column):
                        max_values[column] = col_info["statistics"]["max"]
                    else:
                        max_values[column] = max(max_values[column], col_info["statistics"]["max"])
                nullcounts[column] += col_info["statistics"]["null_count"]
        return cls(numRecords=md["num_rows"],
                   minValues=dict(min_values),
                   maxValues=dict(max_values),
                   nullCount=dict(nullcounts))

@dataclass
class Add(_DeltaLogAction):
    path: str
    partitionValues: dict
    size: int
    modificationTime: int
    stats: str
    dataChange: bool | None = None
    tags: list | None = None
    deletionVector: dict | None = None
    baseRowId: str | None = None
    defaultRowCommitVersion: int | None = None
    clusteringProvider: str | None = None

    def to_action_dict(self) -> dict:
        return {Type.add.name: self.asdict()}

@dataclass
class Remove(_DeltaLogAction):
    path: str
    dataChange: bool
    deletionTimestamp: int
    extendedFileMetadata: bool
    partitionValues: dict
    size: int

    def to_action_dict(self) -> dict:
        return {Type.remove.name: self.asdict()}

class DeltaLogEntry:
    def __init__(self, actions: list | None = None):
        self.actions = actions or list()

    @classmethod
    def with_handle(cls, handle: IO):
        actions = [cls.load_action(line) for line in handle]
        return cls(actions)

    @staticmethod
    def load_action(obj: str | bytes | dict): 
        if isinstance(obj, (str, bytes)):
            info = json.loads(obj)
        else:
            info = obj
        action = Type[set(info.keys()).pop()]
        info = info[action.name]

        match action:
            case Type.commitInfo:
                if info["operation"] in (TableCommitOperation.CREATE, TableCommitOperation.WRITE, TableCommitOperation.DELETE):
                    return TableCommit(**info)
            case Type.metaData:
                return TableMetadata(**info)
            case Type.protocol:
                return Protocol(**info)
            case Type.add:
                return Add(**info)
            case Type.remove:
                return Remove(**info)
            case _:
                raise Exception(f"Cannot handle delta log action '{action}'")

    def write(self, handle):
        handle.write("\n".join([json.dumps(a.to_action_dict()) for a in self.actions]))

    def add_actions(self) -> list[Add]:
        return [a for a in self.actions
                if isinstance(a, Add)]

    def remove_actions(self) -> list[Remove]:
        return [a for a in self.actions
                if isinstance(a, Remove)]

    def partition_columns(self) -> list[str] | None:
        for a in self.actions:
            if isinstance(a, TableCommit):
                if a.operation == TableCommitOperation.WRITE:
                    partition_by = a.operationParameters.get("partitionBy")
                    # deltalake writes a string sometimes?
                    if isinstance(partition_by, str):
                        partition_by = json.loads(partition_by)
                    return partition_by
                elif a.operation == TableCommitOperation.CREATE:
                    return a.metadata["partitionColumns"]
        return None

    @classmethod
    def with_actions(cls, actions: list[_DeltaLogAction]) -> "DeltaLogEntry":
        entry = cls()
        entry.actions.extend(actions)
        return entry

    @classmethod
    def CreateTable(cls, path: str, schema: Schema, partition_by: list, add_actions: list[Add]) -> "DeltaLogEntry":
        protocol = Protocol()
        table_metadata = TableMetadata(schemaString=schema.json(), partitionColumns=partition_by)
        commit = TableCommit.create_with_parms(path, utils.timestamp(), table_metadata, protocol)
        return cls.with_actions([protocol, table_metadata, *add_actions, commit])

    @classmethod
    def AppendTable(cls, partition_by: list, add_actions: list[Add], schema: Schema | None = None) -> "DeltaLogEntry":
        commit = TableCommit.write_with_parms(utils.timestamp(), mode=WriteMode.append.value, partition_by=partition_by)
        actions = add_actions + [commit]
        if schema is not None:
            table_metadata = TableMetadata(schemaString=schema.json(), partitionColumns=partition_by)
            actions = [table_metadata] + actions
        return cls.with_actions(actions)

    @classmethod
    def OverwriteTable(
        cls,
        partition_by: list,
        existing_add_actions: Iterable[Add],
        add_actions: list[Add]
    ) -> "DeltaLogEntry":
        commit = TableCommit.write_with_parms(utils.timestamp(), mode=WriteMode.overwrite.value, partition_by=partition_by)
        remove_actions = generate_remove_acctions(existing_add_actions)
        return cls.with_actions([*remove_actions, *add_actions, commit])

    @classmethod
    def DeleteTable(
        cls,
        *,
        predicate: str,
        add_actions_to_remove: Sequence[Add] | ValuesView[Add],
        add_actions: Sequence[Add] | ValuesView[Add],
        read_version: int,
        num_copied_rows: int,
        num_deleted_rows: int,
    ) -> "DeltaLogEntry":
        operation_metrics = {
            "num_added_files": len(add_actions),
            "num_removed_files": len(add_actions_to_remove),
            "num_copied_rows": num_copied_rows,
            "num_deleted_rows": num_deleted_rows,
        }
        commit = TableCommit.delete_with_parms(utils.timestamp(), predicate, read_version, operation_metrics)
        remove_actions = generate_remove_acctions(add_actions_to_remove)
        return cls.with_actions([*remove_actions, *add_actions, commit])

class DeltaLog:
    def __init__(self):
        self.entries = dict()

    def __setitem__(self, key, val):
        self.entries[key] = val

    def __getitem__(self, key):
        return self.entries[key]

    def __contains__(self, key):
        return key in self.entries

    @property
    def version(self) -> int:
        return self.versions[-1]

    @property
    def versions(self) -> list[int]:
        if self.entries:
            return sorted(self.entries.keys())
        else:
            raise ValueError("This delta log is empty!")

    def schema(self) -> Schema:
        for v in sorted(self.entries.keys(), reverse=True):
            for a in self.entries[v].actions:
                if isinstance(a, TableMetadata):
                    return a.schema
        raise RuntimeError("No schema found in log entries")

    def add_actions(self) -> dict[str, Add]:
        adds = dict()
        for v in sorted(self.entries.keys()):
            entry = self.entries[v]
            for add in entry.add_actions():
                adds[add.path] = add
            for remove in entry.remove_actions():
                del adds[remove.path]
        return adds

    def partition_columns(self) -> list:
        cols = list()
        for v in sorted(self.entries.keys(), reverse=True):
            cols = self.entries[v].partition_columns()
            if cols is not None:
                return cols
        raise ValueError("No partitions found in log entries")

def generate_remove_acctions(add_actions: Iterable[Add]) -> list[Remove]:
    remove_actions = list()
    for add in add_actions:
        remove =  Remove(path=add.path,
                         dataChange=True,
                         deletionTimestamp=utils.timestamp(),
                         extendedFileMetadata=True,
                         partitionValues=add.partitionValues,
                         size=add.size)
        remove_actions.append(remove)
    return remove_actions
