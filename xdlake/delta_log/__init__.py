import json
from enum import Enum
from uuid import uuid4
from collections import defaultdict
from dataclasses import dataclass, asdict, field

import pyarrow as pa

from xdlake import utils


CLIENT_VERSION = "xdlake-0.0.0"

class Type(Enum):
    commitInfo = "commitInfo"
    metaData = "metaData"
    protocol = "protocol"
    add = "add"

def delta_log_entry(obj):
    obj = dataclass(obj)
    if not hasattr(obj, "asdict"):
        setattr(obj, "asdict", lambda obj: asdict(obj))
    if not hasattr(obj, "json"):
        setattr(obj, "json", lambda obj: json.dumps(asdict(obj)))
    return obj

@delta_log_entry
class Protocol:
    minReaderVersion: int = 1
    minWriterVersion: int = 2

@delta_log_entry
class TableFormat:
    provider: str = "parquet"
    options: dict = field(default_factory=lambda: dict())

@delta_log_entry
class TableMetadata:
    schemaString: dict
    createdTime: int = field(default_factory=lambda: utils.timestamp())
    id: str = field(default_factory=lambda: f"{uuid4()}")
    name: str | None = None
    description: str | None = None
    format: dict = field(default_factory=lambda: TableFormat().asdict())
    partitionColumns: dict = field(default_factory=lambda: dict())
    configuration: dict = field(default_factory=lambda: dict())

class TableOperationParm:
    METADATA = "metadata"
    PROTOCOL = "protocol"
    LOCATION = "location"
    MODE = "mode"
    PARTITION_BY = "partitionBy"

class TableCommitOperation:
    CREATE = "CREATE TABLE"
    WRITE = "WRITE"

@delta_log_entry
class TableCommitCreate:
    timestamp: int
    operationParameters: dict
    operation: str = TableCommitOperation.CREATE
    clientVersion: str = CLIENT_VERSION

    @classmethod
    def with_parms(cls, location: str, timestamp: int, metadata: TableMetadata, protocol: Protocol):
        op_parms = {
            TableOperationParm.METADATA: metadata.json(),
            TableOperationParm.PROTOCOL: protocol.json(),
            TableOperationParm.LOCATION: location,
            TableOperationParm.MODE: "ErrorIfExists",
        }
        return cls(timestamp=timestamp, operationParameters=op_parms)

@delta_log_entry
class TableCommitWrite:
    timestamp: int
    operationParameters: dict
    operation: str = TableCommitOperation.WRITE
    clientVersion: str = CLIENT_VERSION

    @classmethod
    def with_parms(cls, timestamp: int, mode: str, partition_by: list | None = None):
        op_parms = {
            TableOperationParm.PARTITION_BY: partition_by or list(),
            TableOperationParm.MODE: mode,
        }
        return cls(timestamp=timestamp, operationParameters=op_parms)

@delta_log_entry
class SchemaField:
    name: str
    type: str
    nullable: bool
    metadata: dict

@delta_log_entry
class Schema:
    fields: list[dict]
    type: str = "struct"

    @classmethod
    def from_pyarrow_table(cls, t: pa.Table) -> "Schema":
        fields = [
            SchemaField(f.name, str(f.type), f.nullable, f.metadata or {}).asdict()
            for f in t.schema
        ]
        return cls(fields=fields)

@delta_log_entry
class Statistics:
    numRecords: int
    minValues: dict
    maxValues: dict
    nullCount: dict

    @classmethod
    def from_parquet_file_metadata(cls, md: pa.parquet.FileMetaData) -> "Statistics":
        md = md.to_dict()
        min_values = defaultdict(dict)
        max_values = defaultdict(dict)
        nullcounts = defaultdict(int)
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

@delta_log_entry
class Add:
    path: str
    partitionValues: dict
    size: int
    modificationTime: int
    stats: str
    dataChange: str | None = None
    tags: list | None = None
    deletionVector: dict | None = None
    baseRowId: str | None = None
    defaultRowCommitVersion: int | None = None
    clusteringProvider: str | None = None

class DeltaLog:
    def __init__(self, handle = None):
        if handle is None:
            self.actions = list()
        else:
            self.actions = [self.load_action(line) for line in handle]

    @staticmethod
    def load_action(info: str | bytes | dict): 
        if isinstance(info, (str, bytes)):
            info = json.loads(info)
        action = Type[set(info.keys()).pop()]
        info = info[action.name]

        match action:
            case Type.commitInfo:
                if TableCommitOperation.CREATE == info["operation"]:
                    return TableCommitCreate(**info)
                elif TableCommitOperation.WRITE == info["operation"]:
                    return TableCommitWrite(**info)
            case Type.metaData:
                return TableMetadata(**info)
            case Type.protocol:
                return Protocol(**info)
            case Type.add:
                return Add(**info)
            case _:
                raise Exception(f"Cannot handle delta log action '{action}'")

    def write(self, handle):
        actions = list()
        for a in self.actions:
            info = a.asdict()
            match a:
                case TableCommitCreate():
                    actions.append({Type.commitInfo.name: info})
                case TableCommitWrite():
                    actions.append({Type.commitInfo.name: info})
                case TableMetadata():
                    actions.append({Type.metaData.name: info})
                case Protocol():
                    actions.append({Type.protocol.name: info})
                case Add():
                    actions.append({Type.add.name: info})
        handle.write("\n".join([json.dumps(a) for a in actions]))
