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
    remove = "remove"

class _DeltaLogItem:
    def asdict(self):
        return asdict(self)

    def json(self):
        return json.dumps(self.asdict())

@dataclass
class Protocol(_DeltaLogItem):
    minReaderVersion: int = 1
    minWriterVersion: int = 2

@dataclass
class TableFormat(_DeltaLogItem):
    provider: str = "parquet"
    options: dict = field(default_factory=lambda: dict())

@dataclass
class TableMetadata(_DeltaLogItem):
    schemaString: dict
    createdTime: int = field(default_factory=lambda: utils.timestamp())
    id: str = field(default_factory=lambda: f"{uuid4()}")
    name: str | None = None
    description: str | None = None
    format: dict = field(default_factory=lambda: TableFormat().asdict())
    partitionColumns: list[str] = field(default_factory=lambda: list())
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

@dataclass
class TableCommitCreate(_DeltaLogItem):
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

@dataclass
class TableCommitWrite(_DeltaLogItem):
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

@dataclass
class SchemaField(_DeltaLogItem):
    name: str
    type: str
    nullable: bool
    metadata: dict

@dataclass
class Schema(_DeltaLogItem):
    fields: list[dict]
    type: str = "struct"

    @classmethod
    def from_pyarrow_table(cls, t: pa.Table) -> "Schema":
        fields = [
            SchemaField(f.name, str(f.type), f.nullable, f.metadata or {}).asdict()
            for f in t.schema
        ]
        return cls(fields=fields)

@dataclass
class Statistics(_DeltaLogItem):
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
class Add(_DeltaLogItem):
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

@dataclass
class Remove(_DeltaLogItem):
    path: str
    dataChange: bool
    deletionTimestamp: int
    extendedFileMetadata: bool
    partitionValues: dict
    size: int

class DeltaLog:
    def __init__(self, handle = None):
        if handle is None:
            self.actions = list()
        else:
            self.actions = [self.load_action(line) for line in handle]

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
            case Type.remove:
                return Remove(**info)
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
                case Remove():
                    actions.append({Type.remove.name: info})
        handle.write("\n".join([json.dumps(a) for a in actions]))

    def add_actions(self) -> list[Add]:
        return [a for a in self.actions
                if isinstance(a, Add)]

    def remove_actions(self) -> list[Remove]:
        return [a for a in self.actions
                if isinstance(a, Remove)]
