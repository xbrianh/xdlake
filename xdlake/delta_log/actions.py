import json
from uuid import uuid4
from dataclasses import dataclass, asdict, field, fields, replace
from typing import Iterable

from xdlake import utils
from xdlake.delta_log import schema, statistics


try:
    from typing import dataclass_transform
except ImportError:
    # support py310
    def dataclass_transform(*args):  # type: ignore
        def decorator(f):
            return f
        return decorator

@dataclass_transform()
class DeltaLogActionMeta(type):
    registered_actions = dict()  # type: ignore

    def __new__(cls, name, bases, dct):
        new_cls = type.__new__(cls, name, bases, dct)
        res = dataclass(new_cls, kw_only=True)
        if hasattr(res, "action_name"):
            cls.registered_actions[res.action_name] = res
        return res

class DeltaLogAction(metaclass=DeltaLogActionMeta):
    extra_info: dict = field(default_factory=dict)

    def asdict(self):
        return asdict(self)

    def json(self):
        return json.dumps(self.asdict(), cls=utils._JSONEncoder)

    def replace(self, **kwargs):
        return replace(self, **kwargs)

    def to_action_dict(self) -> dict:
        action_name = getattr(self, "action_name", None)
        if action_name is not None:
            return {action_name: self.asdict()}
        else:
            raise AttributeError("This is not an action")

    @classmethod
    def with_info(cls, info: dict):
        supported_info = {f.name: info[f.name] for f in fields(cls)
                          if f.name in info}  # type: ignore[arg-type]  # it's OK to call fields on _DeltaLogAction subclasses
        obj = cls(**supported_info)
        obj.extra_info = {k: v for k, v in info.items() if k not in supported_info}
        return obj

class Protocol(DeltaLogAction):
    """Represetns the protocol version of the delta log"""
    minReaderVersion: int = 1
    minWriterVersion: int = 2

    action_name = "protocol"

@dataclass
class _TableFormat:
    """Represents the file format of the table"""
    provider: str = "parquet"
    options: dict = field(default_factory=lambda: dict())

class TableMetadata(DeltaLogAction):
    schemaString: schema.Schema | str
    createdTime: int = field(default_factory=lambda: utils.timestamp())
    id: str = field(default_factory=lambda: f"{uuid4()}")
    name: str | None = None
    description: str | None = None
    format: dict = field(default_factory=lambda: asdict(_TableFormat()))
    partitionColumns: list[str] = field(default_factory=lambda: list())
    configuration: dict = field(default_factory=lambda: dict())

    action_name = "metaData"

    def __post_init__(self):
        match self.schemaString:
            case schema.Schema():
                self.schemaString = json.dumps(asdict(self.schemaString), cls=utils._JSONEncoder)
            case str():
                self.schemaString = self.schemaString
            case _:
                raise ValueError(f"Cannot handle schemaString of type {type(self.schemaString)}")

    @property
    def schema(self) -> schema.Schema:
        if not isinstance(self.schemaString, str):
            raise TypeError("schemaString is not a string")
        return schema.Schema(**json.loads(self.schemaString))

class TableOperationParm:
    METADATA = "metadata"
    PROTOCOL = "protocol"
    LOCATION = "location"
    MODE = "mode"
    PARTITION_BY = "partitionBy"

class TableCommitOperation:
    CREATE = "CREATE TABLE"
    WRITE = "WRITE"
    MERGE = "MERGE"
    DELETE = "DELETE"
    RESTORE = "RESTORE"
    VACUUM_START = "VACUUM START"
    VACUUM_END = "VACUUM END"
    OPTIMIZE = "OPTIMIZE"
    values = [CREATE, WRITE, MERGE, DELETE, RESTORE, VACUUM_START, VACUUM_END, OPTIMIZE]

class TableCommit(DeltaLogAction):
    timestamp: int
    operationParameters: dict
    operationMetrics: dict = field(default_factory=dict)
    operation: str = TableCommitOperation.CREATE
    clientVersion: str = utils.clien_version()
    readVersion: int | None = None

    # spark attributes
    isolationLevel: str | None = None
    isBlindAppend: bool | None = None
    engineInfo: str | None = None
    txnId: str | None = None

    action_name = "commitInfo"

    def to_action_dict(self) -> dict:
        info = {k: v for k, v in self.asdict().items()
                if v is not None}
        info.update(info.pop("extra_info"))
        return {self.action_name: info}

    @property
    def metadata(self):
        if TableOperationParm.METADATA in self.operationParameters:
            return json.loads(self.operationParameters[TableOperationParm.METADATA])
        else:
            return {}

    @classmethod
    def create(cls, location: str, timestamp: int, metadata: TableMetadata, protocol: Protocol):
        op_parms = {
            TableOperationParm.METADATA: metadata.json(),
            TableOperationParm.PROTOCOL: protocol.json(),
            TableOperationParm.LOCATION: location,
            TableOperationParm.MODE: "ErrorIfExists",
        }
        return cls(timestamp=timestamp, operationParameters=op_parms, operation=TableCommitOperation.CREATE)

    @classmethod
    def write(cls, timestamp: int, mode: str, partition_by: list | None = None):
        op_parms = {
            TableOperationParm.PARTITION_BY: partition_by or list(),
            TableOperationParm.MODE: mode,
        }
        return cls(timestamp=timestamp, operationParameters=op_parms, operation=TableCommitOperation.WRITE)

    @classmethod
    def delete(
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

    @classmethod
    def restore(
        cls,
        timestamp: int,
        read_version: int,
        restore_version: int,
        operation_metrics: dict,
    ):
        op_parms = {
            "version": restore_version,
        }

        return cls(
            timestamp=timestamp,
            operationParameters=op_parms,
            operationMetrics=operation_metrics,
            operation=TableCommitOperation.RESTORE,
            readVersion=read_version,
        )

class Add(DeltaLogAction):
    path: str
    partitionValues: dict
    size: int
    modificationTime: int
    stats: statistics.Statistics | str
    dataChange: bool = False
    tags: list | None = None
    deletionVector: dict | None = None
    baseRowId: str | None = None
    defaultRowCommitVersion: int | None = None
    clusteringProvider: str | None = None

    action_name = "add"

    def __post_init__(self):
        if isinstance(self.stats, statistics.Statistics):
            self.stats = json.dumps(asdict(self.stats), cls=utils._JSONEncoder)

class Remove(DeltaLogAction):
    path: str
    dataChange: bool
    deletionTimestamp: int
    partitionValues: dict
    size: int
    extendedFileMetadata: bool = False

    action_name = "remove"

def generate_remove_acctions(add_actions: Iterable[Add]) -> list[Remove]:
    """Generate remove actions from add actions.

    Args:
        add_actions (Iterable[Add]): A list of Add actions

    Returns:
        list[Remove]: A list of Remove actions
    """
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
