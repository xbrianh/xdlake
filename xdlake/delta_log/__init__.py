import json
import datetime
from enum import Enum
from uuid import uuid4
from collections import defaultdict
from dataclasses import dataclass, asdict, field, fields, replace
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

    @classmethod
    def with_info(cls, info: dict):
        supported_info = {f.name: info.get(f.name) for f in fields(cls)}  # type: ignore[arg-type]  # it's OK to call fields on _DeltaLogAction subclasses
        return cls(**supported_info)


@dataclass
class Protocol(_DeltaLogAction):
    """Represetns the protocol version of the delta log"""
    minReaderVersion: int = 1
    minWriterVersion: int = 2

    def to_action_dict(self) -> dict:
        return {Type.protocol.name: self.asdict()}

@dataclass
class TableFormat(_DeltaLogAction):
    """Represents the file format of the table"""
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
        """Create a Schema object from a pyarrow.Schema object

        Args:
            schema (pa.Schema): A pyarrow schema object

        Returns:
            Schema: A new schema object
        """
        fields = [
            SchemaField(f.name, _data_type_from_arrow(f.type), f.nullable, f.metadata or {}).asdict()
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
    VACUUM_START = "VACUUM START"
    VACUUM_END = "VACUUM END"
    OPTIMIZE = "OPTIMIZE"
    values = [CREATE, WRITE, DELETE, VACUUM_START, VACUUM_END, OPTIMIZE]

@dataclass
class TableCommit(_DeltaLogAction):
    timestamp: int
    operationParameters: dict
    operationMetrics: dict | None = field(default_factory=dict)
    operation: str = TableCommitOperation.CREATE
    clientVersion: str = CLIENT_VERSION
    readVersion: int | None = None

    # spark attributes
    isolationLevel: str | None = None
    isBlindAppend: bool | None = None
    engineInfo: str | None = None
    txnId: str | None = None

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
    """A single entry in the delta table transaction log.

    Args:
        actions (list, optional): The actions in this entry.
    """
    def __init__(self, actions: list | None = None):
        self.actions = actions or list()

    @classmethod
    def with_handle(cls, handle: IO):
        """Create a DeltaLogEntry from a file handle.

        Args:
            handle (IO): A file handle

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        actions = [cls.load_action(line) for line in handle]
        return cls(actions)

    @staticmethod
    def load_action(obj: str | bytes | dict) -> _DeltaLogAction: 
        """Load an action from a string, bytes, or dict.

        Args:
            obj (str | bytes | dict): The JSON data to load.

        Returns:
            A new action object

        Raises:
            ValueError: If the action cannot be loaded.
        """
        if isinstance(obj, (str, bytes)):
            info = json.loads(obj)
        else:
            info = obj
        action = Type[set(info.keys()).pop()]
        info = info[action.name]

        match action:
            case Type.commitInfo:
                if info["operation"] in TableCommitOperation.values:
                    return TableCommit.with_info(info)
                else:
                    raise ValueError(f"Unknown operation '{info['operation']}'")
            case Type.metaData:
                return TableMetadata.with_info(info)
            case Type.protocol:
                return Protocol.with_info(info)
            case Type.add:
                return Add.with_info(info)
            case Type.remove:
                return Remove.with_info(info)
            case _:
                raise ValueError(f"Cannot handle delta log action '{action}'")

    def write(self, handle):
        """Write the entry to a file handle.

        Args:
            handle (IO): A file handle
        """
        handle.write("\n".join([json.dumps(a.to_action_dict()) for a in self.actions]))

    def add_actions(self) -> list[Add]:
        """Get all add actions in the entry."""
        return [a for a in self.actions
                if isinstance(a, Add)]

    def remove_actions(self) -> list[Remove]:
        """Get all remove actions in the entry."""
        return [a for a in self.actions
                if isinstance(a, Remove)]

    def partition_columns(self) -> list[str] | None:
        """Get the partition columns in the entry."""
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
        """Create a new DeltaLogEntry with a list of actions.

        Args:
            actions (list): A list of actions

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        entry = cls()
        entry.actions.extend(actions)
        return entry

    @classmethod
    def commit_create_table(cls, path: str, schema: Schema, partition_by: list, add_actions: list[Add]) -> "DeltaLogEntry":
        """Create a new DeltaLogEntry for creating a table.

        Args:
            path (str): The path to
            schema (Schema): The schema of the table
            partition_by (list): The partition columns
            add_actions (list[Add]): The add actions

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        protocol = Protocol()
        table_metadata = TableMetadata(schemaString=schema.json(), partitionColumns=partition_by)
        commit = TableCommit.create_with_parms(path, utils.timestamp(), table_metadata, protocol)
        return cls.with_actions([protocol, table_metadata, *add_actions, commit])

    @classmethod
    def commit_append_table(cls, partition_by: list, add_actions: list[Add], schema: Schema | None = None) -> "DeltaLogEntry":
        """Create a new DeltaLogEntry for appending to a table.

        Args:
            partition_by (list): The partition columns
            add_actions (list[Add]): The add actions
            schema (Schema, optional): The schema of the table.

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        commit = TableCommit.write_with_parms(utils.timestamp(), mode=WriteMode.append.value, partition_by=partition_by)
        actions = add_actions + [commit]
        if schema is not None:
            table_metadata = TableMetadata(schemaString=schema.json(), partitionColumns=partition_by)
            actions = [table_metadata] + actions
        return cls.with_actions(actions)

    @classmethod
    def commit_overwrite_table(
        cls,
        partition_by: list,
        existing_add_actions: Iterable[Add],
        add_actions: list[Add]
    ) -> "DeltaLogEntry":
        """Create a new DeltaLogEntry for overwriting a table.

        Args:
            partition_by (list): The partition columns
            existing_add_actions (Iterable[Add]): The existing add actions
            add_actions (list[Add]): The new add actions

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        commit = TableCommit.write_with_parms(utils.timestamp(), mode=WriteMode.overwrite.value, partition_by=partition_by)
        remove_actions = generate_remove_acctions(existing_add_actions)
        return cls.with_actions([*remove_actions, *add_actions, commit])

    @classmethod
    def commit_delete_table(
        cls,
        *,
        predicate: str,
        add_actions_to_remove: Sequence[Add] | ValuesView[Add],
        add_actions: Sequence[Add] | ValuesView[Add],
        read_version: int,
        num_copied_rows: int,
        num_deleted_rows: int,
    ) -> "DeltaLogEntry":
        """Create a new DeltaLogEntry for deleting rows from a table.

        Args:
            predicate (str): The predicate used to delete rows.
            add_actions_to_remove (Sequence[Add]): The add actions to remove.
            add_actions (Sequence[Add]): The new add actions.
            read_version (int): The input version for the delete operation.
            num_copied_rows (int): The number of rows copied from the input version.
            num_deleted_rows (int): The number of rows deleted from the input version.

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
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
    """The transaction log of a delta table."""

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
        """The largest version in the log."""
        return self.versions[-1]

    @property
    def versions(self) -> list[int]:
        """All versions in the log."""
        if self.entries:
            return sorted(self.entries.keys())
        else:
            raise ValueError("This delta log is empty!")

    def schema(self) -> Schema:
        """The latest schema in the log."""
        for v in sorted(self.entries.keys(), reverse=True):
            for a in self.entries[v].actions:
                if isinstance(a, TableMetadata):
                    return a.schema
        raise RuntimeError("No schema found in log entries")

    def add_actions(self) -> dict[str, Add]:
        """Add actions as of the latest version."""
        adds = dict()
        for v in sorted(self.entries.keys()):
            entry = self.entries[v]
            for add in entry.add_actions():
                adds[add.path] = add
            for remove in entry.remove_actions():
                del adds[remove.path]
        return adds

    def partition_columns(self) -> list:
        """The partition columns of the latest version."""
        cols = list()
        for v in sorted(self.entries.keys(), reverse=True):
            cols = self.entries[v].partition_columns()
            if cols is not None:
                return cols
        return list()

    def validate_partition_by(self, new_partition_by: Iterable[str] | None) -> list:
        """Check if the new partition columns are the same as the existing ones.

        Args:
            new_partition_by (Iterable[str], optional): The new partition columns

        Returns:
            list: The existing partition
        """
        existing_partition_columns = self.partition_columns()
        if new_partition_by is None:
            pass
        elif set(existing_partition_columns) != set(new_partition_by):
            raise ValueError(f"Expected partition columns {existing_partition_columns}, got {new_partition_by}")
        return existing_partition_columns

    def evaluate_schema(self, pyarrow_schema: pa.Schema, write_mode: WriteMode, schema_mode: str) -> Schema:
        """Evaluate a new table schema given write_mode and schema_mode.

        Args:
            pyarrow_schema (pa.Schema): The schema to evaluate
            write_mode (WriteMode): The write mode
            schema_mode (str): The schema mode

        Returns:
            Schema: The evaluated schema
        """
        schema = Schema.from_pyarrow_schema(pyarrow_schema)
        if not self.entries:
            return schema
        else:
            existing_schema = self.schema()
            if WriteMode.append == write_mode:
                if "merge" == schema_mode:
                    schema = existing_schema.merge(schema)
                elif existing_schema != schema:
                    raise ValueError("Schema mismatch")
            return schema


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
