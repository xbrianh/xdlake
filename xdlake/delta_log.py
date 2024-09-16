import re
import json
import datetime
from enum import Enum
from uuid import uuid4
from contextlib import nullcontext
from collections import defaultdict
from dataclasses import dataclass, asdict, field, fields, replace
from collections.abc import ValuesView
from typing import IO, Iterable, Sequence, dataclass_transform

import pyarrow as pa

from xdlake import storage, utils


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


@dataclass_transform()
class DeltaLogActionMeta(type):
    def __new__(cls, name, bases, dct):
        new_cls = type.__new__(cls, name, bases, dct)

        # pull in common dataclass fields - these need to have defaults
        for k, v in cls.__annotations__.items():
            setattr(new_cls, k, v)

        res = dataclass(new_cls)
        return res


class DeltaLogAction(metaclass=DeltaLogActionMeta):
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
        supported_info = {f.name: info.get(f.name) for f in fields(cls)}  # type: ignore[arg-type]  # it's OK to call fields on DeltaLogAction subclasses
        return cls(**supported_info)


class Protocol(DeltaLogAction):
    """Represetns the protocol version of the delta log"""
    minReaderVersion: int = 1
    minWriterVersion: int = 2

    def to_action_dict(self) -> dict:
        return {Type.protocol.name: self.asdict()}

class TableFormat(DeltaLogAction):
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

class SchemaField(DeltaLogAction):
    name: str
    type: str
    nullable: bool
    metadata: dict

class Schema(DeltaLogAction):
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

class TableMetadata(DeltaLogAction):
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

def _data_type_from_arrow(_t):
    if isinstance(_t, pa.lib.TimestampType):
        assert _t.unit == "us"
        return "timestamp"
    elif _t not in ARROW_TO_DELTA_TYPE:
        err = f"Cannot handle arrow type '{_t}', type={type(_t)}"
        raise TypeError(err)
    return ARROW_TO_DELTA_TYPE[_t]

class Statistics(DeltaLogAction):
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

class Add(DeltaLogAction):
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

class Remove(DeltaLogAction):
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
    def load_action(obj: str | bytes | dict) -> DeltaLogAction:
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
    def with_actions(cls, actions: list[DeltaLogAction]) -> "DeltaLogEntry":
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
    def create_table(cls, path: str, schema: Schema, partition_by: list, add_actions: list[Add]) -> "DeltaLogEntry":
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
        commit = TableCommit.create(path, utils.timestamp(), table_metadata, protocol)
        return cls.with_actions([protocol, table_metadata, *add_actions, commit])

    @classmethod
    def append_table(cls, partition_by: list, add_actions: list[Add], schema: Schema | None = None) -> "DeltaLogEntry":
        """Create a new DeltaLogEntry for appending to a table.

        Args:
            partition_by (list): The partition columns
            add_actions (list[Add]): The add actions
            schema (Schema, optional): The schema of the table.

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        commit = TableCommit.write(utils.timestamp(), mode=WriteMode.append.value, partition_by=partition_by)
        actions = add_actions + [commit]
        if schema is not None:
            table_metadata = TableMetadata(schemaString=schema.json(), partitionColumns=partition_by)
            actions = [table_metadata] + actions
        return cls.with_actions(actions)

    @classmethod
    def overwrite_table(
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
        commit = TableCommit.write(utils.timestamp(), mode=WriteMode.overwrite.value, partition_by=partition_by)
        remove_actions = generate_remove_acctions(existing_add_actions)
        return cls.with_actions([*remove_actions, *add_actions, commit])

    @classmethod
    def delete_table(
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
        commit = TableCommit.delete(utils.timestamp(), predicate, read_version, operation_metrics)
        remove_actions = generate_remove_acctions(add_actions_to_remove)
        return cls.with_actions([*remove_actions, *add_actions, commit])

    @classmethod
    def restore_table(
        cls,
        *,
        read_version: int,
        restore_version: int,
        restore_schema: Schema,
        restore_partition_by: list[str],
        add_actions_to_remove: Sequence[Add] | ValuesView[Add],
        add_actions: Sequence[Add] | ValuesView[Add],
    ) -> "DeltaLogEntry":
        operation_metrics = {
            "num_removed_files": len(add_actions_to_remove),
            "num_restored_files": len(add_actions),
        }
        commit = TableCommit.restore(utils.timestamp(), read_version, restore_version, operation_metrics)
        remove_actions = generate_remove_acctions(add_actions_to_remove)
        table_metadata = TableMetadata(schemaString=restore_schema.json(), partitionColumns=restore_partition_by)
        return cls.with_actions([table_metadata, *remove_actions, *add_actions, commit])

class DeltaLog:
    """The transaction log of a delta table."""

    _log_entry_filename_re = re.compile("^\d+\.json$")

    def __init__(self, location: storage.Location):
        self.entries: dict[int, DeltaLogEntry] = dict()
        self.loc = location

    def __setitem__(self, key, val):
        self.entries[key] = val

    def __getitem__(self, key):
        return self.entries[key]

    def __contains__(self, key):
        return key in self.entries

    @classmethod
    def with_location(
        cls,
        loc: str | storage.Location,
        version: int | None = None,
        storage_options: dict | None = None,
    ) -> "DeltaLog":
        """Read a delta table transaction log.

        Args:
            loc (str | Location): Root of the transaction log directory.
            version (int, otional): Read log entries up to this version.
            storage_options (dict, optional): keyword arguments to pass to fsspec.filesystem

        Returns:
            delta_log.DeltaLog
        """
        dlog = cls(storage.Location.with_location(loc, storage_options=storage_options))
        if dlog.loc.exists():
            for entry_loc in dlog.loc.list_files_sorted():
                filename = entry_loc.basename()
                if cls._log_entry_filename_re.match(filename):
                    entry_version = int(filename.split(".", 1)[0])
                    with entry_loc.open() as fh:
                        dlog[entry_version] = DeltaLogEntry.with_handle(fh)
                    if version in dlog:
                        break
        return dlog

    def load_as_version(self, version: int | None = None):
        """Load the delta log up to a specific version.

        Args:
            version (int, optional): The version to load up to.

        Returns:
            DeltaLog: A new DeltaLog object
        """
        if version in self.versions:
            dlog = type(self)(self.loc)
            for v in self.versions:
                if v <= version:
                    dlog[v] = self[v]
                else:
                    break
            return dlog
        else:
            return type(self).with_location(self.loc)

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

    @property
    def version_to_write(self) -> int:
        """The next log version."""
        try:
            return 1 + self.versions[-1]
        except ValueError:
            return 0

    def schema(self) -> Schema:
        """The latest schema in the log."""
        for v in reversed(self.versions):
            for a in self.entries[v].actions:
                if isinstance(a, TableMetadata):
                    return a.schema
        raise RuntimeError("No schema found in log entries")

    def add_actions(self) -> dict[str, Add]:
        """Add actions as of the latest version."""
        adds = dict()
        for v in self.versions:
            entry = self.entries[v]
            for add in entry.add_actions():
                adds[add.path] = add
            for remove in entry.remove_actions():
                del adds[remove.path]
        return adds

    def partition_columns(self) -> list:
        """The partition columns of the latest version."""
        cols: list | None = list()
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

    def entry_for_write_mode(
        self,
        mode,
        schema: Schema,
        add_actions: list[Add],
        partition_by: list | None = None,
    ):
        """Write a new delta log entry.

        Args:
            mode (WriteMode): Write mode.
            schema (Schema): Schema.
            add_actions (list[Add]): Add actions.
            partition_by (list[str], optional): Partition
        """
        partition_by = partition_by or list()
        entry = DeltaLogEntry()
        if 0 == self.version_to_write:
            entry = DeltaLogEntry.create_table(self.loc.path, schema, partition_by, add_actions)
        elif WriteMode.append == mode:
            entry = DeltaLogEntry.append_table(partition_by, add_actions, schema)
        elif WriteMode.overwrite == mode:
            existing_add_actions = self.add_actions().values()
            entry = DeltaLogEntry.overwrite_table(partition_by, existing_add_actions, add_actions)
        return entry

    def commit(self, entry: DeltaLogEntry, context = nullcontext) -> "DeltaLog":
        if 0 == self.version_to_write:
            self.loc.mkdir(exists_ok=True)
        loc = self.loc.append_path(utils.filename_for_version(self.version_to_write))
        with context(loc):
            with loc.open(mode="w") as fh:
                entry.write(fh)
            return type(self).with_location(self.loc)


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
