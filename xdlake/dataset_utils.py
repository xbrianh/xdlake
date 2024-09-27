from collections import defaultdict
from functools import lru_cache
from collections.abc import Iterable
from typing import Union

import pyarrow as pa

from xdlake import storage


RESOLVABLE = Union[str | pa.Table | pa.RecordBatch | pa.dataset.Dataset]


@lru_cache()
def get_py_filesystem(fs: storage.fsspec.AbstractFileSystem) -> pa.fs.PyFileSystem:
    """Get a pyarrow filesystem from a fsspec filesystem.

    Args:
        fs (storage.fsspec.AbstractFileSystem): The fsspec filesystem.

    Retrns:
        pa.fs.PyFileSystem: The pyarrow py filesystem.
    """
    return pa.fs.PyFileSystem(pa.fs.FSSpecHandler(fs))

def intersect_schemas(schemas: list[pa.Schema]) -> pa.Schema:
    """Intersect a list of schemas.

    Args:
        schemas (list[pa.Schema]): A list of schemas.

    Returns:
        pa.Schema: The intersected schema.
    """
    common_fields = set.intersection(*[{f for f in schema} for schema in schemas])
    fields = list()
    present_fields = set()
    for schema in schemas:
        for field in schema:
            if field in common_fields:
                if field not in present_fields:
                    present_fields.add(field)
                    fields.append(field)
    return pa.schema(fields)

def fragments_to_dataset(fragments: list[pa.Table | pa.RecordBatch], schema_mode: str) -> pa.dataset.Dataset:
    """Convert a list of fragments to a dataset.

    Args:
        fragments (list[pa.Table | pa.RecordBatch]): A list of fragments.
        schema_mode (str, optional): The schema mode. Defaults to "common", which drops data that does not match the schema. Use "merge" to combine schema.

    Returns:
        pa.dataset.Dataset: The dataset.
    """
    schemas = defaultdict(list)
    for frag in fragments:
        schemas[frag.schema.remove_metadata()].append(frag)

    datasets = [pa.dataset.dataset(frags) for frags in schemas.values()]

    schema = None
    if "merge" == schema_mode:
        schema = pa.unify_schemas(list(schemas.keys()))
    else:
        schema = intersect_schemas(list(schemas.keys()))

    return pa.dataset.dataset(datasets, schema=schema)

def locations_to_datasets(schema_to_locations: dict[str, list[storage.Location]], schema_mode: str) -> list[pa.dataset.FileSystemDataset]:
    """Convert a dictionary of schema to locations to datasets.

    Args:
        schema_to_locations (dict[str, list[storage.Location]]): A dictionary of schema to locations.
        schema_mode (str, optional): The schema mode. Defaults to "common", which drops data that does not match the schema. Use "merge" to combine schema.

    Returns:
        list[pa.dataset.FileSystemDataset]: The datasets.
    """
    datasets = list()
    for scheme, locations in schema_to_locations.items():
        fs = locations[0].fs

        schemas = [pa.parquet.ParquetFile(loc.path, filesystem=fs).schema.to_arrow_schema()
                   for loc in locations]
        if "merge" == schema_mode:
            schema = pa.unify_schemas(schemas)
        else:
            schema = intersect_schemas(schemas)

        ds = pa.dataset.dataset([loc.path for loc in locations], schema=schema, filesystem=fs)
        datasets.append(ds)
    return datasets

def _is_pandas_dataframe(obj) -> bool:
    return hasattr(obj, '__dataframe__') and callable(obj.__dataframe__)

def union_dataset(data: RESOLVABLE | Iterable[RESOLVABLE], schema_mode: str = "common") -> pa.dataset.UnionDataset:
    """Create a union dataset from data or iterable of data.

    Args:
        data (RESOLVABLE | Iterable[RESOLVABLE]): The data.
        schema_mode (str, optional): The schema mode. Defaults to "common", which drops data that does not match the schema. Use "merge" to combine schema.

    Returns:
        pa.dataset.UnionDataset
    """
    if _is_pandas_dataframe(data) or not isinstance(data, Iterable):
        data = [data]

    datasets = list()
    fragments = list()
    locations = defaultdict(list)

    for item in data:
        match item:
            case pa.Table() | pa.RecordBatch():
                fragments.append(item)
            case str():
                loc = storage.Location.with_location(item)
                locations[loc.scheme].append(loc)
            case storage.Location():
                locations[item.scheme].append(item)
            case pa.dataset.Dataset():
                datasets.append(item)
            case df if _is_pandas_dataframe(df):
                fragments.append(pa.Table.from_pandas(df))
            case _:
                raise TypeError(f"Unsupported type: {item}")

    if fragments:
        datasets.append(fragments_to_dataset(fragments, schema_mode))
    if locations:
        datasets.extend(locations_to_datasets(locations, schema_mode))

    schema = None
    if "merge" == schema_mode:
        schema = pa.unify_schemas([ds.schema for ds in datasets])
    else:
        schema = intersect_schemas([ds.schema for ds in datasets])

    return pa.dataset.dataset(datasets, schema=schema)
