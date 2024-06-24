from collections import defaultdict
from collections.abc import Iterable
from typing import Union

import pyarrow as pa

from xdlake import storage


RESOLVABLE = Union[str | storage.StorageObject | pa.Table | pa.RecordBatch | pa.dataset.Dataset]


def intersect_schemas(schemas) -> pa.Schema | None:
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
    schemas = defaultdict(list)
    for frag in fragments:
        schemas[frag.schema].append(frag)

    datasets = [pa.dataset.dataset(frags) for frags in schemas.values()]

    schema = None
    if "merge" == schema_mode:
        schema = pa.unify_schemas(list(schemas.keys()))
    else:
        schema = intersect_schemas(list(schemas.keys()))

    return pa.dataset.dataset(datasets, schema=schema)

def sobs_to_datasets(schema_to_sobs: dict[str, list[storage.StorageObject]], schema_mode: str) -> list[pa.dataset.FileSystemDataset]:
    datasets = list()
    for scheme, sobs in schema_to_sobs.items():
        fs = sobs[0].fs

        schema = None
        if "merge" == schema_mode:
            schemas = [pa.parquet.ParquetFile(sob.path, filesystem=fs).schema.to_arrow_schema()
                       for sob in sobs]
            schema = pa.unify_schemas(schemas)

        ds = pa.dataset.dataset([so.loc.path for so in sobs], schema=schema, filesystem=fs)
        datasets.append(ds)
    return datasets

def resolve(data: RESOLVABLE | Iterable[RESOLVABLE], schema_mode: str = "common") -> pa.dataset.UnionDataset:
    if not isinstance(data, Iterable):
        data = [data]

    datasets = list()
    fragments = list()
    sobs = defaultdict(list)

    for item in data:
        match item:
            case pa.Table():
                fragments.append(item)
            case pa.RecordBatch():
                fragments.append(item)
            case str():
                sob = storage.StorageObject.resolve(item)
                sobs[sob.loc.scheme].append(sob)
            case storage.StorageObject():
                sobs[item.loc.scheme].append(item)
            case pa.dataset.Dataset():
                datasets.append(item)
            case _:
                raise TypeError(f"Unsupported location type: {item}")

    if fragments:
        datasets.append(fragments_to_dataset(fragments, schema_mode))
    if sobs:
        datasets.extend(sobs_to_datasets(sobs, schema_mode))

    schema = None
    if "merge" == schema_mode:
        schema = pa.unify_schemas([ds.schema for ds in datasets])
    else:
        schema = intersect_schemas([ds.schema for ds in datasets])

    return pa.dataset.dataset(datasets, schema=schema)
