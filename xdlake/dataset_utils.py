from collections import defaultdict
from collections.abc import Iterable
from typing import Union

import pyarrow as pa

from xdlake import storage


RESOLVABLE = Union[str | pa.Table | pa.RecordBatch | pa.dataset.Dataset]


def intersect_schemas(schemas) -> pa.Schema:
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

def locations_to_datasets(schema_to_locations: dict[str, list[storage.Location]], schema_mode: str) -> list[pa.dataset.FileSystemDataset]:
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

def union_dataset(data: RESOLVABLE | Iterable[RESOLVABLE], schema_mode: str = "common") -> pa.dataset.UnionDataset:
    if not isinstance(data, Iterable):
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
            case _:
                raise TypeError(f"Unsupported location type: {item}")

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
