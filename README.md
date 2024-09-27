# xdlake

A loose implementation of [deltalake](https://delta.io), and the deltalake, written in Python on top of
[pyarrow](https://arrow.apache.org/docs/python/index.html), focused on extensibility, customizability, and distributed
data.

This is mostly inspired by the [deltalake package](https://github.com/delta-io/delta-rs), and is (much) less battle tested.
However, it is more flexible given it's Pythonic design. If you're interested give it a shot and maybe even help make it
better.

## Install
```
pip install xdlake
```

## Usage

#### Instantiation

Instantiate a table! This can be a local or remote. For remote, you may need to install the relevant
fsspec implementation, for instance s3fs, gcsfs, adlfs for AWS S3, Google Storage, and Azure Storage,
respectively.

```
dt = xdlake.DeltaTable("path/to/my/cool/local/table")
dt = xdlake.DeltaTable("s3://path/to/my/cool/table")
dt = xdlake.DeltaTable("az://path/to/my/cool/table", storage_options=dict_of_azure_creds)
```

#### Reads

Read the data. For fancy filtering and predicate push down and whatever, use `to_pyarrow_dataset` and 
learn how to [filter pyarrow datasets](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html#pyarrow.dataset.Dataset.filter).

```
ds = dt.to_pyarrow_dataset()
t = dt.to_pyarrow_table()
df = dt.to_pandas()
```

#### Writes

Instances of DeltaTable are immutable: any method that performs a table operation will return a new DeltaTable.

##### Write in-memory data

Write data from memory. Data can be pyarrow tables, datasets, record batches, pandas DataFrames, or iterables of those things.

```
dt = dt.write(my_cool_pandas_dataframe)
dt = dt.write(my_cool_arrow_table)
dt = dt.write(my_cool_arrow_dataset)
dt = dt.write(my_cool_arrow_record_batches)
dt = dt.write(pyarrow.Table.from_pandas(df))
```

##### Import foreign data

Import references to foreign data without copying. Data may be heterogeneously located in s3, gs, azure, and local,
and cn be partitioned differently than the DeltaTable itself. Go hog wild.

See [Credentials](#Credentials) if you need different creds for different storage locations.

Import data from various locations in one go. This only works for non-partitioned data.
```
dt = dt.import_refs(["s3://some/aws/data", "gs://some/gcp/data", "az://some/azure/data" ])
dt = dt.import_refs(my_pyarrow_filesystem_dataset)
```

Partitioned data needs to be handled a differently. First, you'll need to read up on
[pyarrow partitioning](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.partitioning.html) to do it.
Second, you can only import one dataset at a time.
```
foreign_partitioning = pyarrow.dataset.partitioning(...)
ds = pyarrow.dataset.dataset(
    list_of_files,
    partitioning=foreign_partitioning,
    partition_base_dir,
    filesystem=xdlake.storage.get_filesystem(foreign_refs_loc),
)
dt = dt.import_refs(ds, partition_by=my_partition_cols)
```

#### Deletes

Delete rows from a DeltaTable using [pyarrow expressions](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Expression.html#pyarrow.dataset.Expression):
```
import pyarrow.compute as pc
expr = (
    (pc.field("cats") == pc.scalar("A"))
    |
    (pc.field("float64") > pc.scalar(0.9))
)
dt = dt.delete(expr)
```

##### Deletion Vectors

I really want to support
[deletion vectors](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors), but pyarrow can't
filter parquet files by row indices (pretty basic if you ask me). If you also would like xdlake to
support deletion vectors, let the arrow folks know by chiming in
[here](https://github.com/apache/arrow/issues/35301).

#### Clone

You can clone a deltatable. This is a soft clone (no data is copied, and the new table just references the data). The entire version history is preserved. Writes are written to the new location.

```
cloned_dt = dt.clone("the/location/of/the/clone")
```

#### Credentials

DeltaTables that reference distributed data may need credentials for various cloud locations.

To register default credentials for s3, gs, etc.
```
xdlake.storage.register_default_filesystem_for_protocol("s3", s3_creds)
xdlake.storage.register_default_filesystem_for_protocol("gs", gs_creds)
xdlake.storage.register_default_filesystem_for_protocol("az", az_creds)
```

To register specific credentials for various prefixes:
```
xdlake.storage.register_filesystem("s3://bucket-doom/foo/bar", s3_creds)
xdlake.storage.register_filesystem("s3://bucket-zoom/biz/baz", other_s3_creds)
xdlake.storage.register_filesystem("az://container-blah/whiz/whaz", az_creds)
```

## Links
Project home page [GitHub](https://github.com/xbrianh/xdlake)  
The deltalake transaction log [protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/xbrianh/xdlake).

## Gitpod Workspace
[launch gitpod workspace](https://gitpod.io/#https://github.com/xbrianh/xdlake)

## Build Status
![main](https://github.com/xbrianh/xdlake/actions/workflows/cicd.yml/badge.svg)
