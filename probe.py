import fsspec


fs = fsspec.filesystem("s3")

with fs.open("s3://test-xdlake/test", "wb") as fh:
    fh.write(b"asdf")
