import os
from urllib.parse import urlparse
from typing import Any, Generator, NamedTuple

import fsspec
import pyarrow.fs


_filesystems = dict()


def register_filesystem(pfx: str, fs: fsspec.AbstractFileSystem):
    _filesystems[pfx] = fs

def unregister_filesystem(pfx: str):
    del _filesystems[pfx]

def get_filesystem(url: str) -> fsspec.AbstractFileSystem:
    parsed = urlparse(url)
    protocol = parsed.scheme
    if not protocol:
        protocol = "file"
        if parsed.path.startswith(os.path.sep):
            path = parsed.path
        else:
            path = os.path.abspath(parsed.path)
        url = f"{protocol}://{path}"

    match_pfx = None
    for pfx in _filesystems:
        if url.startswith(pfx):
            if match_pfx is None or len(pfx) > len(match_pfx):
                match_pfx = pfx

    if match_pfx:
        return _filesystems[match_pfx]
    else:
        return fsspec.filesystem(protocol)


class Location:
    def __init__(self, scheme: str, path: str):
        self.scheme = scheme
        self.path = path
        self.url = f"{self.scheme}://{self.path}"

    @classmethod
    def with_loc(cls, loc: str | Any) -> "Location":
        if isinstance(loc, cls):
            return loc
        elif not isinstance(loc, str):
            raise TypeError(f"Cannot handle storage location '{loc}'")
        parsed = urlparse(loc)
        scheme = parsed.scheme or "file"
        if "file" == scheme:
            if parsed.path.startswith(os.path.sep):
                path = parsed.path
            else:
                path = os.path.abspath(parsed.path)
        else:
            path = loc
        return cls(scheme, path)

    def append_path(self, *path_components) -> "Location":
        if "file" == self.scheme:
            p = os.path.join(self.path, *path_components)
        else:
            p = "/".join([self.path, *path_components])
        return type(self)(self.scheme, p)

    def dirname(self) -> str:
        if "file" == self.scheme:
            return os.path.dirname(self.path)
        else:
            return self.path.rsplit("/", 1)[0]

    def basename(self) -> str:
        if "file" == self.scheme:
            return os.path.basename(self.path)
        else:
            return self.path.rsplit("/", 1)[-1]

def get_pyarrow_py_filesystem(scheme: str, storage_options: dict | None = None) -> pyarrow.fs.PyFileSystem:
    fs = fsspec.filesystem(scheme, **(storage_options or dict()))
    return pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fs))

class StorageObject(NamedTuple):
    loc: Location

    @property
    def fs(self):
        return get_filesystem(self.loc.url)

    def append_path(self, *path_components):
        return type(self)(self.loc.append_path(*path_components))

    @property
    def path(self):
        return self.loc.path

    def exists(self) -> bool:
        return self.fs.exists(self.path)

    def mkdir(self):
        self.fs.mkdir(self.path)

    def list_files(self) -> Generator["StorageObject", None, None]:
        for info in self.fs.ls(self.loc.path, detail=True):
            if "file" == info["type"]:
                yield type(self)(Location(self.loc.scheme, info["name"]))

    def list_files_sorted(self) -> list["StorageObject"]:
        # TODO don't sort for s3 or gcs
        return sorted([so for so in self.list_files()], key=lambda i: i.path)

    @classmethod
    def with_location(cls, loc, storage_options: dict | None = None) -> "StorageObject":
        if isinstance(loc, cls):
            return loc
        else:
            loc = Location.with_loc(loc)
        return cls(loc)

    def pyarrow_py_filesystem(self) -> pyarrow.fs.PyFileSystem:
        return pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(self.fs))

def open(locfs: StorageObject, mode: str="r") -> fsspec.core.OpenFile:
    if "file" == locfs.loc.scheme and "w" in mode:
        folder = locfs.loc.dirname()
        if locfs.fs.exists(folder):
            if not locfs.fs.isdir(folder):
                raise FileExistsError(locfs.loc.path)
        else:
            locfs.fs.mkdir(folder)
    return locfs.fs.open(locfs.loc.path, mode)
