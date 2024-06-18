import os
from urllib.parse import urlparse
from typing import Any

import fsspec


class Location:
    _filesystem: dict = dict()

    def __init__(self, scheme: str, path: str):
        self.scheme = scheme
        self.path = path

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

def get_filesystem(scheme: str, storage_options: dict | None = None) -> fsspec.AbstractFileSystem:
    return fsspec.filesystem(scheme, **(storage_options or dict()))

def list_files_sorted(loc: str | Location, fs: fsspec.AbstractFileSystem) -> list["Location"]:
    loc = Location.with_loc(loc)
    paths = sorted([info["name"] for info in fs.ls(loc.path, detail=True)
                    if "file" == info["type"]])
    return [Location(loc.scheme, path) for path in paths]

def open(loc: Location | str, fs: fsspec.AbstractFileSystem, mode: str="r") -> fsspec.core.OpenFile:
    loc = Location.with_loc(loc)
    if "file" == loc.scheme and "w" in mode:
        folder = loc.dirname()
        if fs.exists(folder):
            if not fs.isdir(folder):
                raise FileExistsError(loc.path)
        else:
            fs.mkdir(folder)
    return fs.open(loc.path, mode)
