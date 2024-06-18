import os
from urllib.parse import urlparse
from typing import Any, NamedTuple

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

class LocatedFS(NamedTuple):
    loc: Location
    fs: fsspec.AbstractFileSystem

    def append_path(self, *path_components):
        return type(self)(self.loc.append_path(*path_components), self.fs)

    @property
    def path(self):
        return self.loc.path

    def exists(self) -> bool:
        return self.fs.exists(self.path)

    def mkdir(self):
        self.fs.mkdir(self.path)

    @classmethod
    def resolve(cls, loc, storage_options: dict | None = None) -> "LocatedFS":
        if isinstance(loc, cls):
            return loc
        else:
            loc = Location.with_loc(loc)
            fs = get_filesystem(loc.scheme, storage_options)
        return cls(loc, fs)

def list_files_sorted(locfs: LocatedFS) -> list[LocatedFS]:
    paths = sorted([info["name"] for info in locfs.fs.ls(locfs.loc.path, detail=True)
                    if "file" == info["type"]])
    return [LocatedFS(Location(locfs.loc.scheme, path), locfs.fs) for path in paths]

def open(locfs: LocatedFS, mode: str="r") -> fsspec.core.OpenFile:
    if "file" == locfs.loc.scheme and "w" in mode:
        folder = locfs.loc.dirname()
        if locfs.fs.exists(folder):
            if not locfs.fs.isdir(folder):
                raise FileExistsError(locfs.loc.path)
        else:
            locfs.fs.mkdir(folder)
    return locfs.fs.open(locfs.loc.path, mode)
