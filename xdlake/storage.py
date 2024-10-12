import os
from contextlib import nullcontext, suppress
from urllib.parse import urlparse
from typing import Any, Generator

import fsspec
import pyarrow.fs


_filesystems = dict()


def register_filesystem(pfx: str, fs: fsspec.AbstractFileSystem):
    """Register a filesystem for a prefix.

    Args:
        pfx (str): The prefix.
        fs (fsspec.AbstractFileSystem): The filesystem.
    """
    _filesystems[pfx] = fs

def unregister_filesystem(pfx: str):
    """Unregister a filesystem for a prefix.

    Args:
        pfx (str): The prefix.
    """
    del _filesystems[pfx]

def get_filesystem(url: str, storage_options: dict | None = None) -> fsspec.AbstractFileSystem:
    """Get the filesystem for a URL.

    Args:
        url (str): The URL.
        storage_options (dict, optional): keyword options passed to `fsspec.filesystem`

    Returns:
        fsspec.AbstractFileSystem
    """
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
        return fsspec.filesystem(protocol, **(storage_options or dict()))

def register_default_filesystem_for_protocol(protocol: str, storage_options: dict | None = None) -> fsspec.AbstractFileSystem:
    """Create and register default filesystem for a protocol.

    This is useful for filesystems requiring credentials that are not available in the environment.

    Args:
        protocol (str): The protocol for the filesystem, for instance "s3", "gs", or "az" for s3, google storage, and azure storage, respectively.
        storage_options (dict, optional): keyword options passed to `fsspec.filesystem`

    Returns:
        fsspec.AbstractFileSystem
    """
    url = f"{protocol}://"
    fs = get_filesystem(url, storage_options=storage_options)
    register_filesystem(url, fs)
    return fs


class Location:
    """A location in a filesystem.

    This class is not typically instantiated directly, but is instead created by the `with_location` class method.

    Args:
        scheme (str): The scheme for the location, for instance "file", "s3", "gs", or "az" for file, s3, google storage, and azure storage, respectively.
        path (str): The path.
        storage_options (dict, optional): keyword options passed to `fsspec.filesystem`
    """

    def __init__(self, scheme: str, path: str, storage_options: dict | None = None):
        self.scheme = scheme
        self.path = path
        self.storage_options = storage_options

    @property
    def url(self):
        if self.path.startswith(self.scheme):
            return self.path
        else:
            return f"{self.scheme}://{self.path}"

    @classmethod
    def with_location(cls, loc: str | Any, storage_options: dict | None = None) -> "Location":
        """Create a Location from a string or Location.

        Args:
            loc (str, Location): The location string or Location.
            storage_options (dict, optional): keyword options passed to `fsspec.filesystem`.

        Returns:
            Location
        """
        if isinstance(loc, cls):
            if storage_options:
                loc.storage_options = storage_options
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
        return cls(scheme, path, storage_options=storage_options)

    @property
    def fs(self):
        """The filesystem for the location."""
        return get_filesystem(self.url, storage_options=self.storage_options)

    def append_path(self, *path_components) -> "Location":
        """Append path components to the location.

        Args:
            *path_components: The path components.

        Returns:
            Location
        """
        if "file" == self.scheme:
            p = os.path.join(self.path, *path_components)
        else:
            p = "/".join([self.path, *path_components])
        return type(self)(self.scheme, p, storage_options=self.storage_options)

    def dirname(self) -> str:
        """Return the directory name of the location."""
        if "file" == self.scheme:
            return os.path.dirname(self.path)
        else:
            return self.path.rsplit("/", 1)[0]

    def basename(self) -> str:
        """Return the basename of the location."""
        if "file" == self.scheme:
            return os.path.basename(self.path)
        else:
            return self.path.rsplit("/", 1)[-1]

    def exists(self) -> bool:
        """Return whether the location exists."""
        return self.fs.exists(self.path)

    def mkdir(self, exists_ok: bool=False):
        """Create the location as a directory."""
        ctx = suppress if exists_ok else nullcontext
        with ctx(FileExistsError):
            self.fs.mkdir(self.path)

    def list_files(self) -> Generator["Location", None, None]:
        """List files in the location.

        Returns:
            Generator[Location, None, None]
        """
        for info in self.fs.ls(self.path, detail=True):
            if "file" == info["type"]:
                yield type(self)(self.scheme, info["name"], storage_options=self.storage_options)

    def list_files_sorted(self) -> list["Location"]:
        """List files in the location, sorted by path."""
        # TODO don't sort for s3 or gcs
        return sorted([loc for loc in self.list_files()], key=lambda i: i.path)

    def open(self, mode: str="r") -> fsspec.core.OpenFile:
        """Open the location.

        Args:
            mode (str, optional): The mode.

        Returns:
            fsspec.core.OpenFile
        """
        if "file" == self.scheme and "w" in mode:
            folder = self.dirname()
            if self.fs.exists(folder):
                if not self.fs.isdir(folder):
                    raise FileExistsError(self.path)
            else:
                self.fs.mkdir(folder)
        return self.fs.open(self.path, mode)

def get_pyarrow_py_filesystem(scheme: str, storage_options: dict | None = None) -> pyarrow.fs.PyFileSystem:
    """Get a pyarrow filesystem for a scheme.

    Args:
        scheme (str): The scheme.
        storage_options (dict, optional): keyword options passed to `fsspec.filesystem`

    Returns:
        pyarrow.fs.PyFileSystem
    """
    fs = fsspec.filesystem(scheme, **(storage_options or dict()))
    return pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fs))

def absloc(path: str, root: Location) -> Location:
    """Return path as an absolute Location.

    If path is absolute, return Location with path. Otherwise, return Location with path appended onto root.

    Args:
        path (str): The path.
        root(str, Location): Root location.

    Returns:
        Location
    """
    is_absolute = "://" in path
    if is_absolute:
        return Location.with_location(path)
    else:
        return root.append_path(path)
