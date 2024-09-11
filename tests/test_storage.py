import os
import random
import unittest
import warnings
from string import ascii_lowercase
from unittest import mock
from uuid import uuid4
from tempfile import TemporaryDirectory

from xdlake import storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.scratch_folder = os.path.abspath(self.td.name)
        warnings.simplefilter("ignore", DeprecationWarning)

    def tearDown(self):
        self.td.cleanup()
        storage._filesystems = dict()

    def test_resolution(self):
        name = f"{uuid4()}"
        tests = [
            ("local absolute", f"{self.scratch_folder}/tests", f"{self.scratch_folder}/tests/foo/{name}"),
            ("local relative", "tests", f"{os.getcwd()}/tests/foo/{name}"),
            ("local file url", f"file://{self.scratch_folder}/tests", f"{self.scratch_folder}/tests/foo/{name}"),
            ("s3", "s3://test-xdlake/tests", f"s3://test-xdlake/tests/foo/{name}"), 
        ]
        for test_name, url, expected_path in tests:
            with self.subTest(test_name):
                loc = storage.Location.with_location(url)
                new_loc = loc.append_path("foo", name)
                self.assertEqual(new_loc.path, expected_path)
                d = os.urandom(7)
                with new_loc.open(mode="wb") as fh:
                    fh.write(d)
                with new_loc.open(mode="rb") as fh:
                    self.assertEqual(fh.read(), d)

    def test_listing(self):
        names = [f"{uuid4()}" for _ in range(11)]
        for name in names:
            with open(f"{self.scratch_folder}/{name}", "wb") as fh:
                fh.write(os.urandom(7))
        loc = storage.Location.with_location(self.scratch_folder)
        self.assertEqual(sorted(names), [lloc.basename() for lloc in loc.list_files_sorted()])

    def test_get_filesystem(self):
        tests = [
            ("mock://foo", f"{uuid4()}", ["mock://foo", "mock://foo/blah"]),
            ("mock://foo/biz", f"{uuid4()}", ["mock://foo/biz", "mock://foo/biz/baz"]),
            ("file:///foo/biz", f"{uuid4()}", ["/foo/biz", "/foo/biz/baz"]),
        ]

        for pfx, mock_fs, _ in tests:
            storage.register_filesystem(pfx, mock_fs)

        for pfx, mock_fs, urls in tests:
            for url in urls:
                with self.subTest("Should get correct fs for prefix", pfx=pfx, url=url):
                    self.assertEqual(storage.get_filesystem(url), mock_fs)

        protocol = "".join(random.choice(ascii_lowercase) for _ in range(6))
        with mock.patch("fsspec.filesystem") as mock_get_fs:
            with self.subTest("Should request fsspec filesystem for protocol when no fs is registered"):
                storage.get_filesystem(f"{protocol}://tmp")
                mock_get_fs.assert_called_once_with(protocol)

    @mock.patch("fsspec.filesystem")
    def test_register_default_filesystem_for_protocol(self, mocks_fsspec_filesystem):
        protocol = "doom"
        storage_options = dict(foo="bar", biz="baz")
        storage.register_default_filesystem_for_protocol(protocol, storage_options=storage_options)
        mocks_fsspec_filesystem.assert_called_once_with(protocol, **storage_options)
        self.assertIn(f"{protocol}://", storage._filesystems)

    @mock.patch("fsspec.filesystem")
    def test_storage_options(self, *mocks):
        expected_storage_options = dict(boom="zoom", bob="frank")
        loc = storage.Location.with_location("foo://bar/biz", storage_options=expected_storage_options)
        self.assertEqual(loc.storage_options, expected_storage_options)
        self.assertEqual(loc.append_path("alskdf").storage_options, expected_storage_options)
        self.assertEqual(storage.absloc("ljlj", loc).storage_options, expected_storage_options)

    def test_mkdir(self):
        path = f"{self.scratch_folder}/{uuid4()}"
        loc = storage.Location.with_location(path)
        loc.mkdir()
        with self.assertRaises(FileExistsError):
            loc.mkdir()
        loc.mkdir(exists_ok=True)


if __name__ == '__main__':
    unittest.main()
