import os
import unittest
from uuid import uuid4
from tempfile import TemporaryDirectory

from xdlake import storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.scratch_folder = os.path.abspath(self.td.name)

    def tearDown(self):
        self.td.cleanup()

    def test_resolution(self):
        os.chdir(self.scratch_folder)
        name = f"{uuid4()}"
        tests = [
            ("local absolute", f"{self.scratch_folder}/tests", f"{self.scratch_folder}/tests/foo/{name}"),
            ("local relative", "tests", f"{os.getcwd()}/tests/foo/{name}"),
            ("local file url", f"file://{self.scratch_folder}/tests", f"{self.scratch_folder}/tests/foo/{name}"),
            ("s3", "s3://test-xdlake/tests", f"s3://test-xdlake/tests/foo/{name}"), 
        ]
        for test_name, url, expected_path in tests:
            with self.subTest(test_name):
                sob = storage.StorageObject.with_location(url)
                new_loc = sob.append_path("foo", name)
                self.assertEqual(new_loc.path, expected_path)
                d = os.urandom(7)
                with storage.open(new_loc, mode="wb") as fh:
                    fh.write(d)
                with storage.open(new_loc, mode="rb") as fh:
                    self.assertEqual(fh.read(), d)

    def test_listing(self):
        names = [f"{uuid4()}" for _ in range(11)]
        for name in names:
            with open(f"{self.scratch_folder}/{name}", "wb") as fh:
                fh.write(os.urandom(7))
        loc = storage.StorageObject.with_location(self.scratch_folder)
        self.assertEqual(sorted(names), [so.loc.basename() for so in loc.list_files_sorted()])


if __name__ == '__main__':
    unittest.main()
