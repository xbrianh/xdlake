import os
import unittest
from uuid import uuid4

from xdlake import storage


class TestStorage(unittest.TestCase):
    def test_storage(self):
        name = f"{uuid4()}"
        tests = [
            ("/tmp/tests", f"/tmp/tests/foo/{name}"),
            ("tmp/tests", f"{os.getcwd()}/tmp/tests/foo/{name}"),
            ("file:///tmp/tests", f"/tmp/tests/foo/{name}"),
            ("s3://test-xdlake/tests", f"s3://test-xdlake/tests/foo/{name}"), 
        ]
        for url, expected_path in tests:
            lfs = storage.StorageObject.resolve(url)
            new_loc = lfs.append_path("foo", name)
            self.assertEqual(new_loc.path, expected_path)
            d = os.urandom(8)
            with storage.open(new_loc, mode="wb") as fh:
                fh.write(d)
            with storage.open(new_loc, mode="rb") as fh:
                self.assertEqual(fh.read(), d)


if __name__ == '__main__':
    unittest.main()
