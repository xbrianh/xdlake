from collections import defaultdict
from dataclasses import dataclass

import pyarrow as pa


@dataclass
class Statistics:
    numRecords: int
    minValues: dict
    maxValues: dict
    nullCount: dict

    @classmethod
    def from_parquet_file_metadata(cls, md: pa.parquet.FileMetaData) -> "Statistics":
        md = md.to_dict()
        min_values: dict = defaultdict(dict)
        max_values: dict = defaultdict(dict)
        nullcounts: dict = defaultdict(int)
        for rg_info in md["row_groups"]:
            for col_info in rg_info["columns"]:
                column = col_info["path_in_schema"]
                if col_info["statistics"]["has_min_max"]:
                    if not min_values.get(column):
                        min_values[column] = col_info["statistics"]["min"]
                    else:
                        min_values[column] = min(min_values[column], col_info["statistics"]["min"])
                    if not max_values.get(column):
                        max_values[column] = col_info["statistics"]["max"]
                    else:
                        max_values[column] = max(max_values[column], col_info["statistics"]["max"])
                nullcounts[column] += col_info["statistics"]["null_count"]
        return cls(numRecords=md["num_rows"],
                   minValues=dict(min_values),
                   maxValues=dict(max_values),
                   nullCount=dict(nullcounts))

