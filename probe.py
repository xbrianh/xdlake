from enum import Enum

class WriteMode(Enum):
    append = "Append"
    overwrite = "Overwrite"
    error = "Error"
    ignore = "Ignore"

WriteMode["append"]
# WriteMode[WriteMode.append]
