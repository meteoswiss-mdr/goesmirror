# Mirroring GOES-R series data from AWS

You can use the functions in this script to download large amounts of data from the GOES-R (currently GOES-16 and GOES-17) archive on Amazon Web Services (AWS) S3.

## Requirements

* Python 3.6+
* [Dask](https://dask.org/)
* [s3fs](https://github.com/dask/s3fs)

You can install the `dask` and `s3fs` packages from conda.

## Installation

Currently there is no installation script. Just copy `goesmirror.py` wherever you'd like to use it.

## Usage

Check the documentation for the `mirror_s3` function.

### Example

To download GOES-16 full-disk radiances for April 12, 2020 to `/data/goes`:
```python3
from datetime import datetime
import goesmirror

goesmirror.mirror_s3("/data/goes", ["ABI-L1b-RadF"], datetime(2020,4,12), datetime(2020,4,13),
    sats=["16"])
```

If you want e.g. specific channels, you can use the `fn_filter` argument to select files by the file name.
For example, to do the above example but only get channels 09 and 10:
```python3
def channel_filter(fn):
    channel = fn.split("_")[0][-2:] # extract channel from file name
    return (channel in ["09", "10"])
    
goesmirror.mirror_s3("/data/goes", ["ABI-L1b-RadF"], datetime(2020,4,12), datetime(2020,4,13),
    sats=["16"], fn_filter=channel_filter)
```

### Interrupted downloads

If `overwrite=False` (the default), `mirror_s3` will skip files that already exist on the local system and
are the same size as on S3. So if your large download gets interrupted, just use the same command again
and it should continue without downloading everything again.

### Migrating existing datasets

The `mirror_s3` function downloads data according to the directory structure used on AWS.
If you'd like to migrate earlier downloaded files to this structure, you can use the `organize_files` function.
