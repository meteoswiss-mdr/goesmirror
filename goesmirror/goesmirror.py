from typing import Callable, List
from datetime import datetime, timedelta
import os

import dask
import s3fs


def mirror_s3(
    mirror_dir: str,
    products: List[str],
    start_time: datetime,
    end_time: datetime,
    sats: List[str] = ["16"],
    overwrite: bool = False,
    test: bool = False,
    fn_filter: Callable[[str], bool] = None):
    """Mirror GOES-R series data from AWS to a local directory.

    Args:
        mirror_dir: Local path for GOES-R data, *not* including the
            "noaa-goesXX" directory name
        products: a list of the products you want to download, 
            e.g. ABI-L1b-RadF for full-disk radiances
        start_time: the start (inclusive) of the mirroring period
        end_time: the end (exclusive) of the mirroring period
        sats: the GOES-satellite numbers (as strings) in a list
        overwrite: if False, will only attempt to download files if
            they are not alredy present on the local directory or
            if the local file is a different size from the source file
        test: if True, will not download anything, just print what would be
            downloaded and where
        fn_filter: if not None, this should be a function that takes a
            product file name (without the directory) and returns True
            if that file should be downloaded
    """

    fs = s3fs.S3FileSystem(anon=True)

    def should_download(aws_file, local_file):
        fn = aws_file.split("/")[-1]
        if (fn_filter is not None) and not fn_filter(fn):
            return False
        file_parts = fn.split("_")
        timestamp = file_parts[3][1:-1]
        file_time = datetime.strptime(timestamp, "%Y%j%H%M%S")
        if not (start_time <= file_time < end_time):
            return False

        if not overwrite:
            local_exists = os.path.isfile(local_file)
            if local_exists:
                aws_size = fs.info(aws_file)["Size"]
                local_size = os.path.getsize(local_file)                
                return (aws_size != local_size)
            else:
                return True
        else:
            return True

    @dask.delayed
    def download(aws_file, local_file):
        fn = aws_file.split("/")[-1]
        print("Started downloading {}".format(fn))
        if not test:
            fs.get(aws_file,local_file)
        print("Finished downloading {}".format(fn))

    for product in products:
        for sat in sats:

            first_hour = datetime(start_time.year,start_time.month,
                start_time.day,start_time.hour)
            last_hour = datetime(end_time.year,end_time.month,
                end_time.day,end_time.hour)

            h = first_hour
            while h <= last_hour:
                
                try:
                    aws_dir = "s3://" + "/".join([
                        "noaa-goes{}".format(sat), product,
                        h.strftime("%Y"), h.strftime("%j"), h.strftime("%H")
                    ])
                    files = fs.ls(aws_dir)

                    local_dir = os.path.join(
                        mirror_dir, "noaa-goes{}".format(sat), product,
                        h.strftime("%Y"), h.strftime("%j"), h.strftime("%H")
                    )
                    if not test:
                        os.makedirs(local_dir, exist_ok=True)
                    
                    jobs = []
                    
                    for path in files:
                        fn = path.split("/")[-1]
                        aws_file = "/".join([aws_dir,fn])
                        local_file = os.path.join(local_dir,fn)

                        if should_download(aws_file, local_file):
                            jobs.append(download(aws_file, local_file))
                        else:
                            print("File {} already downloaded, skipping.".format(fn))

                    dask.compute(jobs, scheduler='processes')
                except FileNotFoundError:
                    print("Unable to find {}".format(aws_dir))                    
                finally:
                    h += timedelta(hours=1)


def organize_files(from_dir, to_dir, test=True):
    """ Organize an existing directory of GOES files to the directory
    structure used on AWS. Useful if you want to migrate an old dataset
    to the directory structure used in AWS.

    test=True produces an example output of what would be moved, set to False
    in order to do the final move
    """

    for (root, _, files) in os.walk(from_dir):
        for fn in files:
            if not fn.endswith(".nc"):
                continue

            from_path = os.path.join(root,fn)
            
            file_parts = fn.split("_")
            timestamp = file_parts[3]
            year = timestamp[1:5]
            day_of_year = timestamp[5:8]
            hour = timestamp[8:10]
            
            sat = file_parts[2][1:]
            product = "-".join(file_parts[1].split("-")[:-1])

            target_dir = os.path.join(
                to_dir, "noaa-goes{}".format(sat), product,
                year, day_of_year, hour
            )
            to_path = os.path.join(target_dir, fn)

            if test:
                print("From: {}".format(from_path))
                print("To: {}".format(to_path))
                return
            else:
                os.makedirs(target_dir, exist_ok=True)
                os.rename(from_path, to_path)
