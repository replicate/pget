# PGet - Parallel File Downloader & Extractor

PGet is a high performance, concurrent file downloader built in Go. It is designed to speed up and optimize file downloads from cloud storage services such as Amazon S3 and Google Cloud Storage.

The primary advantage of PGet is its ability to download files in parallel using multiple threads. By dividing the file into chunks and downloading multiple chunks simultaneously, PGet significantly reduces the total download time for large files.

If the downloaded file is a tar archive, PGet can automatically extract the contents of the archive in memory, thus removing the need for an additional extraction step.

The efficiency of PGet's tar extraction lies in its approach to handling data. Instead of writing the downloaded tar file to disk and then reading it back into memory for extraction, PGet conducts the extraction directly from the in-memory download buffer. This method avoids unnecessary memory copies and disk I/O, leading to an increase in performance, especially when dealing with large tar files. This makes PGet not just a parallel downloader, but also an efficient file extractor, providing a streamlined solution for fetching and unpacking files.


## Usage


    pget <url> <dest> [-c concurrency] [-x]

Parameters
- \<url\>: The URL of the file to download.
- \<dest\>: The destination where the downloaded file will be stored.
- -c concurrency: The number of concurrent downloads. Default is 4 times the number of cores.
- -x: Extract the tar file after download. If not set, the downloaded file will be saved as is.

Example

    pget https://storage.googleapis.com/replicant-misc/sd15.tar ./sd15 -x

This command will download Stable Diffusion 1.5 weights to the path ./sd15 with high concurrency. After the file is downloaded, it will be automatically extracted.

## Error Handling

PGet includes some error handling:

1. If a download any chunks fails, it will automatically retry up to 5 times before giving up.
2. If the downloaded file size does not match the expected size, it will also retry the download.

## Dependencies

PGet is built in Go and has no external dependencies beyond the Go standard library.

## Building

To build PGet, you need a working Go environment. You can build the binary with the following 

    make

This builds a static binary that can work inside containers.

## Future Improvements

- as chunks are downloaded, start either writing to disk or extracting
- can we check the content hash of the file in the background?
- support for zip files?