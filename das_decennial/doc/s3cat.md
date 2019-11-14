s3cat is a program that we created to combine the multipart files
created by Spark into a single file. It uses the AWS S3 API
`create-multipart-upload`, `upload-part`, and
`complete-multipart-upload` to perform its work.

Ideally, this should be fast, because the files are already in
S3. Unfortunately, `create-multiparty-upload` requires that all
objects in S3 that are combined be >5MiB. As a result, parts that are
smaller are first _downloaded_ so that they can be uploaded.

Downloading objects is slow via the API:

* With our test file of 9,715,304,388 bytes in 5000 parts, all smaller than 5 MiB, it took our single threaded implementation roughly 60 minutes to download using the `get-object` API. 

* For comparision, downloading the 5000 files using `aws s3 cp --recursive` resulted in the files being downloaded in 1 minute, 13 seconds. The files were downloaded out-of-order, so they were clearly downloaded in multiple threads.

* We then combined the files with a program written in python that used `cat` to make 500 files, and uploaded the files with `aws s3 cp`, and all of the files uploaded in 1 minute, 48 seconds.

* We also tried uploading the single file that was created locally, and found that it took 1 min, 54 seconds. (So it's also clearly multi-threaded.)

Our options are obviously:

1. Use `aws s3 cp --recursive` to do the downloading, combine with cat, and upload with `aws s3 cp --recursive`. This has the advantage of using Amazon's code, which is presumably better than ours, but the disadvantage of significant data transfer to the MASTER node.
2. Make our program multi-threaded.
3. Write objects larger than 5MiB.
4. Just copy everything to a big disk, combine, and send back.


Given the various time constraints, we have modified s3cat so that he can do either `multipart` or `download` transactions, and have made download the default.