![Common Crawl Logo](http://commoncrawl.org/wp-content/uploads/2012/04/ccLogo.png)

# Common Crawl WARC Examples

This repository contains both wrappers for processing WARC files in Hadoop MapReduce jobs and also Hadoop examples to get you started.

There are three examples for Hadoop processing:

+ [WARC files] HTML tag frequency counter using raw HTTP responses
+ [WAT files] Server response analysis using response metadata
+ [WET files] Classic word count example using extracted text

All three assume initially that the files are stored locally but can be trivially modified to pull them down from Common Crawl's Amazon S3 bucket.
To acquire the files, you can use [S3Cmd](http://s3tools.org/s3cmd) or similar.

    s3cmd get s3://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/warc/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.gz

# License

MIT License, as per `LICENSE`
