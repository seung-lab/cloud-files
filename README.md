CloudFiles: Fast access to cloud storage and local FS.
========

```python
from cloudfiles import CloudFiles

cf = CloudFiles('gs://bucket/') # google cloud storage
cf = CloudFiles('s3://bucket/') # Amazon S3
cf = CloudFiles('file:///home/coolguy/') # local filesystem
cf = CloudFiles('https://website.com/coolguy/') # arbitrary web server

# more options
cf = CloudFiles(
    's3://bucket/', 
    num_threads=20, 
    progress=True, # display progress bar
    secrets=credential_json, # provide your own secrets
    green=False, # whether to use green threads
)


cf.get('filename')
cf.get([ 'filename_1', 'filename_2' ]) # threaded automatically

cf.put('filename', content)
cf.put_json('filename', content)
cf.puts([{
    'path': 'filename',
    'content': content,
}, ... ]) # automatically threaded
cf.put_jsons(...) # same as puts

cf.list()
cf.delete('filename')
cf.delete([ 'filename_1', 'filename_2', ... ]) # threaded

cf.exists('filename')
cf.exists([ filename_1, ... ]) # threaded
```

CloudFiles is a pure python client for accessing cloud storage or the local file system in a threaded fashion without hassle. 

### Highlights

1. Fast file access due to transparent threading.
2. Supports Google Cloud Storage, Amazone S3, local filesystems, and arbitrary web servers with a similar file access structure making hybrid or multi-cloud easy.
3. Robust to flaky network connections. Retries using an exponential random window to avoid network collisions when working in a large cluster.
4. Supports gzip and brotli\* compression.
5. Supports HTTP Range reads.
6. Supports green threads, which are important for achieving maximum performance on virtualized servers.

\* Except on Google Cloud Storage.

### Credits

CloudFiles is derived from the [CloudVolume.Storage](https://github.com/seung-lab/cloud-volume/tree/master/cloudvolume/storage) system.  

Storage was initially created by William Silversmith and Ignacio Tartavull. It was maintained and improved by William Silversmith and includes improvements by Nico Kemnitz (extremely fast exists) and Ben Falk (brotli).







