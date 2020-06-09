[![PyPI version](https://badge.fury.io/py/cloud-files.svg)](https://badge.fury.io/py/cloud-files)

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

## Highlights

1. Fast file access due to transparent threading.
2. Supports Google Cloud Storage, Amazone S3, local filesystems, and arbitrary web servers with a similar file access structure making hybrid or multi-cloud easy.
3. Robust to flaky network connections. Retries using an exponential random window to avoid network collisions when working in a large cluster.
4. Supports gzip and brotli\* compression.
5. Supports HTTP Range reads.
6. Supports green threads, which are important for achieving maximum performance on virtualized servers.

\* Except on Google Cloud Storage.

## Installation 

```bash
pip install cloud-files
```

You may wish to install credentials under `~/.cloudvolume/secrets`. See [this link](https://github.com/seung-lab/cloud-volume#credentials) for details. CloudFiles is descended from CloudVolume, and for now we'll leave the same configuration structure in place. 

## Documentation  

### Constructor
```python
# import gevent.monkey
# gevent.monkey.patch_all(thread=False)
from cloudfiles import CloudFiles

cf = CloudFiles(
    cloudpath, progress=False, 
    green=False, secrets=None, num_threads=20
)
```

* cloudpath: The path to the bucket you are accessing. The path is formatted as `$PROTOCOL://BUCKET/PATH`. Files will then be accessed relative to the path. The protocols supported are `gs` (GCS), `s3` (AWS S3), `file` (local FS), and `http`/`https`.
* progress: Whether to display a progress bar when processing multiple items simultaneously.
* green: Use green threads. For this to work properly, you must uncomment the top two lines.
* secrets: Provide secrets dynamically rather than fetching from the credentials directory `$HOME/.cloudvolume/secrets`.
* num_threads: Number of simultaneous requests to make. Usually 20 per core is pretty close to optimal unless file sizes are extreme.

### get / get_json

```python
binary = cf.get('filename')
>> b'...'

binaries = cf.get(['filename1', 'filename2'])
>> [ { 'path': 'filename1', 'content': b'...', 'byte_range': (None, None), 'error': None }, { 'path': 'filename2', 'content': b'...', 'byte_range': (None, None), 'error': None } ]

binary = cf.get({ 'path': 'filename', 'start': 0, 'end': 1024 })
>> b'...' # represents byte range 0-1024 of filename
```

`get` supports several different styles of input. The simplest takes a scalar filename and returns the contents of that file. However, you can also specify lists of filenames, a byte range request, and lists of byte range requests. You can provide a generator or iterator as input as well. 

When more than one file is provided at once, the download will be threaded using preemptive or cooperative (green) threads depending on the `green` setting. If `progress` is set to true, a progress bar will be displayed that counts down the number of files to download.

`get_json` is the same as get but it will parse the returned binary as JSON data encoded as utf8 and returns a dictionary.

### put / puts / put_json / put_jsons

```python 
cf.put('filename', b'content')
cf.put_json('digits', [1,2,3,4,5])

cf.puts([{ 
   'path': 'filename',
   'content': b'...',
   'content_type': 'application/octet-stream',
   'compress': 'gzip',
   'compression_level': 6,
   'cache_control': 'no-cache',
}])
```


### delete

```python 
cf.delete('filename')
cf.delete([ 'file1', 'file2', ... ])
```

This will issue a delete request for each file specified in a threaded fashion.

### exists 

```python 
cf.exists('filename') 
>> True # or False

cf.exists([ 'file1', 'file2', ... ]) 
>> { 'file1': True, 'file2': False, ... }
```

Scalar input results in a simple boolean output while iterable input returns a dictionary of input paths mapped to whether they exist. In iterable mode, a progress bar may be displayed and threading is utilized to improve performance. 

### list

```python 
cf.list()
cf.list(prefix="abc")
cf.list(prefix="abc", flat=True)
```

Recall that in object storage, directories do not really exist and file paths are really a key-value mapping. The `list` operator will list everything under the `cloudpath` given in the constructor. The `prefix` operator allows you to efficiently filter some of the results. If `flat` is specified, the results will be filtered to return only a single "level" of the "directory" even though directories are fake. The entire set of all subdirectories will still need to be fetched.

## Credits

CloudFiles is derived from the [CloudVolume.Storage](https://github.com/seung-lab/cloud-volume/tree/master/cloudvolume/storage) system.  

Storage was initially created by William Silversmith and Ignacio Tartavull. It was maintained and improved by William Silversmith and includes improvements by Nico Kemnitz (extremely fast exists) and Ben Falk (brotli).







