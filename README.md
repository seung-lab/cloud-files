[![PyPI version](https://badge.fury.io/py/cloud-files.svg)](https://badge.fury.io/py/cloud-files)

CloudFiles: Fast access to cloud storage and local FS.
========

```python
from cloudfiles import CloudFiles

cf = CloudFiles('gs://bucket', progress=True) # s3://, https://, and file:// also supported
results = cf.get(['file1', 'file2', 'file3', ..., 'fileN']) # threaded
file1 = cf['file1']
part  = cf['file1', 0:30] # first 30 bytes of file1

cf.put('filename', content)
cf.put_json('filename', content)
cf.puts([{
    'path': 'filename',
    'content': content,
}, ... ]) # automatically threaded
cf.put_jsons(...) # same as puts
cf['filename'] = content

for fname in cf.list(prefix='abc123'):
    print(fname)
list(cf) # same as list(cf.list())

cf.delete('filename')
del cf['filename']
cf.delete([ 'filename_1', 'filename_2', ... ]) # threaded

boolean = cf.exists('filename')
results = cf.exists([ 'filename_1', ... ]) # threaded
```

CloudFiles was developed to access files from object storage without ever touching disk. The goal was to reliably and rapidly access a petabyte of image data broken down into tens to hundreds of millions of files being accessed in parallel across thousands of cores. The predecessor of CloudFiles, `CloudVolume.Storage`, the core of which is retained here, has been used to processes dozens of images, many of which were in the hundreds of terabyte range. Storage has reliably read and written tens of billions of files to date.

## Highlights

1. Fast file access with transparent threading.
2. Google Cloud Storage, Amazon S3, local filesystems, and arbitrary web servers making hybrid or multi-cloud easy.
3. Robust to flaky network connections. Uses exponential random window retries to avoid network collisions on a large cluster.
4. gzip and brotli\* compression.
5. Supports HTTP Range reads.
6. Supports green threads, which are important for achieving maximum performance on virtualized servers.

\* Except on Google Cloud Storage.

## Installation 

```bash
pip install cloud-files
```

You may wish to install credentials under `~/.cloudvolume/secrets`. See [this link](https://github.com/seung-lab/cloud-volume#credentials) for details. CloudFiles is descended from CloudVolume, and for now we'll leave the same configuration structure in place. 

## Documentation  

Note that the "Cloud Costs" mentioned below are current as of June 2020 and are subject to change. As of this writing, S3 and Google use identical cost structures for these operations.  

### Constructor
```python
# import gevent.monkey
# gevent.monkey.patch_all(thread=False)
from cloudfiles import CloudFiles

cf = CloudFiles(
    cloudpath, progress=False, 
    green=False, secrets=None, num_threads=20,
    use_https=False, endpoint=None
)

# cloudpath examples:
cf = CloudFiles('gs://bucket/') # google cloud storage
cf = CloudFiles('s3://bucket/') # Amazon S3
cf = CloudFiles('file:///home/coolguy/') # local filesystem
cf = CloudFiles('https://website.com/coolguy/') # arbitrary web server
```

* cloudpath: The path to the bucket you are accessing. The path is formatted as `$PROTOCOL://BUCKET/PATH`. Files will then be accessed relative to the path. The protocols supported are `gs` (GCS), `s3` (AWS S3), `file` (local FS), and `http`/`https`.
* progress: Whether to display a progress bar when processing multiple items simultaneously.
* green: Use green threads. For this to work properly, you must uncomment the top two lines.
* secrets: Provide secrets dynamically rather than fetching from the credentials directory `$HOME/.cloudvolume/secrets`.
* num_threads: Number of simultaneous requests to make. Usually 20 per core is pretty close to optimal unless file sizes are extreme.
* use_https: `gs://` and `s3://` require credentials to access their files. However, each has a read-only https endpoint that sometimes requires no credentials. If True, automatically convert `gs://` to `https://storage.googleapis.com/` and `s3://` to `https://s3.amazonaws.com/`.
* endpoint: (s3 only) provide an alternate endpoint than the official Amazon servers. This is useful for accessing the various S3 emulators offered by on-premises deployments of object storage.

### get / get_json

```python
# Let 'filename' be the file b'hello world'

binary = cf.get('filename')
binary = cf['filename']
>> b'hello world'

binaries = cf.get(['filename1', 'filename2'])
>> [ { 'path': 'filename1', 'content': b'...', 'byte_range': (None, None), 'error': None }, { 'path': 'filename2', 'content': b'...', 'byte_range': (None, None), 'error': None } ]

binary = cf.get({ 'path': 'filename', 'start': 0, 'end': 5 }) # fetches 5 bytes
binary = cf['filename', 0:5] # only fetches 5 bytes
binary = cf['filename'][0:5] # same result, fetches 11 bytes
>> b'hello' # represents byte range 0-4 inclusive of filename
```

`get` supports several different styles of input. The simplest takes a scalar filename and returns the contents of that file. However, you can also specify lists of filenames, a byte range request, and lists of byte range requests. You can provide a generator or iterator as input as well. 

When more than one file is provided at once, the download will be threaded using preemptive or cooperative (green) threads depending on the `green` setting. If `progress` is set to true, a progress bar will be displayed that counts down the number of files to download.

`get_json` is the same as get but it will parse the returned binary as JSON data encoded as utf8 and returns a dictionary.

Cloud Cost: Usually about $0.40 per million requests.

### put / puts / put_json / put_jsons

```python 
cf.put('filename', b'content')
cf['filename'] = b'content'
cf.put_json('digits', [1,2,3,4,5])

cf.puts([{ 
   'path': 'filename',
   'content': b'...',
   'content_type': 'application/octet-stream',
   'compress': 'gzip',
   'compression_level': 6, # parameter for gzip or brotli compressor
   'cache_control': 'no-cache',
}])

cf.puts([ (path, content), (path, content) ], compression='gzip')
cf.put_jsons(...)

# Definition of put, put_json is identical
def put(
    self, 
    path, content,     
    content_type=None, compress=None, 
    compression_level=None, cache_control=None
)

# Definition of puts, put_jsons is identical
def puts(
    self, files, 
    content_type=None, compress=None, 
    compression_level=None, cache_control=None
)
```

The PUT operation is the most complex operation because it's so configurable. Sometimes you want one file, sometimes many. Sometimes you want to configure each file individually, sometimes you want to standardize a bulk upload. Sometimes it's binary data, but oftentimes it's JSON. We provide a simpler interface for uploading a single file `put` and `put_json` (singular) versus the interface for uploading possibly many files `puts` and `put_jsons` (plural). 

In order to upload many files at once (which is much faster due to threading), you need to minimally provide the `path` and `content` for each file. This can be done either as a dict containing those fields or as a tuple `(path, content)`. If dicts are used, the fields (if present) specified in the dict take precedence over the parameters of the function. You can mix tuples with dicts. The input to puts can be a scalar (a single dict or tuple) or an iterable such as a list, iterator, or generator.  

Cloud Cost: Usually about $5 per million files.

### delete

```python 
cf.delete('filename')
cf.delete([ 'file1', 'file2', ... ])
del cf['filename']
```

This will issue a delete request for each file specified in a threaded fashion.

Cloud Cost: Usually free.

### exists 

```python 
cf.exists('filename') 
>> True # or False

cf.exists([ 'file1', 'file2', ... ]) 
>> { 'file1': True, 'file2': False, ... }
```

Scalar input results in a simple boolean output while iterable input returns a dictionary of input paths mapped to whether they exist. In iterable mode, a progress bar may be displayed and threading is utilized to improve performance. 

Cloud Cost: Usually about $0.40 per million requests.

### list

```python 
cf.list() # returns generator
list(cf) # same as list(cf.list())
cf.list(prefix="abc")
cf.list(prefix="abc", flat=True)
```

Recall that in object storage, directories do not really exist and file paths are really a key-value mapping. The `list` operator will list everything under the `cloudpath` given in the constructor. The `prefix` operator allows you to efficiently filter some of the results. If `flat` is specified, the results will be filtered to return only a single "level" of the "directory" even though directories are fake. The entire set of all subdirectories will still need to be fetched.

Cloud Cost: Usually about $5 per million requests, but each request might list 1000 files. The list operation will continuously issue list requests lazily as needed.

## Credits

CloudFiles is derived from the [CloudVolume.Storage](https://github.com/seung-lab/cloud-volume/tree/master/cloudvolume/storage) system.  

Storage was initially created by William Silversmith and Ignacio Tartavull. It was maintained and improved by William Silversmith and includes improvements by Nico Kemnitz (extremely fast exists) and Ben Falk (brotli).







