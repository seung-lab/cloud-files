[![PyPI version](https://badge.fury.io/py/cloud-files.svg)](https://badge.fury.io/py/cloud-files) [![Build Status](https://travis-ci.com/seung-lab/cloud-files.svg?branch=master)](https://travis-ci.com/seung-lab/cloud-files)

CloudFiles: Fast access to cloud storage and local FS.
========

```python
from cloudfiles import CloudFiles, dl

results = dl(["gs://bucket/file1", "gs://bucket2/file2", ... ]) # shorthand

cf = CloudFiles('gs://bucket', progress=True) # s3://, https://, and file:// also supported
results = cf.get(['file1', 'file2', 'file3', ..., 'fileN']) # threaded
results = cf.get(paths, parallel=2) # threaded and two processes
file1 = cf['file1']
part  = cf['file1', 0:30] # first 30 bytes of file1

cf.put('filename', content)
cf.put_json('filename', content)
cf.puts([{
    'path': 'filename',
    'content': content,
}, ... ]) # automatically threaded
cf.puts(content, parallel=2) # threaded + two processes
cf.puts(content, storage_class="NEARLINE") # apply vendor-specific storage class

cf.put_jsons(...) # same as puts
cf['filename'] = content

for fname in cf.list(prefix='abc123'):
    print(fname)
list(cf) # same as list(cf.list())

cf.delete('filename')
del cf['filename']
cf.delete([ 'filename_1', 'filename_2', ... ]) # threaded
cf.delete(paths, parallel=2) # threaded + two processes

boolean = cf.exists('filename')
results = cf.exists([ 'filename_1', ... ]) # threaded
```

CloudFiles was developed to access files from object storage without ever touching disk. The goal was to reliably and rapidly access a petabyte of image data broken down into tens to hundreds of millions of files being accessed in parallel across thousands of cores. The predecessor of CloudFiles, `CloudVolume.Storage`, the core of which is retained here, has been used to processes dozens of images, many of which were in the hundreds of terabyte range. Storage has reliably read and written tens of billions of files to date.

## Highlights

1. Fast file access with transparent threading and optionally multi-process.
2. Google Cloud Storage, Amazon S3, local filesystems, and arbitrary web servers making hybrid or multi-cloud easy.
3. Robust to flaky network connections. Uses exponential random window retries to avoid network collisions on a large cluster. Validates md5 for gcs and s3.
4. gzip, brotli, and zstd compression.
5. Supports HTTP Range reads.
6. Supports green threads, which are important for achieving maximum performance on virtualized servers.
7. High efficiency transfers that avoid compression/decompression cycles.
8. High speed gzip decompression using libdeflate (compared with zlib).
9. Bundled CLI tool.
10. Accepts iterator and generator input.

## Installation 

```bash
pip install cloud-files
pip install cloud-files[test] # to enable testing with pytest
```

If you run into trouble installing dependenies, make sure you're using at least Python3.6 and you have updated pip. On Linux, some dependencies require manylinux2010 or manylinux2014 binaries which earlier versions of pip do not search for. MacOS, Linux, and Windows are supported platforms.

### Credentials

You may wish to install credentials under `~/.cloudvolume/secrets`. CloudFiles is descended from CloudVolume, and for now we'll leave the same configuration structure in place. 

You need credentials only for the services you'll use. The local filesystem doesn't need any. Google Storage ([setup instructions here](https://github.com/seung-lab/cloud-volume/wiki/Setting-up-Google-Cloud-Storage)) will attempt to use default account credentials if no service account is provided.  

If neither of those two conditions apply, you need a service account credential. `google-secret.json` is a service account credential for Google Storage, `aws-secret.json` is a service account for S3, etc. You can support multiple projects at once by prefixing the bucket you are planning to access to the credential filename. `google-secret.json` will be your defaut service account, but if you also want to also access bucket ABC, you can provide `ABC-google-secret.json` and you'll have simultaneous access to your ordinary buckets and ABC. The secondary credentials are accessed on the basis of the bucket name, not the project name.

```bash
mkdir -p ~/.cloudvolume/secrets/
mv aws-secret.json ~/.cloudvolume/secrets/ # needed for Amazon
mv google-secret.json ~/.cloudvolume/secrets/ # needed for Google
mv matrix-secret.json ~/.cloudvolume/secrets/ # needed for Matrix
```

#### `aws-secret.json` and `matrix-secret.json`

Create an [IAM user service account](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users.html) that can read, write, and delete objects from at least one bucket.

```json
{
  "AWS_ACCESS_KEY_ID": "$MY_AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY": "$MY_SECRET_ACCESS_TOKEN",
  "AWS_DEFAULT_REGION": "$MY_AWS_REGION" // defaults to us-east-1
}
```

#### `google-secret.json`

You can create the `google-secret.json` file [here](https://console.cloud.google.com/iam-admin/serviceaccounts). You don't need to manually fill in JSON by hand, the below example is provided to show you what the end result should look like. You should be able to read, write, and delete objects from at least one bucket.

```json
{
  "type": "service_account",
  "project_id": "$YOUR_GOOGLE_PROJECT_ID",
  "private_key_id": "...",
  "private_key": "...",
  "client_email": "...",
  "client_id": "...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": ""
}
```

## API Documentation  

Note that the "Cloud Costs" mentioned below are current as of June 2020 and are subject to change. As of this writing, S3 and Google use identical cost structures for these operations.  

### Constructor
```python
# import gevent.monkey # uncomment when using green threads
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
cf = CloudFiles('mem:///home/coolguy/') # in memory
cf = CloudFiles('https://website.com/coolguy/') # arbitrary web server
```

* cloudpath: The path to the bucket you are accessing. The path is formatted as `$PROTOCOL://BUCKET/PATH`. Files will then be accessed relative to the path. The protocols supported are `gs` (GCS), `s3` (AWS S3), `file` (local FS), `mem` (RAM), and `http`/`https`.
* progress: Whether to display a progress bar when processing multiple items simultaneously.
* green: Use green threads. For this to work properly, you must uncomment the top two lines.
* secrets: Provide secrets dynamically rather than fetching from the credentials directory `$HOME/.cloudvolume/secrets`.
* num_threads: Number of simultaneous requests to make. Usually 20 per core is pretty close to optimal unless file sizes are extreme.
* use_https: `gs://` and `s3://` require credentials to access their files. However, each has a read-only https endpoint that sometimes requires no credentials. If True, automatically convert `gs://` to `https://storage.googleapis.com/` and `s3://` to `https://s3.amazonaws.com/`.
* endpoint: (s3 only) provide an alternate endpoint than the official Amazon servers. This is useful for accessing the various S3 emulators offered by on-premises deployments of object storage.  

The advantage of using `mem://` versus a `dict` has both the advantage of using identical interfaces in your code and it will use compression  automatically.

### get / get_json

```python
# Let 'filename' be the file b'hello world'

binary = cf.get('filename')
binary = cf['filename']
>> b'hello world'

compressed_binary = cf.get('filename', raw=True) 

binaries = cf.get(['filename1', 'filename2'])
>> [ { 'path': 'filename1', 'content': b'...', 'byte_range': (None, None), 'error': None }, { 'path': 'filename2', 'content': b'...', 'byte_range': (None, None), 'error': None } ]

# total provides info for progress bar when using generators.
binaries = cf.get(generator(), total=N) 

binary = cf.get({ 'path': 'filename', 'start': 0, 'end': 5 }) # fetches 5 bytes
binary = cf['filename', 0:5] # only fetches 5 bytes
binary = cf['filename'][0:5] # same result, fetches 11 bytes
>> b'hello' # represents byte range 0-4 inclusive of filename

binaries = cf[:100] # download the first 100 files
```

`get` supports several different styles of input. The simplest takes a scalar filename and returns the contents of that file. However, you can also specify lists of filenames, a byte range request, and lists of byte range requests. You can provide a generator or iterator as input as well. Order is not guaranteed.

When more than one file is provided at once, the download will be threaded using preemptive or cooperative (green) threads depending on the `green` setting. If `progress` is set to true, a progress bar will be displayed that counts down the number of files to download.

`get_json` is the same as get but it will parse the returned binary as JSON data encoded as utf8 and returns a dictionary. Order is guaranteed.

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
    compression_level=None, cache_control=None,
    raw=False
)

# Definition of puts, put_jsons is identical
def puts(
    self, files, 
    content_type=None, compress=None, 
    compression_level=None, cache_control=None,
    total=None, raw=False
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

### size 

```python
cf.size('filename')
>>> 1337 # size in bytes

cf.size([ 'file1', 'nonexistent', 'empty_file', ... ])
>>> { 'file1': 392, 'nonexistent': None, 'empty_file': 0, ... }
```

The output is the size of each file as it is stored in bytes. If the file doesn't exist, `None` is returned. Scalar input results in a simple boolean output while iterable input returns a dictionary of input paths mapped to whether they exist. In iterable mode, a progress bar may be displayed and threading is utilized to improve performance. 

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

### transfer_to / transfer_from

```python
cff = CloudFiles('file:///source_location')
cfg = CloudFiles('gs://dest_location')

# Transfer all files from local filesys to google cloud storage
cfg.transfer_from(cff, block_size=64) # in blocks of 64 files
cff.transfer_to(cfg, block_size=64)
cff.transfer_to(cfg, block_size=64, reencode='br') # change encoding to brotli
cfg[:] = cff # default block size 64
```

Transfer semantics provide a simple way to perform bulk file transfers. Use the `block_size` parameter to adjust the number of files handled in a given pass. This can be important for preventing memory blow-up and reducing latency between batches.

### transcode

```python
from cloudfiles.compression import transcode

files = cf.get(...) 

for file in transcode(files, 'gzip'):
  file['content'] # gzipped file content regardless of source

transcode(files, 
  encoding='gzip', # any cf compatible compression scheme
  in_place=False, # modify the files in-place to save memory
  progress=True # progress bar
)
```

Sometimes we want to change the encoding type of a set of arbitrary files (often when moving them around to another storage system). `transcode` will take the output of `get` and transcode the resultant files into a new format. `transcode` respects the `raw` attribute which indicates that the contents are already compressed and will decompress them first before recompressing. If the input data are already compressed to the correct output encoding, it will simply pass it through without going through a decompression/recompression cycle.

`transcode` returns a generator so that the transcoding can be done in a streaming manner.

## Network Robustness

CloudFiles protects itself from network issues in several ways. 

First, it uses a connection pool to avoid needing to reestablish connections or exhausting the number of available sockets.  

Second, it uses an exponential random window backoff to retry failed connections and requests. The exponential backoff allows increasing breathing room for an overloaded server and the random window decorrelates independent attempts by a cluster. If the backoff was not growing, the retry attempts by a large cluster would be too rapid fire or inefficiently slow. If the attempts were not decorrellated, then regardless of the backoff, the servers will often all try again around the same time. We backoff seven times starting from 0.5 seconds to 60 seconds, doubling the random window each time.

Third, for Google Cloud Storage (GCS) and S3 endpoints, we compute the md5 digest both sending and receiving to ensure data corruption did not occur in transit and that the server sent the full response. We cannot validate the digest for partial ("Range") reads. For [composite objects](https://cloud.google.com/storage/docs/composite-objects) (GCS) we can check the [crc32c](https://pypi.org/project/crc32c/) check-sum which catches transmission errors but not tampering (though MD5 isn't secure at all anymore). We are unable to perform validation for [multi-part uploads](https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html) (S3). Using custom encryption keys may also create validation problems.

## CloudFiles CLI Tool

```bash
# list cloud and local directories
cloudfiles ls gs://bucket-folder/
# parallel file transfer, no decompression
cloudfiles -p 2 cp --progress -r s3://bkt/ gs://bkt2/
# change compression type to brotli
cloudfiles cp -c br s3://bkt/file.txt gs://bkt2/
# decompress
cloudfiles cp -c none s3://bkt/file.txt gs://bkt2/
# pass from stdin (use "-" for source argument)
find some_dir | cloudfiles cp - s3://bkt/
# Get human readable file sizes from anywhere
cloudfiles du -shc ./tmp gs://bkt/dir s3://bkt/dir
# remove files
cloudfiles rm ./tmp gs://bkt/dir/file s3://bkt/dir/file
```

### `cp` Pros and Cons

For the cp command, the bundled CLI tool has a number of advantages vs. `gsutil` when it comes to transfers.

1. No decompression of file transfers (unless you want to).
2. Can shift compression format.
3. Easily control the number of parallel processes.
4. Green threads make core utilization more efficient.
5. Optionally uses libdeflate for faster gzip decompression.

It also has some disadvantages:  

1. gs:// to gs:// transfers are looped through the executing machine.
2. Doesn't support all commands.
3. File suffixes may be added to signify compression type on the local filesystem (e.g. `.gz`, `.br`, or `.zstd`). `cloudfiles ls` will list them without the extension and they will be converted into `Content-Encoding` on cloud storage.

### `ls` Generative Expressions

For the `ls` command, we support (via the `-e` flag) simple generative expressions that enable querying multiple prefixes at once. A generative expression is denoted `[chars]` where `c`,`h`,`a`,`r`, & `s` will be inserted individually into the position where the expression appears. Multiple expressions are allowed and produce a cartesian product of resulting strings. This functionality is very limited at the moment but we intend to improve it.

```bash
cloudfiles ls -e "gs://bucket/prefix[ab]"
# equivalent to:
# cloudfiles ls gs://bucket/prefixa
# cloudfiles ls gs://bucket/prefixb
```

## Credits

CloudFiles is derived from the [CloudVolume.Storage](https://github.com/seung-lab/cloud-volume/tree/master/cloudvolume/storage) system.  

Storage was initially created by William Silversmith and Ignacio Tartavull. It was maintained and improved by William Silversmith and includes improvements by Nico Kemnitz (extremely fast exists) and Ben Falk (brotli). Manuel Castro added the ability to chose cloud storage class. 







