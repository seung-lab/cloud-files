CloudFiles: Fast access to Cloud Storage and local FS.
========

```python
from cloudfiles import CloudFiles

cf = CloudFiles('gs://bucket/')
cf = CloudFiles('s3://bucket/')
cf = CloudFiles('file:///home/coolguy/')
cf = CloudFiles('http://website.com/coolguy/')

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