import base64
import binascii
from collections import defaultdict
import json
import os.path
import posixpath
import re

import boto3
import botocore
from glob import glob
import google.cloud.exceptions
from google.cloud.storage import Batch, Client
import requests
import tenacity

from .compression import COMPRESSION_TYPES
from .connectionpools import S3ConnectionPool, GCloudBucketPool, MemoryPool, MEMORY_DATA
from .lib import mkdir, sip, md5, PYTHON3

COMPRESSION_EXTENSIONS = ('.gz', '.br', '.zstd')
GZIP_TYPES = (True, 'gzip', 1)

# This is just to support pooling by bucket
class keydefaultdict(defaultdict):
  def __missing__(self, key):
    if self.default_factory is None:
      raise KeyError( key )
    else:
      ret = self[key] = self.default_factory(key)
      return ret

S3_POOL = None
GC_POOL = None
MEM_POOL = None
def reset_connection_pools():
  global S3_POOL
  global GC_POOL
  global MEM_POOL
  S3_POOL = keydefaultdict(lambda service: keydefaultdict(lambda bucket_name: S3ConnectionPool(service, bucket_name)))
  GC_POOL = keydefaultdict(lambda bucket_name: GCloudBucketPool(bucket_name))
  MEM_POOL = keydefaultdict(lambda bucket_name: MemoryPool(bucket_name))
  MEMORY_DATA = {}

reset_connection_pools()

retry = tenacity.retry(
  reraise=True, 
  stop=tenacity.stop_after_attempt(7), 
  wait=tenacity.wait_random_exponential(0.5, 60.0),
)

class StorageInterface(object):
  exists_batch_size = 1
  delete_batch_size = 1
  def release_connection(self):
    pass
  def __enter__(self):
    return self
  def __exit__(self, exception_type, exception_value, traceback):
    self.release_connection()

class FileInterface(StorageInterface):
  def __init__(self, path, secrets=None):
    super(StorageInterface, self).__init__()
    self._path = path

  def get_path_to_file(self, file_path):
    return os.path.join(self._path.path, file_path)

  def put_file(
    self, file_path, content, 
    content_type, compress, 
    cache_control=None,
    storage_class=None
  ):
    path = self.get_path_to_file(file_path)
    mkdir(os.path.dirname(path))

    # keep default as gzip
    if compress == "br":
      path += ".br"
    elif compress in GZIP_TYPES:
      path += ".gz"
    elif compress == "zstd":
      path += ".zstd"
    elif compress:
      raise ValueError("Compression type {} not supported.".format(compress))

    if content \
      and content_type \
      and re.search('json|te?xt', content_type) \
      and type(content) is str:

      content = content.encode('utf-8')

    try:
      with open(path, 'wb') as f:
        f.write(content)
    except IOError as err:
      with open(path, 'wb') as f:
        f.write(content)

  def head(self, file_path):
    raise NotImplementedError()

  def get_file(self, file_path, start=None, end=None):
    path = self.get_path_to_file(file_path)

    if os.path.exists(path + '.gz'):
      encoding = "gzip"
      path += '.gz'
    elif os.path.exists(path + '.br'):
      encoding = "br"
      path += ".br"
    elif os.path.exists(path + '.zstd'):
      encoding = "zstd"
      path += ".zstd"
    else:
      encoding = None

    try:
      with open(path, 'rb') as f:
        if start is not None:
          f.seek(start)
        if end is not None:
          start = start if start is not None else 0
          num_bytes = end - start
          data = f.read(num_bytes)
        else:
          data = f.read()
      return (data, encoding, None, None)
    except IOError:
      return (None, encoding, None, None)

  def size(self, file_path):
    path = self.get_path_to_file(file_path)

    exts = ('.gz', '.br', '.zstd', '')

    if PYTHON3:
      errors = (FileNotFoundError,)
    else:
      errors = (OSError,)

    for ext in exts:
      try:
        return os.path.getsize(path + ext)
      except errors:
        continue

    return None

  def exists(self, file_path):
    path = self.get_path_to_file(file_path)
    return os.path.exists(path) or any(( os.path.exists(path + ext) for ext in COMPRESSION_EXTENSIONS ))

  def files_exist(self, file_paths):
    return { path: self.exists(path) for path in file_paths }

  def delete_file(self, file_path):
    path = self.get_path_to_file(file_path)
    if os.path.exists(path):
      os.remove(path)
    elif os.path.exists(path + '.gz'):
      os.remove(path + '.gz')
    elif os.path.exists(path + ".br"):
      os.remove(path + ".br")

  def delete_files(self, file_paths):
    for path in file_paths:
      self.delete_file(path)

  def list_files(self, prefix, flat):
    """
    List the files in the layer with the given prefix. 

    flat means only generate one level of a directory,
    while non-flat means generate all file paths with that 
    prefix.
    """

    layer_path = self.get_path_to_file("")        
    path = os.path.join(layer_path, prefix) + '*'

    filenames = []

    remove = layer_path
    if len(remove) and remove[-1] != '/':
      remove += '/'

    if flat:
      for file_path in glob(path):
        if not os.path.isfile(file_path):
          continue
        filename = file_path.replace(remove, '')
        filenames.append(filename)
    else:
      subdir = os.path.join(layer_path, os.path.dirname(prefix))
      for root, dirs, files in os.walk(subdir):
        files = [ os.path.join(root, f) for f in files ]
        files = [ f.replace(remove, '') for f in files ]
        files = [ f for f in files if f[:len(prefix)] == prefix ]
        
        for filename in files:
          filenames.append(filename)
    
    def stripext(fname):
      (base, ext) = os.path.splitext(fname)
      if ext in COMPRESSION_EXTENSIONS:
        return base
      else:
        return fname

    filenames = list(map(stripext, filenames))
    return iter(_radix_sort(filenames))

class MemoryInterface(StorageInterface):
  def __init__(self, path, secrets=None):
    super(StorageInterface, self).__init__()
    self._path = path
    self._data = MEM_POOL[path.bucket].get_connection(secrets, None)

  def get_path_to_file(self, file_path):
    return os.path.join(
      self._path.path, file_path
    )

  def put_file(
    self, file_path, content, 
    content_type, compress, 
    cache_control=None,
    storage_class=None
  ):
    path = self.get_path_to_file(file_path)

    # keep default as gzip
    if compress == "br":
      path += ".br"
    elif compress in GZIP_TYPES:
      path += ".gz"
    elif compress == "zstd":
      path += ".zstd"
    elif compress:
      raise ValueError("Compression type {} not supported.".format(compress))

    if content \
      and content_type \
      and re.search('json|te?xt', content_type) \
      and type(content) is str:

      content = content.encode('utf-8')

    self._data[path] = content

  def get_file(self, file_path, start=None, end=None):
    path = self.get_path_to_file(file_path)

    if path + '.gz' in self._data:
      encoding = "gzip"
      path += '.gz'
    elif path + '.br' in self._data:
      encoding = "br"
      path += ".br"
    elif path + '.zstd' in self._data:
      encoding = "zstd"
      path += ".zstd"
    else:
      encoding = None

    result = self._data.get(path, None)
    if result:
      result = result[slice(start, end)]
    return (result, encoding, None, None)

  def head(self, file_path):
    raise NotImplementedError()

  def size(self, file_path):
    path = self.get_path_to_file(file_path)

    exts = ('.gz', '.br', '.zstd')

    for ext in exts:
      pathext = path + ext
      if pathext in self._data:
        return len(self._data[pathext])

    if path in self._data:
      data = self._data[path]
      if PYTHON3:
        if isinstance(data, bytes):
          return len(data)
        else:
          return len(data.encode('utf8'))
      else:
        return len(data)

    return None

  def exists(self, file_path):
    path = self.get_path_to_file(file_path)
    return path in self._data or any(( (path + ext in self._data) for ext in COMPRESSION_EXTENSIONS ))

  def files_exist(self, file_paths):
    return { path: self.exists(path) for path in file_paths }

  def delete_file(self, file_path):
    path = self.get_path_to_file(file_path)

    for suffix in ([''] + list(COMPRESSION_EXTENSIONS)):
      try:
        del self._data[path + suffix]
        break
      except KeyError:
        continue

  def delete_files(self, file_paths):
    for path in file_paths:
      self.delete_file(path)

  def list_files(self, prefix, flat):
    """
    List the files in the layer with the given prefix. 

    flat means only generate one level of a directory,
    while non-flat means generate all file paths with that 
    prefix.

    Returns: iterator
    """
    layer_path = self.get_path_to_file("")        
    path = os.path.join(layer_path, prefix) + '*'

    remove = layer_path
    if len(remove) and remove[-1] != '/':
      remove += '/'

    filenames = [ f.replace(remove, '') for f in self._data ]
    filenames = [ f for f in filenames if f[:len(prefix)] == prefix ]

    if flat:
      filenames = [ f for f in filenames if '/' not in f.replace(prefix, '') ]
    
    def stripext(fname):
      (base, ext) = os.path.splitext(fname)
      if ext in COMPRESSION_EXTENSIONS:
        return base
      else:
        return fname

    filenames = list(map(stripext, filenames))
    return iter(_radix_sort(filenames))

class GoogleCloudStorageInterface(StorageInterface):
  exists_batch_size = Batch._MAX_BATCH_SIZE
  delete_batch_size = Batch._MAX_BATCH_SIZE

  def __init__(self, path, secrets=None):
    super(StorageInterface, self).__init__()
    global GC_POOL
    self._path = path
    self._bucket = GC_POOL[path.bucket].get_connection(secrets, None)
    
  def get_path_to_file(self, file_path):
    return posixpath.join(self._path.path, file_path)

  @retry
  def put_file(self, file_path, content, content_type,
               compress, cache_control=None, storage_class=None):
    key = self.get_path_to_file(file_path)
    blob = self._bucket.blob( key )

    if compress == "br":
      blob.content_encoding = "br"
    elif compress in GZIP_TYPES:
      blob.content_encoding = "gzip"
    elif compress == "zstd":
      blob.content_encoding = "zstd"
    elif compress:
      raise ValueError("Compression type {} not supported.".format(compress))

    if cache_control:
      blob.cache_control = cache_control
    if storage_class:
      blob.storage_class = storage_class

    blob.md5_hash = md5(content)
    blob.upload_from_string(content, content_type)

  @retry
  def get_file(self, file_path, start=None, end=None):
    key = self.get_path_to_file(file_path)
    blob = self._bucket.blob( key )

    if start is not None:
      start = int(start)
    if end is not None:
      end = int(end - 1)

    try:
      content = blob.download_as_string(start=start, end=end, raw_download=True)
    except google.cloud.exceptions.NotFound as err:
      return (None, None, None, None)

    hash_type = "md5"
    hash_value = blob.md5_hash if blob.component_count is None else None

    if hash_value is None and blob.crc32c is not None:
      hash_type = "crc32c"
      hash_value = blob.crc32c

    return (content, blob.content_encoding, hash_value, hash_type)

  @retry
  def head(self, file_path):
    key = self.get_path_to_file(file_path)
    blob = self._bucket.get_blob(key)
    return {
      "Cache-Control": blob.cache_control,
      "Content-Length": blob.size,
      "Content-Type": blob.content_type,
      "ETag": blob.etag,
      "Last-Modified": blob.time_created,
      "Content-Md5": blob.md5_hash,
      "Content-Crc32c": blob.crc32c,
      "Content-Encoding": blob.content_encoding,
      "Content-Disposition": blob.content_disposition,
      "Content-Language": blob.content_language,
      "Storage-Class": blob.storage_class,
      "Component-Count": blob.component_count,
    }

  @retry
  def size(self, file_path):
    key = self.get_path_to_file(file_path)
    return self._bucket.get_blob(key).size

  @retry
  def exists(self, file_path):
    key = self.get_path_to_file(file_path)
    blob = self._bucket.blob(key)
    return blob.exists()

  @retry
  def files_exist(self, file_paths):
    result = { path: None for path in file_paths }

    for batch in sip(file_paths, self.exists_batch_size):
      # Retrieve current batch of blobs. On Batch __exit__ it will populate all
      # future responses before raising errors about the (likely) missing keys.
      try:
        with self._bucket.client.batch():
          for file_path in batch:
            key = self.get_path_to_file(file_path)
            result[file_path] = self._bucket.get_blob(key)
      except google.cloud.exceptions.NotFound as err:
        pass  # Missing keys are expected

    for file_path, blob in result.items():
      # Blob exists if ``dict``, missing if ``_FutureDict``
      result[file_path] = isinstance(blob._properties, dict)

    return result

  @retry
  def delete_file(self, file_path):
    key = self.get_path_to_file(file_path)
    
    try:
      self._bucket.delete_blob( key )
    except google.cloud.exceptions.NotFound:
      pass

  @retry
  def delete_files(self, file_paths):
    for batch in sip(file_paths, self.delete_batch_size):
      try:
        with self._bucket.client.batch():
          for file_path in batch:
            key = self.get_path_to_file(file_path)
            self._bucket.delete_blob(key)
      except google.cloud.exceptions.NotFound:
        pass

  @retry
  def list_files(self, prefix, flat=False):
    """
    List the files in the layer with the given prefix. 

    flat means only generate one level of a directory,
    while non-flat means generate all file paths with that 
    prefix.
    """
    layer_path = self.get_path_to_file("")        
    path = posixpath.join(layer_path, prefix)
    for blob in self._bucket.list_blobs(prefix=path):
      filename = blob.name.replace(layer_path, '')
      if not filename:
        continue
      elif not flat and filename[-1] != '/':
        yield filename
      elif flat and '/' not in blob.name.replace(path, ''):
        yield filename

  def release_connection(self):
    global GC_POOL
    GC_POOL[self._path.bucket].release_connection(self._bucket)

class HttpInterface(StorageInterface):
  def __init__(self, path, secrets=None):
    super(StorageInterface, self).__init__()
    self._path = path

  def get_path_to_file(self, file_path):
    return posixpath.join(self._path.host, self._path.path, file_path)

  # @retry
  def delete_file(self, file_path):
    raise NotImplementedError()

  def delete_files(self, file_paths):
    raise NotImplementedError()

  # @retry
  def put_file(self, file_path, content, content_type,
               compress, cache_control=None, storage_class=None):
    raise NotImplementedError()

  def head(self, file_path):
    raise NotImplementedError()

  @retry
  def get_file(self, file_path, start=None, end=None):
    key = self.get_path_to_file(file_path)

    if start is not None or end is not None:
      start = int(start) if start is not None else ''
      end = int(end - 1) if end is not None else ''
      headers = { "Range": "bytes={}-{}".format(start, end) }
      resp = requests.get(key, headers=headers)
    else:
      resp = requests.get(key)
    if resp.status_code in (404, 403):
      return (None, None, None, None)
    resp.raise_for_status()

    # Don't check MD5 for http because the etag can come in many
    # forms from either GCS, S3 or another service entirely. We
    # probably won't figure out how to decode it right.
    # etag = resp.headers.get('etag', None)
    content_encoding = resp.headers.get('Content-Encoding', None)

    # requests automatically decodes these
    if content_encoding in (None, '', 'gzip', 'deflate', 'br'):
      content_encoding = None
    
    return (resp.content, content_encoding, None, None)

  @retry
  def exists(self, file_path):
    key = self.get_path_to_file(file_path)
    resp = requests.get(key, stream=True)
    resp.close()
    return resp.ok

  def files_exist(self, file_paths):
    return {path: self.exists(path) for path in file_paths}

  def list_files(self, prefix, flat=False):
    raise NotImplementedError()

class S3Interface(StorageInterface):
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.delete_objects
  # claims batch size limit is 1000
  delete_batch_size = 1000
  def __init__(self, path, secrets=None):
    super(StorageInterface, self).__init__()
    global S3_POOL
    self._path = path
    self._conn = S3_POOL[path.protocol][path.bucket].get_connection(secrets, path.host)

  def get_path_to_file(self, file_path):
    return posixpath.join(self._path.path, file_path)

  @retry
  def put_file(
    self, file_path, content, 
    content_type, compress, 
    cache_control=None,
    ACL="bucket-owner-full-control",
    storage_class=None
  ):
    key = self.get_path_to_file(file_path)

    attrs = {
      'Bucket': self._path.bucket,
      'Body': content,
      'Key': key,
      'ContentType': (content_type or 'application/octet-stream'),
      'ACL': ACL,
      'ContentMD5': md5(content),
    }

    # keep gzip as default
    if compress == "br":
      attrs['ContentEncoding'] = 'br'
    elif compress in GZIP_TYPES:
      attrs['ContentEncoding'] = 'gzip'
    elif compress == "zstd":
      attrs['ContentEncoding'] = 'zstd'
    elif compress:
      raise ValueError("Compression type {} not supported.".format(compress))

    if cache_control:
      attrs['CacheControl'] = cache_control
    if storage_class:
      attrs['StorageClass'] = storage_class

    self._conn.put_object(**attrs)

  @retry
  def get_file(self, file_path, start=None, end=None):
    """
    There are many types of execptions which can get raised
    from this method. We want to make sure we only return
    None when the file doesn't exist.
    """

    kwargs = {}
    if start is not None or end is not None:
      start = int(start) if start is not None else ''
      end = int(end - 1) if end is not None else ''
      kwargs['Range'] = "bytes={}-{}".format(start, end)

    try:
      resp = self._conn.get_object(
        Bucket=self._path.bucket,
        Key=self.get_path_to_file(file_path),
        **kwargs
      )

      encoding = ''
      if 'ContentEncoding' in resp:
        encoding = resp['ContentEncoding']

      # s3 etags return hex digests but we need the base64 encoding
      # to make uniform comparisons. 
      # example s3 etag: "31ee76261d87fed8cb9d4c465c48158c"
      etag = resp.get('ETag', None)
      if etag is not None:
        etag = etag.lstrip('"').rstrip('"')
        etag = base64.b64encode(binascii.unhexlify(etag)).decode('utf8')

      return (resp['Body'].read(), encoding, etag, "md5")
    except botocore.exceptions.ClientError as err: 
      if err.response['Error']['Code'] == 'NoSuchKey':
        return (None, None, None, None)
      else:
        raise

  @retry
  def head(self, file_path):
    try:
      response = self._conn.head_object(
        Bucket=self._path.bucket,
        Key=self.get_path_to_file(file_path),
      )
      return {
        "Cache-Control": response.get("CacheControl", None),
        "Content-Length": response.get("ContentLength", None),
        "Content-Type": response.get("ContentType", None),
        "ETag": response.get("ETag", None),
        "Last-Modified": response.get("LastModified", None),
        "Content-Md5": response["ResponseMetadata"]["HTTPHeaders"].get("content-md5", None),
        "Content-Encoding": response.get("ContentEncoding", None),
        "Content-Disposition": response.get("ContentDisposition", None),
        "Content-Language": response.get("ContentLanguage", None),
        "Storage-Class": response.get("StorageClass", None),
        "Request-Charged": response.get("RequestCharged", None),
        "Parts-Count": response.get("PartsCount", None),
      }
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
        return None
      raise

  @retry
  def size(self, file_path):
    try:
      response = self._conn.head_object(
        Bucket=self._path.bucket,
        Key=self.get_path_to_file(file_path),
      )
      return response['ContentLength']
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
        return None
      raise

  def exists(self, file_path):
    exists = True
    try:
      self._conn.head_object(
        Bucket=self._path.bucket,
        Key=self.get_path_to_file(file_path),
      )
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
        exists = False
      else:
        raise
    
    return exists

  def files_exist(self, file_paths):
    return { path: self.exists(path) for path in file_paths }

  @retry
  def delete_file(self, file_path):

    # Not necessary to handle 404s here.
    # From the boto3 documentation:

    # delete_object(**kwargs)
    # Removes the null version (if there is one) of an object and inserts a delete marker, 
    # which becomes the latest version of the object. If there isn't a null version, 
    # Amazon S3 does not remove any objects.

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object

    self._conn.delete_object(
      Bucket=self._path.bucket,
      Key=self.get_path_to_file(file_path),
    )

  @retry
  def delete_files(self, file_paths):
    for batch in sip(file_paths, self.delete_batch_size):
      response = self._conn.delete_objects(
        Bucket=self._path.bucket,
        Delete={
          'Objects': [
            { 'Key': self.get_path_to_file(filepath) } for filepath in batch
          ],
          'Quiet': True
        },
      )

  def list_files(self, prefix, flat=False):
    """
    List the files in the layer with the given prefix. 

    flat means only generate one level of a directory,
    while non-flat means generate all file paths with that 
    prefix.
    """

    layer_path = self.get_path_to_file("")        
    path = posixpath.join(layer_path, prefix)

    @retry
    def s3lst(continuation_token=None):
      kwargs = {
        'Bucket': self._path.bucket,
        'Prefix': path,
      }

      if continuation_token:
        kwargs['ContinuationToken'] = continuation_token

      return self._conn.list_objects_v2(**kwargs)

    resp = s3lst()

    def iterate(resp):
      if 'Contents' not in resp.keys():
        resp['Contents'] = []

      for item in resp['Contents']:
        key = item['Key']
        filename = key.replace(layer_path, '')
        if not flat and filename[-1] != '/':
          yield filename
        elif flat and '/' not in key.replace(path, ''):
          yield filename


    for filename in iterate(resp):
      yield filename

    while resp['IsTruncated'] and resp['NextContinuationToken']:
      resp = s3lst(resp['NextContinuationToken'])

      for filename in iterate(resp):
        yield filename

  def release_connection(self):
    global S3_POOL
    S3_POOL[self._path.protocol][self._path.bucket].release_connection(self._conn)


def _radix_sort(L, i=0):
  """
  Most significant char radix sort
  """
  if len(L) <= 1: 
    return L
  done_bucket = []
  buckets = [ [] for x in range(255) ]
  for s in L:
    if i >= len(s):
      done_bucket.append(s)
    else:
      buckets[ ord(s[i]) ].append(s)
  buckets = [ _radix_sort(b, i + 1) for b in buckets ]
  return done_bucket + [ b for blist in buckets for b in blist ]
