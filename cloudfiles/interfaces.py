import base64
import binascii
from collections import defaultdict, namedtuple
from datetime import datetime
from io import BytesIO
import json
import os.path
import posixpath
import re

import boto3
import botocore
import gevent.monkey
import google.cloud.exceptions
from google.cloud.storage import Batch, Client
import requests
import shutil
import threading
import tenacity
import fasteners

from .compression import COMPRESSION_TYPES
from .connectionpools import S3ConnectionPool, GCloudBucketPool, MemoryPool, MEMORY_DATA
from .exceptions import MD5IntegrityError, CompressionError, AuthorizationError
from .lib import mkdir, sip, md5, encode_crc32c_b64, validate_s3_multipart_etag
from .secrets import (
  http_credentials,
  cave_credentials,
  CLOUD_FILES_DIR, 
  CLOUD_FILES_LOCK_DIR,
)

COMPRESSION_EXTENSIONS = ('.gz', '.br', '.zstd','.bz2','.xz')
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

S3_ACLS = {
  "tigerdata": "private",
}

S3ConnectionPoolParams = namedtuple('S3ConnectionPoolParams', 'service bucket_name request_payer')
GCloudBucketPoolParams = namedtuple('GCloudBucketPoolParams', 'bucket_name request_payer')
MemoryPoolParams = namedtuple('MemoryPoolParams', 'bucket_name')

GCS_BUCKET_POOL_LOCK = threading.Lock()
S3_BUCKET_POOL_LOCK = threading.Lock()
MEM_BUCKET_POOL_LOCK = threading.Lock()

def reset_connection_pools():
  global S3_POOL
  global GC_POOL
  global MEM_POOL
  global GCS_BUCKET_POOL_LOCK
  global S3_BUCKET_POOL_LOCK
  global MEM_BUCKET_POOL_LOCK

  with S3_BUCKET_POOL_LOCK:
    S3_POOL = keydefaultdict(lambda params: S3ConnectionPool(params.service, params.bucket_name))

  with GCS_BUCKET_POOL_LOCK:
    GC_POOL = keydefaultdict(lambda params: GCloudBucketPool(params.bucket_name, params.request_payer))
  
  with MEM_BUCKET_POOL_LOCK:
    MEM_POOL = keydefaultdict(lambda params: MemoryPool(params.bucket_name))
    MEMORY_DATA.clear()
  import gc
  gc.collect()

reset_connection_pools()

retry = tenacity.retry(
  reraise=True, 
  stop=tenacity.stop_after_attempt(7), 
  wait=tenacity.wait_random_exponential(0.5, 60.0),
)

def retry_if_not(exception_type):
  if type(exception_type) != list:
    exception_type = [ exception_type ]

  conditions = tenacity.retry_if_not_exception_type(exception_type[0])
  for et in exception_type[1:]:
    conditions = conditions | tenacity.retry_if_not_exception_type(et)

  return tenacity.retry(
    retry=conditions,
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

EXT_TEST_SEQUENCE = [
  ('', None),
  ('.gz', 'gzip'),
  ('.br', 'br'),
  ('.zstd', 'zstd'),
  ('.xz', 'xz'),
  ('.bz2', 'bzip2')
]
EXT_TEST_SEQUENCE_LOCK = threading.Lock()

def read_file(path, encoding, start, end):
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

class FileInterface(StorageInterface):
  def __init__(self, path, secrets=None, request_payer=None, locking=None, lock_dir=None, **kwargs):
    super(StorageInterface, self).__init__()
    self._path = path
    if request_payer is not None:
      raise ValueError("Specifying a request payer for the FileInterface is not supported. request_payer must be None, got '{}'.".format(request_payer))

    self.locking = locking
    self.lock_dir = lock_dir

  def io_with_lock(self, io_func, target, exclusive=False):
    if not self.locking:
      return io_func()
    else:
      abspath = os.path.abspath(target)
      input_bytes = abspath.encode('utf-8')
      crc_value = binascii.crc32(input_bytes)
      lock_path = os.path.join(self.lock_dir, f"{os.path.basename(target)}.{crc_value}")
      rw_lock = fasteners.InterProcessReaderWriterLock(lock_path)
      if exclusive:
        with rw_lock.write_lock():
          return io_func()
      else:
        with rw_lock.read_lock():
          return io_func()


  def get_path_to_file(self, file_path):
    return os.path.join(self._path.path, file_path)

  @classmethod
  def get_encoded_file_path(kls, path):
    global EXT_TEST_SEQUENCE
    
    with EXT_TEST_SEQUENCE_LOCK:
      seq = list(EXT_TEST_SEQUENCE)

    for i, (ext, encoding) in enumerate(seq):
      if os.path.exists(path + ext):
        if i > 0:
          with EXT_TEST_SEQUENCE_LOCK:
            EXT_TEST_SEQUENCE.insert(0, EXT_TEST_SEQUENCE.pop(i))
        return path + ext, encoding
    return '', None

  @classmethod
  def get_extension(kls, compress):
    if not compress:
      return ""
    elif compress == "br":
      return ".br"
    elif compress in GZIP_TYPES:
      return ".gz"
    elif compress == "zstd":
      return ".zstd"
    elif compress in ("xz", "lzma"):
      return ".xz"
    elif compress in ("bzip2", "bz2"):
      return ".bz2"
    elif compress:
      raise ValueError(f"Compression type {compress} not supported.")

  def put_file(
    self, file_path, content, 
    content_type, compress, 
    cache_control=None,
    storage_class=None
  ):
    path = self.get_path_to_file(file_path)
    compress_ext = self.get_extension(compress)
    _, ext = os.path.splitext(path)
    if ext != compress_ext:
      path += compress_ext

    if (
      content
      and type(content) is str
      and content_type
      and re.search('json|te?xt', content_type)
    ):

      content = content.encode('utf-8')

    def do_put_file():
      if hasattr(content, "read") and hasattr(content, "seek"):
        with open(path, 'wb') as f:
          shutil.copyfileobj(content, f)
        return

      try:
        with open(path, 'wb') as f:
          f.write(content)
      except IOError as err:
        mkdir(os.path.dirname(path))
        with open(path, 'wb') as f:
          f.write(content)

    return self.io_with_lock(do_put_file, path, exclusive=True)

  def head(self, file_path):
    path = self.get_path_to_file(file_path)

    path, encoding = self.get_encoded_file_path(path)

    def do_head():
      try:
        statinfo = os.stat(path)
      except FileNotFoundError:
        return None

      return {
        "Cache-Control": None,
        "Content-Length": statinfo.st_size,
        "Content-Type": None,
        "ETag": None,
        "Last-Modified": datetime.utcfromtimestamp(statinfo.st_mtime),
        "Content-Md5": None,
        "Content-Encoding": encoding,
        "Content-Disposition": None,
        "Content-Language": None,
        "Storage-Class": None,
        "Request-Charged": None,
        "Parts-Count": None,
      }

    return self.io_with_lock(do_head, path, exclusive=False)

  def get_file(self, file_path, start=None, end=None, part_size=None):
    global EXT_TEST_SEQUENCE
    global read_file
    path = self.get_path_to_file(file_path)

    def do_get_file():
      with EXT_TEST_SEQUENCE_LOCK:
        seq = list(EXT_TEST_SEQUENCE)

      i = 0
      try:
        for i, (ext, encoding) in enumerate(seq):
          try:
            return read_file(path + ext, encoding, start, end)
          except FileNotFoundError:
            continue
      finally:
        if i > 0:
          with EXT_TEST_SEQUENCE_LOCK:
            EXT_TEST_SEQUENCE.insert(0, EXT_TEST_SEQUENCE.pop(i))

      return (None, None, None, None)

    return self.io_with_lock(do_get_file, path, exclusive=False)

  def size(self, file_path):
    path = self.get_path_to_file(file_path)

    def do_size():
      with EXT_TEST_SEQUENCE_LOCK:
        exts = [ pair[0] for pair in EXT_TEST_SEQUENCE ]
      errors = (FileNotFoundError,)

      for ext in exts:
        try:
          return os.path.getsize(path + ext)
        except errors:
          continue

      return None

    return self.io_with_lock(do_size, path, exclusive=False)

  def exists(self, file_path):
    path = self.get_path_to_file(file_path)
    def do_exists():
      return os.path.exists(path) or any(( os.path.exists(path + ext) for ext in COMPRESSION_EXTENSIONS ))
    return self.io_with_lock(do_exists, path, exclusive=False)

  def files_exist(self, file_paths):
    return { path: self.exists(path) for path in file_paths }

  def delete_file(self, file_path):
    path = self.get_path_to_file(file_path)
    path, encoding = self.get_encoded_file_path(path)

    def do_delete_file():
      try:
        os.remove(path)
      except FileNotFoundError:
        pass

    return self.io_with_lock(do_delete_file, path, exclusive=True)

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
    path = os.path.join(layer_path, prefix)

    filenames = []

    remove = layer_path
    if len(remove) and remove[-1] != os.path.sep:
      remove += os.path.sep

    if flat:
      if os.path.isdir(path):
        list_path = path
        list_prefix = ''
        prepend_prefix = prefix
        if prepend_prefix and prepend_prefix[-1] != os.path.sep:
          prepend_prefix += os.path.sep
      else:  
        list_path = os.path.dirname(path)
        list_prefix = os.path.basename(prefix)
        prepend_prefix = os.path.dirname(prefix)
        if prepend_prefix != '':
          prepend_prefix += os.path.sep

      for fobj in os.scandir(list_path):
        if list_prefix != '' and not fobj.name.startswith(list_prefix):
          continue

        if fobj.is_dir():
          filenames.append(f"{prepend_prefix}{fobj.name}{os.path.sep}")  
        else:
          filenames.append(f"{prepend_prefix}{fobj.name}")
    else:
      subdir = os.path.join(layer_path, os.path.dirname(prefix))
      for root, dirs, files in os.walk(subdir):
        files = ( os.path.join(root, f) for f in files )
        files = ( f.removeprefix(remove) for f in files )
        files = ( f for f in files if f[:len(prefix)] == prefix )
        
        for filename in files:
          filenames.append(filename)
    
    def stripext(fname):
      (base, ext) = os.path.splitext(fname)
      if ext in COMPRESSION_EXTENSIONS:
        return base
      else:
        return fname

    filenames = list(map(stripext, filenames))
    filenames.sort()
    return iter(filenames)
  
class MemoryInterface(StorageInterface):
  def __init__(self, path, secrets=None, request_payer=None, **kwargs):
    global MEM_BUCKET_POOL_LOCK

    super(StorageInterface, self).__init__()
    if request_payer is not None:
      raise ValueError("Specifying a request payer for the MemoryInterface is not supported. request_payer must be None, got '{}'.", request_payer)
    self._path = path

    with MEM_BUCKET_POOL_LOCK:
      pool = MEM_POOL[MemoryPoolParams(path.bucket)]
    self._data = pool.get_connection(secrets, None)

  def get_path_to_file(self, file_path):
    return posixpath.join(
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
    elif compress in ("xz", "lzma"):
      path += ".xz"
    elif compress in ("bzip2", "bz2"):
      path += ".bz2"
    elif compress:
      raise ValueError("Compression type {} not supported.".format(compress))

    if content \
      and content_type \
      and re.search('json|te?xt', content_type) \
      and type(content) is str:

      content = content.encode('utf-8')

    if hasattr(content, "read") and hasattr(content, "seek"):
      self._data[path] = content.read()
    else:
      self._data[path] = content

  def get_file(self, file_path, start=None, end=None, part_size=None):
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
    elif path + '.xz' in self._data:
      encoding = "xz" # aka lzma
      path += ".xz"
    elif path + '.bz2' in self._data:
      encoding = "bzip2"
      path += ".bz2"
    else:
      encoding = None

    result = self._data.get(path, None)
    if result:
      result = result[slice(start, end)]
    return (result, encoding, None, None)

  def save_file(self, src, dest, resumable):
    key = self.get_path_to_file(src)
    with EXT_TEST_SEQUENCE_LOCK:
      exts = list(EXT_TEST_SEQUENCE)
      exts = [ x[0] for x in exts ]

    path = key
    true_ext = ''
    for ext in exts:
      pathext = key + ext
      if pathext in self._data:
        path = pathext
        true_ext = ext
        break

    filepath = os.path.join(dest, os.path.basename(path))

    mkdir(os.path.dirname(dest))
    try:
      with open(dest + true_ext, "wb") as f:
        f.write(self._data[path])
    except KeyError:
      return False

    return True

  def head(self, file_path):
    path = self.get_path_to_file(file_path)

    data = None
    encoding = ''

    with EXT_TEST_SEQUENCE_LOCK:
      for ext, enc in EXT_TEST_SEQUENCE:
        pathext = path + ext
        if pathext in self._data:
          data = self._data[pathext]
          encoding = enc
          break

    return {
      "Cache-Control": None,
      "Content-Length": len(data),
      "Content-Type": None,
      "ETag": None,
      "Last-Modified": None,
      "Content-Md5": None,
      "Content-Encoding": encoding,
      "Content-Disposition": None,
      "Content-Language": None,
      "Storage-Class": None,
      "Request-Charged": None,
      "Parts-Count": None,
    }

  def size(self, file_path):
    path = self.get_path_to_file(file_path)

    exts = ('.gz', '.br', '.zstd', '.xz', '.bz2')

    for ext in exts:
      pathext = path + ext
      if pathext in self._data:
        return len(self._data[pathext])

    if path in self._data:
      data = self._data[path]
      if isinstance(data, bytes):
        return len(data)
      else:
        return len(data.encode('utf8'))

    return None

  def copy_file(self, src_path, dest_bucket, dest_key):
    key = self.get_path_to_file(src_path)
    with MEM_BUCKET_POOL_LOCK:
     pool = MEM_POOL[MemoryPoolParams(dest_bucket)]
    dest_bucket = pool.get_connection(None, None)
    dest_bucket[dest_key] = self._data[key]
    return True

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

    filenames = ( f.removeprefix(remove) for f in self._data )
    filenames = ( f for f in filenames if f[:len(prefix)] == prefix )

    if flat:
      tmp = []
      for f in filenames:
        elems = f.removeprefix(prefix).split('/')
        if len(elems) > 1 and elems[0] == '':
          elems.pop(0)
          elems[0] = f'/{elems[0]}'

        if len(elems) > 1:
          tmp.append(f"{prefix}{elems[0]}/")
        else:
          tmp.append(f"{prefix}{elems[0]}")
      filenames = tmp
    
    def stripext(fname):
      (base, ext) = os.path.splitext(fname)
      if ext in COMPRESSION_EXTENSIONS:
        return base
      else:
        return fname

    filenames = list(map(stripext, filenames))
    filenames.sort()
    return iter(filenames)

class GoogleCloudStorageInterface(StorageInterface):
  exists_batch_size = Batch._MAX_BATCH_SIZE
  delete_batch_size = Batch._MAX_BATCH_SIZE

  def __init__(self, path, secrets=None, request_payer=None, **kwargs):
    super(StorageInterface, self).__init__()
    global GC_POOL
    global GCS_BUCKET_POOL_LOCK
    self._path = path
    self._request_payer = request_payer

    with GCS_BUCKET_POOL_LOCK:
      pool = GC_POOL[GCloudBucketPoolParams(self._path.bucket, self._request_payer)]
    self._bucket = pool.get_connection(secrets, None)
    self._secrets = secrets

  def get_path_to_file(self, file_path):
    return posixpath.join(self._path.path, file_path)

  @retry_if_not(CompressionError)
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
    elif compress in ("xz", "lzma"):
      blob.content_encoding = "xz"
    elif compress in ("bzip2", "bz2"):
      blob.content_encoding = "bz2"
    elif compress:
      raise CompressionError("Compression type {} not supported.".format(compress))

    if cache_control:
      blob.cache_control = cache_control
    if storage_class:
      blob.storage_class = storage_class

    blob.md5_hash = md5(content)
    blob.upload_from_string(content, content_type)

  @retry
  def copy_file(self, src_path, dest_bucket, dest_key):
    key = self.get_path_to_file(src_path)
    source_blob = self._bucket.blob( key )
    with GCS_BUCKET_POOL_LOCK:
     pool = GC_POOL[GCloudBucketPoolParams(dest_bucket, self._request_payer)]
    dest_bucket = pool.get_connection(self._secrets, None)

    try:
      self._bucket.copy_blob(
        source_blob, dest_bucket, dest_key
      )
    except google.api_core.exceptions.NotFound:
      return False

    return True

  @retry_if_not(google.cloud.exceptions.NotFound)
  def get_file(self, file_path, start=None, end=None, part_size=None):
    key = self.get_path_to_file(file_path)
    blob = self._bucket.blob( key )

    if start is not None:
      start = int(start)
    if end is not None:
      end = int(end - 1)

    try:
      content = blob.download_as_bytes(start=start, end=end, raw_download=True, checksum=None)
    except google.cloud.exceptions.NotFound as err:
      return (None, None, None, None)

    hash_type = "md5"
    hash_value = blob.md5_hash if blob.component_count is None else None

    if hash_value is None and blob.crc32c is not None:
      hash_type = "crc32c"
      hash_value = blob.crc32c

    return (content, blob.content_encoding, hash_value, hash_type)

  @retry
  def save_file(self, src, dest, resumable):
    key = self.get_path_to_file(src)
    blob = self._bucket.blob(key)
    try:
      mkdir(os.path.dirname(dest))
      blob.download_to_filename(
        filename=dest,
        raw_download=True, 
        checksum=None
      )
    except google.cloud.exceptions.NotFound:
      return False

    ext = FileInterface.get_extension(blob.content_encoding)
    if not dest.endswith(ext):
      os.rename(dest, dest + ext)

    return True

  @retry_if_not(google.cloud.exceptions.NotFound)
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

  @retry_if_not(google.cloud.exceptions.NotFound)
  def size(self, file_path):
    key = self.get_path_to_file(file_path)
    blob = self._bucket.get_blob(key)
    if blob:
      return blob.size
    return None

  @retry_if_not(google.cloud.exceptions.NotFound)
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

    delimiter = '/' if flat else None

    blobs = self._bucket.list_blobs(
      prefix=path, 
      delimiter=delimiter,
    )

    first = True
    for blob in blobs:
      # This awkward construction is necessary
      # because the request that populates prefixes 
      # isn't made until the iterator is activated.
      if first and blobs.prefixes:
        yield from (
          item.removeprefix(path)
          for item in blobs.prefixes
        )
        first = False

      filename = blob.name.removeprefix(layer_path)
      if not filename:
        continue
      elif not flat and filename[-1] != '/':
        yield filename
      elif flat and '/' not in blob.name.removeprefix(path):
        yield filename

    # When there are no regular items at this level
    # we need to still print the directories.
    if first and blobs.prefixes:
      yield from (
        item.removeprefix(path)
        for item in blobs.prefixes
      )

  def release_connection(self):
    global GC_POOL
    with GCS_BUCKET_POOL_LOCK:
      pool = GC_POOL[GCloudBucketPoolParams(self._path.bucket, self._request_payer)]
    pool.release_connection(self._bucket)

class HttpInterface(StorageInterface):
  adaptor = requests.adapters.HTTPAdapter()
  
  def __init__(self, path, secrets=None, request_payer=None, **kwargs):
    super(StorageInterface, self).__init__()
    self._path = path
    if request_payer is not None:
      raise ValueError("Specifying a request payer for the HttpInterface is not supported. request_payer must be None, got '{}'.".format(request_payer))

    if not secrets:
      secrets = http_credentials()

    self.session = requests.Session()
    self.session.mount('http://', self.adaptor)
    self.session.mount('https://', self.adaptor)
    if secrets and 'user' in secrets and 'password' in secrets:
      self.session.auth = (secrets['user'], secrets['password'])

  def default_headers(self):
    return {}

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

  @retry
  def head(self, file_path):
    key = self.get_path_to_file(file_path)
    headers = self.default_headers()
    with self.session.head(key, headers=headers) as resp:
      resp.raise_for_status()
      return resp.headers

  def size(self, file_path):
    headers = self.head(file_path)
    return int(headers["Content-Length"])

  @retry
  def get_file(self, file_path, start=None, end=None, part_size=None):
    key = self.get_path_to_file(file_path)

    headers = self.default_headers()
    if start is not None or end is not None:
      start = int(start) if start is not None else 0
      end = int(end - 1) if end is not None else ''
      headers["Range"] = f"bytes={start}-{end}"
    
    resp = self.session.get(key, headers=headers)
    
    if resp.status_code in (404, 403):
      return (None, None, None, None)
    resp.close()
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
  def save_file(self, src, dest, resumable):
    key = self.get_path_to_file(src)

    headers = self.head(src)
    content_encoding = headers.get('Content-Encoding', None)

    try:
      ext = FileInterface.get_extension(content_encoding)
    except ValueError:
      ext = ""

    fulldest = dest + ext

    partname = fulldest
    if resumable:
      partname += ".part"

    downloaded_size = 0
    if resumable and os.path.exists(partname):
      downloaded_size = os.path.getsize(partname)        

    range_headers = { "Range": f"bytes={downloaded_size}-" }
    with self.session.get(key, headers=range_headers, stream=True) as resp:
      if resp.status_code not in [200, 206]:
        resp.raise_for_status()
        return False

      with open(partname, 'ab') as f:
        for chunk in resp.iter_content(chunk_size=int(10e6)):
          f.write(chunk)

    if resumable:
      os.rename(partname, fulldest)

    return True

  @retry
  def exists(self, file_path):
    key = self.get_path_to_file(file_path)
    headers = self.default_headers()
    with self.session.get(key, stream=True, headers=headers) as resp:
      return resp.ok

  def files_exist(self, file_paths):
    return {path: self.exists(path) for path in file_paths}

  def _list_files_google(self, prefix, flat):
    bucket = self._path.path.split('/')[0]
    prefix = posixpath.join(
      self._path.path.replace(bucket, '', 1), 
      prefix
    )
    if prefix and prefix[0] == '/':
      prefix = prefix[1:]

    headers = self.default_headers()

    @retry_if_not(AuthorizationError)
    def request(token):
      nonlocal headers
      params = {}
      if prefix:
        params["prefix"] = prefix
      if token is not None:
        params["pageToken"] = token
      if flat:
        params["delimiter"] = '/'

      results = self.session.get(
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o",
        params=params,
        headers=headers,
      )
      if results.status_code in [401,403]:
        raise AuthorizationError(f"http {results.status_code}")

      results.raise_for_status()
      results.close()
      return results.json()

    strip = posixpath.dirname(prefix)
    if strip and strip[-1] != '/':
      strip += '/'

    token = None
    while True:
      results = request(token)

      if 'prefixes' in results:
        yield from (
          item.removeprefix(strip) 
          for item in results["prefixes"]
        )

      for res in results.get("items", []):
        print(res["name"])
        yield res["name"].removeprefix(strip)
      
      token = results.get("nextPageToken", None)
      if token is None:
        break

  def _list_files_apache(self, prefix, flat):
    import lxml.html

    baseurl = posixpath.join(self._path.host, self._path.path)

    directories = ['']
    headers = self.default_headers()

    while directories:
      directory = directories.pop()
      url = posixpath.join(baseurl, directory)

      resp = requests.get(url, headers=headers)
      resp.raise_for_status()

      if 'text/html' not in resp.headers["Content-Type"]:
        raise ValueError("Unable to parse non-HTML output from Apache servers.")

      entities = lxml.html.document_fromstring(resp.content)
      resp.close()

      h1 = entities.xpath("body/h1")[0].text_content()
      if "Index of" not in h1:
        raise ValueError("Unable to parse non-index page.")

      for li in entities.xpath("body/ul/li"):
        txt = li.text_content().strip()
        if txt == "Parent Directory":
          continue
        
        txt = posixpath.join(directory, txt)
        if prefix and not txt.startswith(prefix):
          continue

        if txt[-1] == '/':
          directories.append(txt)
          continue

        yield txt

      if flat:
        break

  def list_files(self, prefix, flat=False):
    if self._path.host == "https://storage.googleapis.com":
      yield from self._list_files_google(prefix, flat)
      return

    url = posixpath.join(self._path.host, self._path.path, prefix)
    resp = requests.head(url)

    server = resp.headers.get("Server", "").lower()
    if 'apache' in server:
      yield from self._list_files_apache(prefix, flat)
      return
    else:
      raise NotImplementedError()

class S3Interface(StorageInterface):
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.delete_objects
  # claims batch size limit is 1000
  delete_batch_size = 1000
  def __init__(
    self, path, secrets=None, 
    request_payer=None, 
    composite_upload_threshold=int(1e8), 
    no_sign_request=False,
    **kwargs
  ):
    super(StorageInterface, self).__init__()
    global S3_POOL

    if request_payer is None:
      self._additional_attrs = {}
    elif request_payer == 'requester':
      self._additional_attrs = {'RequestPayer': request_payer}
    else:
      raise ValueError("request_payer for S3Interface must either be None or 'requester', got '{}'.".format(request_payer))

    self._request_payer = request_payer
    self._path = path
    self._secrets = secrets
    self._conn = self._get_bucket(path.bucket)

    self.composite_upload_threshold = composite_upload_threshold
    self.no_sign_request = no_sign_request

  def _get_bucket(self, bucket_name):
    global S3_BUCKET_POOL_LOCK
    service = self._path.alias or 's3'

    with S3_BUCKET_POOL_LOCK:
      pool = S3_POOL[S3ConnectionPoolParams(service, bucket_name, self._request_payer)]
    
    return pool.get_connection(self._secrets, self._path.host)

  def get_path_to_file(self, file_path):
    return posixpath.join(self._path.path, file_path)

  @retry
  def put_file(
    self, file_path, content, 
    content_type, compress, 
    cache_control=None,
    storage_class=None
  ):
    key = self.get_path_to_file(file_path)

    attrs = {
      'ContentType': (content_type or 'application/octet-stream'),
      'ACL': S3_ACLS.get(self._path.alias, "bucket-owner-full-control"),
      **self._additional_attrs,
    }

    # keep gzip as default
    if compress == "br":
      attrs['ContentEncoding'] = 'br'
    elif compress in GZIP_TYPES:
      attrs['ContentEncoding'] = 'gzip'
    elif compress == "zstd":
      attrs['ContentEncoding'] = 'zstd'
    elif compress in ("xz", "lzma"):
      attrs['ContentEncoding'] = 'xz'
    elif compress in ("bzip2", "bz2"):
      attrs['ContentEncoding'] = 'bzip2'
    elif compress:
      raise ValueError("Compression type {} not supported.".format(compress))

    if cache_control:
      attrs['CacheControl'] = cache_control
    if storage_class:
      attrs['StorageClass'] = storage_class

    multipart = hasattr(content, "read") and hasattr(content, "seek")

    if not multipart and len(content) > int(self.composite_upload_threshold):
      content = BytesIO(content)
      multipart = True

    # gevent monkey patching has a bad interaction with s3's use
    # of concurrent.futures.ThreadPoolExecutor. Just disable multipart
    # upload when monkeypatching is in effect.
    if multipart and (len(gevent.monkey.saved) > 0):
      multipart = False
      content = content.read()

    if multipart:
      self._conn.upload_fileobj(content, self._path.bucket, key, ExtraArgs=attrs)
      # upload_fileobj will add 'aws-chunked' to the ContentEncoding,
      # which after it finishes uploading is useless and messes up our
      # software. Therefore, edit the metadata and replace it (but this incurs
      # 2x class-A...)
      self._conn.copy_object(
        Bucket=self._path.bucket,
        Key=key,
        CopySource={'Bucket': self._path.bucket, 'Key': key},
        MetadataDirective="REPLACE",
        **attrs
      )
    else:
      if isinstance(content, str):
        content = content.encode('utf8')

      attrs['Bucket'] = self._path.bucket
      attrs['Body'] = content
      attrs['Key'] = key
      attrs["ChecksumCRC32C"] = encode_crc32c_b64(content).decode('utf8')
      self._conn.put_object(**attrs)

  @retry
  def copy_file(self, src_path, dest_bucket_name, dest_key):
    key = self.get_path_to_file(src_path)
    s3client = self._get_bucket(dest_bucket_name)
    copy_source = {
      'Bucket': self._path.bucket,
      'Key': key,
    }
    try:
      s3client.copy_object(
          CopySource=copy_source,
          Bucket=dest_bucket_name,
          Key=dest_key,
          MetadataDirective='COPY'  # Ensure metadata like Content-Encoding is copied
      )
    except botocore.exceptions.ClientError as err: 
      if err.response['Error']['Code'] in ('NoSuchKey', '404'):
        return False
      else:
        raise

    return True

  @retry
  def get_file(self, file_path, start=None, end=None, part_size=None):
    """
    There are many types of execptions which can get raised
    from this method. We want to make sure we only return
    None when the file doesn't exist.
    """

    kwargs = self._additional_attrs.copy()
    range_request = start is not None or end is not None
    if range_request:
      start = int(start) if start is not None else 0
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

      encoding = ",".join([ 
        enc for enc in encoding.split(",")
        if enc != "aws-chunked"
      ])

      # s3 etags return hex digests but we need the base64 encoding
      # to make uniform comparisons. 
      # example s3 etag: "31ee76261d87fed8cb9d4c465c48158c"
      # example multipart s3 etag: "cd8d2616dfa6cc80a06a846d3b3f6f30-14"
      # The -14 means 14 parts.

      etag = resp.get('ETag', None)
      content = resp['Body'].read()

      if etag is not None and not range_request:
        etag = etag.lstrip('"').rstrip('"')
        # AWS has special multipart validation
        # so we handle it here... leaky abstraction.
        if '-' in etag: 
          if not validate_s3_multipart_etag(content, etag, part_size):
            raise MD5IntegrityError(f"{file_path} failed its multipart md5 check. server md5: {etag}")
          etag = None
        else:
          etag = base64.b64encode(binascii.unhexlify(etag)).decode('utf8')

      return (content, encoding, etag, "md5")
    except botocore.exceptions.ClientError as err: 
      if err.response['Error']['Code'] == 'NoSuchKey':
        return (None, None, None, None)
      else:
        raise

  @retry
  def save_file(self, src, dest, resumable):
    key = self.get_path_to_file(src)
    kwargs = self._additional_attrs.copy()

    resp = self.head(src)

    if resp is None:
      return False

    mkdir(os.path.dirname(dest))

    encoding = resp.get("Content-Encoding", "") or ""
    encoding = ",".join([ 
      enc for enc in encoding.split(",")
      if enc != "aws-chunked"
    ])
    ext = FileInterface.get_extension(encoding)

    if not dest.endswith(ext):
      dest += ext

    try:
      self._conn.download_file(
        Bucket=self._path.bucket,
        Key=key,
        Filename=dest,
        **kwargs
      )
    except botocore.exceptions.ClientError as err: 
      if err.response['Error']['Code'] in ('NoSuchKey', '404'):
        return False
      else:
        raise

    return True

  @retry
  def head(self, file_path):
    try:
      response = self._conn.head_object(
        Bucket=self._path.bucket,
        Key=self.get_path_to_file(file_path),
        **self._additional_attrs,
      )

      encoding = response.get("ContentEncoding", None)
      if encoding == '':
        encoding = None

      return {
        "Cache-Control": response.get("CacheControl", None),
        "Content-Length": response.get("ContentLength", None),
        "Content-Type": response.get("ContentType", None),
        "ETag": response.get("ETag", None),
        "Last-Modified": response.get("LastModified", None),
        "Content-Md5": response["ResponseMetadata"]["HTTPHeaders"].get("content-md5", None),
        "Content-Encoding": encoding,
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
        **self._additional_attrs,
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
        **self._additional_attrs,
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
      **self._additional_attrs,
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
        **self._additional_attrs,
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
    def s3lst(path, continuation_token=None):
      kwargs = {
        'Bucket': self._path.bucket,
        'Prefix': path,
        **self._additional_attrs
      }
      if flat:
        kwargs['Delimiter'] = '/'

      if continuation_token:
        kwargs['ContinuationToken'] = continuation_token

      return self._conn.list_objects_v2(**kwargs)

    resp = s3lst(path)
    # the case where the prefix is something like "build", but "build" is a subdirectory
    # so requery with "build/" to get the proper behavior
    if (
      flat 
      and path 
      and path[-1] != '/' 
      and 'Contents' not in resp 
      and len(resp.get("CommonPrefixes", [])) == 1
    ):
      path += '/'
      resp = s3lst(path)

    def iterate(resp):
      if 'CommonPrefixes' in resp.keys():
        yield from [ 
          item["Prefix"].removeprefix(layer_path) 
          for item in resp['CommonPrefixes'] 
        ]

      if 'Contents' not in resp.keys():
        resp['Contents'] = []

      for item in resp['Contents']:
        key = item['Key']
        filename = key.removeprefix(layer_path)
        if filename == '':
          continue
        elif not flat and filename[-1] != '/':
          yield filename
        elif flat and '/' not in key.removeprefix(path):
          yield filename

    for filename in iterate(resp):
      yield filename

    while resp['IsTruncated'] and resp['NextContinuationToken']:
      resp = s3lst(path, resp['NextContinuationToken'])

      for filename in iterate(resp):
        yield filename

  def release_connection(self):
    global S3_POOL
    service = self._path.alias or 's3'
    with S3_BUCKET_POOL_LOCK:
      pool = S3_POOL[S3ConnectionPoolParams(service, self._path.bucket, self._request_payer)]
    pool.release_connection(self._conn)

class CaveInterface(HttpInterface):
  """
  CAVE is an internal system that powers proofreading 
  systems in Seung Lab. If you have no idea what this
  is, don't worry about it.
  see: https://github.com/CAVEconnectome
  """
  def default_headers(self):
    cred = cave_credentials()
    return {
      "Authorization": f"Bearer {cred['token']}",
    }
