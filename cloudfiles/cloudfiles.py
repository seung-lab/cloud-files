from typing import (
  Any, Dict, Optional, 
  Union, List, Tuple, 
  Callable, Generator, 
  Sequence, cast, BinaryIO
)

from queue import Queue
from collections import defaultdict
from functools import partial, wraps
import inspect
import math
import multiprocessing
import itertools
import os.path
import platform
import posixpath
import re
import shutil
import types

import orjson
import pathos.pools
from tqdm import tqdm

import google.cloud.storage 

from . import compression, paths, gcs
from .exceptions import UnsupportedProtocolError, MD5IntegrityError, CRC32CIntegrityError
from .lib import (
  mkdir, totalfn, toiter, scatter, jsonify, nvl, 
  duplicates, first, sip, touch,
  md5, crc32c, decode_crc32c_b64
)
from .paths import ALIASES, find_common_buckets
from .secrets import CLOUD_FILES_DIR, CLOUD_FILES_LOCK_DIR
from .threaded_queue import ThreadedQueue, DEFAULT_THREADS
from .typing import (
  CompressType, GetPathType, PutScalarType,
  PutType, ParallelType, SecretsType
)
from .scheduler import schedule_jobs

from .interfaces import (
  FileInterface, HttpInterface, 
  S3Interface, GoogleCloudStorageInterface,
  MemoryInterface, CaveInterface,
)

INTERFACES = {
  'file': FileInterface,
  'gs': GoogleCloudStorageInterface,
  's3': S3Interface,
  'http': HttpInterface,
  'https': HttpInterface,
  'mem': MemoryInterface,
  'middleauth+https': CaveInterface,
}
for alias in ALIASES:
  INTERFACES[alias] = S3Interface

def parallelize(desc=None, returns_list=False):
  """
  @parallelize 

  Allow multiprocess for methods that don't need to return anything.

  desc: progress bar label
  returns_list: accumulate results in a list and return it

  Enables the "parallel" argument to do something.
  """
  def decor(fn):
    @wraps(fn)
    def inner_decor(*args, **kwargs):
      nonlocal fn
      nonlocal desc
      nonlocal returns_list

      while isinstance(fn, partial):
        fn = fn.func

      try:
        sig = inspect.signature(fn).bind(*args, **kwargs)
        parallel = sig.arguments.get("parallel", None)
      except TypeError:
        parallel = kwargs.pop("parallel", None)
        sig = inspect.signature(fn).bind_partial(*args, **kwargs)

      params = sig.arguments
      self = params.get("self", None)
      if "self" in params:
        del params["self"]
      input_key, input_value = first(params.items())
      del params[input_key]

      parallel = nvl(parallel, self.parallel, 1)

      if parallel == 1:
        return fn(*args, **kwargs)

      progress = params.get("progress", False)
      params["progress"] = False
      total = params.get("total", None)

      if self is None:
        fn = partial(fn, **params)
      else:
        fn = partial(fn, self, **params)
      
      return parallel_execute(
        fn, input_value, parallel, total, progress, 
        desc=desc, returns_list=returns_list
      )
  
    return inner_decor
  return decor

def parallel_execute(
    fn, inputs, parallel, 
    total, progress, desc,
    returns_list
  ):
  if parallel == 0:
    parallel = multiprocessing.cpu_count()
  elif parallel < 0:
    raise ValueError(f"parallel must be >= 0. Got: {parallel}")

  total = totalfn(inputs, total)

  if total is not None and total < 0:
    raise ValueError(f"total must be positive. Got: {total}")

  block_size = 250
  if total is not None:
    block_size = max(20, int(math.ceil(total / parallel)))
    block_size = max(block_size, 1)
    block_size = min(1250, block_size)

  if isinstance(progress, tqdm):
    pbar = progress
  else:
    pbar = tqdm(desc=desc, total=total, disable=(not progress))

  # Fix for MacOS which can segfault due to 
  # urllib calling libdispatch which is not fork-safe
  # https://bugs.python.org/issue30385
  no_proxy = os.environ.get("no_proxy", "")
  if platform.system().lower() == "darwin":
    os.environ["no_proxy"] = "*"

  # Don't fork, spawn entirely new processes. This
  # avoids accidental deadlocks.
  multiprocessing.set_start_method("spawn", force=True)

  results = []
  try: 
    with pathos.pools.ProcessPool(parallel) as executor:
      for res in executor.imap(fn, sip(inputs, block_size)):
        if isinstance(res, int):
          pbar.update(res)
        elif isinstance(res, list):
          pbar.update(len(res))
        else:
          pbar.update(block_size)

        if returns_list:
          results.extend(res)
  finally:  
    if platform.system().lower() == "darwin":
      os.environ["no_proxy"] = no_proxy
    pbar.close()

  if returns_list:
    return results

def get_interface_class(protocol):
  if protocol in INTERFACES:
    return INTERFACES[protocol]
  else:
    raise UnsupportedProtocolError("Only {} are supported. Got: {}".format(
      ", ".join(list(INTERFACES.keys())), protocol
    ))

def path_to_byte_range_tags(path):
  if isinstance(path, str):
    return (path, None, None, None)
  return (path['path'], path.get('start', None), path.get('end', None), path.get('tags', None))

def dl(
  cloudpaths:GetPathType, raw:bool=False, **kwargs
) -> Union[bytes,List[dict]]:
  """
  Shorthand for downloading files with CloudFiles.get.
  You can use full paths from multiple buckets and services
  and they'll be noted with key "fullpath" in each returned 
  dict.
  """
  cloudpaths, is_multiple = toiter(cloudpaths, is_iter=True)
  clustered = find_common_buckets(cloudpaths)
  total = sum([ len(bucket) for bucket in clustered.values() ])

  progress = kwargs.get("progress", False) and total > 1
  pbar = tqdm(total=total, desc="Downloading", disable=(not progress))

  try:
    del kwargs["progress"]
  except KeyError:
    pass

  content = []
  for bucketpath, keys in clustered.items():
    cf = CloudFiles(bucketpath, **kwargs)
    sub_content = cf.get(keys, raw=raw, progress=pbar)
    for sct in sub_content:
      sct["fullpath"] = posixpath.join(bucketpath, sct["path"])
    content += sub_content

  pbar.close()

  if not is_multiple:
    return content[0]
  return content

class CloudFiles:
  """
  CloudFiles is a multithreaded key-value object
  management client that supports get, put, delete,
  exists, and list operations.

  It can support any key-value storage system and 
  currently supports local filesystem, Google Cloud Storage,
  Amazon S3 interfaces, and reading from arbitrary HTTP 
  servers.

  cloudpath: a parent directory of the files you want to fetch
    specified as:
      e.g. gs://bucket/dir/
           s3://bucket/dir/
           s3://https://myendpoint.com/dir/
           file://./dir
           ./dir
           https://some.host.edu/dir/
           mem://bucket/dir
      Key:
       gs: Google Cloud Storage
       s3: Amazon S3
       file: Local Filesystem (including network mounts)
       mem: In-Memory storage

  progress: display progress bar measured in files
  green: whether to use green threads (uses gevent library)
  secrets: you can provide GCS, S3, CAVE, etc credentials
    via the constructor here instead of the default secrets 
    files
  num_threads: number of threads to launch for remote server 
    IO. No effect on local file fetching (always single threaded
    for maximum performance).
  use_https: use the public https API for GCS and S3 instead of
    boto or google-storage-python
  endpoint: for S3 emulators, you can provide a different endpoint
    like https://s3-storage.university.edu. This can also be specified
    in the secrets file.
  parallel: number of separate processes to launch (each will use num_threads)
  request_payer: bill your s3 usage to someone other than the bucket owner
  locking: for local filesystems, you can use advisory file locking to avoid
    separate cloudfiles instances from interfering with each other
  lock_dir: you can specify your own directory for the advisory lock files
  composite_upload_threshold: GCS and S3 both support multi-part uploads. 
    For files larger than this threshold, use that facility.
  no_sign_request: (s3 only) don't sign the request with credentials
  """
  def __init__(
    self,
    cloudpath:str, 
    progress:bool = False, 
    green:Optional[bool] = None, 
    secrets:SecretsType = None,
    num_threads:int = 20,
    use_https:bool = False, 
    endpoint:Optional[str] = None, 
    parallel:ParallelType = 1,
    request_payer:Optional[str] = None,
    locking:Optional[bool] = None,
    lock_dir:Optional[str] = None,
    composite_upload_threshold:int = int(1e8),
    no_sign_request:bool = False,
  ):
    if use_https:
      cloudpath = paths.to_https_protocol(cloudpath)

    cloudpath = paths.normalize(cloudpath)
    
    self.cloudpath = cloudpath
    self.progress = progress
    self.secrets = secrets
    self.num_threads = num_threads
    self.green = green
    self.parallel = int(parallel)
    self.request_payer = request_payer
    self.locking = locking
    self.composite_upload_threshold = composite_upload_threshold
    self.no_sign_request = bool(no_sign_request)

    self._lock_dir = lock_dir
    self._path = paths.extract(cloudpath)
    if endpoint:
      self._path = paths.ExtractedPath(host=endpoint, **self._path)
    self._interface_cls = get_interface_class(self._path.protocol)

    if self._path.protocol == 'mem':
      self.num_threads = 0

  @property
  def lock_dir(self):
    if self._lock_dir is not None:
      return self._lock_dir

    self._lock_dir = CLOUD_FILES_LOCK_DIR

    if self._lock_dir is None:
      self._lock_dir = os.path.join(CLOUD_FILES_DIR, "locks")

    if self.protocol == "file" and self.locking and not os.path.exists(self._lock_dir):
      mkdir(self._lock_dir)
      if not (os.path.isdir(self._lock_dir) and os.access(self._lock_dir, os.R_OK|os.W_OK|os.X_OK)):
        self._lock_dir = None
        raise PermissionError(f"Unable to access {self._lock_dir}")

    return self._lock_dir

  def clear_locks(self):
    """Removes temporary lock files in locking directory."""
    if self.lock_dir is None:
      return

    for direntry in os.scandir(self.lock_dir):
      os.remove(direntry.path)

  def _progress_description(self, prefix):
    if isinstance(self.progress, str):
      return prefix + ' ' + self.progress
    else:
      return prefix if self.progress else None

  def _get_connection(self):
    return self._interface_cls(
      self._path, 
      secrets=self.secrets,
      request_payer=self.request_payer,
      locking=self.locking,
      lock_dir=self.lock_dir,
      composite_upload_threshold=self.composite_upload_threshold,
      no_sign_request=self.no_sign_request,
    )

  @property
  def protocol(self):
    return self._path.protocol

  def abspath(self, path):
    sep = posixpath.sep
    if self._path.protocol == "file":
      sep = os.path.sep
    return sep.join((paths.asprotocolpath(self._path), path))

  @parallelize(desc="Download", returns_list=True)
  def get(
    self, paths:GetPathType, total:Optional[int] = None, 
    raw:bool = False, progress:Optional[bool] = None, 
    parallel:Optional[ParallelType] = None,
    return_dict:bool = False, raise_errors:bool = True,
    part_size:Optional[int] = None
  ) -> Union[dict,bytes,List[dict]]:
    """
    Download one or more files. Return order is not guaranteed to match input.

    paths: scalar or iterable of:
      filename (strings)
      OR
      { 
        'path': filename, 
        'start': (int) start byte, 
        'end': (int) end byte,
        'tags': Any (optional) (pass through)
      }
    total: manually provide a progress bar size if paths does
      not support the `len` operator.
    raw: download without decompressing
    parallel: number of concurrent processes (0 means all cores)
    part_size: when working with composite s3 files on s3 emulators,
      they may use a different part size when computing the etag 
      checksum. if you know the part size (in bytes) you can provide
      it here. This only has meaning for s3 paths with multi-part objects.
    return_dict: Turn output into { path: binary } and drops the 
      extra information. Errors will be raised immediately.
    raise_errors: Raise the first error immediately instead 
      of returning them as part of the output.

    Returns:
      if return_dict:
        { path: binary, path: binary } (errors raised immediately)
      elif paths is scalar:
        binary (errors raised immediately)
      else:
        [
          {
            'path': path, 
            'content': content, 
            'byte_range': (start, end),
            'error': error,
            'compress': compression type,
            'raw': boolean,
          }
        ]
    """
    paths, multiple_return = toiter(paths, is_iter=True)
    progress = nvl(progress, self.progress)
    # return_dict prevents the user from having a chance
    # to inspect errors, so we must raise here.
    raise_errors = raise_errors or return_dict or (not multiple_return)

    def check_md5(path, content, server_hash):
      if server_hash is None:
        return
      
      computed_md5 = md5(content)

      if computed_md5.rstrip("==") != server_hash.rstrip("=="):
        raise MD5IntegrityError("{} failed its md5 check. server md5: {} computed md5: {}".format(
          path, server_hash, computed_md5
        ))

    def check_crc32c(path, content, server_hash):
      if server_hash is None:
        return

      server_hash = decode_crc32c_b64(server_hash)
      crc = crc32c(content)

      if crc != server_hash:
        raise CRC32CIntegrityError("crc32c mismatch for {}: server {} ; client {}".format(path, server_hash, crc))

    def download(path):
      path, start, end, tags = path_to_byte_range_tags(path)
      error = None
      content = None
      encoding = None
      server_hash = None
      server_hash_type = None
      try:
        with self._get_connection() as conn:
          content, encoding, server_hash, server_hash_type = conn.get_file(
            path, start=start, end=end, part_size=part_size
          )
        
        # md5s don't match for partial reads
        if start is None and end is None:
          if server_hash_type == "md5":
            check_md5(path, content, server_hash)
          elif server_hash_type == "crc32c":
            check_crc32c(path, content, server_hash)
        
        if not raw:
          content = compression.decompress(content, encoding, filename=path)
      except Exception as err:
        error = err

      if raise_errors and error:
        raise error

      return { 
        'path': path, 
        'content': content, 
        'byte_range': (start, end),
        'error': error,
        'compress': encoding,
        'raw': raw,
        'tags': tags,
      }
    
    total = totalfn(paths, total)

    if total == 1:
      ret = download(first(paths))
      if return_dict:
        return { ret["path"]: ret["content"] }
      elif multiple_return:
        return [ ret ]
      else:
        return ret['content']

    num_threads = self.num_threads
    if self.protocol == "file":
      num_threads = 1
    elif self.protocol == "mem":
      num_threads = 0

    results = schedule_jobs(
      fns=( partial(download, path) for path in paths ), 
      concurrency=num_threads, 
      progress=progress,
      total=total,
      green=self.green,
    )

    if return_dict:
      return { res["path"]: res["content"] for res in results }  

    return results

  def get_json(
    self, paths:GetPathType, total:Optional[int] = None
  ) -> Union[dict,int,float,list]:
    """
    Download one or more JSON files and decode them into a python object.
    Return order is guaranteed to match input.

    paths: scalar or iterable of:
      filename (strings)
      OR
      { 'path': filename, 'start': (int) start byte, 'end': (int) end byte }

    total: Can be used to provide a size for the progress bar if the 
      input type of paths does not support `len`.

    Returns:
      if paths is scalar:
        decoded json object
      else:
        [ decoded json objects ]
    """
    paths, multiple_return = toiter(paths, is_iter=True)

    def decode(content):
      content = content['content']
      if content is None:
        return None
      return orjson.loads(content.decode('utf8'))

    desc = self.progress if isinstance(self.progress, str) else "Downloading JSON"

    contents:List = []
    with tqdm(total=totalfn(paths, total), desc=desc, disable=(not self.progress)) as pbar:
      for paths_chunk in sip(paths, 2000):
        contents_chunk = self.get(paths_chunk, total=total, progress=pbar)
        pathidx = { content["path"]: content for content in contents_chunk }
        contents.extend(( pathidx[pth] for pth in paths_chunk ))

    contents = [ decode(content) for content in contents ]
    if multiple_return:
      return contents
    return contents[0]

  @parallelize(desc="Upload")
  def puts(
    self, files:PutType, 
    content_type:Optional[str] = None, compress:CompressType = None, 
    compression_level:Optional[int] = None, cache_control:Optional[str] = None,
    total:Optional[int] = None, raw:bool = False, progress:Optional[bool] = None,
    parallel:ParallelType = 1, storage_class:Optional[str] = None
  ) -> int:
    """
    Writes one or more files at a given location.

    files: scalar or list of:
      tuple: (filepath, content)
      dict: must contain 'content' and 'path' fields:
        {
          'content': b'json data', # must by binary data
          'path': 'info', # specified relative to the cloudpath
          
          # optional fields
          'content_type': 'application/json' or 'application/octet-stream', 
          'compress': None, 'gzip', 'br', 'zstd'
          'compression_level': e.g. 6, # for gzip, brotli, or zstd
          'cache_control': specify the header the way you want 
              e.g. 'no-cache' or 'public; max-age=3600' etc
          'storage_class': for relevant cloud providers, specify the storage class of the file
              e.g. 'STANDARD', 'COLDLINE', etc. (note: these are vendor-specific)
        }

      If the additional fields are specified, they will override the 
      defaults provided by arguments to the function. e.g. you
      can specify cache_control for an list but provide an exception
      for one or more files.

    total: Can be used to provide a size for the progress bar if the 
      input type of files does not support `len`.
    raw: upload without applying additional compression but 
      label the Content-Encoding using the compress parameter. 
      This is useful for file transfers.
    progress: selectively enable or disable progress just for this
      function call. If progress is a string, it sets the 
      text of the progress bar.
    parallel: number of concurrent processes (0 means all cores)

    Returns: number of files uploaded
    """
    files = toiter(files)
    progress = nvl(progress, self.progress)

    def todict(file):
      if isinstance(file, tuple):
        return { 'path': file[0], 'content': file[1] }
      return file

    def uploadfn(file):
      file = todict(file)

      file_compress = file.get('compress', compress)
      if file_compress not in compression.COMPRESSION_TYPES:
        raise ValueError('{} is not a supported compression type.'.format(file_compress))

      content = file['content']
      if not raw:
        content = compression.compress(
          content, 
          method=file_compress,
          compress_level=file.get('compression_level', compression_level),
        )

      if (
        self.protocol == "gs" 
        and (
          (hasattr(content, "read") and hasattr(content, "seek"))
          or (hasattr(content, "__len__") and len(content) > self.composite_upload_threshold)
        )
      ):
        gcs.composite_upload(
          f"{self.cloudpath}/{file['path']}", 
          content, 
          part_size=self.composite_upload_threshold,
          secrets=self.secrets,
          progress=self.progress,
          content_type=content_type,
          cache_control=cache_control,
          storage_class=storage_class,
          compress=file_compress,
          skip_compress=True,
        )
        return

      with self._get_connection() as conn:
        conn.put_file(
          file_path=file['path'], 
          content=content, 
          content_type=file.get('content_type', content_type),
          compress=file_compress,
          cache_control=file.get('cache_control', cache_control),
          storage_class=file.get('storage_class', storage_class)
        )

    if not isinstance(files, (types.GeneratorType, zip)):
      dupes = duplicates([ todict(file)['path'] for file in files ])
      if dupes:
        raise ValueError("Cannot write the same file multiple times in one pass. This causes a race condition. Files: " + ", ".join(dupes))
    
    total = totalfn(files, total)

    if total == 1:
      uploadfn(first(files))
      return 1

    fns = ( partial(uploadfn, file) for file in files )
    desc = self._progress_description("Upload")
    results = schedule_jobs(
      fns=fns,
      concurrency=self.num_threads,
      progress=(desc if progress else None),
      total=total,
      green=self.green,
    )
    return len(results)

  def put(
    self, 
    path:str, content:Union[BinaryIO,bytes], 
    content_type:str = None, compress:CompressType = None, 
    compression_level:Optional[int] = None, cache_control:Optional[str] = None,
    raw:bool = False, storage_class:Optional[str] = None
  ) -> int:
    """
    Write a single file.

    path: (str) file path relative to cloudpath
    content: binary string 
    content_type: e.g. 'application/json' or 'application/octet-stream'
    compress: None, 'gzip', 'br' (brotli), 'zstd'
    compression_level: (None or int) input to compressor, None means use default
    cache_control: (str) HTTP Cache-Control header.
    storage_class: (str) Storage class for the file.
    raw: (bool) if true, content is pre-compressed and 
      will bypass the compressor

    Returns: number of files uploaded
    """
    return self.puts({
      'path': path,
      'content': content,
      'content_type': content_type,
      'compress': compress,
      'compression_level': compression_level,
      'cache_control': cache_control,
      'storage_class': storage_class,
    }, raw=raw)

  def put_jsons(
    self, files:PutType,     
    compress:CompressType = None, 
    compression_level:Optional[int] = None, 
    cache_control:Optional[str] = None, total:Optional[int] = None,
    raw:bool = False, progress:Optional[bool] = None, parallel:ParallelType = 1,
    storage_class:Optional[str] = None
  ) -> int:
    """
    Write one or more files as JSON.

    See puts for details. The major difference is that
    the 'content' field is converted from a Python object 
    to JSON.

    Returns: number of files uploaded
    """
    files = toiter(files)

    def jsonify_file(file):
      if isinstance(file, list):
        return [ file[0], jsonify(file[1]) ]
      elif isinstance(file, tuple):
        return (file[0], jsonify(file[1]))
      else:
        file['content'] = jsonify(file['content'])
        return file

    total = totalfn(files, total)

    return self.puts( 
      (jsonify_file(file) for file in files), 
      compress=compress, compression_level=compression_level,
      content_type='application/json', storage_class=storage_class,
      total=total, raw=raw,
      progress=progress, parallel=parallel
    )

  def put_json(
    self, path:str, content:bytes,
    compress:CompressType = None, compression_level:Optional[int] = None, 
    cache_control:Optional[str] = None, storage_class:Optional[str] = None
  ) -> int:
    """
    Write a single JSON file. Automatically supplies the
    content_type 'application/json'.

    path: (str) file path relative to cloudpath
    content: JSON serializable Python object
    compress: None, 'gzip', 'br' (brotli), 'zstd'
    compression_level: (None or int) input to compressor, None means use default
    cache_control: (str) HTTP Cache-Control header.
    storage_class: (str) Storage class for the file.

    Returns: number of files uploaded
    """
    return self.put_jsons({
      'path': path,
      'content': content,
      'content_type': 'application/json',
      'compress': compress,
      'compression_level': compression_level,
      'cache_control': cache_control,
      'storage_class': storage_class
    })

  def isdir(self, prefix:str = "") -> bool:
    """
    Tests if the given path points to a directory.
    This has a typical meaning on a filesystem,
    but on object storage, since directories don't
    exist, it just means "is there at least one object
    located at or below this path".
    """
    if self._path.protocol == "file":
      fullpath = paths.asfilepath(self._path)
      fullpath = os.path.join(fullpath, prefix)
      return os.path.isdir(fullpath)

    if prefix == "":
      fullpath = paths.ascloudpath(self._path)
      if fullpath[-1] == "/":
        return True
    elif prefix[-1] == "/":
      return True
    try:
      res = first(self.list(prefix=prefix))
      return res is not None
    except NotImplementedError as err:
      return not CloudFile(self.cloudpath).exists()

  def exists(
    self, paths:GetPathType, 
    total:Optional[int] = None, progress:Optional[bool] = None
  ) -> Union[bool,Dict[str,bool]]:
    """
    Test if the given file paths exist.

    paths: one or more file paths relative to the cloudpath.
    total: manually provide a progress bar size if paths does
      not support the `len` operator.
    progress: selectively enable or disable progress just for this
      function call. If progress is a string, it sets the 
      text of the progress bar.

    Returns:
      If paths is a scalar:
        boolean
      Else:
        {
          filename_1: boolean,
          filename_2: boolean,
          ...
        }
    """
    paths, return_multiple = toiter(paths, is_iter=True)
    progress = nvl(progress, self.progress)
    results = {}

    total = totalfn(paths, total)
    desc = self._progress_description("Exists")
    if isinstance(progress, tqdm):
      pbar = progress
    else:
      pbar = tqdm(total=total, desc=desc, disable=(not progress))

    def exist_thunk(paths):
      with self._get_connection() as conn:
        results.update(conn.files_exist(paths))
        pbar.update(len(paths))

    batch_size = self._interface_cls.exists_batch_size
    schedule_jobs(
      fns=( partial(exist_thunk, paths) for paths in sip(paths, batch_size) ),
      progress=False,
      concurrency=self.num_threads,
      green=self.green,
      total=total,
    )
    pbar.close()

    if return_multiple:
      return results
    return first(results.values())

  def head(
    self, paths:GetPathType, 
    total:Optional[int] = None, progress:Optional[bool] = None
  ) -> Union[dict,List[dict]]:
    """
    Retrieves basic metadata about the indicated files including
    last modified, file size in bytes, and an integrity check
    hash if available (usually md5 or crc32c).

    paths: one or more file paths relative to the cloudpath.
    total: manually provide a progress bar size if paths does
      not support the `len` operator.
    progress: selectively enable or disable progress just for this
      function call. If progress is a string, it sets the 
      text of the progress bar.
    """
    paths, return_multiple = toiter(paths, is_iter=True)
    progress = nvl(progress, self.progress)
    results = {}

    def size_thunk(path):
      with self._get_connection() as conn:
        results[path] = conn.head(path)
    
    desc = self._progress_description('Head')
    schedule_jobs(
      fns=( partial(size_thunk, path) for path in paths ),
      progress=(desc if progress else None),
      concurrency=self.num_threads,
      total=totalfn(paths, total),
      green=self.green,
    )

    if return_multiple:
      return results
    return first(results.values())   

  def size(
    self, paths:GetPathType, 
    total:Optional[int] = None, 
    progress:Optional[bool] = None,
    return_sum:bool = False,
  ) -> Union[Dict[str,int],List[Dict[str,int]],int]:
    """
    Get the size in bytes of one or more files in its stored state.
    """
    paths, return_multiple = toiter(paths, is_iter=True)
    progress = nvl(progress, self.progress)
    results = {}

    def size_thunk(path):
      with self._get_connection() as conn:
        results[path] = conn.size(path)
    
    desc = self._progress_description('Measuring Sizes')
    schedule_jobs(
      fns=( partial(size_thunk, path) for path in paths ),
      progress=(desc if progress else None),
      concurrency=self.num_threads,
      total=totalfn(paths, total),
      green=self.green,
    )

    if return_sum:
      return sum(( sz for sz in results.values() ))

    if return_multiple:
      return results
    return first(results.values())

  @parallelize(desc="Delete")
  def delete(
    self, paths:GetPathType, total:Optional[int] = None, 
    progress:Optional[bool] = None, parallel:ParallelType = 1
  ) -> int:
    """
    Delete one or more files.

    paths: (str) one or more file paths relative 
      to the class instance's cloudpath.
    total: manually provide a progress bar size if paths does
      not support the `len` operator.
    progress: selectively enable or disable progress just for this
      function call. If progress is a string, it sets the 
      text of the progress bar.
    parallel: number of concurrent processes (0 means all cores)

    Returns: number of items deleted
    """
    paths = toiter(paths)
    progress = nvl(progress, self.progress)

    def thunk_delete(path):
      with self._get_connection() as conn:
        conn.delete_files(path)
      return len(toiter(path))

    desc = self._progress_description('Delete')

    batch_size = self._interface_cls.delete_batch_size

    results = schedule_jobs(
      fns=( partial(thunk_delete, path) for path in sip(paths, batch_size) ),
      progress=(desc if progress else None),
      concurrency=self.num_threads,
      total=totalfn(paths, total),
      green=self.green,
      count_return=True,
    )
    return len(results)

  def touch(
    self, 
    paths:GetPathType,
    progress:Optional[bool] = None,
    total:Optional[int] = None,
    nocopy:bool = False,
  ):
    """
    Create a zero byte file if it doesn't exist.
    """
    paths = toiter(paths)
    progress = nvl(progress, self.progress)
    total = totalfn(paths, total)

    if self.protocol == "file":
      basepath = self.cloudpath.replace("file://", "")
      for path in tqdm(paths, disable=(not progress), total=total):
        pth = path
        if isinstance(path, dict):
          pth = path["path"]
        touch(self.join(basepath, pth))
      return

    results = self.exists(paths, total=total, progress=progress)

    dne = [ 
      (fname, b'') 
      for fname, exists in results.items() 
      if not exists 
    ]

    self.puts(dne, progress=progress)

    # def thunk_copy(path):
    #   with self._get_connection() as conn:
    #     conn.copy_file(path, self._path.bucket, self.join(self._path.path, path))
    #   return 1

    # if not nocopy:
    #   already_exists = ( 
    #     fname
    #     for fname, exists in results.items() 
    #     if exists 
    #   )

    #   results = schedule_jobs(
    #     fns=( partial(thunk_copy, path) for path in already_exists ),
    #     progress=progress,
    #     total=(total - len(dne)),
    #     concurrency=self.num_threads,
    #     green=self.green,
    #     count_return=True,
    #   )

  def list(
    self, prefix:str = "", flat:bool = False
  ) -> Generator[str,None,None]:
    """
    List files with the given prefix. 

    flat means only generate one level of a directory,
    while non-flat means generate all file paths with that 
    prefix.

    Here's how flat=True handles different senarios:
      1. partial directory name prefix = 'bigarr'
        - lists the '' directory and filters on key 'bigarr'
      2. full directory name prefix = 'bigarray'
        - Same as (1), but using key 'bigarray'
      3. full directory name + "/" prefix = 'bigarray/'
        - Lists the 'bigarray' directory
      4. partial file name prefix = 'bigarray/chunk_'
        - Lists the 'bigarray/' directory and filters on 'chunk_'
    
    Return: generated sequence of file paths relative to cloudpath
    """
    with self._get_connection() as conn:
      for f in conn.list_files(prefix, flat):
        yield f

  def transfer_to(
    self, 
    cf_dest:Any, 
    paths:Any = None, # recursive CloudFiles not supported as type
    block_size:int = 64, 
    reencode:Optional[str] = None,
    content_type:Optional[str] = None,
    allow_missing:bool = False,
    progress:Optional[bool] = None,
    resumable:bool = False,
  ) -> None:
    """
    Transfer all files from this CloudFiles storage 
    to the destination CloudFiles in batches sized 
    in the number of files.

    Where possible (file->file, file->cloud, gs->gs, and s3->s3),
    this function will attempt to improve performance by using
    specialized functions to perform the transfer.

      - file->file: Uses OS functions for fast file copying (Python 3.8+)
      - file->cloud: Uses file handles to allow low-memory
        multipart upload
      - gs->gs: Uses GCS copy API to minimize data movement
      - s3->s3: Uses boto s3 copy API to minimize data movement

    cf_dest: another CloudFiles instance or cloudpath 
    paths: if None transfer all files from src, else if
      an iterable, transfer only these files.

      A path is an iterable that contains str, dict, tuple, or list
      elements. If dict, by adding the "dest_path" key, you can 
      rename objects being copied. With tuple or list, the first
      element of the pair is the source key, the second element
      is the destination key.

      A CloudFiles object may be supplied as a paths object
      which will trigger the listing operation.

    block_size: number of files to transfer per a batch
    reencode: if not None, reencode the compression type
      as '' (None), 'gzip', 'br', 'zstd'
    content_type: if provided, set the Content-Type header
      on the upload. This is necessary for e.g. file->cloud

    resumable: for remote->file downloads, download to a .part
      file and rename it when the download completes. If the 
      download does not complete, it can be resumed. Only 
      supported for https->file currently. 
    """
    if isinstance(cf_dest, str):
      cf_dest = CloudFiles(
        cf_dest, progress=False, 
        green=self.green, num_threads=self.num_threads,
      )

    return cf_dest.transfer_from(
      self, paths, block_size, 
      reencode, content_type,
      allow_missing, 
      progress, resumable,
    )

  def transfer_from(
    self, 
    cf_src:Any, 
    paths:Any = None, # recursive CloudFiles not supported as type
    block_size:int = 64, 
    reencode:Optional[str] = None,
    content_type:Optional[str] = None,
    allow_missing:bool = False,
    progress:Optional[bool] = None,
    resumable:bool = False,
  ) -> None:
    """
    Transfer all files from the source CloudFiles storage 
    to this CloudFiles in batches sized in the 
    number of files.

    Where possible (file->file, file->cloud, gs->gs, and s3->s3),
    this function will attempt to improve performance by using
    specialized functions to perform the transfer.

      - file->file: Uses OS functions for fast file copying (Python 3.8+)
      - file->cloud: Uses file handles to allow low-memory
        multipart upload
      - gs->gs: Uses GCS copy API to minimize data movement
      - s3->s3: Uses boto s3 copy API to minimize data movement

    cf_src: another CloudFiles instance or cloudpath 
    paths: if None transfer all files from src, else if
      an iterable, transfer only these files.

      A path is an iterable that contains str, dict, tuple, or list
      elements. If dict, by adding the "dest_path" key, you can 
      rename objects being copied. With tuple or list, the first
      element of the pair is the source key, the second element
      is the destination key.

      A CloudFiles object may be supplied as a paths object
      which will trigger the listing operation.

    block_size: number of files to transfer per a batch
    reencode: if not None, reencode the compression type
      as '' (None), 'gzip', 'br', 'zstd'
    content_type: if provided, set the Content-Type header
      on the upload. This is necessary for e.g. file->cloud
    resumable: for remote->file downloads, download to a .part
      file and rename it when the download completes. If the 
      download does not complete, it can be resumed. Only 
      supported for https->file currently. 
    """
    if isinstance(cf_src, str):
      cf_src = CloudFiles(
        cf_src, progress=False, 
        green=self.green, num_threads=self.num_threads,
      )

    if paths is None:
      paths = cf_src

    total = totalfn(paths, None)

    disable = progress
    if disable is None:
      disable = self.progress
    if disable is None:
      disable = False
    else:
      disable = not disable

    with tqdm(desc="Transferring", total=total, disable=disable) as pbar:
      if (
        cf_src.protocol == "file"
        and self.protocol == "file"
        and reencode is None
      ):
        self.__transfer_file_to_file(
          cf_src, self, paths, total, 
          pbar, block_size, allow_missing
        )
      elif (
        cf_src.protocol != "file"
        and self.protocol == "file"
        and reencode is None
      ):
        self.__transfer_remote_to_file(
          cf_src, self, paths, total, 
          pbar, block_size, content_type,
          allow_missing, resumable,
        )
      elif (
        cf_src.protocol == "file"
        and self.protocol != "file"
        and reencode is None
      ):
        self.__transfer_file_to_remote(
          cf_src, self, paths, total, 
          pbar, block_size, content_type,
          allow_missing,
        )
      elif (
        (
          (cf_src.protocol == "gs" and self.protocol == "gs")
          or (
            cf_src.protocol == "s3" and self.protocol == "s3"
            and cf_src._path.host == self._path.host
            and cf_src._path.alias == self._path.alias
          )
        )
        and reencode is None
      ):
        self.__transfer_cloud_internal(
          cf_src, self, paths, 
          total, pbar, block_size, 
          allow_missing,
        )
      else:
        self.__transfer_general(
          cf_src, self, paths, total, 
          pbar, block_size, 
          reencode, content_type, 
          allow_missing,
        )

  def __transfer_general(
    self, cf_src, cf_dest, paths, 
    total, pbar, block_size,
    reencode, content_type, 
    allow_missing
  ):
    """
    Downloads the file into RAM, transforms
    the data, and uploads it. This is the slowest and
    most memory intensive transfer variant, and 
    doesn't avoid any data movement, but it can handle any
    pair of endpoints as well as transcoding compression
    formats.
    """
    for block_paths in sip(paths, block_size):
      for path in block_paths:
        if isinstance(path, dict):
          if "dest_path" in path:
            path["tags"] = { "dest_path": path["dest_path"] }
      downloaded = cf_src.get(block_paths, raw=True, progress=False)
      if reencode is not None:
        downloaded = compression.transcode(downloaded, reencode, in_place=True)
      def renameiter():
        nonlocal allow_missing
        for item in downloaded:
          if item["content"] is None:
            if allow_missing:
              item["content"] = b""
            else:
              raise FileNotFoundError(f"{item['path']}")
          if (
            item["tags"] is not None
            and "dest_path" in item["tags"]
          ):
            item["path"] = item["tags"]["dest_path"]
            del item["tags"]["dest_path"]
          yield item
      self.puts(
        renameiter(), 
        raw=True, 
        progress=False, 
        compress=reencode,
        content_type=content_type,
      )
      pbar.update(len(block_paths))

  def __transfer_file_to_file(
    self, cf_src, cf_dest, paths, 
    total, pbar, block_size, allow_missing
  ):
    """
    shutil.copyfile, starting in Python 3.8, uses
    special OS kernel functions to accelerate file copies
    """
    srcdir = cf_src.cloudpath.replace("file://", "")
    destdir = mkdir(cf_dest.cloudpath.replace("file://", ""))
    for path in paths:
      if isinstance(path, dict):
        src = os.path.join(srcdir, path["path"])
        dest = os.path.join(destdir, path["dest_path"])
      else:
        src = os.path.join(srcdir, path)
        dest = os.path.join(destdir, path)
      mkdir(os.path.dirname(dest))
      src, encoding = FileInterface.get_encoded_file_path(src)
      _, dest_ext = os.path.splitext(dest)
      dest_ext_compress = FileInterface.get_extension(encoding)
      if dest_ext_compress != dest_ext:
        dest += dest_ext_compress

      try:
        shutil.copyfile(src, dest) # avoids user space
      except FileNotFoundError:
        if allow_missing:
          with open(dest, "wb") as f:
            f.write(b'')
        else:
          raise

      pbar.update(1)

  def __transfer_remote_to_file(
    self, cf_src, cf_dest, paths, 
    total, pbar, block_size, content_type,
    allow_missing, resumable,
  ):
    def thunk_save(key):
      with cf_src._get_connection() as conn:
        if isinstance(key, dict):
          dest_key = key.get("dest_path", key["path"])
          src_key = key["path"]
        else:
          src_key = key
          dest_key = key

        dest_key = os.path.join(cf_dest._path.path, dest_key)
        found = conn.save_file(src_key, dest_key, resumable=resumable)

      if found == False and not allow_missing:
        raise FileNotFoundError(src_key)

      return int(found)

    results = schedule_jobs(
      fns=( partial(thunk_save, path) for path in paths ),
      progress=pbar,
      concurrency=self.num_threads,
      total=totalfn(paths, total),
      green=self.green,
      count_return=True,
    )
    return len(results)

  def __transfer_file_to_remote(
    self, cf_src, cf_dest, paths, 
    total, pbar, block_size, content_type,
    allow_missing
  ):
    """
    Provide file handles instead of slurped binaries 
    so that GCS and S3 can do low-memory chunked multi-part 
    uploads if necessary.
    """
    srcdir = cf_src.cloudpath.replace("file://", "")
    for block_paths in sip(paths, block_size):
      to_upload = []
      for path in block_paths:
        if isinstance(path, dict):
          src_path = path["path"]
          dest_path = path.get("dest_path", path["path"])
        else:
          src_path = path
          dest_path = posixpath.sep.join(path.split(os.path.sep))

        handle_path, encoding = FileInterface.get_encoded_file_path(
          os.path.join(srcdir, src_path)
        )
        try: 
          handle = open(handle_path, "rb")
        except FileNotFoundError:
          if allow_missing:
            handle = b''
          else:
            raise

        if dest_path == '':
          dest_path = src_path

        to_upload.append({
          "path": dest_path,
          "content": handle,
          "compress": encoding,
        })
      cf_dest.puts(to_upload, raw=True, progress=False, content_type=content_type)
      for item in to_upload:
        handle = item["content"]
        if hasattr(handle, "close"):
          handle.close()
      pbar.update(len(block_paths))

  def __transfer_cloud_internal(
    self, cf_src, cf_dest, paths, 
    total, pbar, block_size, allow_missing
  ):
    """
    For performing internal transfers in gs or s3.
    This avoids a large amount of data movement as 
    otherwise the client would have to download and
    then upload again. If the client is located outside
    of the cloud, this is much slower and more expensive
    than necessary.
    """
    def thunk_copy(key):
      with cf_src._get_connection() as conn:
        if isinstance(key, dict):
          dest_key = key.get("dest_path", key["path"])
          src_key = key["path"]
        else:
          src_key = key
          dest_key = key

        dest_key = posixpath.join(cf_dest._path.path, dest_key)
        found = conn.copy_file(src_key, cf_dest._path.bucket, dest_key)

      if found == False and not allow_missing:
        raise FileNotFoundError(src_key)

      return int(found)

    results = schedule_jobs(
      fns=( partial(thunk_copy, path) for path in paths ),
      progress=pbar,
      concurrency=self.num_threads,
      total=totalfn(paths, total),
      green=self.green,
      count_return=True,
    )
    return len(results)

  def move(self, src:str, dest:str):
    """Move (rename) src to dest.

    src and dest do not have to be on the same filesystem.
    """
    epath = paths.extract(dest)
    full_cloudpath = paths.asprotocolpath(epath)
    dest_cloudpath = paths.dirname(full_cloudpath)
    base_dest = paths.basename(full_cloudpath)

    return self.moves(dest_cloudpath, [
      (src, base_dest)
    ], block_size=1, progress=False)

  def moves(
    self,
    cf_dest:Any,
    paths:Union[Sequence[str], Sequence[Tuple[str, str]]],
    block_size:int = 64, 
    total:Optional[int] = None,
    progress:Optional[bool] = None,
  ):
    """
    Move (rename) files.

    pairs: [ (src, dest), (src, dest), ... ]
    """
    if isinstance(cf_dest, str):
      cf_dest = CloudFiles(
        cf_dest, progress=False, 
        green=self.green, num_threads=self.num_threads,
      )
    
    total = totalfn(paths, total)

    disable = not (self.progress if progress is None else progress)

    if self.protocol == "file" and cf_dest.protocol == "file":
      self.__moves_file_to_file(
        cf_dest, paths, total, 
        disable, block_size
      )
      return

    pbar = tqdm(total=total, disable=disable, desc="Moving")

    with pbar:
      for subpairs in sip(paths, block_size):
        subpairs = [
          ((pair, pair) if isinstance(pair, str) else pair)
          for pair in subpairs
        ]

        self.transfer_to(cf_dest, paths=(
          {
            "path": src,
            "dest_path": dest,
          } 
          for src, dest in subpairs
        ), progress=False)
        self.delete(( src for src, dest in subpairs ), progress=False)
        pbar.update(len(subpairs))

  def __moves_file_to_file(
    self, 
    cf_dest:Any, 
    paths:Union[Sequence[str], Sequence[Tuple[str,str]]],
    total:Optional[int], 
    disable:bool,
    block_size:int,
  ):
    for pair in tqdm(paths, total=total, disable=disable, desc="Moving"):
      if isinstance(pair, str):
        src = pair
        dest = pair
      else:
        (src, dest) = pair

      src = self.join(self.cloudpath, src).replace("file://", "")
      dest = cf_dest.join(cf_dest.cloudpath, dest).replace("file://", "")

      if os.path.isdir(dest):
        dest = cf_dest.join(dest, os.path.basename(src))
      else:
        mkdir(os.path.dirname(dest))

      src, encoding = FileInterface.get_encoded_file_path(src)
      _, dest_ext = os.path.splitext(dest)
      dest_ext_compress = FileInterface.get_extension(encoding)
      if dest_ext_compress != dest_ext:
        dest += dest_ext_compress
      shutil.move(src, dest)

  def join(self, *paths:str) -> str:
    """
    Convenience method for joining path strings
    together. This is usually trivial except when
    the file paths are local windows paths which
    must use backslash. This method automatically 
    uses the correct joining convention.
    """
    if len(paths) == 0:
      return ''

    if self._path.protocol == "file":
      return os.path.join(*paths)
    return posixpath.join(*paths)

  def __getitem__(self, key) -> Union[dict,bytes,List[dict]]:
    if isinstance(key, tuple) and len(key) == 2 and isinstance(key[1], slice) and isinstance(key[0], str):
      return self.get({ 'path': key[0], 'start': key[1].start, 'end': key[1].stop })
    elif key == slice(None, None, None):
      return self.get(self.list())
    elif isinstance(key, slice):
      return self.get(itertools.islice(self.list(), key.start, key.stop, key.step))

    return self.get(key)

  # recursive CloudFiles not supported as type
  def __setitem__(self, key:Union[str, slice], value:Any): 
    if isinstance(value, CloudFiles):
      if key == slice(None, None, None):
        self.transfer_from(value)
        return
      else:
        raise KeyError("We only support the complete set slice. `:`. Got: " + str(key))

    key = cast(str, key)
    self.put(key, value)

  def __delitem__(self, key:GetPathType):
    return self.delete(key)

  def __iter__(self) -> Generator[str,None,None]:
    return self.list()

class CloudFile:
  def __init__(
    self, path:str, cache_meta:bool = False, 
    secrets:SecretsType = None,
    composite_upload_threshold:int = int(1e8),
    locking:bool = True,
    lock_dir:Optional[str] = None,
  ):
    path = paths.normalize(path)
    self.cf = CloudFiles(
      paths.dirname(path), 
      secrets=secrets, 
      composite_upload_threshold=composite_upload_threshold,
      locking=locking,
      lock_dir=lock_dir,
    )
    self.filename = paths.basename(path)
    
    self.cache_meta = cache_meta
    self._size:Optional[int] = None
    self._head = None

  @property
  def protocol(self):
    return self.cf.protocol

  @property
  def lock_dir(self):
    return self.cf.lock_dir

  def clear_locks(self):
    return self.cf.clear_locks()

  def delete(self) -> None:
    """Deletes the file."""
    return self.cf.delete(self.filename)

  def exists(self) -> bool:
    """Does the file exist?"""
    return self.cf.exists(self.filename)

  def get(self, ranges=None, raw:bool = False):
    """Download all or part of a file."""
    if ranges is None:
      res = self.cf.get(self.filename, raw=raw)
      if raw == True:
        self._size = len(res)
      return res

    reqs = []
    for rng in ranges:
      if isinstance(rng, slice):
        reqs.append({
          "path": self.filename,
          "start": rng.start,
          "end": rng.end,
        })
      else:
        reqs.append(rng)

    return self.cf.get(reqs, raw=raw)

  def get_json(self) -> Union[dict,list]:
    """Download a json file and decode to a dict or list."""
    return self.cf.get_json(self.filename)

  def put(self, content:bytes, *args, **kwargs):
    """Upload a file."""
    res = self.cf.put(self.filename, content, *args, **kwargs)
    if hasattr(content, "__len__"):
      self._size = len(content)
    else:
      content.seek(0, os.SEEK_END)
      self._size = content.tell()
    return res

  def put_json(self, content, *args, **kwargs):
    """Upload a file as JSON."""
    content = jsonify(content)

    content_type = "application/json"
    if content_type not in args:
      kwargs["content_type"] = "application/json"
    
    try:
      return self.cf.put(self.filename, content, *args, **kwargs)
    finally:
      if hasattr(content, "__len__"):
        self._size = len(content)
      else:
        content.seek(0, os.SEEK_END)
        self._size = content.tell()

  def head(self) -> dict:
    """Get the file metadata."""
    if self._head is not None and self.cache_meta:
      return self._head
    self._head = self.cf.head(self.filename)
    return self._head

  def size(self) -> int:
    """Get the file size in bytes."""
    if self._size is not None and self.cache_meta:
      return self._size
    self._size = self.cf.size(self.filename)
    return self._size

  def transfer_to(    
    self, cloudpath:str, 
    reencode:Optional[str] = None
  ):
    epath = paths.extract(cloudpath)
    self.cf.transfer_to(
      paths.asbucketpath(epath),
      paths=[{
        "path": self.filename,
        "dest_path": epath.path,
      }],
      reencode=reencode,
    )

  def transfer_from(
    self, cloudpath:str, 
    reencode:Optional[str] = None
  ):
    epath = paths.extract(cloudpath)
    self.cf.transfer_from(
      paths.asbucketpath(epath), 
      paths=[{
        "path": epath.path,
        "dest_path": self.filename,
      }],
      reencode=reencode,
    )

  def join(self, *args):
    return self.cf.join(*args)

  def touch(self):
    return self.cf.touch(self.filename)

  def move(self, dest):
    """Move (rename) this file to dest."""
    return self.cf.move(self.filename, dest)

  def __len__(self):
    return self.size()

  def __getitem__(self, rng) -> bytes:
    """Download the file."""
    return self.cf[self.filename, rng]

  def __setitem__(self, content:bytes):
    """Upload the file."""
    self.cf[self.filename] = content