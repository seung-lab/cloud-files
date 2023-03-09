from typing import (
  Any, Dict, Optional, 
  Union, List, Tuple, 
  Callable, Generator, 
  Iterable, cast
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
import types

import orjson
import pathos.pools
from tqdm import tqdm

import google.cloud.storage 

from . import compression, paths
from .exceptions import UnsupportedProtocolError, MD5IntegrityError, CRC32CIntegrityError
from .lib import (
  mkdir, totalfn, toiter, scatter, jsonify, nvl, 
  duplicates, first, sip,
  md5, crc32c, decode_crc32c_b64
)
from .paths import ALIASES
from .threaded_queue import ThreadedQueue, DEFAULT_THREADS
from .typing import (
  CompressType, GetPathType, PutScalarType,
  PutType, ParallelType, SecretsType
)
from .scheduler import schedule_jobs

from .interfaces import (
  FileInterface, HttpInterface, 
  S3Interface, GoogleCloudStorageInterface,
  MemoryInterface
)

INTERFACES = {
  'file': FileInterface,
  'gs': GoogleCloudStorageInterface,
  's3': S3Interface,
  'http': HttpInterface,
  'https': HttpInterface,
  'mem': MemoryInterface,
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
        parallel = kwargs.get("parallel", None)
        del kwargs["parallel"]
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
  clustered = defaultdict(list)
  total = 0
  for path in cloudpaths:
    pth = path
    byte_range = None
    if isinstance(path, dict):
      pth = path["path"]
      byte_range = path["byte_range"]

    epath = paths.extract(pth)
    bucketpath = paths.asbucketpath(epath)
    clustered[bucketpath].append({ 
      "path": epath.path,
      "start": (byte_range[0] if byte_range else None), # type: ignore
      "end": (byte_range[1] if byte_range else None), # type: ignore
    })
    total += 1

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
  """
  def __init__(
    self, cloudpath:str, progress:bool = False, 
    green:bool = False, secrets:SecretsType = None, num_threads:int = 20,
    use_https:bool = False, endpoint:Optional[str] = None, 
    parallel:ParallelType = 1, request_payer:Optional[str] = None
  ):
    if use_https:
      cloudpath = paths.to_https_protocol(cloudpath)

    self.cloudpath = cloudpath
    self.progress = progress
    self.secrets = secrets
    self.num_threads = num_threads
    self.green = bool(green)
    self.parallel = int(parallel)
    self.request_payer = request_payer

    self._path = paths.extract(cloudpath)
    if endpoint:
      self._path = paths.ExtractedPath(host=endpoint, **self._path)
    self._interface_cls = get_interface_class(self._path.protocol)

    if self._path.protocol == 'mem':
      self.num_threads = 0

  def _progress_description(self, prefix):
    if isinstance(self.progress, str):
      return prefix + ' ' + self.progress
    else:
      return prefix if self.progress else None

  def _get_connection(self):
    return self._interface_cls(
      self._path, 
      secrets=self.secrets,
      request_payer=self.request_payer
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
    return_dict:bool = False, raise_errors:bool = True
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
          content, encoding, server_hash, server_hash_type = conn.get_file(path, start=start, end=end)
        
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

    results = schedule_jobs(
      fns=( partial(download, path) for path in paths ), 
      concurrency=self.num_threads, 
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
    compression_level:Optional[int]=None, cache_control:Optional[str] = None,
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
    path:str, content:bytes, 
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
    
    res = first(self.list(prefix=prefix))
    return res is not None

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
    total:Optional[int] = None, progress:Optional[bool] = None
  ) -> Union[Dict[str,int],List[Dict[str,int]]]:
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
    self, cf_dest:Any, paths:Any = None, # recursive CloudFiles not supported as type
    block_size:int = 64, reencode:Optional[str] = None
  ) -> None:
    """
    Transfer all files from this CloudFiles storage 
    to the destination CloudFiles in batches sized 
    in the number of files.

    cf_dest: another CloudFiles instance or a cloudpath
    paths: if None transfer all files from src, else if
      an iterable, transfer only these files.
    block_size: number of files to transfer per a batch
    reencode: if not None, reencode the compression type
      as '' (None), 'gzip', 'br', 'zstd'
    """
    if isinstance(cf_dest, str):
      cf_dest = CloudFiles(
        cf_dest, progress=False, 
        green=self.green, num_threads=self.num_threads,
      )

    return cf_dest.transfer_from(self, paths, block_size, reencode)

  def transfer_from(
    self, cf_src:Any, paths:Any = None, # recursive CloudFiles not supported as type
    block_size:int = 64, reencode:Optional[str] = None
  ) -> None:
    """
    Transfer all files from the source CloudFiles storage 
    to this CloudFiles in batches sized in the 
    number of files.

    cf_src: another CloudFiles instance or cloudpath 
    paths: if None transfer all files from src, else if
      an iterable, transfer only these files.
    block_size: number of files to transfer per a batch
    reencode: if not None, reencode the compression type
      as '' (None), 'gzip', 'br', 'zstd'
    """
    if isinstance(cf_src, str):
      cf_src = CloudFiles(
        cf_src, progress=False, 
        green=self.green, num_threads=self.num_threads,
      )

    if paths is None:
      paths = cf_src

    total = totalfn(paths, None)

    with tqdm(desc="Transferring", total=total, disable=(not self.progress)) as pbar:
      for block_paths in sip(paths, block_size):
        downloaded = cf_src.get(block_paths, raw=True, progress=False)
        if reencode is not None:
          downloaded = compression.transcode(downloaded, reencode, in_place=True)

        if (
              cf_src._path.protocol == "file"
          and self._path.protocol != "file"
          and os.path.sep != posixpath.sep
        ):
          for download in downloaded:
            download["path"] = posixpath.sep.join(download["path"].split(os.path.sep))

        self.puts(downloaded, raw=True, progress=False)
        pbar.update(len(block_paths))

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
  def __init__(self, path:str, cache_meta:bool = False):
    path = paths.normalize(path)
    self.cf = CloudFiles(paths.dirname(path))
    self.filename = paths.basename(path)
    
    self.cache_meta = cache_meta
    self._size:Optional[int] = None
    self._head = None

  @property
  def protocol(self):
    return self.cf.protocol

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

  def put(self, content:bytes, *args, **kwargs):
    """Upload a file."""
    res = self.cf.put(self.filename, content, *args, **kwargs)
    self._size = len(content)
    return res

  def put_json(self, content, *args, **kwargs):
    """Upload a file as JSON."""
    return self.put(self.filename, content, *args, **kwargs)    

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

  def __len__(self):
    return self.size()

  def __getitem__(self, rng) -> bytes:
    """Download the file."""
    return self.cf[self.filename, rng]

  def __setitem__(self, content:bytes):
    """Upload the file."""
    self.cf[self.filename] = content