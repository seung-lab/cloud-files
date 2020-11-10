from queue import Queue
from collections import defaultdict
import itertools
import os.path
import posixpath
import re
from functools import partial
import types

import orjson
from tqdm import tqdm

import google.cloud.storage 

from . import compression, paths
from .exceptions import UnsupportedProtocolError, MD5IntegrityError, CRC32CIntegrityError
from .lib import (
  mkdir, toiter, scatter, jsonify, nvl, 
  duplicates, first, sip, STRING_TYPES, 
  md5, crc32c, decode_crc32c_b64
)
from .threaded_queue import ThreadedQueue, DEFAULT_THREADS
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
  'matrix': S3Interface,
  'http': HttpInterface,
  'https': HttpInterface,
  'mem': MemoryInterface,
}

def get_interface_class(protocol):
  if protocol in INTERFACES:
    return INTERFACES[protocol]
  else:
    raise UnsupportedProtocolError("Only {} are supported. Got: {}".format(
      ", ".join(list(INTERFACES.keys())), protocol
    ))

def path_to_byte_range(path):
  if isinstance(path, STRING_TYPES):
    return (path, None, None)
  return (path['path'], path['start'], path['end'])

def totalfn(files, total):
  if total is not None:
    return total
  try:
    return len(files)
  except TypeError:
    return None
  
class CloudFiles(object):
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
    self, cloudpath, progress=False, 
    green=False, secrets=None, num_threads=20,
    use_https=False, endpoint=None
  ):
    if use_https:
      cloudpath = paths.to_https_protocol(cloudpath)

    self.cloudpath = cloudpath
    self.progress = progress
    self.secrets = secrets
    self.num_threads = num_threads
    self.green = bool(green)

    self._path = paths.extract(cloudpath)
    if endpoint:
      self._path = paths.ExtractedPath(host=endpoint, **self._path)
    self._interface_cls = get_interface_class(self._path.protocol)

    if self._path.protocol == 'mem':
      self.num_threads = 0

  def _progress_description(self, prefix):
    if isinstance(self.progress, STRING_TYPES):
      return prefix + ' ' + self.progress
    else:
      return prefix if self.progress else None

  def _get_connection(self):
    return self._interface_cls(
      self._path, 
      secrets=self.secrets,
    )

  def get(self, paths, total=None, raw=False, progress=None):
    """
    Download one or more files.

    paths: scalar or iterable of:
      filename (strings)
      OR
      { 'path': filename, 'start': (int) start byte, 'end': (int) end byte }
    total: manually provide a progress bar size if paths does
      not support the `len` operator.
    raw: download without decompressing
    
    Returns:
      if paths is scalar:
        binary
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
      path, start, end = path_to_byte_range(path)
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

      return { 
        'path': path, 
        'content': content, 
        'byte_range': (start, end),
        'error': error,
        'compress': encoding,
        'raw': raw,
      }
    
    total = totalfn(paths, total)

    if total == 1:
      ret = download(paths[0])
      if multiple_return:
        return [ ret ]
      elif ret['error']:
        raise ret['error']
      else:
        return ret['content']

    return schedule_jobs(
      fns=( partial(download, path) for path in paths ), 
      concurrency=self.num_threads, 
      progress=progress,
      total=total,
      green=self.green,
    )

  def get_json(self, paths, total=None):
    """
    Download one or more JSON files and decode them into a python object.

    paths: scalar or iterable of:
      filename (strings)
      OR
      { 'path': filename, 'start': (int) start byte, 'end': (int) end byte }

    total: Can be used to provide a size for the progress bar if the 
      input type of paths does not support `len`.

    Returns:
      if paths is scalar:
        object
      else:
        [
          {
            'path': path, 
            'content': object, 
            'byte_range': (start, end),
            'error': error,
          }
        ]
    """
    paths, multiple_return = toiter(paths, is_iter=True)
    contents = self.get(paths, total=total)

    def decode(content):
      content = content['content']
      if content is None:
        return None
      return orjson.loads(content.decode('utf8'))

    if not multiple_return and contents and contents[0]['error']:
      raise contents[0]['error']

    contents = [ decode(content) for content in contents ]
    if multiple_return:
      return contents
    return contents[0]

  def puts(
    self, files, 
    content_type=None, compress=None, 
    compression_level=None, cache_control=None,
    total=None, raw=False, progress=None
  ):
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
        )

    if not isinstance(files, types.GeneratorType):
      dupes = duplicates([ todict(file)['path'] for file in files ])
      if dupes:
        raise ValueError("Cannot write the same file multiple times in one pass. This causes a race condition. Files: " + ", ".join(dupes))
    
    total = totalfn(files, total)

    if total == 1:
      uploadfn(first(files))
      return

    fns = ( partial(uploadfn, file) for file in files )
    desc = self._progress_description('Uploading')
    schedule_jobs(
      fns=fns,
      concurrency=self.num_threads,
      progress=(desc if progress else None),
      total=total,
      green=self.green,
    )

  def put(
    self, 
    path, content,     
    content_type=None, compress=None, 
    compression_level=None, cache_control=None,
    raw=False
  ):
    """
    Write a single file.

    path: (str) file path relative to cloudpath
    content: binary string 
    content_type: e.g. 'application/json' or 'application/octet-stream'
    compress: None, 'gzip', 'br' (brotli), 'zstd'
    compression_level: (None or int) input to compressor, None means use default
    cache_control: (str) HTTP Cache-Control header.
    raw: (bool) if true, content is pre-compressed and 
      will bypass the compressor

    Returns: void
    """
    return self.puts({
      'path': path,
      'content': content,
      'content_type': content_type,
      'compress': compress,
      'compression_level': compression_level,
      'cache_control': cache_control,
    }, raw=raw)

  def put_jsons(
    self, files,     
    compress=None, compression_level=None, 
    cache_control=None, total=None, raw=False,
    progress=None
  ):
    """
    Write one or more files as JSON.

    See puts for details. The major difference is that
    the 'content' field is converted from a Python object 
    to JSON.

    Returns: void
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
      content_type='application/json', total=total, raw=raw,
      progress=progress
    )

  def put_json(
    self, path, content,
    compress=None, compression_level=None, 
    cache_control=None
  ):
    """
    Write a single JSON file. Automatically supplies the
    content_type 'application/json'.

    path: (str) file path relative to cloudpath
    content: JSON serializable Python object
    compress: None, 'gzip', 'br' (brotli), 'zstd'
    compression_level: (None or int) input to compressor, None means use default
    cache_control: (str) HTTP Cache-Control header.

    Returns: void
    """
    return self.put_jsons({
      'path': path,
      'content': content,
      'content_type': 'application/json',
      'compress': compress,
      'compression_level': compression_level,
      'cache_control': cache_control,
    })

  def exists(self, paths, total=None, progress=None):
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

    def exist_thunk(paths):
      with self._get_connection() as conn:
        results.update(conn.files_exist(paths))

    batch_size = self._interface_cls.exists_batch_size
    
    desc = self._progress_description('Existence Testing')
    schedule_jobs(
      fns=( partial(exist_thunk, paths) for paths in sip(paths, batch_size) ),
      progress=(desc if progress else None),
      concurrency=self.num_threads,
      total=totalfn(paths, total),
      green=self.green,
    )

    if return_multiple:
      return results
    return first(results.values())

  def size(self, paths, total=None, progress=None):
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

  def delete(self, paths, total=None, progress=None):
    """
    Delete one or more files.

    paths: (str) one or more file paths relative 
      to the class instance's cloudpath.
    total: manually provide a progress bar size if paths does
      not support the `len` operator.
    progress: selectively enable or disable progress just for this
      function call. If progress is a string, it sets the 
      text of the progress bar.

    Returns: void
    """
    paths = toiter(paths)
    progress = nvl(progress, self.progress)

    def thunk_delete(path):
      with self._get_connection() as conn:
        conn.delete_files(path)

    desc = self._progress_description('Deleting')

    batch_size = self._interface_cls.delete_batch_size

    schedule_jobs(
      fns=( partial(thunk_delete, path) for path in sip(paths, batch_size) ),
      progress=(desc if progress else None),
      concurrency=self.num_threads,
      total=totalfn(paths, total),
      green=self.green,
    )

  def list(self, prefix="", flat=False):
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
    self, cf_dest, paths=None, 
    block_size=64, reencode=None
  ):
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
    if isinstance(cf_dest, STRING_TYPES):
      cf_dest = CloudFiles(
        cf_dest, progress=False, 
        green=self.green, num_threads=self.num_threads,
      )

    return cf_dest.transfer_from(self, paths, block_size, reencode)

  def transfer_from(
    self, cf_src, paths=None, 
    block_size=64, reencode=None
  ):
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
    if isinstance(cf_src, STRING_TYPES):
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
        self.puts(downloaded, raw=True, progress=False)
        pbar.update(len(block_paths))

  def __getitem__(self, key):
    if isinstance(key, tuple) and len(key) == 2 and isinstance(key[1], slice) and isinstance(key[0], STRING_TYPES):
      return self.get({ 'path': key[0], 'start': key[1].start, 'end': key[1].stop })
    elif key == slice(None, None, None):
      return self.get(self.list())
    elif isinstance(key, slice):
      return self.get(itertools.islice(self.list(), key.start, key.stop, key.step))

    return self.get(key)

  def __setitem__(self, key, value):
    if isinstance(value, CloudFiles):
      if key == slice(None, None, None):
        self.transfer_from(value)
        return
      else:
        raise KeyError("We only support the complete set slice. `:`. Got: " + str(key))

    self.put(key, value)

  def __delitem__(self, key):
    return self.delete(key)

  def __iter__(self):
    return self.list()

