import six
from six.moves import queue as Queue
from collections import defaultdict
import itertools
import json
import os.path
import posixpath
import re
from functools import partial
import types

from tqdm import tqdm

from . import compression, paths
from .exceptions import UnsupportedProtocolError
from .lib import mkdir, toiter, scatter, jsonify, duplicates, first
from .threaded_queue import ThreadedQueue, DEFAULT_THREADS
from .scheduler import schedule_jobs

from .interfaces import (
  FileInterface, HttpInterface, 
  S3Interface, GoogleCloudStorageInterface
)

def get_interface_class(protocol):
  if protocol == 'file':
    return FileInterface
  elif protocol == 'gs':
    return GoogleCloudStorageInterface
  elif protocol in ('s3', 'matrix'):
    return S3Interface
  elif protocol in ('http', 'https'):
    return HttpInterface
  else:
    raise UnsupportedProtocolError(str(self._path))

def path_to_byte_range(path):
  if isinstance(path, str):
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
    self.endpoint = endpoint

    self._path = paths.extract(cloudpath)
    self._interface_cls = get_interface_class(self._path.protocol)

  def _progress_description(self, prefix):
    if isinstance(self.progress, str):
      return prefix + ' ' + self.progress
    else:
      return prefix if self.progress else None

  def _get_connection(self):
    return self._interface_cls(
      self._path, 
      secrets=self.secrets, 
      endpoint=self.endpoint
    )

  def get(self, paths, total=None):
    """
    Download one or more files.

    paths: scalar or iterable of:
      filename (strings)
      OR
      { 'path': filename, 'start': (int) start byte, 'end': (int) end byte }
    total: manually provide a progress bar size if paths does
      not support the `len` operator.

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
          }
        ]
    """
    paths, mutliple_return = toiter(paths, is_iter=True)

    def download(path):
      path, start, end = path_to_byte_range(path)
      error = None
      content = None
      try:
        with self._get_connection() as conn:
          content, encoding = conn.get_file(path, start=start, end=end)
        content = compression.decompress(content, encoding, filename=path)
      except Exception as err:
        error = err

      return { 
        'path': path, 
        'content': content, 
        'byte_range': (start, end),
        'error': error,
      }

    if total is None:
      try:
        total = len(paths)
      except TypeError:
        pass

    if total == 1:
      ret = download(paths[0])
      if mutliple_return:
        return [ ret ]
      else:
        return ret['content']

    return schedule_jobs(
      fns=( partial(download, path) for path in paths ), 
      concurrency=self.num_threads, 
      progress=self.progress,
      total=paths,
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
      return json.loads(content.decode('utf8'))

    contents = [ decode(content) for content in contents ]
    if multiple_return:
      return contents
    return contents[0]

  def puts(
    self, files, 
    content_type=None, compress=None, 
    compression_level=None, cache_control=None,
    total=None
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
          'compress': None, 'gzip', or 'br',
          'compression_level': e.g. 6, # for gzip or brotli
          'cache_control': specify the header the way you want 
              e.g. 'no-cache' or 'public; max-age=3600' etc
        }

      If the additional fields are specified, they will override the 
      defaults provided by arguments to the function. e.g. you
      can specify cache_control for an list but provide an exception
      for one or more files.

    total: Can be used to provide a size for the progress bar if the 
      input type of files does not support `len`.
    """
    files = toiter(files)

    def uploadfn(file):
      if isinstance(file, tuple):
        file = { 'path': file[0], 'content': file[1] }

      compress = file.get('compress', None)
      if compress not in compression.COMPRESSION_TYPES:
        raise ValueError('{} is not a supported compression type.'.format(compress))

      with self._get_connection() as conn:
        content = compression.compress(
          file['content'], 
          method=compress,
          compress_level=file.get('compression_level', compression_level),
        )
        conn.put_file(
          file_path=file['path'], 
          content=content, 
          content_type=file.get('content_type', content_type),
          compress=compress,
          cache_control=file.get('cache_control', cache_control),
        )

    if not isinstance(files, types.GeneratorType):
      dupes = duplicates([ file['path'] for file in files ])
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
      progress=(desc if self.progress else None),
      total=total,
      green=self.green,
    )

  def put(
    self, 
    path, content,     
    content_type=None, compress=None, 
    compression_level=None, cache_control=None
  ):
    """
    Write a single file.

    path: (str) file path relative to cloudpath
    content: binary string 
    content_type: e.g. 'application/json' or 'application/octet-stream'
    compress: None, 'gzip', 'br' (brotli)
    compression_level: (None or int) input to compressor, None means use default
    cache_control: (str) HTTP Cache-Control header.

    Returns: void
    """
    return self.puts({
      'path': path,
      'content': content,
      'content_type': content_type,
      'compress': compress,
      'compression_level': compression_level,
      'cache_control': cache_control,
    })

  def put_jsons(self, files, total=None):
    """
    Write one or more files as JSON.

    See puts for details. The major difference is that
    the 'content' field is converted from a Python object 
    to JSON.

    Returns: void
    """
    files = toiter(files)

    tojson = lambda x: jsonify(x).encode('utf-8')

    def jsonify_file(file):
      if isinstance(file, list):
        return [ file[0], tojson(file[1]) ]
      elif isinstance(file, tuple):
        return (file[0], tojson(file[1]))
      else:
        file['content'] = tojson(file['content'])
        return file

    total = totalfn(files, total)

    return self.puts( 
      (jsonify_file(file) for file in files), 
      content_type='application/json', total=total
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
    compress: None, 'gzip', 'br' (brotli)
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

  def exists(self, paths, total=None):
    """
    Test if the given file paths exist.

    paths: one or more file paths relative to the cloudpath.
    total: manually provide a progress bar size if paths does
      not support the `len` operator.

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
    results = {}

    def exist_thunk(paths):
      with self._get_connection() as conn:
        results.update(conn.files_exist(paths))

    n = max(self.num_threads, 1)

    desc = self._progress_description('Existence Testing')
    schedule_jobs(  
      fns=( partial(exist_thunk, paths) for paths in scatter(paths, n) ),
      progress=(desc if self.progress else None),
      concurrency=self.num_threads,
      total=totalfn(paths, total),
      green=self.green,
    )

    if return_multiple:
      return results
    return first(results.values())

  def delete(self, paths, total=None):
    """
    Delete one or more files.

    paths: (str) one or more file paths relative 
      to the class instance's cloudpath.
    total: manually provide a progress bar size if paths does
      not support the `len` operator.

    Returns: void
    """
    paths = toiter(paths)

    def thunk_delete(path):
      with self._get_connection() as conn:
        conn.delete_file(path)

    desc = self._progress_description('Deleting')

    schedule_jobs(
      fns=( partial(thunk_delete, path) for path in paths ),
      progress=(desc if self.progress else None),
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

  def __getitem__(self, key):
    if isinstance(key, tuple) and len(key) == 2 and isinstance(key[1], slice) and isinstance(key[0], str):
      return self.get({ 'path': key[0], 'start': key[1].start, 'end': key[1].stop })

    return self.get(key)

  def __setitem__(self, key, value):
    return self.put(key, value)

  def __delitem__(self, key):
    return self.delete(key)

  def __iter__(self):
    return self.list()

