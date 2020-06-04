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
from .lib import mkdir, toiter, scatter, jsonify, duplicates
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

def default_byte_iterator(starts, ends):
  if starts is None:
    starts = itertools.repeat(None)
  if ends is None:
    ends = itertools.repeat(None)
  return iter(starts), iter(ends)

class CloudFile(object):
  """A container for some file information."""
  def __init__(
    self, path, content, content_type=None,
    compress=None, compression_level=None, 
    cache_control=None, byte_range=None
  ):
    self.path = path
    self.content = content 
    self.content_type = content_type
    self.compress = compress 
    self.compression_level = compression_level
    self.cache_control = cache_control
    self.byte_range = byte_range

  def __getitem__(self, key):
    if key == 'path':
      return self.path
    elif key == 'content':
      return self.content
    elif key == 'content_type':
      return self.content_type
    elif key == 'compression_level':
      return self.compression_level
    elif key == 'cache_control':
      return self.cache_control
    elif key == 'byte_range':
      return self.byte_range

    raise KeyError('{} is not implemented.'.format(key))

class CloudFiles(object):
  def __init__(
    self, cloudpath, progress=False, 
    green=False, secrets=None, num_threads=20
  ):
    self.progress = progress
    self.cloudpath = cloudpath
    self.secrets = secrets
    self.num_threads = num_threads
    self.green = bool(green)

    self._path = paths.extract(cloudpath)
    self._interface_cls = get_interface_class(self._path.protocol)

  def progress_description(self, prefix):
    if isinstance(self.progress, str):
      return prefix + ' ' + self.progress
    else:
      return prefix if self.progress else None

  def get_connection(self, secrets=None):
    return self._interface_cls(self._path, secrets=secrets)

  def get(self, paths):
    paths, mutliple_return = toiter(paths, is_iter=True)

    def download(path):
      path, start, end = path_to_byte_range(path)
      with self.get_connection(self.secrets) as conn:
        content, encoding = conn.get_file(path, start=start, end=end)
      content = compression.decompress(content, encoding, filename=path)
      return CloudFile(path, content, byte_range=(start, end))

    if len(paths) == 1:
      ret = download(paths[0])
      if mutliple_return:
        return [ ret ]
      else:
        return ret['content']

    return schedule_jobs(
      fns=( partial(download, path) for path in paths ), 
      concurrency=self.num_threads, 
      progress=self.progress,
      total=len(paths),
      green=self.green,
    )

  def get_json(self, paths):
    paths, multiple_return = toiter(paths, is_iter=True)
    contents = self.get(paths)

    def decode(content):
      content = content['content']
      if content is None:
        return None
      return json.loads(content.decode('utf8'))

    contents = [ decode(content) for content in contents ]
    if multiple_return:
      return contents
    return contents[0]

  def put(
    self, files, 
    content_type=None, compress=None, 
    compression_level=None, cache_control=None
  ):
    """
    Places one or more files at a given location.

    files: dict, CloudFile, or list thereof.
      If dict, must contain 'content' and 'path' fields:
        {
          'content': b'json data', # must by binary data
          'path': 'info', # specified relative to the cloudpath
        }
      If additional fields are specified, they will override the 
      defaults provided by arguments to the function. e.g. you
      can specify cache_control for an list but provide an exception
      for one or more files.
    """
    files = toiter(files)

    def nvl(val, default):
      if val is not None:
        return val
      return default

    def uploadfn(file):
      if instanceof(file, dict):
        file = CloudFile(**file)

      if file.compress not in compression.COMPRESSION_TYPES:
        raise ValueError('{} is not a supported compression type.'.format(file.compress))

      with self.get_connection() as conn:
        content = compression.compress(
          file.content, 
          method=nvl(file.compress, compress),
          compress_level=nvl(file.compression_level, compression_level),
        )
        conn.put_file(
          file_path=file.path, 
          content=content, 
          content_type=nvl(file.content_type, content_type),
          compress=nvl(file.compress, compress),
          cache_control=nvl(file.cache_control, cache_control),
        )

    if not isinstance(gen, types.GeneratorType):
      dupes = duplicates([ file for file in files ])
      if dupes:
        raise ValueError("Cannot write the same file multiple times in one pass. This causes a race condition. Files: " + ", ".join(dupes))
      total = len(files)
    else:
      total = None

    fns = ( partial(uploadfn, file) for file in files )
    desc = self.progress_description('Uploading')
    schedule_jobs(
      fns=fns,
      progress=(desc if self.progress else None),
      concurrency=self.concurrency,
      total=total,
      green=self.green,
    )

  def put_json(self, files):
    files = toiter(files)
    for i, file in enumerate(files):
      if isinstance(file, dict):
        file['content'] = jsonify(file['content'])
      else:
        file.content = jsonify(file.content)
    return self.put(files)

  def exists(self, paths):
    paths = toiter(paths)

    results = {}

    def exist_thunk(paths):
      with self.get_connection() as conn:
        results.update(conn.files_exist(paths))
    
    desc = self.progress_description('Existence Testing')
    schedule_jobs(  
      fns=( partial(exist_thunk, paths) for paths in scatter(paths, self.concurrency) ),
      progress=(desc if self.progress else None),
      concurrency=self.concurrency,
      total=len(paths),
      green=self.green,
    )

    return results

  def delete(self, paths):
    paths = toiter(paths)

    def thunk_delete(path):
      with self.get_connection() as conn:
        conn.delete_file(path)

    desc = self.progress_description('Deleting')

    schedule_jobs(
      fns=( partial(thunk_delete, path) for path in paths ),
      progress=(desc if self.progress else None),
      concurrency=self.concurrency,
      total=len(paths),
      green=self.green,
    )

  def list(self, prefix="", flat=False):
    """
    List the files in the layer with the given prefix. 

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
    
    Return: generated sequence of file paths relative to layer_path
    """
    with self.get_connection() as conn:
      for f in conn.list_files(prefix, flat):
        yield f


