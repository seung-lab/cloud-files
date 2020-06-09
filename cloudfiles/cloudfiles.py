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

  def _progress_description(self, prefix):
    if isinstance(self.progress, str):
      return prefix + ' ' + self.progress
    else:
      return prefix if self.progress else None

  def _get_connection(self):
    return self._interface_cls(self._path, secrets=self.secrets)

  def get(self, paths):
    paths, mutliple_return = toiter(paths, is_iter=True)

    def download(path):
      path, start, end = path_to_byte_range(path)
      error = None
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

  def puts(
    self, files, 
    content_type=None, compress=None, 
    compression_level=None, cache_control=None
  ):
    """
    Places one or more files at a given location.

    files: dict or list thereof.
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

    def uploadfn(file):
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
      total = len(files)
    else:
      total = None

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
    return self.puts({
      'path': path,
      'content': content,
      'content_type': content_type,
      'compress': compress,
      'compression_level': compression_level,
      'cache_control': cache_control,
    })

  def put_jsons(self, files):
    files = toiter(files)
    for i, file in enumerate(files):
      file['content'] = jsonify(file['content']).encode('utf-8')
    return self.puts(files)

  def put_json(
    self, path, content, 
    content_type=None, compress=None, 
    compression_level=None, cache_control=None
  ):
    return self.put_jsons({
      'path': path,
      'content': content,
      'content_type': content_type,
      'compress': compress,
      'compression_level': compression_level,
      'cache_control': cache_control,
    })

  def exists(self, paths):
    paths, return_multiple = toiter(paths, is_iter=True)

    results = {}

    def exist_thunk(paths):
      with self._get_connection() as conn:
        results.update(conn.files_exist(paths))
    
    desc = self._progress_description('Existence Testing')
    schedule_jobs(  
      fns=( partial(exist_thunk, paths) for paths in scatter(paths, self.num_threads) ),
      progress=(desc if self.progress else None),
      concurrency=self.num_threads,
      total=len(paths),
      green=self.green,
    )

    if return_multiple:
      return results
    return first(results.values())

  def delete(self, paths):
    paths = toiter(paths)

    def thunk_delete(path):
      with self._get_connection() as conn:
        conn.delete_file(path)

    desc = self._progress_description('Deleting')

    schedule_jobs(
      fns=( partial(thunk_delete, path) for path in paths ),
      progress=(desc if self.progress else None),
      concurrency=self.num_threads,
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
    with self._get_connection() as conn:
      for f in conn.list_files(prefix, flat):
        yield f


