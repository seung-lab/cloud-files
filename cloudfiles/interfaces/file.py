from typing import Optional, Callable, Any

from .base import (
	COMPRESSION_EXTENSIONS, 
	EXT_TEST_SEQUENCE, 
	EXT_TEST_SEQUENCE_LOCK,
	GZIP_TYPES, 
	StorageInterface, 
)

import binascii
import os
import re
import shutil
import threading

import fasteners

from ..lib import mkdir

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

  def _try_extensions(self, file_path:str, fn:Callable, null_return:Any):
    global EXT_TEST_SEQUENCE
    path = self.get_path_to_file(file_path)

    def _try_extensions_helper():
      with EXT_TEST_SEQUENCE_LOCK:
        seq = list(EXT_TEST_SEQUENCE)

      i = 0
      try:
        for i, (ext, encoding) in enumerate(seq):
          try:
            return fn(path + ext, encoding)
          except FileNotFoundError:
            continue
      finally:
        if i > 0:
          with EXT_TEST_SEQUENCE_LOCK:
            EXT_TEST_SEQUENCE.insert(0, EXT_TEST_SEQUENCE.pop(i))

      return null_return

    return self.io_with_lock(_try_extensions_helper, path, exclusive=False)

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
    global read_file

    def do_get_file(path:str, encoding:str):
      return read_file(path, encoding, start, end)
    return self._try_extensions(file_path, do_get_file, (None, None, None, None))

  def get_file_acl(self, file_path:str):
    def do_stat_file(path:str, encoding:str):
      return os.stat(path).st_mode
    return self._try_extensions(file_path, do_stat_file, None)

  def size(self, file_path):
    def do_size(path:str, encoding:str):
      return os.path.getsize(path)
    return self._try_extensions(file_path, do_size, None)

  def subtree_size(self, prefix:str = "") -> tuple[int,int]:
    total_bytes = 0
    total_files = 0

    subdir = self.get_path_to_file("")
    if prefix:
      subdir = os.path.join(subdir, os.path.dirname(prefix))

    for root, dirs, files in os.walk(subdir):
      for f in files:
          path = os.path.join(root, f)
          total_files += 1
          total_bytes += os.path.getsize(path)

    return (total_files, total_bytes)

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

  def list_files(
    self, 
    prefix:str, 
    flat:bool = False,
    size:bool = False,
    return_resume_token:bool = False,
    resume_token:Optional[str] = None,
  ):
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

    if not size and not return_resume_token:
      return iter(filenames)
    elif size and return_resume_token:
      return ( (filename, os.path.getsize(filename), None) for filename in filenames )
    elif size and not return_resume_token:
      return ( (filename, os.path.getsize(filename)) for filename in filenames )
    elif not size and return_resume_token:
      return ( (filename, None) for filename in filenames )
