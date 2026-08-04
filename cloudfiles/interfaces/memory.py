from typing import Optional

import os
import posixpath

from .base import (
	COMPRESSION_EXTENSIONS,
	EXT_TEST_SEQUENCE,
	EXT_TEST_SEQUENCE_LOCK,
	GZIP_TYPES,
	MEM_BUCKET_POOL_LOCK,
	MEM_POOL,
	MemoryPoolParams,
	StorageInterface,
)
from ..lib import mkdir

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

    if (
      isinstance(content, str)
      and len(content) > 0
      and content_type
      and re.search('json|te?xt', content_type)
    ):
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
    if isinstance(result, (bytes, bytearray, str)):
      result = result[slice(start, end)]
    return (result, encoding, None, None)

  def get_file_acl(self, file_path:str):
    return None

  def save_file(self, src, dest, resumable) -> tuple[bool,int]:
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
      return (False, 0)

    return (True, len(self._data[path]))

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

  def copy_file(self, src_path, dest_bucket, dest_key) -> tuple[bool,int]:
    key = self.get_path_to_file(src_path)
    with MEM_BUCKET_POOL_LOCK:
     pool = MEM_POOL[MemoryPoolParams(dest_bucket)]
    dest_bucket = pool.get_connection(None, None)
    dest_bucket[dest_key] = self._data[key]
    return (True, len(self._data[key]))

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

    Returns: iterator
    """
    layer_path = self.get_path_to_file("")

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

    # The size operation could be made much faster
    # but will require some surgery
    if not size and not return_resume_token:
      return iter(filenames)
    elif size and return_resume_token:
      return ( (filename, self.size(filename), None) for filename in filenames )
    elif size and not return_resume_token:
      return ( (filename, self.size(filename)) for filename in filenames )
    elif not size and return_resume_token:
      return ( (filename, None) for filename in filenames )

  def subtree_size(self, prefix:str = "") -> tuple[int,int]:
    layer_path = self.get_path_to_file("")        

    remove = layer_path
    if len(remove) and remove[-1] != '/':
      remove += '/'

    total_bytes = 0
    total_files = 0
    for filename, binary in self._data.items():
      f_prefix = f.removeprefix(remove)[:len(prefix)]
      if f_prefix == prefix:
        total_bytes += len(binary)
        total_files += 1

    return (total_files, total_bytes)
