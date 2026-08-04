from typing import Optional
import posixpath

from .base import (
  StorageInterface, GC_POOL, GCS_BUCKET_POOL_LOCK, GCloudBucketPoolParams,
  retry, retry_if_not
)
from ..exceptions import CompressionError

import google.cloud.exceptions

class GoogleCloudStorageInterface(StorageInterface):
  # Broke this out to avoid overhead of importing GCS library
  exists_batch_size = 1000 # Batch._MAX_BATCH_SIZE
  delete_batch_size = 1000 # Batch._MAX_BATCH_SIZE

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
  def copy_file(self, src_path, dest_bucket, dest_key) -> tuple[bool,int]:
    key = self.get_path_to_file(src_path)
    source_blob = self._bucket.blob( key )
    with GCS_BUCKET_POOL_LOCK:
     pool = GC_POOL[GCloudBucketPoolParams(dest_bucket, self._request_payer)]
    dest_bucket = pool.get_connection(self._secrets, None)

    try:
      blob = self._bucket.copy_blob(
        source_blob, dest_bucket, dest_key
      )
    except google.api_core.exceptions.NotFound:
      return (False, 0)

    return (True, blob.size)

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

  @retry_if_not(google.cloud.exceptions.NotFound)
  def get_file_acl(self, file_path:str):
    key = self.get_path_to_file(file_path)
    blob = self._bucket.get_blob( key )
    return list(blob.acl)

  @retry
  def save_file(self, src, dest, resumable) -> tuple[bool, int]:
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
      return (False, 0)

    num_bytes = os.path.getsize(dest)

    ext = FileInterface.get_extension(blob.content_encoding)
    if not dest.endswith(ext):
      os.rename(dest, dest + ext)

    return (True, num_bytes)

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
    path = posixpath.join(layer_path, prefix)

    delimiter = '/' if flat else None

    items = "name"
    if size:
      items += ",size"

    blobs = self._bucket.list_blobs(
      prefix=path, 
      delimiter=delimiter,
      page_size=2500,
      fields=f"items({items}),nextPageToken,prefixes",
      page_token=resume_token,
    )

    def return_args(filename, blob, page):
      nonlocal blobs
      args = [ filename ]
      if size:
        args.append(blob.size)
      if return_resume_token:
        args.append(blobs.next_page_token)
      
      if len(args) == 1:
        return args[0]
      else:
        return tuple(args)

    for page in blobs.pages:
      if page.prefixes:
        for item in page.prefixes:
          ret = [ item.removeprefix(path) ]
          if size:
            ret.append(0)
          if return_resume_token:
            ret.append(page.next_page_token)

          if len(ret) == 1:
            yield ret[0]
          else:
            yield tuple(ret)

      for blob in page:
        filename = blob.name.removeprefix(layer_path)
        if not filename:
          continue
        elif not flat and filename[-1] != '/':
          yield return_args(filename, blob, page)
        elif flat and '/' not in blob.name.removeprefix(path):
          yield return_args(filename, blob, page)

  @retry
  def subtree_size(self, prefix:str = "") -> tuple[int,int]:
    layer_path = self.get_path_to_file("")        
    path = posixpath.join(layer_path, prefix)

    blobs = self._bucket.list_blobs(
      prefix=path,
      page_size=5000,
      fields="items(name,size),nextPageToken",
    )

    total_bytes = 0
    total_files = 0
    for page in blobs.pages:
      for blob in page:
        total_bytes += blob.size
        total_files += 1

    return (total_files, total_bytes)

  def release_connection(self):
    global GC_POOL
    with GCS_BUCKET_POOL_LOCK:
      pool = GC_POOL[GCloudBucketPoolParams(self._path.bucket, self._request_payer)]
    pool.release_connection(self._bucket)