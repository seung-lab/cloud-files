"""
Specialized logic for performing composite parallel uploads
to GCS.
"""
from typing import Union, BinaryIO, Optional

import io
import math
import os
import posixpath

from google.cloud.storage import Client

from . import paths, compression
from .lib import sip
from .secrets import google_credentials
from .typing import SecretsType, CompressType

MAX_COMPOSITE_PARTS = 32 # google specified restriction

def merge(
  cf, bucket, path, 
  depth, max_depth, num_parts, 
  content_type, cache_control, 
  storage_class, content_encoding
):
  subpath = posixpath.dirname(path.path)
  base_key = posixpath.basename(path.path)

  names = [ f"{base_key}.{depth}.{i}.part" for i in range(num_parts) ]

  i = 0
  for subnames in sip(names, MAX_COMPOSITE_PARTS):
    if depth == max_depth:
      node_name = base_key
    else:
      node_name = f"{base_key}.{depth+1}.{i}.part"

    node_name = posixpath.join(subpath, node_name)
    i += 1

    destination = bucket.blob(node_name)
    if content_type:
      destination.content_type = content_type
    if cache_control:
      destination.cache_control = cache_control
    if storage_class:
      destination.storage_class = storage_class

    # avoid early deletion fees
    if depth < max_depth:
      destination.storage_class = "STANDARD"
    elif content_encoding:
      destination.content_encoding = content_encoding

    destination.compose(
      [ bucket.blob(posixpath.join(subpath, key)) for key in subnames ]
    )

    cf.delete(subnames)

  if depth < max_depth:
    merge(
      cf, bucket, path, 
      depth+1, max_depth, 
      int(math.ceil(num_parts / MAX_COMPOSITE_PARTS)), 
      content_type, cache_control,
      storage_class, content_encoding
    )

def composite_upload(
  cloudpath:str, 
  handle:Union[BinaryIO,bytes],
  part_size:int = int(1e8),
  secrets:SecretsType = None,
  content_type:Optional[str] = None,
  progress:bool = False,
  cache_control:Optional[str] = None,
  storage_class:Optional[str] = None,
  compress:CompressType = None,
  skip_compress:bool = False,
) -> int:
  from .cloudfiles import CloudFiles, CloudFile
  
  content_encoding = compression.normalize_encoding(compress)
  if isinstance(handle, bytes):
    if skip_compress:
      handle = io.BytesIO(handle)
    else:
      handle = io.BytesIO(
        compression.compress(handle, compress)
      )
    content_encoding = compression.normalize_encoding(compress)

  path = paths.extract(cloudpath)

  handle.seek(0, os.SEEK_END)
  file_size = handle.tell()
  handle.seek(0, os.SEEK_SET)

  num_parts = int(math.ceil(file_size / part_size))

  if num_parts <= 1:
    CloudFile(cloudpath, secrets=secrets).put(
      handle.read(), 
      content_type=content_type,
      cache_control=cache_control,
      storage_class=storage_class,
      compress=content_encoding,
      raw=True,
    )
    return 1

  cf = CloudFiles(
    posixpath.dirname(cloudpath), 
    secrets=secrets, 
    progress=progress
  )
  base_key = posixpath.basename(cloudpath)

  def partiter():
    cur = 0
    while cur < file_size:
      binary = handle.read(part_size)
      cur += len(binary)
      yield binary

  parts = (
    (f"{base_key}.0.{i}.part", binary)  
    for i, binary in enumerate(partiter())
  )

  # avoid early deletion fees w/ standard storage class
  cf.puts(parts, total=num_parts, storage_class="STANDARD")

  tree_depth = int(math.ceil(math.log2(num_parts) / math.log2(MAX_COMPOSITE_PARTS)))
  
  project, credentials = google_credentials(path.bucket)
  if secrets:
    credentials = secrets

  client = Client(
    credentials=credentials,
    project=project,
  )
  bucket = client.bucket(path.bucket)

  merge(
    cf, bucket, path, 
    0, tree_depth - 1, num_parts, 
    content_type, cache_control,
    storage_class, content_encoding
  )

  return 1

  
