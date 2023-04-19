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
from google.oauth2 import service_account

from . import paths
from .lib import sip
from .secrets import google_credentials
from .typing import SecretsType

MAX_COMPOSITE_PARTS = 32 # google specified restriction

def merge(cf, bucket, path, depth, max_depth, num_parts, content_type):
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

    destination.compose(
      [ bucket.blob(posixpath.join(subpath, key)) for key in subnames ]
    )

    cf.delete(subnames)

  if depth < max_depth:
    merge(
      cf, bucket, path, 
      depth+1, max_depth, 
      int(math.ceil(num_parts / MAX_COMPOSITE_PARTS)), 
      content_type
    )

def composite_upload(
  cloudpath:str, 
  handle:Union[BinaryIO,bytes],
  part_size:int = int(1e8),
  secrets:SecretsType = None,
  content_type:Optional[str] = None
) -> int:
  from .cloudfiles import CloudFiles, CloudFile
  
  if isinstance(handle, bytes):
    handle = io.BytesIO(handle)

  path = paths.extract(cloudpath)

  handle.seek(0, os.SEEK_END)
  file_size = handle.tell()
  handle.seek(0, os.SEEK_SET)

  num_parts = int(math.ceil(file_size / part_size))

  cur = 0
  parts = []
  while cur < file_size:
    binary = handle.read(part_size)
    cur += len(binary)
    parts.append(binary)

  if len(parts) == 1:
    CloudFile(cloudpath, secrets=secrets).put(
      parts[0], content_type=content_type
    )
    return 1

  cf = CloudFiles(posixpath.dirname(cloudpath), secrets=secrets)
  base_key = posixpath.basename(cloudpath)

  parts = [ 
    (f"{base_key}.0.{i}.part", binary)  
    for i, binary in enumerate(parts)
  ] 

  cf.puts(parts)

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
    content_type
  )

  return 1

  
