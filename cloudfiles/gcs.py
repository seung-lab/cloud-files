"""

"""
from typing import BinaryIO, Optional

import os
import math
import posixpath

from google.cloud.storage import Client
from google.oauth2 import service_account

from . import paths
from .secrets import google_credentials

MAX_COMPOSITE_PARTS = 32 # google specified restriction

def composite_upload(
  cloudpath:str, 
  handle:BinaryIO,
  part_size:int = int(1e8),
  content_type:Optional[str] = None
) -> int:
  from .cloudfiles import CloudFiles
  
  path = paths.extract(cloudpath)

  handle.seek(0, os.SEEK_END)
  file_size = handle.tell()
  handle.seek(0, os.SEEK_SET)

  num_parts = int(math.ceil(file_size / part_size))

  if num_parts > MAX_COMPOSITE_PARTS:
    raise ValueError(f"file size {file_size} not supported yet")

  cur = 0
  parts = []
  while cur < file_size:
    binary = handle.read(part_size)
    cur += len(binary)
    parts.append(binary)

  cf = CloudFiles(posixpath.dirname(cloudpath))
  base_key = posixpath.basename(cloudpath)

  parts = [ 
    (f"{base_key}.{i}.part", binary)  
    for i, binary in enumerate(parts)
  ] 

  cf.puts(parts)
  names = [ name for name, binary in parts ]

  project, credentials = google_credentials(path.bucket)

  client = Client(
    credentials=credentials,
    project=project,
  )
  bucket = client.bucket(path.bucket)

  subpath = posixpath.dirname(path.path)
  destination = bucket.blob(posixpath.join(subpath, base_key))
  if content_type:
    destination.content_type = content_type

  destination.compose(
    [ bucket.blob(posixpath.join(subpath, key)) for key in names ]
  )

  cf.delete(names)

  return 1

  
