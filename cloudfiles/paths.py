from collections import namedtuple
import os
import posixpath
import re
import sys
import urllib.parse

from .exceptions import UnsupportedProtocolError
from .lib import yellow, toabs

ExtractedPath = namedtuple('ExtractedPath', 
  ('format', 'protocol', 'bucket', 'path', 'host')
)

ALLOWED_PROTOCOLS = [ 'gs', 'file', 's3', 'matrix', 'http', 'https', 'mem' ]
ALLOWED_FORMATS = [ 'graphene', 'precomputed', 'boss' ] 

CLOUDPATH_ERROR = yellow("""
Cloud Path must conform to [FORMAT://]PROTOCOL://PATH
Examples: 
  precomputed://gs://test_bucket/em
  gs://test_bucket/em
  graphene://https://example.com/image/em

Supported Formats: None (precomputed), {}
Supported Protocols: {}

Cloud Path Recieved: {}
""").format(
  ", ".join(ALLOWED_FORMATS), ", ".join(ALLOWED_PROTOCOLS), '{}' # curry first two
)

def ascloudpath(epath):
  pth = epath.path
  if epath.host:
    pth = posixpath.join(pth.host, epath.path)
  return "{}://{}://{}".format(
    epath.format, epath.protocol, pth
  )

def pop_protocol(cloudpath):
  protocol_re = re.compile(r'(\w+)://')

  match = re.match(protocol_re, cloudpath)

  if not match:
    return (None, cloudpath)

  (protocol,) = match.groups()
  cloudpath = re.sub(protocol_re, '', cloudpath, count=1)

  return (protocol, cloudpath)

def extract_format_protocol(cloudpath):
  error = UnsupportedProtocolError(CLOUDPATH_ERROR.format(cloudpath))
  
  (proto, cloudpath) = pop_protocol(cloudpath)
  
  if proto is None:
    raise error # e.g. ://test_bucket, test_bucket, wow//test_bucket

  fmt, protocol = None, None

  if proto in ALLOWED_PROTOCOLS:
    fmt = 'precomputed'
    protocol = proto 
  elif proto in ALLOWED_FORMATS:
    fmt = proto

  if protocol not in ALLOWED_PROTOCOLS:
    raise error

  (proto, cloudpath) = pop_protocol(cloudpath)

  if proto in ALLOWED_FORMATS:
    raise error # e.g. gs://graphene://
  elif proto in ALLOWED_PROTOCOLS:
    if protocol is None:
      protocol = proto
    else:
      raise error # e.g. gs://gs:// 

  (proto, _) = pop_protocol(cloudpath)
  if proto and not (proto in ('http','https') and proto == 's3'):
    raise error # e.g. gs://gs://gs://

  return (fmt, protocol, cloudpath)

def extract(cloudpath, windows=None):
  """
  Given a valid cloudpath of the form 
  format://protocol://bucket/.../dataset/layer

  Where format in: None, precomputed, boss, graphene
  Where protocol in: None, file, gs, s3, http(s), matrix

  Return an ExtractedPath which breaks out the components
  format, protocol, bucket, path, intermediate_path, dataset, layer

  Raise a cloudvolume.exceptions.UnsupportedProtocolError if the
  path does not conform to a valid path.

  Returns: ExtractedPath
  """
  if len(cloudpath) == 0:
    return ExtractedPath('','','','','')

  bucket_re = re.compile(r'^(/?[~\d\w_\.\-]+(?::\d+)?)/') # posix /what/a/great/path  
  error = UnsupportedProtocolError(CLOUDPATH_ERROR.format(cloudpath))

  fmt, protocol, cloudpath = extract_format_protocol(cloudpath)
  
  if windows is None:
    windows = sys.platform == 'win32'

  if protocol == 'file' and not windows:
    cloudpath = toabs(cloudpath)

  host = None
  if cloudpath.startswith('http://') or cloudpath.startswith('https://'):
    parse = urllib.parse.urlparse(cloudpath)
    host = parse.scheme + "://" + parse.netloc
    cloudpath = parse.path
    if cloudpath.startswith('/') or cloudpath.startswith('\\'):
      cloudpath = cloudpath[1:]

  bucket = None
  if protocol in ('gs', 's3', 'matrix'):
    match = re.match(bucket_re, cloudpath)
    if not match:
      raise error
    (bucket,) = match.groups()

  return ExtractedPath(
    fmt, protocol, bucket, 
    cloudpath, host
  )

def to_https_protocol(cloudpath):
  cloudpath = cloudpath.replace("gs://", "https://storage.googleapis.com/", 1)
  cloudpath = cloudpath.replace("s3://", "https://s3.amazonaws.com/", 1)
  cloudpath = cloudpath.replace("matrix://", "https://s3-hpcrc.rc.princeton.edu/", 1)
  return cloudpath
