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

def mkregexp():
  fmt_capture = r'|'.join(ALLOWED_FORMATS)
  fmt_capture = "(?:(?P<fmt>{})://)".format(fmt_capture)
  proto_capture = r'|'.join(ALLOWED_PROTOCOLS)
  proto_capture = "(?:(?P<proto>{})://)".format(proto_capture)
  regexp = "{}?{}?".format(fmt_capture, proto_capture)
  return regexp

CLOUDPATH_REGEXP = re.compile(mkregexp())

def ascloudpath(epath):
  pth = ''
  lst = [ epath.host, epath.bucket, epath.path ]
  while lst:
    elem = lst.pop(0)
    if not elem:
      continue
    pth = posixpath.join(pth, elem)

  if not (pth[:4] == 'http' and epath.protocol in ('http', 'https')):
    pth = f"{epath.protocol}://{pth}"

  if epath.format:
    return f"{epath.format}://" + pth
  return pth

def get_protocol(cloudpath):
  m = re.match(CLOUDPATH_REGEXP, cloudpath)
  return m.group('proto')

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

  m = re.match(CLOUDPATH_REGEXP, cloudpath)
  if m is None:
    raise error

  groups = m.groups()
  cloudpath = re.sub(CLOUDPATH_REGEXP, '', cloudpath, count=1)

  fmt = m.group('fmt') or 'precomputed'
  proto = m.group('proto')
  endpoint = None

  if proto in ('http', 'https'):
    cloudpath = proto + "://" + cloudpath
    parse = urllib.parse.urlparse(cloudpath)
    endpoint = parse.scheme + "://" + parse.netloc
    cloudpath = cloudpath.replace(endpoint, '')
    if cloudpath and cloudpath[0] == '/':
      cloudpath = cloudpath[1:]
  elif proto == 's3' and cloudpath[:4] == 'http':
    parse = urllib.parse.urlparse(cloudpath)
    endpoint = parse.scheme + "://" + parse.netloc
    cloudpath = cloudpath.replace(endpoint, '')
    if cloudpath and cloudpath[0] == '/':
      cloudpath = cloudpath[1:]

  return (fmt, proto, endpoint, cloudpath)

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

  bucket_re = re.compile(r'^(/?[~\d\w_\.\-]+(?::\d+)?)(?:/|$)') # posix /what/a/great/path  
  error = UnsupportedProtocolError(CLOUDPATH_ERROR.format(cloudpath))

  fmt, protocol, host, cloudpath = extract_format_protocol(cloudpath)

  if windows is None:
    windows = sys.platform == 'win32'

  if protocol == 'file' and not windows:
    cloudpath = toabs(cloudpath)

  bucket = None
  if protocol in ('gs', 's3', 'matrix'):
    match = re.match(bucket_re, cloudpath)
    if not match:
      raise error
    (bucket,) = match.groups()
    cloudpath = cloudpath.replace(bucket, '')
    if cloudpath and cloudpath[0] == '/':
      cloudpath = cloudpath[1:]
    bucket = bucket.replace('/', '')

  (proto, _) = pop_protocol(cloudpath)
  if proto is not None:
    raise error

  if protocol is None:
    raise error

  return ExtractedPath(
    fmt, protocol, bucket, 
    cloudpath, host
  )

def to_https_protocol(cloudpath):
  if isinstance(cloudpath, ExtractedPath):
    if cloudpath.protocol in ('gs', 's3', 'matrix'):
      return extract(to_https_protocol(ascloudpath(cloudpath)))
    return cloudpath

  cloudpath = cloudpath.replace("gs://", "https://storage.googleapis.com/", 1)
  cloudpath = cloudpath.replace("s3://", "https://s3.amazonaws.com/", 1)
  cloudpath = cloudpath.replace("matrix://", "https://s3-hpcrc.rc.princeton.edu/", 1)
  return cloudpath
