from collections import namedtuple
import orjson
import os.path
import posixpath
import re
import sys
import urllib.parse

from typing import Tuple, Optional

from .exceptions import UnsupportedProtocolError
from .lib import yellow, toabs

ExtractedPath = namedtuple('ExtractedPath', 
  ('format', 'protocol', 'bucket', 'path', 'host', 'alias')
)

ALIAS_FILE = os.path.expanduser("~/.cloudfiles/aliases.json")
OFFICIAL_ALIASES = {
  "matrix": "s3://https://s3-hpcrc.rc.princeton.edu/",
  "tigerdata": "s3://https://tigerdata.princeton.edu/",
}
ALIASES_FROM_FILE = None
ALIASES = {}
BASE_ALLOWED_PROTOCOLS = [ 
  'gs', 'file', 's3', 
  'http', 'https', 'mem' 
]
ALLOWED_PROTOCOLS = list(BASE_ALLOWED_PROTOCOLS)
ALLOWED_FORMATS = [ 'graphene', 'precomputed', 'boss' ] 

def update_aliases_from_file():
  global ALIASES_FROM_FILE
  global ALIAS_FILE
  if ALIASES_FROM_FILE is not None:
    return

  aliases = {}

  if os.path.exists(ALIAS_FILE) and os.path.getsize(ALIAS_FILE) > 0:
    with open(ALIAS_FILE, "rt") as f:
      aliases = orjson.loads(f.read())

  ALIASES_FROM_FILE = aliases

  for alias, val in aliases.items():
    add_alias(alias, val["host"])

def cloudpath_error(cloudpath):
  return yellow("""
    Cloud Path must conform to [FORMAT://]PROTOCOL://PATH
    Examples: 
      precomputed://gs://test_bucket/em
      gs://test_bucket/em
      graphene://https://example.com/image/em

    Supported Formats: None (precomputed), {}
    Supported Protocols: {}

    Cloud Path Recieved: {}
  """).format(
    ", ".join(ALLOWED_FORMATS), 
    ", ".join(ALLOWED_PROTOCOLS), 
    cloudpath
  )

def mkregexp():
  fmt_capture = r'|'.join(ALLOWED_FORMATS)
  fmt_capture = "(?:(?P<fmt>{})://)".format(fmt_capture)
  proto_capture = r'|'.join(ALLOWED_PROTOCOLS)
  proto_capture = "(?:(?P<proto>{})://)".format(proto_capture)
  regexp = "{}?{}?".format(fmt_capture, proto_capture)
  return regexp

CLOUDPATH_REGEXP = re.compile(mkregexp())

def add_alias(alias:str, host:str):
  global ALIASES
  global ALLOWED_PROTOCOLS
  global BASE_ALLOWED_PROTOCOLS
  global CLOUDPATH_REGEXP

  if host[-1] != '/':
    host += '/'

  if alias in BASE_ALLOWED_PROTOCOLS:
    raise ValueError(f"Unable to override base protocols with alias {alias}")

  if alias in ALLOWED_FORMATS:
    raise ValueError(f"Naming collision between protocols and formats with alias {alias}")

  ALIASES[alias] = host
  ALLOWED_PROTOCOLS = BASE_ALLOWED_PROTOCOLS + list(ALIASES.keys())
  CLOUDPATH_REGEXP = re.compile(mkregexp())

def remove_alias(alias:str):
  global ALIASES
  global ALLOWED_PROTOCOLS
  global BASE_ALLOWED_PROTOCOLS
  global CLOUDPATH_REGEXP

  del ALIASES[alias]
  ALLOWED_PROTOCOLS = BASE_ALLOWED_PROTOCOLS + list(ALIASES.keys())
  CLOUDPATH_REGEXP = re.compile(mkregexp())  

def resolve_alias(cloudpath:str) -> Tuple[Optional[str],str]:
  proto = get_protocol(cloudpath)

  if proto not in ALIASES:
    return None, cloudpath

  return proto, cloudpath.replace(f"{proto}://", ALIASES[proto], 1)

## OFFICAL ALIASES

for alias, host in OFFICIAL_ALIASES.items():
  add_alias(alias, host)

## Other Path Library Functions

def normalize(path):
  proto = get_protocol(path)
  if proto is None:
    proto = "file"
    path = toabs(path)
    return f"file://{path}"
  return path

def asfilepath(epath):
  """For paths known to be file protocol."""
  if epath.protocol != "file":
    raise ValueError(f"{epath.protocol} protocol must be \"file\".")

  pth = ''
  lst = [ epath.bucket, epath.path ]
  while lst:
    elem = lst.pop(0)
    if not elem:
      continue
    pth = os.path.join(pth, elem)

  return pth

def ascloudpath(epath):
  pth = asprotocolpath(epath)
  if epath.format:
    return f"{epath.format}://" + pth
  return pth

def asprotocolpath(epath):
  pth = ''

  host = epath.host if not epath.alias else None
  lst = [ host, epath.bucket, epath.path ]
  while lst:
    elem = lst.pop(0)
    if not elem:
      continue
    pth = posixpath.join(pth, elem)

  if epath.alias:
    return f"{epath.alias}://{pth}"

  if not (pth[:4] == 'http' and epath.protocol in ('http', 'https')):
    pth = f"{epath.protocol}://{pth}"
  return pth  

def asbucketpath(cloudpath):
  """
  Returns the cloudpath containing the information needed to 
  connect to a bucket without the sub path.
  """
  if isinstance(cloudpath, str):
    epath = extract(cloudpath)
  elif isinstance(cloudpath, ExtractedPath):
    epath = cloudpath
  else:
    raise TypeError(f"Input must be str or ExtractedPath. Got: {cloudpath}")

  return ascloudpath(ExtractedPath(
    epath.format, epath.protocol, epath.bucket, 
    None, epath.host, epath.alias
  ))

def get_any_protocol(cloudpath):
  """
  Get the string in the protocol position even
  if its not a valid one.
  """
  protocol_re = re.compile(r'(?P<proto>[\w\d]+)://')
  match = re.match(protocol_re, cloudpath)
  if not match:
    return None
  return match.group("proto")

def get_protocol(cloudpath):
  global ALIASES_FROM_FILE
  m = re.match(CLOUDPATH_REGEXP, cloudpath)
  proto = m.group('proto')
  
  if proto is None:
    unknown_proto = get_any_protocol(cloudpath)
  
    if unknown_proto is not None and ALIASES_FROM_FILE is None:
      update_aliases_from_file()
      m = re.match(CLOUDPATH_REGEXP, cloudpath)
      proto = m.group('proto')
  
  return proto

def pop_protocol(cloudpath):
  protocol_re = re.compile(r'(\w+)://')

  match = re.match(protocol_re, cloudpath)

  if not match:
    return (None, cloudpath)

  (protocol,) = match.groups()
  cloudpath = re.sub(protocol_re, '', cloudpath, count=1)

  return (protocol, cloudpath)

def extract_format_protocol(cloudpath:str) -> tuple:
  error = UnsupportedProtocolError(cloudpath_error(cloudpath))

  alias, cloudpath = resolve_alias(cloudpath)

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
    cloudpath = cloudpath.replace(endpoint, '', 1)
    if cloudpath and cloudpath[0] == '/':
      cloudpath = cloudpath[1:]
  elif proto == 's3' and cloudpath[:4] == 'http':
    parse = urllib.parse.urlparse(cloudpath)
    endpoint = parse.scheme + "://" + parse.netloc
    cloudpath = cloudpath.replace(endpoint, '', 1)
    if cloudpath and cloudpath[0] == '/':
      cloudpath = cloudpath[1:]

  return (fmt, proto, endpoint, cloudpath, alias)

def extract(cloudpath:str, windows=None) -> ExtractedPath:
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
  error = UnsupportedProtocolError(cloudpath_error(cloudpath))

  fmt, protocol, host, cloudpath, alias = extract_format_protocol(cloudpath)

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
    cloudpath = cloudpath.replace(bucket, '', 1)
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
    cloudpath, host, alias
  )

def to_https_protocol(cloudpath):
  if isinstance(cloudpath, ExtractedPath):
    if cloudpath.protocol in ('gs', 's3', 'matrix'):
      return extract(to_https_protocol(ascloudpath(cloudpath)))
    return cloudpath

  proto = get_protocol(cloudpath) # side effect of loading aliases if needed

  if "s3://http://" in cloudpath or "s3://https://" in cloudpath:
    return cloudpath.replace("s3://", "", 1)

  cloudpath = cloudpath.replace("gs://", "https://storage.googleapis.com/", 1)
  cloudpath = cloudpath.replace("s3://", "https://s3.amazonaws.com/", 1)

  for alias, host in ALIASES.items():
    cloudpath = cloudpath.replace(f"{alias}://", host, 1)

  return cloudpath.replace("s3://", "", 1)
