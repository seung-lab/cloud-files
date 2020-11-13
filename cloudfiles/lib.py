import base64
import hashlib
import itertools
import orjson
import os.path
import time
import types
import struct
import sys

import crc32c as crc32clib

if sys.version_info < (3,0,0):
  STRING_TYPES = (str, unicode)
  UNICODE_TYPE = unicode
  PYTHON3 = False
else:
  STRING_TYPES = (str,)
  UNICODE_TYPE = str
  PYTHON3 = True

COLORS = {
  'RESET': "\033[m",
  'YELLOW': "\033[1;93m",
  'RED': '\033[1;91m',
  'GREEN': '\033[1;92m',
}

def green(text):
  return colorize('green', text)

def yellow(text):
  return colorize('yellow', text)

def red(text):
  return colorize('red', text)

def colorize(color, text):
  color = color.upper()
  return COLORS[color] + text + COLORS['RESET']

def toabs(path):
  path = os.path.expanduser(path)
  return os.path.abspath(path)

def mkdir(path):
  path = toabs(path)

  try:
    if path != '' and not os.path.exists(path):
      os.makedirs(path)
  except OSError as e:
    if e.errno == 17: # File Exists
      time.sleep(0.1)
      return mkdir(path)
    else:
      raise

  return path

def touch(path):
  mkdir(os.path.dirname(path))
  open(path, 'a').close()

def nvl(*args):
  """Return the leftmost argument that is not None."""
  if len(args) < 2:
    raise IndexError("nvl takes at least two arguments.")
  for arg in args:
    if arg is not None:
      return arg
  return args[-1]

def sip(iterable, block_size):
  """Sips a fixed size from the iterable."""
  ct = 0
  block = []
  for x in iterable:
    ct += 1
    block.append(x)
    if ct == block_size:
      yield block
      ct = 0
      block = []

  if len(block) > 0:
    yield block

def jsonify(obj, **kwargs):
  return orjson.dumps(obj, option=(orjson.OPT_SERIALIZE_NUMPY|orjson.OPT_NON_STR_KEYS), **kwargs)

def first(lst):
  if isinstance(lst, types.GeneratorType):
    return next(lst)
  try:
    return lst[0]
  except TypeError:
    return next(iter(lst))

def toiter(obj, is_iter=False):
  if isinstance(obj, STRING_TYPES) or isinstance(obj, dict):
    if is_iter:
      return [ obj ], False
    return [ obj ]

  try:
    iter(obj)
    if is_iter:
      return obj, True
    return obj 
  except TypeError:
    if is_iter:
      return [ obj ], False
    return [ obj ]

def duplicates(lst):
  dupes = []
  seen = set()
  for elem in lst:
    if elem in seen:
      dupes.append(elem)
    seen.add(elem)
  return set(dupes)

def scatter(sequence, n):
  """Scatters elements of ``sequence`` into ``n`` blocks. Returns generator."""
  if n < 1:
    raise ValueError('n cannot be less than one. Got: ' + str(n))
  sequence = list(sequence)
  for i in range(n):
    yield sequence[i::n]

def decode_crc32c_b64(b64digest):
  """
  Decode a Google provided crc32c digest into
  an integer. Accomodate a bug I introduced in
  GCP where padding '=' were stripped off.
  """
  b64digest += "=" * (len(b64digest) % 4)
  # !I means network order (big endian) and unsigned int
  return struct.unpack("!I", base64.b64decode(b64digest))[0]

def crc32c(binary):
  """
  Computes the crc32c of a binary string 
  and returns it as an integer.
  """
  return crc32clib.value(binary) # an integer

def md5(binary):
  """
  Returns the md5 of a binary string 
  in base64 format.
  """
  if isinstance(binary, UNICODE_TYPE):
    binary = binary.encode('utf8')

  return base64.b64encode(
    hashlib.md5(binary).digest()
  ).decode('utf8')
