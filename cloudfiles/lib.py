import base64
import binascii
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
    try:
      return next(lst)
    except StopIteration:
      return None
  try:
    return lst[0]
  except TypeError:
    try:
      return next(iter(lst))
    except StopIteration:
      return None
  except IndexError:
    return None

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

def md5(binary, base=64):
  """
  Returns the md5 of a binary string 
  in base64 format.
  """
  if isinstance(binary, UNICODE_TYPE):
    binary = binary.encode('utf8')

  digest = hashlib.md5(binary)
  if base == 64:
    return base64.b64encode(digest.digest()).decode('utf8')
  elif base == 16:
    return digest.hexdigest()
  else:
    raise ValueError(f"base {base} must be 16 or 64.")

def md5_equal(hash_a, hash_b):
  hash_a = hash_a.rstrip('"').lstrip('"')
  hash_b = hash_b.rstrip('"').lstrip('"')

  b16_to_b64 = lambda hash_x: base64.b64encode(binascii.unhexlify(hash_x)).decode('utf8')

  if len(hash_a) == 32:
    hash_a = b16_to_b64(hash_a)
  if len(hash_b) == 32:
    hash_b = b16_to_b64(hash_b)

  return hash_a == hash_b

# Below code adapted from: 
# https://teppen.io/2018/10/23/aws_s3_verify_etags/

def calc_s3_multipart_etag(content, partsize):
  md5_digests = []

  for i in range(0, len(content), partsize):
    chunk = content[i:i+partsize]
    md5_digests.append(hashlib.md5(chunk).digest())
  return hashlib.md5(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))

def validate_s3_multipart_etag(content, etag):
  filesize = len(content)
  num_parts = int(etag.split('-')[1])

  def factor_of_1MB():
    x = filesize / int(num_parts)
    y = x % 1048576 # 2**20 or 1 MiB
    return int(x + 1048576 - y)

  def possible_partsizes(partsize):
    return partsize < filesize and (float(filesize) / float(partsize)) <= num_parts

  partsizes = [ 
    8388608, # aws_cli/boto3 aka 8MiB (8 * 2**20)
    15728640, # s3cmd aka 15 MiB (15 * 2**20)
    factor_of_1MB() # Used by many clients to upload large files
  ]

  for partsize in filter(possible_partsizes, partsizes):
    if etag == calc_s3_multipart_etag(content, partsize):
      return True

  return False




