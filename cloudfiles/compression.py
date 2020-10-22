from io import BytesIO

import copy
import gzip
import sys

try:
  import deflate
except ImportError:
  deflate = None

import brotli
import zstandard as zstd

from tqdm import tqdm

from .lib import STRING_TYPES, toiter
from .exceptions import DecompressionError, CompressionError

COMPRESSION_TYPES = [ 
  None, False, True,
  '', 'gzip', 'br', 'zstd'
]

def transcode(
  files, encoding, level=None, 
  progress=False, in_place=False
):
  """
  Given the output of get, which may include raw files, 
  transcode the compresson scheme into the one specified by 
  encoding. If the files are raw and the schemes match, 
  the decompression-compression cycle will be skipped.

  level: input of compression level e.g. 6 for gzip
  progress: show a progress bar
  in_place: modify the files in-place to save memory

  Yields: {
    'path': ,
    'content': ,
    'compress': ,
    'raw': ,
  }
  """
  files = toiter(files)
  encoding = normalize_encoding(encoding)
  for f in tqdm(files, disable=(not progress), desc="Transcoding"):
    # Accomodate (filename, content) inputs
    if isinstance(f, tuple):
      f = {
        "path": f[0],
        "content": f[1],
        "compress": None,
        "raw": False,
      }

    f_encoding = normalize_encoding(f['compress'])
    if not in_place:
      f = copy.deepcopy(f)

    raw = f.get('raw', False)
    content = f['content']

    if content is None:
      yield f

    if raw:
      if encoding == f_encoding:
        yield f
      content = decompress(content, f_encoding, f['path'])

    content = compress(content, encoding, level)

    f['raw'] = True
    f['compress'] = encoding
    f['content'] = content
    yield f
    
def normalize_encoding(encoding):
  if isinstance(encoding, STRING_TYPES):
    encoding = encoding.lower()

  if encoding in (None, False, '', 0):
    return None
  elif encoding in (True, 'gzip', 1):
    return 'gzip'
  
  return encoding

def decompress(content, encoding, filename='N/A'):
  """
  Decompress file content. 

  Required: 
    content (bytes): a file to be compressed
    encoding: None (no compression) or 'gzip' or 'br'
  Optional:   
    filename (str:default:'N/A'): Used for debugging messages
  Raises: 
    NotImplementedError if an unsupported codec is specified. 
    compression.EncodeError if the encoder has an issue

  Return: decompressed content
  """
  try:
    encoding = (encoding or '').lower()
    if encoding == '':
      return content
    elif len(content) == 0:
      raise DecompressionError('File contains zero bytes: ' + str(filename))
    elif encoding == 'gzip':
      return gunzip(content)
    elif encoding == 'br':
      return brotli_decompress(content)
    elif encoding == 'zstd':
      return zstd_decompress(content)
  except DecompressionError as err:
    print("Filename: " + str(filename))
    raise
  
  raise ValueError(str(encoding) + ' is not currently supported. Supported Options: None, gzip, br')

def compress(content, method='gzip', compress_level=None):
  """
  Compresses file content.

  Required:
    content (bytes): The information to be compressed
    method (str, default: 'gzip'): None or gzip
  Raises: 
    NotImplementedError if an unsupported codec is specified. 
    compression.DecodeError if the encoder has an issue

  Return: compressed content
  """
  if method == True:
    method = 'gzip' # backwards compatibility

  method = (method or '').lower()

  if method == '':
    return content
  elif method == 'gzip': 
    return gzip_compress(content, compresslevel=compress_level)
  elif method == 'br':
    return brotli_compress(content, quality=compress_level)
  elif method == 'zstd':
    return zstd_compress(content, compress_level)
  
  raise ValueError(str(method) + ' is not currently supported. Supported Options: None, gzip, br')

def gzip_compress(content, compresslevel=None):
  if compresslevel is None:
    compresslevel = 9

  if deflate:
    return deflate.gzip_compress(content, compresslevel)

  stringio = BytesIO()
  gzip_obj = gzip.GzipFile(mode='wb', fileobj=stringio, compresslevel=compresslevel)
  gzip_obj.write(content)
  gzip_obj.close()
  return stringio.getvalue()  

def gunzip(content):
  """ 
  Decompression is applied if the first to bytes matches with
  the gzip magic numbers. 
  There is once chance in 65536 that a file that is not gzipped will
  be ungzipped.
  """
  if len(content) == 0:
    raise DecompressionError('File contains zero bytes.')

  gzip_magic_numbers = [ 0x1f, 0x8b ]
  first_two_bytes = [ byte for byte in bytearray(content)[:2] ]
  if first_two_bytes != gzip_magic_numbers:
    raise DecompressionError('File is not in gzip format. Magic numbers {}, {} did not match {}, {}.'.format(
      hex(first_two_bytes[0]), hex(first_two_bytes[1]), hex(gzip_magic_numbers[0]), hex(gzip_magic_numbers[1])
    ))

  if deflate:
    return deflate.gzip_decompress(content)

  stringio = BytesIO(content)
  with gzip.GzipFile(mode='rb', fileobj=stringio) as gfile:
    return gfile.read()

def brotli_compress(content, quality=None):
  if quality is None:
    # 5/6 are good balance between compression speed and compression rate
    quality = 5
  return brotli.compress(content, quality=quality)

def brotli_decompress(content):
  if len(content) == 0:
    raise DecompressionError('File contains zero bytes.')

  return brotli.decompress(content)

def zstd_compress(content, level=None):
  kwargs = {}
  if level is not None:
    kwargs['level'] = int(level)

  ctx = zstd.ZstdCompressor(**kwargs)
  return ctx.compress(content)

def zstd_decompress(content):
  ctx = zstd.ZstdDecompressor()
  return ctx.decompress(content)




