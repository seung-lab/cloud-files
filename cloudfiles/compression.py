from typing import Union, Optional, Iterable

from io import BytesIO

import bz2
import copy
import gzip
import lzma
import sys

try:
  import deflate
except ImportError:
  deflate = None

import brotli
import zstandard as zstd

from tqdm import tqdm

from .lib import toiter
from .exceptions import DecompressionError, CompressionError

COMPRESSION_TYPES = [ 
  None, False, True,
  '', 'bz2', 'bzip2', 'gzip', 'br', 'zstd', 
  'xz', 'lzma'
]

def normalize_encoding(
  encoding:Union[bool,str,list[str]]
) -> list[str]:
  encodings = encoding
  if isinstance(encoding, str):
    encoding = encoding.lower()
    encodings = encoding.split(',')
  elif encoding in (None, False, '', 0):
    return []
  elif isinstance(encoding, bool):
    encodings = [ encoding ]
  
  for i in range(len(encodings)):
    enc = encodings[i]
    if enc in (True, 'gzip', 1):
      enc = 'gzip'
    encodings[i] = enc.lower()

  return encodings

def transcode(
  files:Union[
    bytes, Iterable[bytes], 
    dict, Iterable[dict],
  ], 
  encoding:Union[str,list[str]], 
  level:Optional[int] = None, 
  progress:bool = False, 
  in_place:bool = False,
) -> Iterable[dict]:
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
  encoding_str = ','.join(encoding)
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
      continue

    if raw:
      if encoding == f_encoding:
        yield f
        continue
      content = decompress(content, f_encoding, f['path'])

    content = compress(content, encoding, level)

    f['raw'] = True
    f['compress'] = encoding_str
    f['content'] = content
    yield f

def decompress(
  content:bytes, 
  encoding:Optional[Union[str,bool,int,list[str]]], 
  filename:str = 'N/A',
) -> bytes:
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
  encodings = normalize_encoding(encoding)  

  for enc in encodings:
    try:
      if enc == '':
        continue
      elif len(content) == 0:
        raise DecompressionError(f'File contains zero bytes: {str(filename)}')
      elif enc == 'gzip':
        content = gunzip(content)
      elif enc == 'br':
        content = brotli_decompress(content)
      elif enc == 'zstd':
        content = zstd_decompress(content)
      elif enc in ('bz2', 'bzip2'):
        content = bz2.decompress(content)
      elif enc in ('xz', 'lzma'):
        content = lzma.decompress(content)
      elif enc == "aws-chunked":
        continue
      else:
        raise ValueError(
          f'{enc} is not currently supported. Supported Options: None, {", ".join(COMPRESSION_TYPES[4:])}'
        )
    except DecompressionError as err:
      print(f"Filename: {filename}")
      raise

  return content

def compress(
  content:bytes,
  method:Union[str,bool] = 'gzip', 
  compress_level:Optional[int] = None,
) -> bytes:
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
  method = normalize_encoding(method)

  for enc in method:
    if enc == '':
      continue
    elif enc == 'gzip': 
      content = gzip_compress(content, compresslevel=compress_level)
    elif enc == 'br':
      content = brotli_compress(content, quality=compress_level)
    elif enc == 'zstd':
      content = zstd_compress(content, compress_level)
    elif enc in ("bz2", "bzip2"):
      compress_level = 9 if compress_level is None else compress_level
      content = bz2.compress(content, compresslevel=compress_level)
    elif enc in ('xz', 'lzma'):
      content = lzma.compress(content, preset=compress_level)
    elif enc == "aws-chunked":
      continue
    else:
      raise ValueError(
        f'{enc} is not currently supported. '
        f'Supported Options: None, {", ".join(COMPRESSION_TYPES[4:])}'
      )

  return content

def gzip_compress(content:bytes, compresslevel:Optional[int] = None) -> bytes:
  if compresslevel is None:
    compresslevel = 9

  if deflate:
    return bytes(deflate.gzip_compress(content, compresslevel))

  stringio = BytesIO()
  gzip_obj = gzip.GzipFile(mode='wb', fileobj=stringio, compresslevel=compresslevel)
  gzip_obj.write(content)
  gzip_obj.close()
  return stringio.getvalue()  

def gunzip(content:bytes) -> bytes:
  """ 
  Decompression is applied if the first to bytes matches with
  the gzip magic numbers. 
  There is once chance in 65536 that a file that is not gzipped will
  be ungzipped.
  """
  if len(content) == 0:
    raise DecompressionError('File contains zero bytes.')

  gzip_magic_numbers = [ 0x1f, 0x8b ]

  if isinstance(content, (bytes, bytearray)):
    first_two_bytes = list(content[:2])
  else:
    first_two_bytes = [ byte for byte in bytearray(content)[:2] ]

  if first_two_bytes != gzip_magic_numbers:
    raise DecompressionError('File is not in gzip format. Magic numbers {}, {} did not match {}, {}.'.format(
      hex(first_two_bytes[0]), hex(first_two_bytes[1]), hex(gzip_magic_numbers[0]), hex(gzip_magic_numbers[1])
    ))

  if deflate:
    return bytes(deflate.gzip_decompress(content))

  stringio = BytesIO(content)
  with gzip.GzipFile(mode='rb', fileobj=stringio) as gfile:
    return gfile.read()

def brotli_compress(content:bytes, quality:Optional[int] = None) -> bytes:
  if quality is None:
    # 5/6 are good balance between compression speed and compression rate
    quality = 5
  return brotli.compress(content, quality=quality)

def brotli_decompress(content:bytes) -> bytes:
  if len(content) == 0:
    raise DecompressionError('File contains zero bytes.')

  return brotli.decompress(content)

def zstd_compress(content:bytes, level:Optional[int] = None) -> bytes:
  kwargs = {}
  if level is not None:
    kwargs['level'] = int(level)

  ctx = zstd.ZstdCompressor(**kwargs)
  return ctx.compress(content)

def zstd_decompress(content:bytes) -> bytes:
  ctx = zstd.ZstdDecompressor()
  return ctx.decompress(content)




