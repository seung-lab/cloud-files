
class UnsupportedProtocolError(Exception):
  pass

class DecompressionError(Exception):
  """
  Decompression failed.
  """
  pass

class CompressionError(Exception):
  """
  Compression failed.
  """
  pass

class UnsupportedCompressionType(Exception):
  """
  Raised when attempting to use a compression type which is unsupported
  by the storage interface.
  """
  pass

class IntegrityError(Exception):
  """Data failed an integrity check."""
  pass 

class MD5IntegrityError(IntegrityError):
  """
  Failed MD5 digest check. This could indicate
  data tampering, network failures, or server error.
  """
  pass

class CRC32CIntegrityError(IntegrityError):
  """
  Failed CRC32C check. This usually indicates
  network data corruption or server errors.
  """
  pass