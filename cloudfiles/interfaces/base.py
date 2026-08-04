from typing import Optional, Callable, Any

from collections import defaultdict, namedtuple

import gevent.monkey

import threading
import tenacity

from ..connectionpools import S3ConnectionPool, GCloudBucketPool, MemoryPool, MEMORY_DATA
from ..exceptions import MD5IntegrityError, CompressionError, AuthorizationError

COMPRESSION_EXTENSIONS = ('.gz', '.br', '.zstd','.bz2','.xz')
GZIP_TYPES = (True, 'gzip', 1)

EXT_TEST_SEQUENCE = [
  ('', None),
  ('.gz', 'gzip'),
  ('.br', 'br'),
  ('.zstd', 'zstd'),
  ('.xz', 'xz'),
  ('.bz2', 'bzip2')
]
EXT_TEST_SEQUENCE_LOCK = threading.Lock()

# This is just to support pooling by bucket
class keydefaultdict(defaultdict):
  def __missing__(self, key):
    if self.default_factory is None:
      raise KeyError( key )
    else:
      ret = self[key] = self.default_factory(key)
      return ret

S3_POOL = None
GC_POOL = None
MEM_POOL = None

S3ConnectionPoolParams = namedtuple('S3ConnectionPoolParams', 'service bucket_name request_payer')
GCloudBucketPoolParams = namedtuple('GCloudBucketPoolParams', 'bucket_name request_payer')
MemoryPoolParams = namedtuple('MemoryPoolParams', 'bucket_name')

GCS_BUCKET_POOL_LOCK = threading.Lock()
S3_BUCKET_POOL_LOCK = threading.Lock()
MEM_BUCKET_POOL_LOCK = threading.Lock()

def reset_connection_pools():
  global S3_POOL
  global GC_POOL
  global MEM_POOL
  global GCS_BUCKET_POOL_LOCK
  global S3_BUCKET_POOL_LOCK
  global MEM_BUCKET_POOL_LOCK

  with S3_BUCKET_POOL_LOCK:
    S3_POOL = keydefaultdict(lambda params: S3ConnectionPool(params.service, params.bucket_name))

  with GCS_BUCKET_POOL_LOCK:
    GC_POOL = keydefaultdict(lambda params: GCloudBucketPool(params.bucket_name, params.request_payer))
  
  with MEM_BUCKET_POOL_LOCK:
    MEM_POOL = keydefaultdict(lambda params: MemoryPool(params.bucket_name))
    MEMORY_DATA.clear()
  import gc
  gc.collect()

reset_connection_pools()

retry = tenacity.retry(
  reraise=True, 
  stop=tenacity.stop_after_attempt(7), 
  wait=tenacity.wait_random_exponential(0.5, 60.0),
)

def retry_if_not(exception_type):
  if type(exception_type) != list:
    exception_type = [ exception_type ]

  conditions = tenacity.retry_if_not_exception_type(exception_type[0])
  for et in exception_type[1:]:
    conditions = conditions | tenacity.retry_if_not_exception_type(et)

  return tenacity.retry(
    retry=conditions,
    reraise=True, 
    stop=tenacity.stop_after_attempt(7), 
    wait=tenacity.wait_random_exponential(0.5, 60.0),
  ) 

class StorageInterface(object):
  exists_batch_size = 1
  delete_batch_size = 1
  def release_connection(self):
    pass
  def __enter__(self):
    return self
  def __exit__(self, exception_type, exception_value, traceback):
    self.release_connection()
