"""
CloudFiles is a multithreaded key-value object
management client that supports GET, PUT, DELETE,
EXISTS, and LIST operations.

It can support any key-value storage system and 
currently supports local filesystem, Google Cloud Storage,
Amazon S3 interfaces, and reading from arbitrary HTTP 
servers.
"""

from .storage import (
  SimpleStorage, ThreadedStorage, GreenStorage,  
  DEFAULT_THREADS
)
from .interfaces import reset_connection_pools

# For backwards compatibility
Storage = ThreadedStorage
