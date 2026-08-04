from .base import reset_connection_pools, MEMORY_DATA, COMPRESSION_EXTENSIONS

from .cave import CaveInterface
from .file import FileInterface
from .memory import MemoryInterface
from .s3 import S3Interface
# from .gcs import GoogleCloudStorageInterface
from .http import HttpInterface

def __getattr__(name):
    if name == 'GoogleCloudStorageInterface':
        from .gcs import GoogleCloudStorageInterface
        return GoogleCloudStorageInterface
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
