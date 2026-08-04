from typing import Optional

from .base import (
  GZIP_TYPES,
  retry, 
  retry_if_not, 
  S3_BUCKET_POOL_LOCK, 
  S3_POOL, 
  S3ConnectionPoolParams,
  StorageInterface, 
)

import boto3
import botocore

import base64
import binascii
from io import BytesIO
import os
import posixpath
import re

from ..lib import mkdir, sip, md5, encode_crc32c_b64, validate_s3_multipart_etag

S3_ACLS = {
  "tigerdata": "private",
  "nokura": "public-read",
}

DEFAULT_S3_ACL = "bucket-owner-full-control"
NFS_ETAG_REGEXP = re.compile(r'\d+\-$')

class S3Interface(StorageInterface):
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.delete_objects
  # claims batch size limit is 1000
  delete_batch_size = 1000
  def __init__(
    self, path, secrets=None, 
    request_payer=None, 
    composite_upload_threshold=int(1e8), 
    no_sign_request=False,
    **kwargs
  ):
    super(StorageInterface, self).__init__()
    global S3_POOL

    if request_payer is None:
      self._additional_attrs = {}
    elif request_payer == 'requester':
      self._additional_attrs = {'RequestPayer': request_payer}
    else:
      raise ValueError("request_payer for S3Interface must either be None or 'requester', got '{}'.".format(request_payer))

    self._request_payer = request_payer
    self._path = path
    self._secrets = secrets
    self._conn = self._get_bucket(path.bucket)

    self.composite_upload_threshold = composite_upload_threshold
    self.no_sign_request = no_sign_request

  def _get_bucket(self, bucket_name):
    global S3_BUCKET_POOL_LOCK
    service = self._path.alias or 's3'

    with S3_BUCKET_POOL_LOCK:
      pool = S3_POOL[S3ConnectionPoolParams(service, bucket_name, self._request_payer)]
    
    return pool.get_connection(self._secrets, self._path.host)

  def get_path_to_file(self, file_path):
    return posixpath.join(self._path.path, file_path)

  @retry
  def put_file(
    self, file_path, content, 
    content_type, compress, 
    cache_control=None,
    storage_class=None
  ):
    key = self.get_path_to_file(file_path)

    attrs = {
      'ContentType': (content_type or 'application/octet-stream'),
      'ACL': S3_ACLS.get(self._path.alias, DEFAULT_S3_ACL),
      **self._additional_attrs,
    }

    # keep gzip as default
    if compress == "br":
      attrs['ContentEncoding'] = 'br'
    elif compress in GZIP_TYPES:
      attrs['ContentEncoding'] = 'gzip'
    elif compress == "zstd":
      attrs['ContentEncoding'] = 'zstd'
    elif compress in ("xz", "lzma"):
      attrs['ContentEncoding'] = 'xz'
    elif compress in ("bzip2", "bz2"):
      attrs['ContentEncoding'] = 'bzip2'
    elif compress:
      raise ValueError("Compression type {} not supported.".format(compress))

    if cache_control:
      attrs['CacheControl'] = cache_control
    if storage_class:
      attrs['StorageClass'] = storage_class

    multipart = False
    is_file_handle = hasattr(content, "read") and hasattr(content, "seek")

    if is_file_handle:
      content_length = os.fstat(content.fileno()).st_size
    else:
      content_length = len(content)

    if not multipart and content_length > int(self.composite_upload_threshold):
      if not is_file_handle:
        content = BytesIO(content)
      multipart = True

    # gevent monkey patching has a bad interaction with s3's use
    # of concurrent.futures.ThreadPoolExecutor. Just disable multipart
    # upload when monkeypatching is in effect.
    if multipart and (len(gevent.monkey.saved) > 0):
      multipart = False
      content = content.read()

    # WMS 2025-07-05: 
    # Currently, boto3 does not properly support streaming smaller files.
    # It uses an S3 API that requires a checksum up-front, but streaming 
    # checksums can only be provided at the end.
    # https://github.com/boto/boto3/issues/3738
    # https://github.com/boto/boto3/issues/4392
    # https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html
    if not multipart and is_file_handle and content_length < int(self.composite_upload_threshold):
      content = content.read()

    if multipart:
      self._conn.upload_fileobj(content, self._path.bucket, key, ExtraArgs=attrs)
    else:
      if isinstance(content, str):
        content = content.encode('utf8')

      attrs['Bucket'] = self._path.bucket
      attrs['Body'] = content
      attrs['Key'] = key
      attrs["ChecksumCRC32C"] = encode_crc32c_b64(content).decode('utf8')
      self._conn.put_object(**attrs)

  @retry
  def copy_file(self, src_path, dest_bucket_name, dest_key) -> tuple[bool,int]:
    key = self.get_path_to_file(src_path)
    s3client = self._get_bucket(dest_bucket_name)
    copy_source = {
      'Bucket': self._path.bucket,
      'Key': key,
    }
    try:
      response = s3client.copy_object(
          CopySource=copy_source,
          Bucket=dest_bucket_name,
          Key=dest_key,
          MetadataDirective='COPY',  # Ensure metadata like Content-Encoding is copied
          ACL=S3_ACLS.get(self._path.alias, DEFAULT_S3_ACL),
      )
    except botocore.exceptions.ClientError as err: 
      if err.response['Error']['Code'] in ('NoSuchKey', '404'):
        return (False, 0)
      else:
        raise

    try:
      num_bytes = int(response["ResponseMetadata"]["HTTPHeaders"]["content-length"])
    except KeyError:
      num_bytes = 0

    return (True, num_bytes)

  @retry
  def get_file(self, file_path, start=None, end=None, part_size=None):
    """
    There are many types of execptions which can get raised
    from this method. We want to make sure we only return
    None when the file doesn't exist.
    """

    kwargs = self._additional_attrs.copy()
    range_request = start is not None or end is not None
    if range_request:
      start = int(start) if start is not None else 0
      end = int(end - 1) if end is not None else ''
      kwargs['Range'] = "bytes={}-{}".format(start, end)

    try:
      resp = self._conn.get_object(
        Bucket=self._path.bucket,
        Key=self.get_path_to_file(file_path),
        **kwargs
      )

      encoding = ''
      if 'ContentEncoding' in resp:
        encoding = resp['ContentEncoding']

      encoding = ",".join([ 
        enc for enc in encoding.split(",")
        if enc != "aws-chunked"
      ])

      # s3 etags return hex digests but we need the base64 encoding
      # to make uniform comparisons. 
      # example s3 etag: "31ee76261d87fed8cb9d4c465c48158c"
      # example multipart s3 etag: "cd8d2616dfa6cc80a06a846d3b3f6f30-14"
      # The -14 means 14 parts.

      etag = resp.get('ETag', None)
      content = resp['Body'].read()

      if etag is not None and not range_request:
        etag = etag.lstrip('"').rstrip('"')
        # AWS has special multipart validation
        # so we handle it here... leaky abstraction.
        if '-' in etag:
          # Dell ECS S3 uses a synthetic Etag of "1-" (and similar) for objects 
          # ingested via NFS. Not good behavior, but nothing much we can do other than ignore it.
          if not NFS_ETAG_REGEXP.match(etag) and not validate_s3_multipart_etag(content, etag, part_size):
            raise MD5IntegrityError(f"{file_path} failed its multipart md5 check. server md5: {etag}")
          etag = None
        else:
          etag = base64.b64encode(binascii.unhexlify(etag)).decode('utf8')

      return (content, encoding, etag, "md5")
    except botocore.exceptions.ClientError as err: 
      if err.response['Error']['Code'] == 'NoSuchKey':
        return (None, None, None, None)
      else:
        raise

  @retry
  def get_file_acl(self, file_path:str):
    try:
      kwargs = self._additional_attrs.copy()
      return self._conn.get_object_acl(
        Bucket=self._path.bucket,
        Key=self.get_path_to_file(file_path),
        **kwargs
      )
    except botocore.exceptions.ClientError as err: 
      if err.response['Error']['Code'] == 'NoSuchKey':
        return None
      else:
        raise

  @retry
  def save_file(self, src, dest, resumable) -> tuple[bool,int]:
    key = self.get_path_to_file(src)
    kwargs = self._additional_attrs.copy()

    resp = self.head(src)

    if resp is None:
      return (False, 0)

    mkdir(os.path.dirname(dest))

    encoding = resp.get("Content-Encoding", "") or ""
    encoding = ",".join([ 
      enc for enc in encoding.split(",")
      if enc != "aws-chunked"
    ])
    ext = FileInterface.get_extension(encoding)

    if not dest.endswith(ext):
      dest += ext

    try:
      self._conn.download_file(
        Bucket=self._path.bucket,
        Key=key,
        Filename=dest,
        **kwargs
      )
    except botocore.exceptions.ClientError as err: 
      if err.response['Error']['Code'] in ('NoSuchKey', '404'):
        return (False, 0)
      else:
        raise

    num_bytes = os.path.getsize(dest)
    return (True, num_bytes)

  @retry
  def head(self, file_path):
    try:
      response = self._conn.head_object(
        Bucket=self._path.bucket,
        Key=self.get_path_to_file(file_path),
        **self._additional_attrs,
      )

      encoding = response.get("ContentEncoding", None)
      if encoding == '':
        encoding = None

      return {
        "Cache-Control": response.get("CacheControl", None),
        "Content-Length": response.get("ContentLength", None),
        "Content-Type": response.get("ContentType", None),
        "ETag": response.get("ETag", None),
        "Last-Modified": response.get("LastModified", None),
        "Content-Md5": response["ResponseMetadata"]["HTTPHeaders"].get("content-md5", None),
        "Content-Encoding": encoding,
        "Content-Disposition": response.get("ContentDisposition", None),
        "Content-Language": response.get("ContentLanguage", None),
        "Storage-Class": response.get("StorageClass", None),
        "Request-Charged": response.get("RequestCharged", None),
        "Parts-Count": response.get("PartsCount", None),
      }
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
        return None
      raise

  @retry
  def size(self, file_path):
    try:
      response = self._conn.head_object(
        Bucket=self._path.bucket,
        Key=self.get_path_to_file(file_path),
        **self._additional_attrs,
      )
      return response['ContentLength']
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
        return None
      raise

  def exists(self, file_path):
    exists = True
    try:
      self._conn.head_object(
        Bucket=self._path.bucket,
        Key=self.get_path_to_file(file_path),
        **self._additional_attrs,
      )
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
        exists = False
      else:
        raise
    
    return exists

  def files_exist(self, file_paths):
    return { path: self.exists(path) for path in file_paths }

  @retry
  def delete_file(self, file_path):

    # Not necessary to handle 404s here.
    # From the boto3 documentation:

    # delete_object(**kwargs)
    # Removes the null version (if there is one) of an object and inserts a delete marker, 
    # which becomes the latest version of the object. If there isn't a null version, 
    # Amazon S3 does not remove any objects.

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object

    self._conn.delete_object(
      Bucket=self._path.bucket,
      Key=self.get_path_to_file(file_path),
      **self._additional_attrs,
    )

  @retry
  def delete_files(self, file_paths):
    # Dell ECS requires Content-MD5 for batch delete,
    # but the XML is constructed inside the s3 object...
    # To solve this, we would need to implement a REST API call
    # compute md5 and sign it. For now, let's just delete single files.
    # it's less performant, but it works.
    if self._path.alias == "nokura":
      for path in file_paths:
        self.delete_file(path)
      return

    for batch in sip(file_paths, self.delete_batch_size):
      response = self._conn.delete_objects(
        Bucket=self._path.bucket,
        Delete={
          'Objects': [
            { 'Key': self.get_path_to_file(filepath) } for filepath in batch
          ],
          'Quiet': True
        },
        **self._additional_attrs,
      )

  def list_files(
    self, 
    prefix:str, 
    flat:bool = False,
    size:bool = False,
    return_resume_token:bool = False,
    resume_token:Optional[str] = None,
  ):
    """
    List the files in the layer with the given prefix. 

    flat means only generate one level of a directory,
    while non-flat means generate all file paths with that 
    prefix.
    """

    layer_path = self.get_path_to_file("")        
    path = posixpath.join(layer_path, prefix)

    @retry
    def s3lst(path, continuation_token=None):
      kwargs = {
        'Bucket': self._path.bucket,
        'Prefix': path,
        **self._additional_attrs
      }
      if flat:
        kwargs['Delimiter'] = '/'

      if continuation_token:
        kwargs['ContinuationToken'] = continuation_token

      return self._conn.list_objects_v2(**kwargs)

    resp = s3lst(path, continuation_token=resume_token)
    # the case where the prefix is something like "build", but "build" is a subdirectory
    # so requery with "build/" to get the proper behavior
    if (
      flat 
      and path 
      and path[-1] != '/' 
      and 'Contents' not in resp 
      and len(resp.get("CommonPrefixes", [])) == 1
    ):
      path += '/'
      resp = s3lst(path, continuation_token=resume_token)

    def iterate(resp):
      if 'CommonPrefixes' in resp.keys():
        yield from [ 
          item["Prefix"].removeprefix(layer_path) 
          for item in resp['CommonPrefixes'] 
        ]

      if 'Contents' not in resp.keys():
        resp['Contents'] = []

      token = None
      if resp["IsTruncated"]:
        token = resp["NextContinuationToken"]

      for item in resp['Contents']:
        key = item['Key']
        filename = key.removeprefix(layer_path)
        if filename == '':
          continue
        elif flat and '/' in key.removeprefix(path):
          continue
        elif not flat and filename[-1] == "/":
          continue

        if not size and not return_resume_token:
          yield filename
        elif size and not return_resume_token:
          yield (filename, int(item["Size"]))
        elif not size and return_resume_token:
          yield (filename, token)
        else:
          yield (filename, int(item["Size"]), token)

    for result in iterate(resp):
      yield result

    while resp['IsTruncated'] and resp['NextContinuationToken']:
      resp = s3lst(path, resp['NextContinuationToken'])

      for result in iterate(resp):
        yield result

  def subtree_size(self, prefix:str = "") -> tuple[int,int]:
    layer_path = self.get_path_to_file("")        
    path = posixpath.join(layer_path, prefix)

    @retry
    def s3lst(path, continuation_token=None):
      kwargs = {
        'Bucket': self._path.bucket,
        'Prefix': path,
        **self._additional_attrs
      }

      if continuation_token:
        kwargs['ContinuationToken'] = continuation_token

      return self._conn.list_objects_v2(**kwargs)

    resp = s3lst(path)
    
    def iterate(resp):
      if 'Contents' not in resp.keys():
        resp['Contents'] = []

      for item in resp['Contents']:
        yield item.get('Size', 0)

    total_bytes = 0
    total_files = 0
    for num_bytes in iterate(resp):
      total_files += 1
      total_bytes += num_bytes

    while resp['IsTruncated'] and resp['NextContinuationToken']:
      resp = s3lst(path, resp['NextContinuationToken'])

      for num_bytes in iterate(resp):
        total_files += 1
        total_bytes += num_bytes

    return (total_files, total_bytes)

  def release_connection(self):
    global S3_POOL
    service = self._path.alias or 's3'
    with S3_BUCKET_POOL_LOCK:
      pool = S3_POOL[S3ConnectionPoolParams(service, self._path.bucket, self._request_payer)]
    pool.release_connection(self._conn)