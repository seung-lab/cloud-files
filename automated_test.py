import os
import pytest
import re
import shutil
import time

from moto import mock_aws

COMPRESSION_TYPES = [ 
  None, False, True,
  '', 'bzip2', 'gzip', 'br', 'zstd', 
  'xz', 'lzma'
]

def rmtree(path):
  path = path.replace("file://", "")
  if os.path.exists(path):
    shutil.rmtree(path)

def compute_url(protocol, name):
  from cloudfiles import CloudFiles, exceptions
  if protocol == 'file':
    rmtree("/tmp/cloudfiles/" + name)
    return "file:///tmp/cloudfiles/" + name
  else:
    return "{}://cloudfiles/{}".format(protocol, name)

@pytest.fixture(scope='function')
def aws_credentials():
  """Mocked AWS Credentials for moto."""
  os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
  os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
  os.environ['AWS_SECURITY_TOKEN'] = 'testing'
  os.environ['AWS_SESSION_TOKEN'] = 'testing'
  os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture(scope='function')
def s3(aws_credentials):
  with mock_aws():
    import boto3
    conn = boto3.client('s3', region_name='us-east-1')
    conn.create_bucket(Bucket="cloudfiles")
    conn.create_bucket(Bucket="cloudfiles_src")
    conn.create_bucket(Bucket="cloudfiles_dest")
    yield conn

def test_aws_credentials(aws_credentials):
  from cloudfiles import secrets
  expected = {
    'AWS_ACCESS_KEY_ID': 'testing',
    'AWS_SECRET_ACCESS_KEY': 'testing',
    'AWS_SESSION_TOKEN': 'testing',
    'AWS_DEFAULT_REGION': 'us-east-1',
  }
  assert secrets.aws_credentials(skip_files=True) == expected

@pytest.mark.parametrize("green", (False, True))
@pytest.mark.parametrize("num_threads", (0, 5, 20))
@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))#'gs'))
def test_read_write(s3, protocol, num_threads, green):
  from cloudfiles import CloudFiles, CloudFile, exceptions
  url = compute_url(protocol, "rw")

  cf = CloudFiles(url, num_threads=num_threads, green=green)
  
  content = b'some_string'
  cf.put('info', content, compress=None, cache_control='no-cache')
  cf['info2'] = content

  f = CloudFile(cf.join(url, "info"))
  assert cf.get('info') == content
  assert cf['info2'] == content
  assert cf['info2', 0:3] == content[0:3]
  assert cf['info2', :] == content[:]
  assert f.get() == content
  assert cf.get('nonexistentfile') is None

  res = { 
    "path": "info", 
    "content": content,
    "byte_range": (None, None),
    "error": None,
    "compress": None if protocol != "s3" else '',
    "raw": False,
    "tags": None,
  }

  assert cf.get([{ 'path': 'info' }]) == [ res ]
  tags = "some crazy thing to pass through"
  res["tags"] = tags
  assert cf.get([{ 'path': 'info', 'tags': tags }]) == [ res ]

  assert cf.get('info', return_dict=True) == { "info": content }
  assert cf.get(['info', 'info2'], return_dict=True) == { "info": content, "info2": content }

  del cf['info2']
  assert cf.exists('info2') == False

  num_infos = max(num_threads, 1)
  results = cf.get([ 'info' for i in range(num_infos) ])

  assert len(results) == num_infos
  assert results[0]['path'] == 'info'
  assert results[0]['content'] == content
  assert all(map(lambda x: x['error'] is None, results))
  assert cf.get([ 'nonexistentfile' ])[0]['content'] is None
  
  cf.delete('info')

  cf.put_json('info', { 'omg': 'wow' }, cache_control='no-cache')
  results = cf.get_json('info')
  assert results == { 'omg': 'wow' }

  cf = CloudFile(cf.join(url, "info"))
  cf.put_json({ 'omg': 'wow' }, cache_control='no-cache')
  results = cf.get_json()
  assert results == { 'omg': 'wow' }

  cf.delete()

  if protocol == 'file':
    rmtree(url)

@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))#'gs'))
def test_get_json_order(s3, protocol):
  from cloudfiles import CloudFiles
  url = compute_url(protocol, 'get_json_order')
  cf = CloudFiles(url)

  N = 5300
  cf.put_jsons(( (str(z), [ z ]) for z in range(N) ))

  contents = cf.get_json(( str(z) for z in range(N) ))

  for z, content in enumerate(contents):
    assert content[0] == z

  cf.delete(( str(z) for z in range(N) ))

@pytest.mark.parametrize("green", (False, True))
@pytest.mark.parametrize("compress", (None, 'gzip','br','zstd','xz','bz2'))
@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))#'gs'))
def test_size(s3, protocol, compress, green):
  from cloudfiles import CloudFiles, exceptions, compression

  url = compute_url(protocol, 'size')
  cf = CloudFiles(url)
  
  content = b'some_string'
  cf.put('info', content, compress=compress, cache_control='no-cache')
  cf['info2'] = content
  cf.put('zero', b'', compress=None, cache_control='no-cache')

  compressed_content = compression.compress(content, compress)
  assert cf.size('info') == len(compressed_content)
  assert cf.size(['info', 'info2']) == { 
    "info": len(compressed_content), 
    "info2": len(content) 
  }
  assert cf.size('nonexistent') is None
  assert cf.size('zero') == 0

  cf.delete(['info', 'info2', 'zero'])

@pytest.mark.parametrize("green", (False, True))
@pytest.mark.parametrize("num_threads", (0, 5, 20))
def test_get_generator(num_threads, green):
  from cloudfiles import CloudFiles, exceptions
  path = '/tmp/cloudfiles/gen'
  rmtree(path)
  url = 'file://' + path

  cf = CloudFiles(url, num_threads=num_threads, green=green)

  gen = ( (str(i), b'hello world') for i in range(100) )
  cf.puts(gen)

  files = cf.get(( str(i) for i in range(100) ), total=100)

  assert all([ f['error'] is None for f in files ])
  assert len(files) == 100
  assert all([ f['content'] == b'hello world' for f in files ])

  fnames = [ str(i) for i in range(100) ]
  assert sorted(list(cf.list())) == sorted(fnames)

  cf.delete(( str(i) for i in range(100) ))
  assert list(cf.list()) == []

@pytest.mark.parametrize("secrets", [
  None,
  { "token": "hello world" },
  { "user": "hello", "password": "world" },
])
def test_http_read(secrets):
  from cloudfiles import CloudFiles, exceptions
  import requests
  cf = CloudFiles("https://storage.googleapis.com/seunglab-test/test_v0/black/", secrets=secrets)
  
  try:
    head = cf.head('info')
    info = cf.get_json('info')
  except requests.exceptions.HTTPError:
    return

  assert info == {
    "data_type": "uint8",
    "num_channels": 1,
    "scales": [
      {
        "chunk_sizes": [
          [
            64,
            64,
            50
          ]
        ],
        "encoding": "raw",
        "key": "6_6_30",
        "resolution": [
          6,
          6,
          30
        ],
        "size": [
          1024,
          1024,
          100
        ],
        "voxel_offset": [
          0,
          0,
          0
        ]
      }
    ],
    "type": "image"
  }

def test_http_read_brotli_image():
  from cloudfiles import CloudFiles, exceptions
  cf = CloudFiles('https://open-neurodata.s3.amazonaws.com/kharris15/apical/em')
  
  imgbytes = cf.get("2_2_50/4096-4608_4096-4608_112-128")
  assert len(imgbytes) == 4194304
  
  expected = b'v\\BAT[]\\TVcsxshj{\x84vjo\x7f}oqyz\x89\x92\x91\x98\x81\x99\xb2\xb2\xb1\xa9\x9d\xa3\xb4\xb8'
  assert imgbytes[:len(expected)] == expected
  
@pytest.mark.parametrize("green", (False, True))
@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))
def test_delete(s3, green, protocol):
  from cloudfiles import CloudFiles, exceptions
  if protocol == 'file':
    url = "file:///tmp/cloudfiles/delete"
  else:
    url = "{}://cloudfiles/delete".format(protocol)

  cf = CloudFiles(url, green=green, num_threads=1)    
  content = b'some_string'
  cf.put('delete-test', content, compress=None, cache_control='no-cache')
  cf.put('delete-test-compressed', content, compress='gzip', cache_control='no-cache')
  assert cf.get('delete-test') == content
  cf.delete('delete-test')
  assert cf.get('delete-test') is None

  assert cf.get('delete-test-compressed') == content
  cf.delete('delete-test-compressed')
  assert cf.get('delete-test-compressed') is None

  # Reset for batch delete
  cf.put('delete-test', content, compress=None, cache_control='no-cache')
  cf.put('delete-test-compressed', content, compress='gzip', cache_control='no-cache')
  assert cf.get('delete-test') == content
  assert cf.get('delete-test-compressed') == content

  cf.delete(['delete-test', 'delete-nonexistent', 'delete-test-compressed'])
  assert cf.get('delete-test') is None
  assert cf.get('delete-test-compressed') is None

@pytest.mark.parametrize("green", (False, True))
@pytest.mark.parametrize("method", ('', None, True, False, 'gzip','br','zstd','xz','lzma','bzip2'))
@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))
def test_compression(s3, protocol, method, green):
  from cloudfiles import CloudFiles, exceptions
  url = compute_url(protocol, "compress")

  cf = CloudFiles(url, num_threads=5, green=green)
  content = b'some_string'

  cf.put('info', content, compress=method)
  retrieved = cf.get('info')
  assert content == retrieved

  assert cf.get('nonexistentfile') is None

  try:
    cf.put('info', content, compress='nonexistent')
    assert False
  except ValueError:
    pass

  cf.delete(iter(cf))

@pytest.mark.parametrize("compression_method", ("gzip", "br", "zstd", "xz", "bzip2"))
def test_compress_level(compression_method):
  from cloudfiles import CloudFiles, exceptions
  filepath = "/tmp/cloudfiles/compress_level"
  url = "file://" + filepath

  content = b'some_string' * 1000

  compress_levels = range(1, 9, 2)
  for compress_level in compress_levels:
    cf = CloudFiles(url, num_threads=5)
    cf.put('info', content, compress=compression_method, compression_level=compress_level)

    retrieved = cf.get('info')
    assert content == retrieved

    conn = cf._get_connection()
    _, encoding, server_md5, hash_type = conn.get_file("info")
    assert encoding == compression_method
    assert hash_type in ('md5', None)

    assert cf.get('nonexistentfile') is None

    rmtree(filepath)

@pytest.mark.parametrize("dest_encoding", (None, "gzip", "br", "zstd", "xz", "bz2"))
def test_transcode(dest_encoding):
  from cloudfiles import CloudFiles, compression
  base_text = b'hello world'
  encodings = [ None, "gzip", "br", "zstd" ]

  varied_texts = []

  ans = compression.compress(base_text, dest_encoding)

  for i in range(200):
    src_encoding = encodings[i % len(encodings)]
    varied_texts.append({
      "path": str(i),
      "content": compression.compress(base_text, src_encoding),
      "raw": src_encoding is not None,
      "compress": src_encoding,
    })

  transcoded = (x['content'] for x in compression.transcode(varied_texts, dest_encoding))
  for content in transcoded:
    assert content == ans

@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))
def test_list(s3, protocol):  
  from cloudfiles import CloudFiles, exceptions
  url = compute_url(protocol, "list")

  cf = CloudFiles(url, num_threads=5)
  content = b'some_string'
  cf.put('info1', content, compress=None)
  cf.put('info2', content, compress=None)
  cf.put('build/info3', content, compress=None)
  cf.put('level1/level2/info4', content, compress=None)
  cf.put('info5', content, compress='gzip')
  cf.put('info.txt', content, compress=None)

  # time.sleep(1) # sometimes it takes a moment for google to update the list
  assert set(cf.list(prefix='')) == set(['build/info3','info1', 'info2', 'level1/level2/info4', 'info5', 'info.txt'])
  assert set(list(cf)) == set(cf.list(prefix=''))
  
  assert set(cf.list(prefix='inf')) == set(['info1','info2','info5','info.txt'])
  assert set(cf.list(prefix='info1')) == set(['info1'])
  assert set(cf.list(prefix='build')) == set(['build/info3'])
  assert set(cf.list(prefix='build/')) == set(['build/info3'])
  assert set(cf.list(prefix='level1/')) == set(['level1/level2/info4'])
  assert set(cf.list(prefix='nofolder/')) == set([])

  # Tests (1)
  assert set(cf.list(prefix='', flat=True)) == set(['info1','info2','info5','info.txt', 'build/', 'level1/'])
  assert set(cf.list(prefix='inf', flat=True)) == set(['info1','info2','info5','info.txt'])
  # Tests (2)
  assert set(cf.list(prefix='build', flat=True)) == set(['build/info3'])
  # Tests (3)
  assert set(cf.list(prefix='level1/', flat=True)) == set(['level1/level2/'])
  assert set(cf.list(prefix='build/', flat=True)) == set(['build/info3'])
  # Tests (4)
  assert set(cf.list(prefix='build/inf', flat=True)) == set(['build/info3'])

  for file_path in ('info1', 'info2', 'build/info3', 'level1/level2/info4', 'info5', 'info.txt'):
    cf.delete(file_path)
  
  if protocol == 'file':
    rmtree("/tmp/cloudfiles/list")

@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))
def test_exists(s3, protocol):
  from cloudfiles import CloudFiles, exceptions
  url = compute_url(protocol, "exists")

  cf = CloudFiles(url, num_threads=5)
  content = b'some_string'
  cf.put('info', content, compress=None)
  
  assert cf.exists('info')
  assert not cf.exists('doesntexist')

  assert cf.exists(['info'])['info']
  assert not cf.exists(['doesntexist'])['doesntexist']

  cf.delete('info')

@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))
def test_isdir(s3, protocol):
  from cloudfiles import CloudFiles, exceptions
  url = compute_url(protocol, "isdir")

  cf = CloudFiles(url, num_threads=5)
  assert not cf.isdir()

  content = b'some_string'
  cf.put('info', content, compress=None)
  
  assert cf.isdir()
  cf.delete('info')

@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))
def test_access_non_cannonical_paths(s3, protocol):
  from cloudfiles import CloudFiles, exceptions
  if protocol == 'file':
    url = "file:///tmp/noncanon"
  else:
    url = "{}://cloudfiles/noncanon".format(protocol)
  
  cf = CloudFiles(url, num_threads=5)
  content = b'some_string'
  cf.put('info', content, compress=None)
  
  # time.sleep(0.5) # sometimes it takes a moment for google to update the list
  
  assert cf.get('info') == content
  assert cf.get('nonexistentfile') is None
  cf.delete('info')

def test_path_extraction():
  from cloudfiles import paths, exceptions, lib
  ExtractedPath = paths.ExtractedPath
  def shoulderror(url):
    try:
        pth = paths.extract(url)
        assert False, url
    except exceptions.UnsupportedProtocolError:
        pass

  def okgoogle(url):
    path = paths.extract(url)
    assert path.protocol == 'gs', url
    assert path.bucket == 'bucket', url
    assert path.path in ('dataset/layer', 'dataset/layer/'), url
    assert path.host is None
    assert path.format == 'precomputed', url

  okgoogle('gs://bucket/dataset/layer') 
  shoulderror('s4://dataset/layer')
  shoulderror('dataset/layer')

  # don't error
  assert (paths.extract('graphene://http://localhost:8080/segmentation/1.0/testvol')
    == ExtractedPath(
      'graphene', 'http', None, 
      'segmentation/1.0/testvol', 'http://localhost:8080', None))

  assert (paths.extract('precomputed://gs://fafb-ffn1-1234567')
    == ExtractedPath(
      'precomputed', 'gs', 'fafb-ffn1-1234567', 
      '', None, None))

  assert (paths.extract('precomputed://gs://fafb-ffn1-1234567/segmentation')
    == ExtractedPath(
      'precomputed', 'gs', 'fafb-ffn1-1234567', 
      'segmentation', None, None))

  firstdir = lambda x: '/' + x.split('/')[1]

  homepath = lib.toabs('~')
  homerintermediate = homepath.replace(firstdir(homepath), '')[1:]

  curpath = lib.toabs('.')
  curintermediate = curpath.replace(firstdir(curpath), '')[1:]
  
  match = re.match(r'((?:(?:\w:\\\\)|/).+?)\b', lib.toabs('.'))
  bucket, = match.groups()

  assert (paths.extract('s3://seunglab-test/intermediate/path/dataset/layer') 
      == ExtractedPath(
        'precomputed', 's3', 'seunglab-test', 
        'intermediate/path/dataset/layer', None, None
      ))

  assert (paths.extract('file:///tmp/dataset/layer') 
      == ExtractedPath(
        'precomputed', 'file', None, 
        "/tmp/dataset/layer", None, None
      ))

  assert (paths.extract('file://seunglab-test/intermediate/path/dataset/layer') 
      == ExtractedPath(
        'precomputed', 'file', None,
        os.path.join(curpath, 'seunglab-test/intermediate/path/dataset/layer'), 
        None, None
      ))

  assert (paths.extract('gs://seunglab-test/intermediate/path/dataset/layer') 
      == ExtractedPath(
        'precomputed', 'gs', 'seunglab-test',
        'intermediate/path/dataset/layer', None, None
      ))

  assert (paths.extract('file://~/seunglab-test/intermediate/path/dataset/layer') 
      == ExtractedPath(
        'precomputed', 'file', None, 
        os.path.join(homepath, 'seunglab-test/intermediate/path/dataset/layer'),
        None, None
      )
  )

  assert (paths.extract('file:///User/me/.cloudvolume/cache/gs/bucket/dataset/layer') 
      == ExtractedPath(
        'precomputed', 'file', None, 
        '/User/me/.cloudvolume/cache/gs/bucket/dataset/layer', None, None
      ))

  shoulderror('ou3bouqjsa fkj aojsf oaojf ojsaf')

  assert (
    paths.extract('gs://bucket/dataset/layer|neuroglancer-precomputed:') 
      == ExtractedPath(
        'precomputed', 'gs', 'bucket', 
        'dataset/layer', None, None
      ))

  assert (
    paths.extract('gs://bucket/dataset/layer|zarr2:') 
      == ExtractedPath(
        'zarr2', 'gs', 'bucket', 
        'dataset/layer', None, None
      ))

  okgoogle('gs://bucket/dataset/layer/')
  # shoulderror('gs://bucket/dataset/layer/info')

  path = paths.extract('s3://bucketxxxxxx/datasetzzzzz91h8__3/layer1br9bobasjf/')
  assert path.format == 'precomputed'
  assert path.protocol == 's3'
  assert path.bucket == 'bucketxxxxxx'
  assert path.path == 'datasetzzzzz91h8__3/layer1br9bobasjf/'
  assert path.host is None

  path = paths.extract('file:///bucket/dataset/layer/')
  assert path.format == 'precomputed'
  assert path.protocol == 'file'
  assert path.bucket is None
  assert path.path == '/bucket/dataset/layer'
  assert path.host is None

  shoulderror('lucifer://bucket/dataset/layer/')
  shoulderror('gs://///')

  path = paths.extract('file:///tmp/removeme/layer/')
  assert path.format == 'precomputed'
  assert path.protocol == 'file'
  assert path.bucket is None
  assert path.path == '/tmp/removeme/layer'
  assert path.host is None

  assert (paths.extract('gs://username/a/username2/b/c/d') 
      == ExtractedPath(
        'precomputed', 'gs', 'username', 
        'a/username2/b/c/d', None, None
      ))

def test_middleauth_path_extraction():
  import cloudfiles.paths
  path = cloudfiles.paths.extract('middleauth+https://example.com/wow/cool/')
  assert path.format == 'precomputed'
  assert path.protocol == 'middleauth+https'
  assert path.bucket is None
  assert path.path == 'wow/cool/'
  assert path.host == "https://example.com"

@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))
def test_access_non_cannonical_minimal_path(s3, protocol):
  from cloudfiles import CloudFiles, exceptions
  if protocol == 'file':
    url = "file:///tmp/"
  else:
    url = "{}://cloudfiles/".format(protocol)
  
  cf = CloudFiles(url, num_threads=5)
  content = b'some_string'
  cf.put('info', content, compress=None)
  
  # time.sleep(0.5) # sometimes it takes a moment for google to update the list
  
  assert cf.get('info') == content
  assert cf.get('nonexistentfile') is None
  cf.delete('info')

def test_windows_path_extraction():
  from cloudfiles import paths
  extract = paths.extract(r'file://C:\wow\this\is\a\cool\path', windows=True)
  assert extract.format == 'precomputed'
  assert extract.protocol == 'file'
  assert extract.bucket is None
  assert extract.path == 'C:\\wow\\this\\is\\a\\cool\\path'
  assert extract.host is None 

  extract = paths.extract('file://C:\\wow\\this\\is\\a\\cool\\path\\', windows=True)
  assert extract.format == 'precomputed'
  assert extract.protocol == 'file'
  assert extract.bucket is None
  assert extract.path == 'C:\\wow\\this\\is\\a\\cool\\path\\'
  assert extract.host is None

  extract = paths.extract('precomputed://https://storage.googleapis.com/neuroglancer-public-data/kasthuri2011/ground_truth', windows=True)
  assert extract.format == 'precomputed'
  assert extract.protocol == 'https'
  assert extract.bucket == None
  assert extract.path == 'neuroglancer-public-data/kasthuri2011/ground_truth'
  assert extract.host == 'https://storage.googleapis.com'

def test_s3_custom_endpoint_path():
  from cloudfiles import paths
  extract = paths.extract("precomputed://s3://https://s3-hpcrc.rc.princeton.edu/hello/world")
  assert extract.format == 'precomputed'
  assert extract.protocol == 's3'
  assert extract.bucket == 'hello'
  assert extract.path == 'world'
  assert extract.host == 'https://s3-hpcrc.rc.princeton.edu'

@pytest.mark.parametrize('allow_missing', [False, True])
@pytest.mark.parametrize('compression', (None, 'gzip', 'br', 'zstd', 'xz', 'bz2'))
@pytest.mark.parametrize('src_protocol', ['mem', 'file', 's3'])
@pytest.mark.parametrize('dest_protocol', ['mem', 'file', 's3'])
def test_transfer_semantics(s3, compression, src_protocol, dest_protocol, allow_missing):
  from cloudfiles import CloudFiles, exceptions

  if src_protocol == "file":
    path = '/tmp/cloudfiles_src/xfer'
  else:
    path = "cloudfiles_src/xfer"

  if dest_protocol == "file":
    dest_path = "/tmp/cloudfiles_dest/xfer"
  else:
    dest_path = "cloudfiles_dest/xfer"

  rmtree(path)
  cff = CloudFiles(f'{src_protocol}://{path}')
  cfm = CloudFiles(f'{dest_protocol}://{dest_path}')
  
  N = 128

  content = b'some_string'
  cff.puts(( (str(i), content) for i in  range(N) ), compress=compression)
  assert sorted(list(cff)) == sorted([ str(i) for i in range(N) ])
  assert [ f['content'] for f in cff[:] ] == [ content ] * N

  assert sorted([ f['path'] for f in cff[:100] ]) == sorted([ str(i) for i in range(N) ])[:100]
  assert [ f['content'] for f in cff[:100] ] == [ content ] * 100

  cfm[:] = cff
  assert sorted(list(cfm)) == sorted([ str(i) for i in range(N) ])
  assert [ f['content'] for f in cfm[:] ] == [ content ] * N

  cfm.delete(list(cfm))
  assert list(cfm) == []

  cfm.transfer_from(f'{src_protocol}://' + path, allow_missing=allow_missing)
  assert sorted(list(cfm)) == sorted([ str(i) for i in range(N) ])
  assert [ f['content'] for f in cfm[:] ] == [ content ] * N

  cfm.delete(list(cfm))

  cff.transfer_to(cfm.cloudpath, allow_missing=allow_missing)
  assert sorted(list(cfm)) == sorted([ str(i) for i in range(N) ])
  assert [ f['content'] for f in cfm[:] ] == [ content ] * N  
  assert cfm.head("1")["Content-Encoding"] == cff.head("1")["Content-Encoding"]
  cfm.delete(list(cfm))

  cff.transfer_to(cfm.cloudpath, reencode='br', allow_missing=allow_missing)
  assert sorted(list(cfm)) == sorted([ str(i) for i in range(N) ])
  assert [ f['content'] for f in cfm[:] ] == [ content ] * N  
  assert 'br' in cfm.head("1")["Content-Encoding"]

  if dest_protocol == "mem":
    data = cfm._get_connection()._data
    data = [ os.path.splitext(d)[1] for d in data.keys() ] 
    assert all([ ext == '.br' for ext in data ])

  if not allow_missing:
    try:
      cff.transfer_to(cfm.cloudpath, paths=["dne"], allow_missing=False)
      assert False
    except FileNotFoundError:
      pass
  else:
    cff.transfer_to(cfm.cloudpath, paths=["dne"], allow_missing=True)
    assert not cfm.exists("dne") or cfm.get("dne") == b''


  cfm.delete(list(cfm))
  cff.delete(list(cff))

def test_slice_notation():
  from cloudfiles import CloudFiles, exceptions
  path = '/tmp/cloudfiles/slice_notation'
  rmtree(path)
  cf = CloudFiles('file://' + path)
  
  N = 128

  content = b'some_string'
  cf.puts(( (str(i), content) for i in  range(N) ))
  assert sorted(list(cf)) == sorted([ str(i) for i in range(N) ])
  assert [ f['content'] for f in cf[:] ] == [ content ] * N

  assert sorted([ f['path'] for f in cf[:100] ]) == sorted([ str(i) for i in range(N) ])[:100]
  assert [ f['content'] for f in cf[:100] ] == [ content ] * 100

  assert sorted([ f['path'] for f in cf[100:] ]) == sorted([ str(i) for i in range(N) ])[100:]
  assert [ f['content'] for f in cf[100:] ] == [ content ] * (N - 100)

  assert sorted([ f['path'] for f in cf[50:60] ]) == sorted([ str(i) for i in range(N) ])[50:60]
  assert [ f['content'] for f in cf[50:60] ] == [ content ] * 10

  assert sorted([ f['path'] for f in cf[:0] ]) == sorted([ str(i) for i in range(N) ])[:0]
  assert [ f['content'] for f in cf[:0] ] == [ content ] * 0


def test_to_https_protocol():
  from cloudfiles.paths import extract, to_https_protocol, ExtractedPath

  pth = to_https_protocol("gs://my_bucket/to/heaven")
  assert pth == "https://storage.googleapis.com/my_bucket/to/heaven"

  pth = to_https_protocol("s3://my_bucket/to/heaven")
  assert pth == "https://s3.amazonaws.com/my_bucket/to/heaven"

  pth = to_https_protocol("matrix://my_bucket/to/heaven")
  assert pth == "https://s3-hpcrc.rc.princeton.edu/my_bucket/to/heaven"

  pth = to_https_protocol("nokura://my_bucket/to/heaven")
  assert pth == "https://nokura.pni.princeton.edu/my_bucket/to/heaven"

  pth = to_https_protocol("tigerdata://my_bucket/to/heaven")
  assert pth == "https://td.princeton.edu/my_bucket/to/heaven"

  pth = to_https_protocol("file://my_bucket/to/heaven")
  assert pth == "file://my_bucket/to/heaven"

  pth = to_https_protocol("mem://my_bucket/to/heaven")
  assert pth == "mem://my_bucket/to/heaven"

  pth = ExtractedPath('precomputed', 'gs', 'my_bucket', 'to/heaven', None, None)
  pth = to_https_protocol(pth)
  assert pth == extract("https://storage.googleapis.com/my_bucket/to/heaven")

def test_ascloudpath():
  from cloudfiles.paths import ascloudpath, ExtractedPath
  pth = ExtractedPath('precomputed', 'gs', 'my_bucket', 'of/heaven', None, None)
  assert ascloudpath(pth) == "precomputed://gs://my_bucket/of/heaven"

  pth = ExtractedPath(None, 'gs', 'my_bucket', 'of/heaven', None, None)
  assert ascloudpath(pth) == "gs://my_bucket/of/heaven"

  pth = ExtractedPath(None, 'file', 'my_bucket', 'of/heaven', None, None)
  assert ascloudpath(pth) == "file://my_bucket/of/heaven"

  pth = ExtractedPath(None, 'mem', 'my_bucket', 'of/heaven', None, None)
  assert ascloudpath(pth) == "mem://my_bucket/of/heaven"

  pth = ExtractedPath(None, 'https', 'my_bucket', 'of/heaven', 'https://some.domain.com', None)
  assert ascloudpath(pth) == "https://some.domain.com/my_bucket/of/heaven"

  pth = ExtractedPath('graphene', 'https', 'my_bucket', 'of/heaven', 'https://some.domain.com', None)
  assert ascloudpath(pth) == "graphene://https://some.domain.com/my_bucket/of/heaven"

  pth = ExtractedPath(None, 's3', 'my_bucket', 'of/heaven', 'https://some.domain.com', None)
  assert ascloudpath(pth) == "s3://https://some.domain.com/my_bucket/of/heaven"

  pth = ExtractedPath("precomputed", 's3', 'my_bucket', 'of/heaven', 'https://some.domain.com', None)
  assert ascloudpath(pth) == "precomputed://s3://https://some.domain.com/my_bucket/of/heaven"


def test_cli_cp():
  import subprocess
  from cloudfiles.lib import mkdir, touch
  test_dir = os.path.dirname(os.path.abspath(__file__))
  srcdir = os.path.join(test_dir, "testfiles_src")
  destdir = os.path.join(test_dir, "testfiles_dest")
  N = 100

  try:
    shutil.rmtree(srcdir)
  except FileNotFoundError:
    pass

  if os.path.isfile(f"{destdir}"):
    os.remove(destdir)

  try:
    shutil.rmtree(destdir)
  except FileNotFoundError:
    pass

  def mkfiles(mkdirname):
    try:
      shutil.rmtree(mkdirname)
    except FileNotFoundError:
      pass
    mkdir(mkdirname)
    for i in range(N):
      touch(os.path.join(mkdirname, str(i)))

  mkfiles(srcdir)

  # cp -r has different behavior depending on if the dest 
  # directory exists or not so we run the test twice
  # in both conditions.
  subprocess.run(["cloudfiles", "cp", "-r", srcdir, destdir])
  assert len(os.listdir(srcdir)) == N
  assert os.listdir(srcdir) == os.listdir(destdir)

  subprocess.run(["cloudfiles", "cp", "-r", srcdir, destdir])
  assert len(os.listdir(srcdir)) == N
  assert os.listdir(srcdir) == os.listdir(os.path.join(destdir, os.path.basename(srcdir)))
  shutil.rmtree(destdir)

  subprocess.run(["cloudfiles", "cp", "-r", srcdir  + "/1*", destdir])
  assert set(os.listdir(destdir)) == set([ "1"] + [ str(i) for i in range(10, 20) ])

  shutil.rmtree(destdir)
  subprocess.run(["cloudfiles", "cp", srcdir + "/**", destdir])
  assert os.listdir(srcdir) == os.listdir(destdir)

  shutil.rmtree(destdir)
  subprocess.run(["cloudfiles", "cp", srcdir + "/*", destdir])
  assert os.listdir(srcdir) == os.listdir(destdir)

  shutil.rmtree(destdir)
  subprocess.run(f"find {srcdir} -type f | cloudfiles cp - {destdir}", shell=True)
  assert os.listdir(srcdir) == os.listdir(destdir)

  shutil.rmtree(destdir)
  mkdir(destdir)
  subprocess.run(f"cloudfiles cp {srcdir}/10 {destdir}", shell=True)
  assert os.listdir(destdir) == ["10"]

  shutil.rmtree(destdir)
  subprocess.run(f"cloudfiles cp {srcdir}/10 {destdir}", shell=True)
  assert os.path.isfile(f"{destdir}")
  os.remove(destdir)

  mkdir(destdir)
  subprocess.run(f"cloudfiles cp {srcdir}/8 {srcdir}/9 {destdir}", shell=True)
  assert sorted(os.listdir(destdir)) == ["8","9"]
  shutil.rmtree(destdir)

  subprocess.run(f"cloudfiles cp {destdir}", shell=True)
  assert not os.path.exists(destdir)

  try:
    shutil.rmtree(srcdir)
  except FileNotFoundError:
    pass

  try:
    shutil.rmtree(destdir)
  except FileNotFoundError:
    pass

def test_xfer():
  import subprocess
  from cloudfiles.lib import mkdir, touch
  test_dir = os.path.dirname(os.path.abspath(__file__))
  srcdir = os.path.join(test_dir, "testfiles_src")
  destdir = os.path.join(test_dir, "testfiles_dest")
  N = 100

  try:
    shutil.rmtree(srcdir)
  except FileNotFoundError:
    pass

  if os.path.isfile(f"{destdir}"):
    os.remove(destdir)

  try:
    shutil.rmtree(destdir)
  except FileNotFoundError:
    pass

  def mkfiles(mkdirname):
    try:
      shutil.rmtree(mkdirname)
    except FileNotFoundError:
      pass
    mkdir(mkdirname)
    for i in range(N):
      touch(os.path.join(mkdirname, str(i)))

  mkfiles(srcdir)

  subprocess.run(["cloudfiles", "xfer", "init", srcdir, destdir, "--db", "bananas.db"])
  subprocess.run(["cloudfiles", "xfer", "execute", "bananas.db"])
  assert len(os.listdir(srcdir)) == N
  assert os.listdir(srcdir) == os.listdir(destdir)

def test_cli_cat():
  import subprocess
  from cloudfiles.lib import mkdir, touch
  test_dir = os.path.dirname(os.path.abspath(__file__))
  srcdir = os.path.join(test_dir, "testfiles")
  
  def mkfiles(mkdirname):
    try:
      shutil.rmtree(mkdirname)
    except FileNotFoundError:
      pass
    mkdir(mkdirname)
    for i in range(10):
      with open(os.path.join(mkdirname, str(i)), "wt") as f:
        f.write("hello world")

  mkfiles(srcdir)
  try:
    out = subprocess.run(["cloudfiles", "cat", f"{srcdir}/1"], capture_output=True)
    assert out.stdout.decode("utf8") == "hello world"
  except TypeError: # python3.6 doesn't support capture_output
    pass

  try:
    out = subprocess.run(["cloudfiles", "cat", f"{srcdir}/1", f"{srcdir}/2"], capture_output=True)
    assert out.stdout.decode("utf8") == "hello worldhello world"
  except TypeError: # python3.6 doesn't support capture_output
    pass

  def rngtest(rng, val):
    try:
      mkfiles(srcdir)
      out = subprocess.run(["cloudfiles", "cat", "-r", rng, f"{srcdir}/1"], capture_output=True)
      assert out.stdout.decode("utf8") == val
    except TypeError: # python3.6 doesn't support capture_output
      pass

  rngtest("0-5", "hello")
  rngtest("-5", "hello")
  rngtest("0-", "hello world")
  rngtest("6-11", "world")

  out = subprocess.run(["cloudfiles", "cat", "doesnotexist"], capture_output=True)
  assert 'does not exist' in out.stdout.decode("utf8")

  out = subprocess.run(["cloudfiles", "cat", "-r", "0-5", "doesnotexist"], capture_output=True)
  assert 'does not exist' in out.stdout.decode("utf8")

  try:
    shutil.rmtree(srcdir)
  except FileNotFoundError:
    pass

def test_cli_rm_shell():
  import subprocess
  from cloudfiles.lib import mkdir, touch
  test_dir = os.path.dirname(os.path.abspath(__file__))
  test_dir = os.path.join(test_dir, "testfiles")

  N = 100

  def mkfiles():
    try:
      shutil.rmtree(test_dir)
    except FileNotFoundError:
      pass
    mkdir(test_dir)
    for i in range(N):
      touch(os.path.join(test_dir, str(i)))

  mkfiles()
  subprocess.run(["cloudfiles", "rm", "-r", test_dir])
  assert os.listdir(test_dir) == []

  try:
    mkfiles()
    out = subprocess.run(["cloudfiles", "rm", test_dir], capture_output=True)
    assert b'is a directory' in out.stdout 
    assert len(os.listdir(test_dir)) == N
  except TypeError: # python3.6 doesn't support capture_output
    pass

  mkfiles()
  subprocess.run(["cloudfiles", "rm", test_dir + "/*"])
  assert os.listdir(test_dir) == []

  mkfiles()
  subprocess.run(["cloudfiles", "rm", test_dir + "/**"])
  assert os.listdir(test_dir) == []

  mkfiles()
  subprocess.run(["cloudfiles", "rm", test_dir + "/0"])
  assert set(os.listdir(test_dir)) == set([ str(_) for _ in range(1, N) ])

  mkfiles()
  subprocess.run(["cloudfiles", "rm", test_dir + "/0", test_dir + "/1"])
  assert set(os.listdir(test_dir)) == set([ str(_) for _ in range(2, N) ])

  mkfiles()
  subprocess.run(["cloudfiles", "rm", test_dir + "/1*"])
  res = set([ str(_) for _ in range(N) ])
  res.remove("1")
  for x in range(10, 20):
    res.remove(str(x))
  assert set(os.listdir(test_dir)) == res

  touch("./cloudfiles-deletable-test-file")
  subprocess.run(["cloudfiles", "rm", "./cloudfiles-deletable-test-file"])
  assert not os.path.exists("./cloudfiles-deletable-test-file")

  try:
    shutil.rmtree(test_dir)
  except FileNotFoundError:
    pass

@pytest.mark.parametrize("protocol", ["file", "s3"])
def test_cli_rm_python(s3, protocol):
  from cloudfiles_cli.cloudfiles_cli import _rm_many
  from cloudfiles import CloudFiles, exceptions

  test_dir = compute_url(protocol, "cli_rm_python")
  cf = CloudFiles(test_dir)

  N = 100

  def mkfiles():
    cf.delete(cf.list())
    for i in range(N):
      cf[str(i)] = b"hello world"

  def run_rm(path, recursive=False):
    _rm_many(
      path, recursive=recursive, progress=False, 
      parallel=1, block_size=128
    )


  mkfiles()
  run_rm(test_dir, recursive=True)
  assert list(cf) == []

  mkfiles()
  run_rm(test_dir, recursive=False)
  assert len(list(cf)) == N

  mkfiles()
  run_rm(test_dir + "/*")
  print(list(cf))
  assert list(cf) == []

  mkfiles()
  run_rm(test_dir + "/**")
  assert list(cf) == []

  mkfiles()
  run_rm(test_dir + "/0")
  assert set(list(cf)) == set([ str(_) for _ in range(1, N) ])

  mkfiles()
  run_rm(test_dir + "/1*")
  res = set([ str(_) for _ in range(N) ])
  res.remove("1")
  for x in range(10, 20):
    res.remove(str(x))
  assert set(list(cf)) == res

  cf.delete(cf.list())


@pytest.mark.parametrize("slash", ["", "/"])
@pytest.mark.parametrize("protocol", ["file", "s3"])
def test_cli_rm_recursive(s3, protocol, slash):
  import cloudfiles_cli.cloudfiles_cli
  from cloudfiles import CloudFiles, exceptions

  path = compute_url(protocol, "remove_cli")
  cf = CloudFiles(path)
  cf["test/wow"] = b"hello world"
  cf["test2/wow"] = b"hello world"

  # can't use subprocess b/c it can't access the s3 mock
  cloudfiles_cli.cloudfiles_cli._rm_many(
    f"{path}/test{slash}",
    recursive=True, 
    progress=False, 
    parallel=1,
    block_size=128
  )

  assert list(cf) == [ "test2/wow" ]

  del cf["test/wow"]
  del cf["test2/wow"]

@pytest.mark.parametrize("green", (True, False))
def test_exceptions_raised(green):
  from cloudfiles import CloudFiles, exceptions
  from cloudfiles.lib import mkdir
  path = compute_url("file", "exceptions_raised")
  cf = CloudFiles(path, green=green)

  pth = mkdir(path.replace("file://", ""))
  with open(f"{pth}/wontdecompress.gz", "wb") as f:
    f.write(b"not a valid gzip stream")

  try:
    x = cf.get("wontdecompress")
    assert False
  except exceptions.DecompressionError:
    pass

  try:
    x = cf.get(["wontdecompress"], raise_errors=True)
    assert False
  except exceptions.DecompressionError:
    pass

  try:
    x = cf.get(["wontdecompress"], return_dict=True)
    assert False
  except exceptions.DecompressionError:
    pass

  cf.delete("wontdecompress")


def test_add_persistent_alias():
  from cloudfiles import CloudFiles
  import cloudfiles.paths
  cloudfiles.paths.add_persistent_alias("example", "mem://example/")

  cf = CloudFiles("example://hello")
  cf.put("world", b"earth")
  assert cf.get("world") == b"earth"

  cloudfiles.paths.remove_persistent_alias("example")  


def test_lock_clearing():
  from cloudfiles import CloudFiles
  import re

  cf = CloudFiles("file:///tmp/cloudfiles/", locking=True)
  cf.clear_locks()
  cf.put("hello.txt", b"hello world")
  lst = os.listdir(cf.lock_dir)
  match = re.match(r"^hello\.txt\.\d+", lst[0])
  if not match:
    assert False, "Unable to locate a lock for hello.txt"
  cf.clear_locks()
  lst = os.listdir(cf.lock_dir)
  assert len(lst) == 0


@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))
def test_move(s3, protocol):
  from cloudfiles import CloudFiles

  url = compute_url(protocol, "move")

  cf = CloudFiles(url)
  cf.puts([
    ('hello', b'world'),
    ('lamp', b'emporium'),
  ])
  cf.move("hello", f"{url}/hola")
  
  assert all(cf.exists(["hola"]).values()) == True
  assert all(cf.exists(["hello"]).values()) == False

  cf.puts([
    ('hello', b'world'),
    ('lamp', b'emporium'),
  ])

  cf.delete("hola")

  cf.moves(f"{url}", [
    ("hello", f"hola"),
    ("lamp", f"lampara"),
  ])

  assert all(cf.exists(["hola", "lampara"]).values()) == True
  assert all(cf.exists(["hello", "lamp"]).values()) == False

  cf.delete([ "hola", "hello", "lamp", "lampara" ])

@pytest.mark.parametrize("protocol", ["file", "s3"])
def test_cli_move_python(s3, protocol):
  from cloudfiles_cli.cloudfiles_cli import _mv_single
  from cloudfiles import CloudFiles, exceptions

  test_dir = compute_url(protocol, "cli_mv_python")
  test_dir2 = compute_url(protocol, "cli_mv_python2")
  cf = CloudFiles(test_dir)

  N = 100

  def mkfiles():
    cf.delete(cf.list())
    for i in range(N):
      cf[str(i)] = b"hello world"

  def run_mv(src, dest):
    _mv_single(
      src, dest, 
      progress=False, block_size=5,
      part_bytes=int(100e6), no_sign_request=True,
      parallel=1
    )

  mkfiles()
  run_mv(test_dir, test_dir2)
  assert sorted(list(cf)) == []

  cf2 = CloudFiles(test_dir2)
  print(sorted(list(cf2)))
  assert sorted(list(cf2)) == sorted([ f'{i}' for i in range(N) ])

  mkfiles()
  run_mv(f"{test_dir}/*", f"{test_dir}/move/")
  assert sorted(list(cf.list(prefix="move"))) == sorted([ f'move/{i}' for i in range(N) ])

  mkfiles()
  run_mv(f"{test_dir}/1", f"{test_dir}/move/1")
  assert cf.exists("move/1") == True
  assert cf.exists("1") == False

@pytest.mark.parametrize("protocol", ["file", "mem", "s3"])
def test_touch(s3, protocol):
  from cloudfiles import CloudFiles

  url = compute_url(protocol, "touch")

  cf = CloudFiles(url)

  cf.touch([ str(i) for i in range(20) ])

  assert sorted(list(cf)) == sorted([ str(i) for i in range(20) ])

  cf.touch([ str(i) for i in range(20) ])

  assert sorted(list(cf)) == sorted([ str(i) for i in range(20) ])
