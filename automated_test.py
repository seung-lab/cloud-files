import os
import pytest
import re
import shutil
import time

from moto import mock_s3

def rmtree(path):
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
  with mock_s3():
    import boto3
    conn = boto3.client('s3', region_name='us-east-1')
    conn.create_bucket(Bucket="cloudfiles")
    yield conn

@pytest.mark.parametrize("green", (False, True))
@pytest.mark.parametrize("num_threads", (0, 5, 20))
@pytest.mark.parametrize("protocol", ('mem', 'file', 's3'))#'gs'))
def test_read_write(s3, protocol, num_threads, green):
  from cloudfiles import CloudFiles, exceptions
  if protocol == 'file':
    path = '/tmp/cloudfiles/rw'
    rmtree(path)
    url = 'file://' + path
  else:
    url = '{}://cloudfiles/rw'.format(protocol)

  cf = CloudFiles(url, num_threads=num_threads, green=green)
  
  content = b'some_string'
  cf.put('info', content, compress=None, cache_control='no-cache')
  cf['info2'] = content

  assert cf.get('info') == content
  assert cf['info2'] == content
  assert cf['info2', 0:3] == content[0:3]
  assert cf['info2', :] == content[:]
  assert cf.get('nonexistentfile') is None

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

  cf.delete('info')

  if protocol == 'file':
    rmtree(path)

@pytest.mark.parametrize("green", (False, True))
@pytest.mark.parametrize("compress", (None, 'gzip','br','zstd'))
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

def test_http_read():
  from cloudfiles import CloudFiles, exceptions
  cf = CloudFiles("https://storage.googleapis.com/seunglab-test/test_v0/black/")
  info = cf.get_json('info')

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
@pytest.mark.parametrize("method", ('', None, True, False, 'gzip','br','zstd'))
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

@pytest.mark.parametrize("compression_method", ("gzip", "br", "zstd"))
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

@pytest.mark.parametrize("dest_encoding", (None, "gzip", "br", "zstd"))
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
  assert set(cf.list(prefix='', flat=True)) == set(['info1','info2','info5','info.txt'])
  assert set(cf.list(prefix='inf', flat=True)) == set(['info1','info2','info5','info.txt'])
  # Tests (2)
  assert set(cf.list(prefix='build', flat=True)) == set([])
  # Tests (3)
  assert set(cf.list(prefix='level1/', flat=True)) == set([])
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
      'segmentation/1.0/testvol', 'http://localhost:8080'))

  assert (paths.extract('precomputed://gs://fafb-ffn1-1234567')
    == ExtractedPath(
      'precomputed', 'gs', 'fafb-ffn1-1234567', 
      '', None))

  assert (paths.extract('precomputed://gs://fafb-ffn1-1234567/segmentation')
    == ExtractedPath(
      'precomputed', 'gs', 'fafb-ffn1-1234567', 
      'segmentation', None))

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
        'intermediate/path/dataset/layer', None
      ))

  assert (paths.extract('file:///tmp/dataset/layer') 
      == ExtractedPath(
        'precomputed', 'file', None, 
        "/tmp/dataset/layer", None
      ))

  assert (paths.extract('file://seunglab-test/intermediate/path/dataset/layer') 
      == ExtractedPath(
        'precomputed', 'file', None,
        os.path.join(curpath, 'seunglab-test/intermediate/path/dataset/layer'), None
      ))

  assert (paths.extract('gs://seunglab-test/intermediate/path/dataset/layer') 
      == ExtractedPath(
        'precomputed', 'gs', 'seunglab-test',
        'intermediate/path/dataset/layer', None
      ))

  assert (paths.extract('file://~/seunglab-test/intermediate/path/dataset/layer') 
      == ExtractedPath(
        'precomputed', 'file', None, 
        os.path.join(homepath, 'seunglab-test/intermediate/path/dataset/layer'),
        None
      )
  )

  assert (paths.extract('file:///User/me/.cloudvolume/cache/gs/bucket/dataset/layer') 
      == ExtractedPath(
        'precomputed', 'file', None, 
        '/User/me/.cloudvolume/cache/gs/bucket/dataset/layer', None
      ))

  shoulderror('ou3bouqjsa fkj aojsf oaojf ojsaf')

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

@pytest.mark.parametrize('compression', (None, 'gzip', 'br', 'zstd'))
def test_transfer_semantics(compression):
  from cloudfiles import CloudFiles, exceptions
  path = '/tmp/cloudfiles/xfer'
  rmtree(path)
  cff = CloudFiles('file://' + path)
  cfm = CloudFiles('mem://cloudfiles/xfer')
  
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

  cfm.transfer_from('file://' + path)
  assert sorted(list(cfm)) == sorted([ str(i) for i in range(N) ])
  assert [ f['content'] for f in cfm[:] ] == [ content ] * N

  cfm.delete(list(cfm))

  cff.transfer_to(cfm.cloudpath)
  assert sorted(list(cfm)) == sorted([ str(i) for i in range(N) ])
  assert [ f['content'] for f in cfm[:] ] == [ content ] * N  
  cfm.delete(list(cfm))

  cff.transfer_to(cfm.cloudpath, reencode='br')
  assert sorted(list(cfm)) == sorted([ str(i) for i in range(N) ])
  assert [ f['content'] for f in cfm[:] ] == [ content ] * N  

  data = cfm._get_connection()._data
  data = [ os.path.splitext(d)[1] for d in data.keys() ] 
  assert all([ ext == '.br' for ext in data ])

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

  pth = to_https_protocol("file://my_bucket/to/heaven")
  assert pth == "file://my_bucket/to/heaven"

  pth = to_https_protocol("mem://my_bucket/to/heaven")
  assert pth == "mem://my_bucket/to/heaven"

  pth = ExtractedPath('precomputed', 'gs', 'my_bucket', 'to/heaven', None)
  pth = to_https_protocol(pth)
  assert pth == extract("https://storage.googleapis.com/my_bucket/to/heaven")

def test_ascloudpath():
  from cloudfiles.paths import ascloudpath, ExtractedPath
  pth = ExtractedPath('precomputed', 'gs', 'my_bucket', 'of/heaven', None)
  assert ascloudpath(pth) == "precomputed://gs://my_bucket/of/heaven"

  pth = ExtractedPath(None, 'gs', 'my_bucket', 'of/heaven', None)
  assert ascloudpath(pth) == "gs://my_bucket/of/heaven"

  pth = ExtractedPath(None, 'file', 'my_bucket', 'of/heaven', None)
  assert ascloudpath(pth) == "file://my_bucket/of/heaven"

  pth = ExtractedPath(None, 'mem', 'my_bucket', 'of/heaven', None)
  assert ascloudpath(pth) == "mem://my_bucket/of/heaven"

  pth = ExtractedPath(None, 'https', 'my_bucket', 'of/heaven', 'https://some.domain.com')
  assert ascloudpath(pth) == "https://some.domain.com/my_bucket/of/heaven"

  pth = ExtractedPath('graphene', 'https', 'my_bucket', 'of/heaven', 'https://some.domain.com')
  assert ascloudpath(pth) == "graphene://https://some.domain.com/my_bucket/of/heaven"

  pth = ExtractedPath(None, 's3', 'my_bucket', 'of/heaven', 'https://some.domain.com')
  assert ascloudpath(pth) == "s3://https://some.domain.com/my_bucket/of/heaven"

  pth = ExtractedPath("precomputed", 's3', 'my_bucket', 'of/heaven', 'https://some.domain.com')
  assert ascloudpath(pth) == "precomputed://s3://https://some.domain.com/my_bucket/of/heaven"



