from __future__ import print_function
from six.moves import range

import os
import pytest
import re
import shutil
import time

from moto import mock_s3

def rmtree(path):
  if os.path.exists(path):
    shutil.rmtree(path)

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
@pytest.mark.parametrize("protocol", ('file', 's3'))#'gs'))
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

  if protocol == 'file':
    rmtree(path)

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
@pytest.mark.parametrize("protocol", ('file', 's3'))
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
@pytest.mark.parametrize("method", ('', None, True, False, 'gzip','br',))
def test_compression(method, green):
  from cloudfiles import CloudFiles, exceptions
  rmtree("/tmp/cloudfiles/compress")

  url = "file:///tmp/cloudfiles/compress"

  cf = CloudFiles(url, num_threads=5, green=green)
  content = b'some_string'

  # remove when GCS enables "br"
  if method == "br" and "gs://" in url:
    with pytest.raises(
        exceptions.UnsupportedCompressionType, 
        match="Brotli unsupported on google cloud storage"
    ):
      cf.put('info', content, compress=method)
      retrieved = cf.get('info')
  else:
    cf.put('info', content, compress=method)
    retrieved = cf.get('info')
    assert content == retrieved

  assert cf.get('nonexistentfile') is None

  try:
    cf.put('info', content, compress='nonexistent')
    assert False
  except ValueError:
    pass

  if "file://" in url:
    rmtree("/tmp/cloudfiles/compress")

@pytest.mark.parametrize("compression_method", ("gzip", "br"))
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
    _, e = conn.get_file("info")
    assert e == compression_method

    assert cf.get('nonexistentfile') is None

    rmtree(filepath)

@pytest.mark.parametrize("protocol", ('file', 's3'))
def test_list(s3, protocol):  
  from cloudfiles import CloudFiles, exceptions
  if protocol == 'file':
    rmtree('/tmp/cloudfiles/list')
    url = "file:///tmp/cloudfiles/list"
  else:
    url = "{}://cloudfiles/list".format(protocol)

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

@pytest.mark.parametrize("protocol", ('file', 's3'))
def test_exists(s3, protocol):
  from cloudfiles import CloudFiles, exceptions
  if protocol == 'file':
    url = "file:///tmp/cloudfiles/exists"
  else:
    url = "{}://cloudfiles/exists".format(protocol)

  cf = CloudFiles(url, num_threads=5)
  content = b'some_string'
  cf.put('info', content, compress=None)
  
  assert cf.exists('info')
  assert not cf.exists('doesntexist')

  assert cf.exists(['info'])['info']
  assert not cf.exists(['doesntexist'])['doesntexist']

  cf.delete('info')

@pytest.mark.parametrize("protocol", ('file', 's3'))
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