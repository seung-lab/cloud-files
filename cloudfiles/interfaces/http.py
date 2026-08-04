from typing import Optional

import posixpath

from .base import StorageInterface, retry, retry_if_not
from ..secrets import http_credentials

from ..exceptions import AuthorizationError

class HttpInterface(StorageInterface):  
  def __init__(self, path, secrets=None, request_payer=None, **kwargs):
    import requests
    super(StorageInterface, self).__init__()
    self.adaptor = requests.adapters.HTTPAdapter()
    self._path = path
    if request_payer is not None:
      raise ValueError("Specifying a request payer for the HttpInterface is not supported. request_payer must be None, got '{}'.".format(request_payer))

    if not secrets:
      secrets = http_credentials()

    self.session = requests.Session()
    self.session.mount('http://', self.adaptor)
    self.session.mount('https://', self.adaptor)
    if secrets and 'user' in secrets and 'password' in secrets:
      self.session.auth = (secrets['user'], secrets['password'])

  def default_headers(self):
    return {}

  def get_path_to_file(self, file_path):
    return posixpath.join(self._path.host, self._path.path, file_path)

  # @retry
  def delete_file(self, file_path):
    raise NotImplementedError()

  def delete_files(self, file_paths):
    raise NotImplementedError()

  # @retry
  def put_file(self, file_path, content, content_type,
               compress, cache_control=None, storage_class=None):
    raise NotImplementedError()

  @retry
  def head(self, file_path):
    key = self.get_path_to_file(file_path)
    headers = self.default_headers()
    with self.session.head(key, headers=headers) as resp:
      if resp.status_code in (404, 403):
        return None
      resp.raise_for_status()
      return resp.headers

  def size(self, file_path):
    headers = self.head(file_path)
    return int(headers["Content-Length"])

  def subtree_size(self, prefix:str = "") -> tuple[int,int]:
    raise NotImplementedError()

  @retry
  def get_file(self, file_path, start=None, end=None, part_size=None):
    key = self.get_path_to_file(file_path)

    headers = self.default_headers()
    if start is not None or end is not None:
      start = int(start) if start is not None else 0
      end = int(end - 1) if end is not None else ''
      headers["Range"] = f"bytes={start}-{end}"
    
    with self.session.get(key, headers=headers, stream=True) as resp:    
      if resp.status_code in (404, 403):
        return (None, None, None, None)
      resp.raise_for_status()
      resp.raw.decode_content = False
      content = resp.raw.read()
      content_encoding = resp.headers.get('Content-Encoding', None)  

    # Don't check MD5 for http because the etag can come in many
    # forms from either GCS, S3 or another service entirely. We
    # probably won't figure out how to decode it right.
    # etag = resp.headers.get('etag', None)
    
    return (content, content_encoding, None, None)

  def get_file_acl(self, file_path:str):
    raise NotImplementedError()

  @retry
  def save_file(self, src, dest, resumable) -> tuple[bool, int]:
    key = self.get_path_to_file(src)

    headers = self.head(src)
    content_encoding = headers.get('Content-Encoding', None)

    try:
      ext = FileInterface.get_extension(content_encoding)
    except ValueError:
      ext = ""

    fulldest = dest + ext

    partname = fulldest
    if resumable:
      partname += ".part"

    downloaded_size = 0
    if resumable and os.path.exists(partname):
      downloaded_size = os.path.getsize(partname)        

    streamed_bytes = 0

    range_headers = { "Range": f"bytes={downloaded_size}-" }
    with self.session.get(key, headers=range_headers, stream=True) as resp:
      if resp.status_code not in [200, 206]:
        resp.raise_for_status()
        return (False, 0)

      with open(partname, 'ab') as f:
        for chunk in resp.iter_content(chunk_size=int(10e6)):
          f.write(chunk)
          streamed_bytes += len(chunk)

    if resumable:
      os.rename(partname, fulldest)

    return (True, streamed_bytes)

  @retry
  def exists(self, file_path):
    key = self.get_path_to_file(file_path)
    headers = self.default_headers()
    with self.session.get(key, stream=True, headers=headers) as resp:
      return resp.ok

  def files_exist(self, file_paths):
    return {path: self.exists(path) for path in file_paths}

  def _list_files_google(
    self, 
    prefix:str, 
    flat:bool = False,
    size:bool = False,
    return_resume_token:bool = False,
    resume_token:Optional[str] = None,
  ):
    bucket = self._path.path.split('/')[0]
    prefix = posixpath.join(
      self._path.path.replace(bucket, '', 1), 
      prefix
    )
    if prefix and prefix[0] == '/':
      prefix = prefix[1:]

    headers = self.default_headers()

    items = "name"
    if size:
      items += ",size"

    fields = f"items({items}),nextPageToken,prefixes"

    @retry_if_not(AuthorizationError)
    def request(token):
      nonlocal headers
      params = {
        "fields": fields,
      }
      if prefix:
        params["prefix"] = prefix
      if token is not None:
        params["pageToken"] = token
      if flat:
        params["delimiter"] = '/'


      results = self.session.get(
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o",
        params=params,
        headers=headers,
      )
      if results.status_code in [401,403]:
        raise AuthorizationError(f"http {results.status_code}")

      results.raise_for_status()
      results.close()
      return results.json()

    strip = posixpath.dirname(prefix)
    if strip and strip[-1] != '/':
      strip += '/'

    token = None
    while True:
      results = request(token)
      token = results.get("nextPageToken", None)

      if 'prefixes' in results:
        itr = (
          item.removeprefix(strip) 
          for item in results["prefixes"]
        )
        if not size and not return_resume_token:
          yield from itr
        elif not size and return_resume_token:
          yield from ( (pre, token) for pre in itr )
        elif size and not return_resume_token:
          yield from ( (pre, 0) for pre in itr )
        else:
          yield from ( (pre, 0, token) for pre in itr )

      for res in results.get("items", []):
        name = res["name"].removeprefix(strip)
        if not size and not return_resume_token:
          yield name
        elif size and not return_resume_token:
          yield (name, int(res["size"]))
        elif not size and return_resume_token:
          yield (name, token)
        else:
          yield (name, int(res["size"]), token)
      
      if token is None:
        break

  def _list_files_apache(self, prefix, flat):
    import lxml.html
    import requests

    baseurl = posixpath.join(self._path.host, self._path.path)

    directories = ['']
    headers = self.default_headers()

    while directories:
      directory = directories.pop()
      url = posixpath.join(baseurl, directory)

      resp = requests.get(url, headers=headers)
      resp.raise_for_status()

      if 'text/html' not in resp.headers["Content-Type"]:
        raise ValueError("Unable to parse non-HTML output from Apache servers.")

      entities = lxml.html.document_fromstring(resp.content)
      resp.close()

      h1 = entities.xpath("body/h1")[0].text_content()
      if "Index of" not in h1:
        raise ValueError("Unable to parse non-index page.")

      for li in entities.xpath("body/ul/li"):
        txt = li.text_content().strip()
        if txt == "Parent Directory":
          continue
        
        txt = posixpath.join(directory, txt)
        if prefix and not txt.startswith(prefix):
          continue

        if txt[-1] == '/':
          directories.append(txt)
          continue

        yield txt

      if flat:
        break

  def list_files(
    self, 
    prefix:str, 
    flat:bool = False,
    size:bool = False,
    return_resume_token:bool = False,
    resume_token:Optional[str] = None,
  ):
    import requests
    if self._path.host == "https://storage.googleapis.com":
      yield from self._list_files_google(prefix, flat, size, return_resume_token, resume_token)
      return

    if size or resume_token or return_resume_token:
      raise NotImplementedError("size, resume_token, and return_resume_token are not yet implemented.")

    url = posixpath.join(self._path.host, self._path.path, prefix)
    resp = requests.head(url)

    server = resp.headers.get("Server", "").lower()
    if 'apache' in server:
      yield from self._list_files_apache(prefix, flat)
      return
    else:
      raise NotImplementedError()
