from .http import HttpInterface
from ..secrets import cave_credentials

class CaveInterface(HttpInterface):
  """
  CAVE is an internal system that powers proofreading 
  systems in Seung Lab. If you have no idea what this
  is, don't worry about it.
  see: https://github.com/CAVEconnectome
  """
  def __init__(self, path, secrets=None, **kwargs):
    super().__init__(path, secrets=secrets, **kwargs)

    secrets = kwargs.get('secrets', None)
    if secrets is None:
      secrets = {}

    self._token = secrets.get('token', None)
    if self._token is None:
      server = self._path.host.replace("https://", "", 1)
      server = server.replace("http://", "", 1)
      self._token = cave_credentials(server)
      if self._token is not None:
        self._token = self._token.get('token', None)

  def default_headers(self) -> dict:
    if self._token is None:
      return {}
    
    return {
      "Authorization": f"Bearer {self._token}",
    }