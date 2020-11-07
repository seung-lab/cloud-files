import gevent.monkey
gevent.monkey.patch_all(threads=False)

import os.path

from cloudfiles import CloudFiles
from cloudfiles.compression import transcode
from cloudfiles.paths import has_protocol
from cloudfiles.lib import toabs, sip
import click

def normalize_path(cloudpath):
  if not has_protocol(cloudpath):
    return "file://" + toabs(cloudpath)
  return cloudpath

@click.group()
# @click.option('-p', '--parallel', default=1, help='Number of parallel processes.')
def main():
  pass

@main.command()
def license():
  """Prints the license for this library and cli tool."""
  path = os.path.join(os.path.dirname(__file__), 'LICENSE')
  with open(path, 'rt') as f:
    print(f.read())

@main.command()
@click.argument("cloudpath")
def ls(cloudpath):
  """Recursively lists the contents of a directory."""
  cloudpath = normalize_path(cloudpath)
  cf = CloudFiles(cloudpath, green=True)

  for pathset in sip(cf.list(), 1000):
    print("\n".join(pathset))

@main.command()
@click.argument("source")
@click.argument("destination")
@click.option('-r', '--recursive', is_flag=True, default=False, help='Recursive copy.')
@click.option('-c', '--compression', default='same', help="Destination compression type. Options: same (default), none, gzip, br, zstd")
def cp(source, destination, recursive, compression):
  """
  Copy one or more files from a source to destination.

  Note that for gs:// to gs:// transfers, the gsutil
  tool is more efficient because the files never leave
  Google's network.
  """
  source = normalize_path(source)
  destination = normalize_path(destination)

  cfsrc = CloudFiles(os.path.dirname(source), green=True)
  path = os.path.basename(source)

  flat = False
  if path[-2:] == "**":
    recursive = True
    path = path[:-2]
  elif path[-1:] == "*":
    recursive = True
    flat = True
    path = path[:-1]

  if recursive:
    path = cfsrc.list(prefix=path, flat=flat)

  if recursive:
    cfdest = CloudFiles(destination, green=True)
  else:
    cfdest = CloudFiles(os.path.dirname(destination), green=True)

  if compression == "same":
    compression = None
  elif compression == "none":
    compression = False

  if recursive:
    cfsrc.transfer_to(cfdest, paths=path, reencode=compression)
  else:
    downloaded = cfsrc.get(path, raw=True)
    if compression is not None:
      downloaded = transcode(downloaded, compression, in_place=True)
    cfdest.put(os.path.basename(destination), downloaded, raw=True)



