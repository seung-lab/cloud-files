import gevent.monkey
gevent.monkey.patch_all(threads=False)

import os.path

import click

from cloudfiles import CloudFiles
from cloudfiles.compression import transcode
from cloudfiles.paths import extract, has_protocol
from cloudfiles.lib import toabs, sip

def normalize_path(cloudpath):
  if not has_protocol(cloudpath):
    return "file://" + toabs(cloudpath)
  return cloudpath

def ispathdir(cloudpath):
  expath = extract(normalize_path(cloudpath))
  return (
    (expath.protocol != "file" and cloudpath[-1] == "/")
    or (expath.protocol == "file" and cloudpath[-1] == os.path.sep)
    or (expath.protocol == "file" and os.path.isdir(expath.path))
  )

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
@click.option('--flat', is_flag=True, default=False, help='Only produce a single level of directory hierarchy.')
@click.argument("cloudpath")
def ls(flat, cloudpath):
  """Recursively lists the contents of a directory."""
  cloudpath = normalize_path(cloudpath)
  cf = CloudFiles(cloudpath, green=True)

  for pathset in sip(cf.list(flat=flat), 1000):
    print("\n".join(pathset))

@main.command()
@click.argument("source")
@click.argument("destination")
@click.option('-r', '--recursive', is_flag=True, default=False, help='Recursive copy.')
@click.option('-c', '--compression', default='same', help="Destination compression type. Options: same (default), none, gzip, br, zstd")
@click.option('--progress', is_flag=True, default=False, help="Show transfer progress.")
@click.option('-b', '--block-size', default=128, help="Number of files to download at a time.")
def cp(source, destination, recursive, compression, progress, block_size):
  """
  Copy one or more files from a source to destination.

  Note that for gs:// to gs:// transfers, the gsutil
  tool is more efficient because the files never leave
  Google's network.
  """
  nsrc = normalize_path(source)
  ndest = normalize_path(destination)

  issrcdir = ispathdir(source)
  isdestdir = ispathdir(destination)

  srcpath = nsrc if issrcdir else os.path.dirname(nsrc)

  cfsrc = CloudFiles(srcpath, green=True)

  flat = False
  prefix = ""
  if nsrc[-2:] == "**":
    recursive = True
    prefix = os.path.basename(nsrc[:-2])
  elif nsrc[-1:] == "*":
    recursive = True
    flat = True
    prefix = os.path.basename(nsrc[:-1])

  if issrcdir and not recursive:
    print(f"cloudfiles: {source} is a directory (not copied).")
    return

  xferpaths = os.path.basename(nsrc)
  if recursive:
    xferpaths = cfsrc.list(prefix=prefix, flat=flat)

  destpath = ndest if isdestdir else os.path.dirname(ndest)
  cfdest = CloudFiles(destpath, green=True, progress=progress)

  if compression == "same":
    compression = None
  elif compression == "none":
    compression = False

  if recursive:
    cfsrc.transfer_to(
      cfdest, paths=xferpaths, 
      reencode=compression, block_size=block_size
    )
  else:
    downloaded = cfsrc.get(xferpaths, raw=True)
    if compression is not None:
      downloaded = transcode(downloaded, compression, in_place=True)

    if isdestdir:
      cfdest.put(os.path.basename(nsrc), downloaded, raw=True)
    else:
      cfdest.put(os.path.basename(ndest), downloaded, raw=True)





