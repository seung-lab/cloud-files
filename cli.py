import gevent.monkey
gevent.monkey.patch_all(thread=False)

from concurrent.futures import ProcessPoolExecutor
from functools import partial
import multiprocessing as mp
import os.path
from tqdm import tqdm

import click
import pathos.pools

from cloudfiles import CloudFiles
from cloudfiles.compression import transcode
from cloudfiles.paths import extract, get_protocol
from cloudfiles.lib import toabs, sip

def normalize_path(cloudpath):
  if not get_protocol(cloudpath):
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
@click.option('-p', '--parallel', default=1, help='Number of parallel processes. <= 0 for all cores.')
@click.pass_context
def main(ctx, parallel):
  parallel = int(parallel)
  if parallel <= 0:
    parallel = mp.cpu_count()
  ctx.ensure_object(dict)
  ctx.obj["parallel"] = parallel

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
@click.pass_context
def cp(ctx, source, destination, recursive, compression, progress, block_size):
  """
  Copy one or more files from a source to destination.

  Note that for gs:// to gs:// transfers, the gsutil
  tool is more efficient because the files never leave
  Google's network.
  """
  nsrc = normalize_path(source)
  ndest = normalize_path(destination)

  ctx.ensure_object(dict)
  parallel = int(ctx.obj.get("parallel", 1))

  issrcdir = ispathdir(source)
  isdestdir = ispathdir(destination)

  srcpath = nsrc if issrcdir else os.path.dirname(nsrc)

  flat = not recursive
  many = recursive
  prefix = ""
  if nsrc[-2:] == "**":
    many = True
    flat = False
    prefix = os.path.basename(nsrc[:-2])
  elif nsrc[-1:] == "*":
    many = True
    flat = True
    prefix = os.path.basename(nsrc[:-1])

  if issrcdir and not many:
    print(f"cloudfiles: {source} is a directory (not copied).")
    return

  xferpaths = os.path.basename(nsrc)
  if many:
    xferpaths = CloudFiles(srcpath, green=True).list(prefix=prefix, flat=flat)

  destpath = ndest if isdestdir else os.path.dirname(ndest)

  if compression == "same":
    compression = None
  elif compression == "none":
    compression = False

  if many:
    if parallel == 1:
      _cp(srcpath, destpath, compression, progress, block_size, xferpaths)
      return 
    # print(xferpaths, list(xferpaths))
    fn = partial(_cp, srcpath, destpath, compression, False, block_size)
    with tqdm(desc="Transferring", disable=(not progress)) as pbar:
      with pathos.pools.ProcessPool(parallel) as executor:
        for _ in executor.imap(fn, sip(xferpaths, block_size)):
          pbar.update(block_size)
  else:
    downloaded = cfsrc.get(xferpaths, raw=True)
    if compression is not None:
      downloaded = transcode(downloaded, compression, in_place=True)

    if isdestdir:
      cfdest.put(os.path.basename(nsrc), downloaded, raw=True)
    else:
      cfdest.put(os.path.basename(ndest), downloaded, raw=True)

def _cp(src, dst, compression, progress, block_size, paths):
  cfsrc = CloudFiles(src, green=True, progress=progress)
  cfdest = CloudFiles(dst, green=True, progress=progress)
  cfsrc.transfer_to(
    cfdest, paths=paths, 
    reencode=compression, block_size=block_size
  )



