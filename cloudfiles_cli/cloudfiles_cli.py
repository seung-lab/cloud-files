from concurrent.futures import ProcessPoolExecutor
from functools import partial
import itertools
import re
import multiprocessing as mp
import posixpath
import pprint
import os.path
from tqdm import tqdm
import sys

import click
import pathos.pools

from cloudfiles import CloudFiles
from cloudfiles.compression import transcode
from cloudfiles.paths import extract, get_protocol
from cloudfiles.lib import toabs, sip, toiter, first

def cloudpathjoin(cloudpath, *args):
  cloudpath = normalize_path(cloudpath)
  proto = get_protocol(cloudpath)
  if proto == "file":
    # join function can strip "file://"
    return "file://" + os.path.join(cloudpath, *args).replace("file://", "")
  else:
    return posixpath.join(cloudpath, *args)

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
@click.option('--shortpath', is_flag=True, default=False, help='Don\'t print the common base path for each listed path.')
@click.option('--flat', is_flag=True, default=False, help='Only produce a single level of directory hierarchy.')
@click.option('-e','--expr',is_flag=True, default=False, help='Use a limited regexp language (e.g. [abc123]\{3\}) to generate prefixes.')
@click.argument("cloudpath")
def ls(shortpath, flat, expr, cloudpath):
  """Recursively lists the contents of a directory."""
  cloudpath = normalize_path(cloudpath)

  _, flt, prefix = get_mfp(cloudpath, True)
  epath = extract(cloudpath)
  if len(epath.path) > 0:
    if prefix == "" and flt == False:
      prefix = os.path.basename(cloudpath)
    cloudpath = os.path.dirname(cloudpath)

  flat = flat or flt

  cf = CloudFiles(cloudpath, green=True)
  iterables = []
  if expr:
    # TODO: make this a reality using a parser
    # match "[abc]{2}" or "[123]" meaning generate a 2 character cartesian
    # product of a,b, and c or a 1 character cartesian product of 1,2,3
    # e.g. aa, ab, ac, ba, bb, bc, ca, cb, cc
    #      1, 2, 3
    matches = re.findall(r'\[([a-zA-Z0-9]+)\]', prefix)

    if len(matches):
      iterables.extend(
        [ cf.list(prefix=pfx, flat=flat) for pfx in exprgen(prefix, matches) ]
      )
    else:
      iterables.append(
        cf.list(flat=flat)
      )
  else:
    iterables = [ cf.list(prefix=prefix, flat=flat) ]

  iterables = itertools.chain(*iterables)
  for pathset in sip(iterables, 1000):
    if not shortpath:
      pathset = [ cloudpathjoin(cloudpath, pth) for pth in pathset ]
    print("\n".join(pathset))

def exprgen(prefix, matches):
  """
  Given a string "hello[world]" and matches := ["world"]
  return ["hellow", "helloo", "hellor", "hellol", "hellod"]
  """
  if len(matches) == 0:
    return [ prefix ]

  match = matches[0]
  prefixes = []
  for char in match:
    prefixes.append(prefix.replace(f"[{match}]", char, 1))
  
  finished_prefixes = []
  for pfx in prefixes:
    finished_prefixes += exprgen(pfx, matches[1:])

  return finished_prefixes


def get_mfp(path, recursive):
  """many,flat,prefix"""
  path = normalize_path(path)
  flat = not recursive
  many = recursive
  prefix = ""
  if path[-2:] == "**":
    many = True
    flat = False
    prefix = os.path.basename(path[:-2])
  elif path[-1:] == "*":
    many = True
    flat = True
    prefix = os.path.basename(path[:-1])

  return (many, flat, prefix)

@main.command()
@click.argument("source", nargs=-1)
@click.argument("destination", nargs=1)
@click.option('-r', '--recursive', is_flag=True, default=False, help='Recursive copy.')
@click.option('-c', '--compression', default='same', help="Destination compression type. Options: same (default), none, gzip, br, zstd")
@click.option('--progress', is_flag=True, default=False, help="Show transfer progress.")
@click.option('-b', '--block-size', default=128, help="Number of files to download at a time.")
@click.pass_context
def cp(ctx, source, destination, recursive, compression, progress, block_size):
  """
  Copy one or more files from a source to destination.

  If source is "-" read newline delimited filenames from stdin.
  If destination is "-" output to stdout.

  Note that for gs:// to gs:// transfers, the gsutil
  tool is more efficient because the files never leave
  Google's network.
  """
  use_stdout = (destination == '-')
  if len(source) > 1 and not ispathdir(destination) and not use_stdout:
    print("cloudfiles: destination must be a directory for multiple source files.")
    return

  for src in source:
    _cp_single(ctx, src, destination, recursive, compression, progress, block_size)

def _cp_single(ctx, source, destination, recursive, compression, progress, block_size):
  use_stdin = (source == '-')
  use_stdout = (destination == '-')

  if use_stdout:
    progress = False # can't have the progress bar interfering

  nsrc = normalize_path(source)
  ndest = normalize_path(destination)

  # For more information see:
  # https://cloud.google.com/storage/docs/gsutil/commands/cp#how-names-are-constructed
  # Try to follow cp rules. If the directory exists,
  # copy the base source directory into the dest directory
  # If the directory does not exist, then we copy into
  # the dest directory.
  # Both x* and x** should not copy the base directory
  if recursive and nsrc[-1] != "*":
    if CloudFiles(ndest).isdir():
      if nsrc[-1] == '/':
        nsrc = nsrc[:-1]
      ndest = cloudpathjoin(ndest, os.path.basename(nsrc))

  ctx.ensure_object(dict)
  parallel = int(ctx.obj.get("parallel", 1))

  issrcdir = ispathdir(source) and use_stdin == False
  isdestdir = ispathdir(destination)

  srcpath = nsrc if issrcdir else os.path.dirname(nsrc)
  many, flat, prefix = get_mfp(nsrc, recursive)

  if issrcdir and not many:
    print(f"cloudfiles: {source} is a directory (not copied).")
    return

  xferpaths = os.path.basename(nsrc)
  if use_stdin:
    xferpaths = sys.stdin.readlines()
    xferpaths = [ x.replace("\n", "") for x in xferpaths ]
    prefix = os.path.commonprefix(xferpaths)
    xferpaths = [ x.replace(prefix, "") for x in xferpaths ]
    srcpath = cloudpathjoin(srcpath, prefix)
  elif many:
    xferpaths = CloudFiles(srcpath, green=True).list(prefix=prefix, flat=flat)

  destpath = ndest
  if isinstance(xferpaths, str):
    destpath = ndest if isdestdir else os.path.dirname(ndest)
  elif not isdestdir:
    if os.path.exists(ndest.replace("file://", "")):
      print(f"cloudfiles: {ndest} is not a directory (not copied).")
      return

  if compression == "same":
    compression = None
  elif compression == "none":
    compression = False

  if not isinstance(xferpaths, str):
    if parallel == 1:
      _cp(srcpath, destpath, compression, progress, block_size, xferpaths)
      return 

    total = None
    try:
      total = len(xferpaths)
    except TypeError:
      pass

    if use_stdout:
      fn = partial(_cp_stdout, srcpath)
    else:
      fn = partial(_cp, srcpath, destpath, compression, False, block_size)

    with tqdm(desc="Transferring", total=total, disable=(not progress)) as pbar:
      with pathos.pools.ProcessPool(parallel) as executor:
        for _ in executor.imap(fn, sip(xferpaths, block_size)):
          pbar.update(block_size)
  else:
    cfsrc = CloudFiles(srcpath, green=True, progress=progress)
    if not cfsrc.exists(xferpaths):
      print(f"cloudfiles: source path not found: {cfsrc.abspath(xferpaths).replace('file://','')}")
      return

    if use_stdout:
      _cp_stdout(srcpath, xferpaths)
      return
    
    downloaded = cfsrc.get(xferpaths, raw=True)
    if compression is not None:
      downloaded = transcode(downloaded, compression, in_place=True)

    cfdest = CloudFiles(destpath, green=True, progress=progress)
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

def _cp_stdout(src, paths):
  paths = toiter(paths)
  cf = CloudFiles(src, green=True, progress=False)
  for res in cf.get(paths):
    content = res["content"].decode("utf8")
    sys.stdout.write(content)

@main.command()
@click.argument('paths', nargs=-1)
@click.option('-r', '--recursive', is_flag=True, default=False, help='Descend into directories.')
@click.option('--progress', is_flag=True, default=False, help="Show transfer progress.")
@click.option('-b', '--block-size', default=128, help="Number of files to process at a time.")
@click.pass_context
def rm(ctx, paths, recursive, progress, block_size):
  """
  Remove file objects.

  Note that if the only path provided is "-",
  rm will read the paths from STDIN separated by 
  newlines.
  """
  ctx.ensure_object(dict)
  parallel = int(ctx.obj.get("parallel", 1))

  if len(paths) == 1 and paths[0] == "-":
    paths = sys.stdin.readlines()
    paths = [ path[:-1] for path in paths ] # clip "\n"

  for path in paths:
    many, flat, prefix = get_mfp(path, recursive)
    if ispathdir(path) and not many:
      print(f"cloudfiles: {path}: is a directory.")
      return

  for path in paths:
    _rm(path, recursive, progress, parallel, block_size)

def _rm(path, recursive, progress, parallel, block_size):
  npath = normalize_path(path)
  many, flat, prefix = get_mfp(path, recursive)

  cfpath = npath if ispathdir(path) else os.path.dirname(npath)
  xferpaths = os.path.basename(npath)
  if many:
    xferpaths = CloudFiles(cfpath, green=True).list(prefix=prefix, flat=flat)

  if parallel == 1 or not many:
    __rm(cfpath, progress, xferpaths)
    return 
  
  fn = partial(__rm, cfpath, False)
  with tqdm(desc="Deleting", disable=(not progress)) as pbar:
    with pathos.pools.ProcessPool(parallel) as executor:
      for _ in executor.imap(fn, sip(xferpaths, block_size)):
        pbar.update(block_size)

def __rm(cloudpath, progress, paths):
  CloudFiles(cloudpath, green=True, progress=progress).delete(paths)

@main.command()
@click.argument('paths', nargs=-1)
@click.option('-c', '--grand-total', is_flag=True, default=False, help="Sum a grand total of all inputs.")
@click.option('-s', '--summarize', is_flag=True, default=False, help="Sum a total for each input argument.")
@click.option('-h', '--human-readable', is_flag=True, default=False, help='"Human-readable" output. Use unit suffixes: Bytes, KiB, MiB, GiB, TiB, PiB, and EiB.')
def du(paths, grand_total, summarize, human_readable):
  """Display disk usage statistics."""
  results = []
  for path in paths:
    npath = normalize_path(path)
    if ispathdir(path):
      cf = CloudFiles(npath, green=True)
      results.append(cf.size(cf.list()))
    else:
      cf = CloudFiles(os.path.dirname(npath), green=True)
      results.append({ path: cf.size(os.path.basename(npath)) })

  def SI(val):
    if not human_readable:
      return val

    if val < 1024:
      return f"{val} Bytes"
    elif val < 2**20:
      return f"{(val / 2**10):.2f} KiB"
    elif val < 2**30:
      return f"{(val / 2**20):.2f} MiB"
    elif val < 2**40:
      return f"{(val / 2**30):.2f} GiB"
    elif val < 2**50:
      return f"{(val / 2**40):.2f} TiB"
    elif val < 2**60:
      return f"{(val / 2**50):.2f} PiB"
    else:
      return f"{(val / 2**60):.2f} EiB"

  summary = {}
  for path, res in zip(paths, results):
    summary[path] = sum(res.values())
    if summarize:
      print(f"{SI(summary[path])}\t{path}")

  if not summarize:
    for res in results:
      for pth, size in res.items():
        print(f"{SI(size)}\t{pth}")

  if grand_total:
    print(f"{SI(sum(summary.values()))}\ttotal") 

@main.command()
@click.argument('paths', nargs=-1)
def head(paths):
  results = {}
  for path in paths:
    npath = normalize_path(path)
    npath = re.sub(r'\*+$', '', path)
    many, flat, prefix = get_mfp(path, False)
    if many:
      cf = CloudFiles(npath, green=True)
      res = cf.head(cf.list(prefix=prefix, flat=flat))
      results.update(res)
    else:
      cf = CloudFiles(os.path.dirname(npath), green=True)
      results[path] = cf.head(os.path.basename(npath))

  pp = pprint.PrettyPrinter(indent=2)

  if len(paths) == 1 and len(results) == 1:
    val = first(results.values())
    if val is not None:
      print(val)
    else:
      print("cloudfiles: head: File not found: {}".format(paths[0]))
  elif len(paths) > 0:
    pp.pprint(results)


