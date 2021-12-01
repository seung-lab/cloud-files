from concurrent.futures import ProcessPoolExecutor
from functools import partial
import itertools
import json
import re
import multiprocessing as mp
import posixpath
import pprint
import os.path
from tqdm import tqdm
import sys

import click
import pathos.pools

import cloudfiles
import cloudfiles.paths
from cloudfiles import CloudFiles
from cloudfiles.compression import transcode
from cloudfiles.paths import extract, get_protocol
from cloudfiles.lib import (
  mkdir, toabs, sip, toiter, 
  first, red, green,
  md5_equal
)
import cloudfiles.lib

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

def get_sep(cloudpath):
  proto = get_protocol(cloudpath)
  if proto == "file":
    return os.path.sep
  else:
    return "/"

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
@click.argument("sources", nargs=-1)
@click.option('-r', '--range', 'byte_range', default=None, help='Retrieve start-end bytes.')
def cat(sources, byte_range):
  """Concatenate the contents of each input file and write to stdout."""
  if '-' in sources and len(sources) == 1:
    sources = sys.stdin.readlines()
    sources = [ source[:-1] for source in sources ] # clip "\n"

  if byte_range is not None and len(sources) > 1:
    print("cloudfiles: cat: range argument can only be used with a single source.")
    return
  elif byte_range is not None and len(sources):
    byte_range = byte_range.split("-")
    byte_range[0] = int(byte_range[0] or 0)
    byte_range[1] = int(byte_range[1]) if byte_range[1] not in ("", None) else None
    src = normalize_path(sources[0])
    cf = CloudFiles(os.path.dirname(src))
    download = cf[os.path.basename(src), byte_range[0]:byte_range[1]]
    if download is None:
      print(f'cloudfiles: {src} does not exist')
      return
    sys.stdout.write(download.decode("utf8"))
    return

  for srcs in sip(sources, 10):
    srcs = [ normalize_path(src) for src in srcs ]
    order = { src: i for i, src in enumerate(srcs) }
    files = cloudfiles.dl(srcs)
    output = [ None for _ in range(len(srcs)) ]
    for res in files:
      if res["content"] is None:
        print(f'cloudfiles: {res["path"]} does not exist')
        return
      fullpath = normalize_path(res["fullpath"].replace("precomputed://", ""))
      output[order[fullpath]] = res["content"].decode("utf8")
    del files
    for out in output:
      sys.stdout.write(out)

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
  # Correct the situation where on non-file:// systems
  # rm -r s3://bucket/test incorreclty means test** 
  # but on file it means test/ because it checks to see
  # if there's a directory. The correct behavior is that
  # on both it deletes rm -r test/.
  sep = get_sep(path)
  if recursive and path[-1] not in ("*", sep):
    path += sep

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
      sz = cf.size(os.path.basename(npath))
      if sz is None:
        print(f"cloudfiles: du: {path} does not exist")
        return
      results.append({ path: sz })

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
  """Retrieve metadata for one or more files."""
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

def populate_md5(cf, metadata, threshold=1e9):
  """threshold: parallel download up to this many bytes of files at once"""
  sz = lambda fname: metadata[fname]["Content-Length"]
  etag = lambda fname: metadata[fname]["ETag"] 
  md5content = lambda fname: metadata[fname]["Content-Md5"]

  filenames = list(metadata.keys())
  filenames = [ fname for fname in filenames if not etag(fname) and not md5content(fname) ]

  while filenames:
    filename = filenames.pop()
    paths = [ filename ]
    size = sz(filename)
  
    while filenames and size <= threshold:
      if size + sz(filenames[-1]) > threshold:
        break
      paths.append( filenames.pop() )
      size += sz(paths[-1])

    results = cf.get(paths, raw=True)
    for result in results: 
      filename = result["path"]
      metadata[filename]["ETag"] = cloudfiles.lib.md5(result["content"], base=64)
      metadata[filename]["Content-Md5"] = metadata[filename]["ETag"]

  return metadata

@main.command()
@click.argument("source")
@click.argument("target")
@click.option('-m', '--only-matching', is_flag=True, default=False, help="Only check files with matching filenames.", show_default=True)
@click.option('-v', '--verbose', is_flag=True, default=False, help="Output detailed information of failed matches.", show_default=True)
@click.option('--md5', is_flag=True, default=False, help="Compute the md5 hash if the Etag is missing. Can be slow!", show_default=True)
def verify(source, target, only_matching, verbose, md5):
  """
  Validates checksums of two files or two directories 
  match. These tags are usually either md5 or crc32c 
  generated strings. These are not secure hashes so they 
  will only catch accidental changes to files, not 
  intentionally malicious changes.
  """
  source = normalize_path(source)
  target = normalize_path(target)
  if ispathdir(source) != ispathdir(target):
    print("cloudfiles: verify source and target must both be files or directories.")
    return

  if not md5 and (get_protocol(source) == "file" or get_protocol(target) == "file"):
    print("cloudfiles: verify source and target must be object storage without --md5 option. The filesystem does not store hash information.")
    return

  if ispathdir(source):
    cfsrc = CloudFiles(source)
    src_files = set(list(cfsrc))
  else:
    cfsrc = CloudFiles(os.path.dirname(source))
    src_files = set([ os.path.basename(source) ])
  
  if ispathdir(target):
    cftarget = CloudFiles(target)
    target_files = set(list(cftarget))
  else:
    cftarget = CloudFiles(os.path.dirname(target))
    target_files = set([ os.path.basename(target) ])  
  
  matching_files = src_files.intersection(target_files)
  mismatched_files = src_files | target_files
  mismatched_files -= matching_files

  if not only_matching:
    if len(mismatched_files) > 0:
      if verbose:
        print(f"Extra source files:")
        print("\n".join(src_files - matching_files))
        print(f"Extra target files:")
        print("\n".join(target_files - matching_files))
      print(red(f"failed. {len(src_files)} source files, {len(target_files)} target files."))
      return

  src_meta = cfsrc.head(matching_files)
  target_meta = cftarget.head(matching_files)

  if md5:
    src_meta = populate_md5(cfsrc, src_meta)
    target_meta = populate_md5(cftarget, target_meta)

  failed_files = []
  for filename in src_meta:
    sm = src_meta[filename]
    tm = target_meta[filename]
    if sm["Content-Length"] != tm["Content-Length"]:
      failed_files.append(filename)
      continue
    elif not (
      (
        (sm["ETag"] and tm["ETag"])
        and (
          sm["ETag"] == tm["ETag"]
          or md5_equal(sm["ETag"], tm["ETag"])
        )
      )
      or (
        sm["Content-Md5"] and tm["Content-Md5"]
        and md5_equal(sm["Content-Md5"], tm["Content-Md5"])
      )
    ):
      failed_files.append(filename)
      continue
    elif sm["ETag"] in ("", None):
      failed_files.append(filename)

  if not failed_files:
    print(green(f"success. {len(matching_files)} files matching. {len(mismatched_files)} ignored."))
    return

  if verbose:
    failed_files.sort()

    header = [
      "src bytes".ljust(12+1),
      "target bytes".ljust(12+1),
      "senc".ljust(4+1),
      "tenc".ljust(4+1),
      "src etag".ljust(34+1),
      "target etag".ljust(34+1),
      "src md5".ljust(24+1),
      "target md5".ljust(24+1),
      "filename"
    ]

    print("".join(header))
    for filename in failed_files:
      sm = src_meta[filename]
      tm = target_meta[filename]
      print(f'{sm["Content-Length"]:<12} {tm["Content-Length"]:<12} {sm["Content-Encoding"] or "None":<4} {tm["Content-Encoding"] or "None":<4} {sm["ETag"] or "None":<34} {tm["ETag"] or "None":<34} {sm["Content-Md5"] or "None":<24} {tm["Content-Md5"] or "None":<24} {filename}')
    print("--")

  print(red(f"failed. {len(failed_files)} failed. {len(matching_files) - len(failed_files)} succeeded. {len(mismatched_files)} ignored."))

@main.group("alias")
def aliasgroup():
  """
  Add, list, and remove aliases for alternate s3 endpoints.

  Aliases can be used to name new protocol prefixes for
  your system. Be warned that future updates to cloudfiles
  may claim new "official" protocol prefixes that will
  override unoffical ones (so pick something obscure).

  Persistent aliases are saved to ~/.cloudfiles/aliases.json
  
  Example:

  cloudfiles alias add example s3://https://example.com/

  Which would then be used as:

  cloudfiles head example://bucket/info.txt
  CloudFiles("example://bucket/").get("info.txt")
  """
  pass

@aliasgroup.command("ls")
def alias_ls():
  """List all aliases."""
  aliasfile = cloudfiles.paths.ALIAS_FILE
  
  aliases = {}
  if os.path.exists(aliasfile):
    with open(aliasfile, "rt") as f:
      aliases = json.loads(f.read())

  for name,host in cloudfiles.paths.ALIASES.items():
    aliases[name] = { "host": host }

  for alias, vals in aliases.items():
    official = ""
    if alias in cloudfiles.paths.OFFICIAL_ALIASES:
      official = "(official)"

    alias = f"{alias}://"
    print(f"{alias:15} -> {vals['host']} {official}")
  
  if len(aliases) == 0:
    print("No aliases.")

@aliasgroup.command("add")
@click.argument("name")
@click.argument("host")
def alias_add(name, host):
  """Add an unofficial alias.

  Example:

  cloudfiles alias add example s3://https://example.com/

  Which would then be used as:

  cloudfiles head example://bucket/info.txt
  CloudFiles("example://bucket/").get("info.txt")
  """
  aliasfile = cloudfiles.paths.ALIAS_FILE
  
  aliases = {}
  if os.path.exists(aliasfile):
    with open(aliasfile, "rt") as f:
      aliases = json.loads(f.read())

  if name in cloudfiles.paths.BASE_ALLOWED_PROTOCOLS:
    print(f"{name} cannot be aliased.")
    return

  if name in cloudfiles.paths.ALIASES:
    print(f"{name} already exists.")
    return

  # leave room for adding other attributes like ACL
  aliases[name] = { "host": host } 

  with open(aliasfile, "wt") as f:
    f.write(json.dumps(aliases))

@aliasgroup.command("rm")
@click.argument("name")
def alias_rm(name):
  """Remove unofficial aliases."""
  aliasfile = cloudfiles.paths.ALIAS_FILE
  
  aliases = {}
  if os.path.exists(aliasfile):
    with open(aliasfile, "rt") as f:
      aliases = json.loads(f.read())

  if name in cloudfiles.paths.OFFICIAL_ALIASES:
    print("Cannot remove an official alias.")
    return

  try:
    del aliases[name]

    with open(aliasfile, "wt") as f:
      f.write(json.dumps(aliases))
  except KeyError:
    pass


