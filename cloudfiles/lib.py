import itertools
import json
import os.path
import time
import types
import sys

if sys.version_info < (3,0,0):
  STRING_TYPES = (str, unicode)
else:
  STRING_TYPES = (str,)

COLORS = {
  'RESET': "\033[m",
  'YELLOW': "\033[1;93m",
  'RED': '\033[1;91m',
  'GREEN': '\033[1;92m',
}

def green(text):
  return colorize('green', text)

def yellow(text):
  return colorize('yellow', text)

def red(text):
  return colorize('red', text)

def colorize(color, text):
  color = color.upper()
  return COLORS[color] + text + COLORS['RESET']

def toabs(path):
  path = os.path.expanduser(path)
  return os.path.abspath(path)

def mkdir(path):
  path = toabs(path)

  try:
    if path != '' and not os.path.exists(path):
      os.makedirs(path)
  except OSError as e:
    if e.errno == 17: # File Exists
      time.sleep(0.1)
      return mkdir(path)
    else:
      raise

  return path

def sip(iterable, block_size):
  """Sips a fixed size from the iterable."""
  ct = 0
  block = []
  for x in iterable:
    ct += 1
    block.append(x)
    if ct == block_size:
      yield block
      ct = 0
      block = []

  if len(block) > 0:
    yield block

class NumpyEncoder(json.JSONEncoder):
  def default(self, obj):
    try:
      import numpy as np
    except ImportError:
      try:
        return json.JSONEncoder.default(self, obj)
      except TypeError:
        if 'numpy' in str(type(obj)):
          print(yellow("Type " + str(type(obj)) + " requires a numpy installation to encode. Try `pip install numpy`."))
        raise

    if isinstance(obj, np.ndarray):
      return obj.tolist()
    if isinstance(obj, np.integer):
      return int(obj)
    if isinstance(obj, np.floating):
      return float(obj)
    return json.JSONEncoder.default(self, obj)

def jsonify(obj, **kwargs):
  return json.dumps(obj, cls=NumpyEncoder, **kwargs)

def first(lst):
  if isinstance(lst, types.GeneratorType):
    return next(lst)
  try:
    return lst[0]
  except TypeError:
    return next(iter(lst))

def toiter(obj, is_iter=False):
  if isinstance(obj, STRING_TYPES) or isinstance(obj, dict):
    if is_iter:
      return [ obj ], False
    return [ obj ]

  try:
    iter(obj)
    if is_iter:
      return obj, True
    return obj 
  except TypeError:
    if is_iter:
      return [ obj ], False
    return [ obj ]

def duplicates(lst):
  dupes = []
  seen = set()
  for elem in lst:
    if elem in seen:
      dupes.append(elem)
    seen.add(elem)
  return set(dupes)

def scatter(sequence, n):
  """Scatters elements of ``sequence`` into ``n`` blocks. Returns generator."""
  if n < 1:
    raise ValueError('n cannot be less than one. Got: ' + str(n))
  sequence = list(sequence)
  for i in range(n):
    yield sequence[i::n]
