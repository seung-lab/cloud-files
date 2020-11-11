import gevent.monkey
gevent.monkey.patch_all(thread=False)

from .cloudfiles_cli import *