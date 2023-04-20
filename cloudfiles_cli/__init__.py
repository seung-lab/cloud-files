import gevent.monkey
gevent.monkey.patch_all(thread=True)

from .cloudfiles_cli import *