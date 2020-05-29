import os
import setuptools
import sys

setuptools.setup(
  setup_requires=['pbr'],
  install_requires=[],
  extras_require={
    ':python_version == "2.7"': ['futures'],
    ':python_version == "2.6"': ['futures'],
  },
  pbr=True
)





