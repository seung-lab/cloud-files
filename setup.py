import os
import setuptools
import sys

setuptools.setup(
  setup_requires=['pbr'],
  python_requires=">=3.9,<4.0",
  include_package_data=True,
  entry_points={
    "console_scripts": [
      "cloudfiles=cloudfiles_cli:main"
    ],
  },
  extras_require={
    "apache": [ "lxml", ],
  },
  pbr=True
)

