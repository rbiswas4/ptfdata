from distutils.core import setup
import sys
import os
import re

PACKAGENAME = 'ptfims'
packageDir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          PACKAGENAME)
versionFile = os.path.join(packageDir, 'version.py')

# Obtain the package version

with open(versionFile, 'r') as f:
          s = f.read()
# Look up the string value assigned to __version__ in version.py using regexp
versionRegExp = re.compile("__VERSION__ = \"(.*?)\"")
# Assign to __version__
__version__ =  versionRegExp.findall(s)[0]
print(__version__)
setup(# package information
      name=PACKAGENAME,
      version=__version__,
      description='simple repo to study OpSim output summaries',
      long_description=''' ''',
      # What code to include as packages
      packages=[PACKAGENAME],
      packagedir={PACKAGENAME: 'opsimsummary'},
      # What data to include as packages
      include_package_data=True,
      package_data={PACKAGENAME:['example_data/*.dat', 'example_data/*.simlib',
                                 'example_data/enigma_1189_micro.db',
                                 'example_data/healpixels_micro.db',
                                 'example_data/enigma_1189_micro.hdf']}
      )
