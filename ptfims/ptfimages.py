"""
Objects related to data downloads from the public PTF data server. Based on information and examples
in  

Should work in python 2 and 3.
"""
import requests
import pandas as pd
from future.standard_library import install_aliases
install_aliases()
from urllib.request import urlopen
import os

__all__ = ['PTFImages']

class PTFImages(object):
    """
    A class useful for downloading images that enclose point sources at different times as a function of
    coordinates (ra, dec). Choices in time ranges can also be made.
    
    """
    def __init__(self, ra, dec):
        self.ra = ra
        self.dec = dec
        self.fixedurl = "http://irsa.ipac.caltech.edu/ibe/search/ptf/images/level1?POS="
        self.imageurl = 'http://irsa.ipac.caltech.edu/ibe/data/ptf/images/level1/'
        self.dtypedict = None
        self._response = None
    
    def _get_url(self, ra=None, dec=None, fixedurl=None):
        """get url from ra, dec 
    
        Parameters
        ----------
        ra : ra in degrees
        dec : dec in degrees
        fixedurl : fixed url string
        """
        if ra is None:
            ra = self.ra
        if dec is None:
            dec = self.dec
        if fixedurl is None:
            fixedurl =  self.fixedurl
        pos = "{:3.6f},{:3.6f}".format(ra, dec)
        url = fixedurl + pos
        return url
    @property
    def response(self):
        """
        text of the response from the url
        """
        if self._response is None:
            
            url = self._get_url()
            r = requests.get(url)
            if r.status_code != 200:
                raise ValueError('response from url resulted in incorrect status', r.status)
            else:
                self._response = r.text
        return self._response
    
    @property
    def response_cat(self):
        """ A `pd.DataFrame` showing the catalog of images enclosing coordinates provided to
        create class instances
        """
        
        linesu = list(l for l in self.response.split('\n'))
        headers = []
        data = []
        for l in linesu:
            if '|' in l:
                headers.append(l)
            elif len(l.strip()) > 0 and '\\' not in l:
                data.append(self._process_dataline(l))
            else:
                # blank line
                pass
        #return pd.DataFrame(data), headers
        header_names, header_types, header_units = self.get_header(headers[:3])
        return pd.DataFrame(data, columns=header_names).convert_objects(convert_numeric=True)

    @staticmethod
    def _process_dataline(line):
        """tokenise each line of the data by splitting on whitespace, but then joining the 5th and 6th element,
        and the two final elements
    
        Parameters
        ----------
        line : string (unicode/bytes)
            line of the return split on '\n'
        
        .. notes : It is important that this does not include the newline character 
        """
    
        l = list(x.strip() for x in line.split())
    
        # join the fields to create strings
        l[4] = ' '.join([l[4], l[5]])
        l[-2] = ' '.join([l[-2], l[-1]])
    
        # pop the extra fields
        l.pop(-1)
        l.pop(5)
    
        return l

    def get_header(self, headers):
        """tuples of header strings
        """
        header_names, types, units = tuple(self._process_headerline(line) for line in headers)
        return header_names, types, units
    
    @staticmethod
    def _process_headerline(line):
        """tokenise each line of the data by splitting on whitespace, but then joining the 5th and 6th element,
        and the two final elements
    
        Parameters
        ----------
        line : string (unicode/bytes)
            line of the return split on '\n'
        
        .. notes : It is important that this does not include the newline character 
        """
    
        l = list(x.strip() for x in line.split('|'))
    
        return l[1:-1]

    def get_urls(self, query=None):
        """return a list of image urls based on a dataframe query of
        self.response_cat

        Parameters
        ----------
        query : string
            `pd.dataFrame.queries` of any kind are allowed
        """
        if query is not None:
            x = self.response_cat.query(query)
        else:
            x = self.response_cat
        pfilenames = list(self.imageurl + fname for fname in  x['pfilename'].values)
        afilename1 = list(self.imageurl + fname for fname in  x['afilename1'].values)
        return afilename1, pfilenames

    @staticmethod
    def downloadfilefromurl(url, outdir='./'):
        """
        Download a single file into the directory `outdir`
        Parameters
        ----------
        url : string
            url from which to download file
        outdir : string, defaults to `./`
            absolute path to existing output directory into which files will be downloaded
	.. note : `outdir` needs to exist
        """
        # Filename to write to is the same as the original file
        outdir = os.path.abspath(outdir)
        wfilename = os.path.join(outdir, url.split('/')[-1])
        resp = urlopen(url)

        with open(wfilename, 'wb') as f:
            while True:
                chunk = resp.read()
                if not chunk:
                    break
            f.write(chunk)
        return 0

    def downloadimages(self, query=None):
        """
        """
        afilename1, pfilenames = self.get_urls(query)
        print (afilename1, pfilenames)
        x = list(self.downloadfilefromurl(fname) for fname in afilename1)
        x = list(self.downloadfilefromurl(fname) for fname in pfilenames)
        return x

