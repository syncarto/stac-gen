
import pkgutil
import pprint
import sys

__path__ = pkgutil.extend_path(__path__, __name__)

sys.path += __path__
#print ('stacgen.__path__ after:')
#pprint.pprint(sys.path)

from . import satstac
from .create_stac_catalog import create_stac_catalog
__all__ = ['create_stac_catalog','satstac']
