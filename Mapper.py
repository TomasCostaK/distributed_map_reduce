import logging
import re
import locale
import string
from functools import cmp_to_key

locale.setlocale(locale.LC_ALL, 'pt_PT.UTF-8')

class Mapper():
	"""docstring for Mapper"""
	def __init__(self):
	    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
	    self.logger = logging.getLogger('mapper')

	def map(self,blob):
            tokens = blob.lower()
            tokens = tokens.translate(str.maketrans('', '', string.digits))
            tokens = tokens.translate(str.maketrans('', '', string.punctuation))
            tokens = tokens.rstrip()
			
            wordArray = []
            arrayMapper = sorted( tokens.split(), key=cmp_to_key(locale.strcoll))

            for word in arrayMapper:
                if word.isalpha():
                    wordArray.append((word.lower(),1))
            return wordArray
