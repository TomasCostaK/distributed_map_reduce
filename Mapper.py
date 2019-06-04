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
            table = str.maketrans(dict.fromkeys('0123456789'))
            translator = str.maketrans('', '', string.punctuation)

            translated = blob.translate(translator)
            # remove digits
            translated = translated.translate(table)

            # remove line breaks
            translated = translated.replace('\n',' ')
			
            wordArray = []
            arrayMapper = sorted( re.split("\s",translated), key=cmp_to_key(locale.strcoll))

            for word in arrayMapper:
                if word.isalpha():
                    wordArray.append((word.lower(),1))

            return wordArray
