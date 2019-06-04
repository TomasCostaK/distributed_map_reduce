import logging
import re
import locale
from functools import cmp_to_key

locale.setlocale(locale.LC_ALL, 'pt_PT.UTF-8')

class Mapper():
	"""docstring for Mapper"""
	def __init__(self):
		logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
		self.logger = logging.getLogger('mapper')

	def map(self,blob):
		#Ver um algoritmo melhor que isto?
		wordArray = []
		arrayMapper = sorted(re.split("[\s*\d*',''.']+",blob), key=cmp_to_key(locale.strcoll))

		for word in arrayMapper:
			word = word.lower()
			if word.isalpha():
				wordArray.append((word,1))

		return wordArray


					

		
