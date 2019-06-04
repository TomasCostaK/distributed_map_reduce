import logging
import re
<<<<<<< HEAD
import locale
from functools import cmp_to_key

locale.setlocale(locale.LC_ALL, 'pt_PT.UTF-8')
=======

#esta a receber = 1\nbrilha brilha olá
#retorna texto tipo = [('brilha',1), ('brilha',1), ('ola',1)]
>>>>>>> 3c896b0d83723792434b6ff4606cdc0c253641b4

class Mapper():
	"""docstring for Mapper"""
	def __init__(self):
		logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
		self.logger = logging.getLogger('mapper')

	def map(self,blob):
		#Ver um algoritmo melhor que isto?
		wordArray = []
<<<<<<< HEAD
		arrayMapper = sorted(re.split("[\s*\d*',''.']+",blob), key=cmp_to_key(locale.strcoll))

=======
		arrayMapper = re.split("[\s*\d*',''.']+",blob)
		#Algorithm
		# for word in arrayMapper:
		# 	inThere = False
		# 	for mapWord in wordArray:
		# 		if mapWord[0] == word:
		# 			print(word)
		# 			mapWord[1]+=1
		# 			inThere = True
		# 			break
		# 	if not inThere:
		# 		wordArray.append((word,1))
>>>>>>> 3c896b0d83723792434b6ff4606cdc0c253641b4
		for word in arrayMapper:
			word = word.lower()
			if word.isalpha():
				wordArray.append((word,1))

<<<<<<< HEAD
=======

		print("WORDMAPPER:")
		for word in wordArray:
			print(word, end=", ")
		print("\n")

>>>>>>> 3c896b0d83723792434b6ff4606cdc0c253641b4
		return wordArray


					

<<<<<<< HEAD
		
=======
		
>>>>>>> 3c896b0d83723792434b6ff4606cdc0c253641b4
