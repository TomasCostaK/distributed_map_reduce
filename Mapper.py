import logging
import re

#esta a receber = 1\nbrilha brilha olá
#retorna texto tipo = [('brilha',1), ('brilha',1), ('ola',1)]

class Mapper():
	"""docstring for Mapper"""
	def __init__(self):
		logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
		self.logger = logging.getLogger('mapper')

	def map(self,blob):
		#Ver um algoritmo melhor que isto?
		wordArray = []
		arrayMapper = re.split("[\s*\d*',''.']+",blob)
		#Algorithm
		for word in arrayMapper:
			inThere = False
			for mapWord in wordArray:
				numGotten = mapWord.get(word)
				if numGotten != None:
					mapWord[word]=numGotten+1
					inThere = True
					break
			if not inThere:
				wordArray.append({word:1})


		print("WORDMAPPER:")
		for word in wordArray:
			print(word, end=", ")
		print("\n")

		return wordArray


					

		