import logging

class Reducer:
	"""docstring for Reducer"""
	def __init__(self):
		logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
		self.logger = logging.getLogger('reducer')

	def reduce(self,array1,array2):
		arrayFinal = []

		for mapSet in array1:
			inThere = False
			word = next(iter(mapSet))
			numero = mapSet.get(word)

			for w1 in arrayFinal:
				if mapSet == w1:
					numGotten = arrayFinal.get(mapSet)
					w1.put(numGotten+numero)

			else:
				arrayFinal.append({word:1})		


		return arrayFinal