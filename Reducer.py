import logging

class Reducer:
	"""docstring for Reducer"""
	def __init__(self):
		logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
		self.logger = logging.getLogger('reducer')

	def reduce(self,arrayMapper):
		wordMap = {}
		for word in arrayMapper:
			if word not in wordMap:
				wordMap[word] = 1
			else:
				value = wordMap.get(word,"")
				value+=1
				wordMap[word] = value
		return wordMap