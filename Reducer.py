import logging

class Reducer:
	"""docstring for Reducer"""
	def __init__(self):
<<<<<<< HEAD
            logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
            self.logger = logging.getLogger('reducer')

	def reduce(self, array_of_arrays):
            array1 = array_of_arrays[0]
            print(array1)
            array2 = array_of_arrays[1]
            print(array2)
            arrayFinal = []
            # for tup in array1:
            #     notThere = True
            #     word = tup[0]
            #     value = tup[1]
            #     for tup2 in arrayFinal:
            #         if word==tup2[0]:
            #             value=tup2[1]
            #             arrayFinal.remove(tup2)
            #             arrayFinal.append((word,value+1))
            #             notThere= False
            #             break
            #     if notThere:
            #         arrayFinal.append(tup)

            # for tup in array2:
            #     notThere = True
            #     word = tup[0]
            #     value = tup[1]
            #     for tup2 in arrayFinal:
            #         if word==tup2[0]:
            #             value=tup2[1]
            #             arrayFinal.remove(tup2)
            #             arrayFinal.append((word,value+1))
            #             notThere = False
            #             break
            #     if notThere:
            #         arrayFinal.append(tup)
       	    return arrayFinal
=======
		logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
		self.logger = logging.getLogger('reducer')

	def reduce(self,array1,array2):
		arrayFinal = []
		for tup in array1:
			notThere = True
			word = tup[0]
			value = tup[1]
			for tup2 in arrayFinal:
				if word==tup2[0]:
					value=tup2[1]
					arrayFinal.remove(tup2)
					arrayFinal.append((word,value+1))
					notThere= False
					break
			if notThere:
				arrayFinal.append(tup)

		for tup in array2:
			notThere = True
			word = tup[0]
			value = tup[1]
			for tup2 in arrayFinal:
				if word==tup2[0]:
					value=tup2[1]
					arrayFinal.remove(tup2)
					arrayFinal.append((word,value+1))
					notThere = False
					break
			if notThere:
				arrayFinal.append(tup)

		# for mapSet in array1:
		# 	inThere = False
		# 	word = next(iter(mapSet))
		# 	numero = mapSet.get(word)
		# 	print(word, end=' ')
		# 	print(numero)

		# 	if mapSet in arrayFinal:
		# 		print("Im in")

		# 	else:
		# 		arrayFinal.append((word,1))	
		return arrayFinal
>>>>>>> 3c896b0d83723792434b6ff4606cdc0c253641b4
