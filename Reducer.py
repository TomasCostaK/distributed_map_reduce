import logging

class Reducer:
    """docstring for Reducer"""
    def __init__(self):
        logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
        self.logger = logging.getLogger('reducer')

    def reduce(self, array_of_arrays):
        arrayFinal = []
        while(len(array_of_arrays) > 1):
            array1 = array_of_arrays[0]
            print("ARRAYS: ", array_of_arrays)
            array2 = array_of_arrays[1]

            array_of_arrays.remove(array1)
            array_of_arrays.remove(array2)

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

            array_of_arrays.append(arrayFinal)		
        return array_of_arrays
