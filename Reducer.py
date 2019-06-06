import logging
import locale
from functools import cmp_to_key

locale.setlocale(locale.LC_ALL, 'pt_PT.UTF-8')

class Reducer:
    """docstring for Reducer"""
    def __init__(self):
        logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
        self.logger = logging.getLogger('reducer')

    def reduce(self, array_of_arrays):
        arrayFinal = []
        while(len(array_of_arrays) > 1):
            #Escolher os 2 primeiros enviados
            array1 = array_of_arrays[0]
            array2 = array_of_arrays[1]

            #Retirar os 2 primeiros enviados, fazer o seu processing
            array_of_arrays.remove(array1)
            array_of_arrays.remove(array2)

            #Remover repetidos que estao seguidos
            arraySize = len(array1)-1
            ind = 0

            while ind <= arraySize:
                count = 1
                while(ind<arraySize and array1[ind]==array1[ind+1]):
                    array1.pop(ind+1)
                    count+=1
                    arraySize = len(array1)-1
                arrayFinal.append((array1[ind][0],count))
                ind+=1
            print("ARRAYFINAL: ", arrayFinal)

            array_of_arrays.append(arrayFinal)		
        return sorted(array_of_arrays, key=cmp_to_key(locale.strcoll))
