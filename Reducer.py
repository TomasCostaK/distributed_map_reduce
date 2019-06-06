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
        #Caso so receba um map para dar reduce, vamos ter que enviar juntamento com uma lista vazia pq so aceitamos 2 listas ou mais
        while(len(array_of_arrays) > 1):
            #Criar sempre o arrayFinal que vai estar reduced
            arrayFinal = []
            
            #Criar 2 arrays temporarios para nao manipularmos arrays enquanto o iteramos
            arrayTemp1 = []
            arrayTemp2 = []

            #Escolher os 2 primeiros enviados
            array1 = array_of_arrays[0]
            array2 = array_of_arrays[1]

            #Retirar os 2 primeiros enviados, fazer o seu processing
            array_of_arrays.remove(array1)
            array_of_arrays.remove(array2)

            #Remover repetidos que estao seguidos do array1
            arraySize = len(array1)-1
            ind = 0
            while ind <= arraySize:
                count = array1[ind][1]
                while(ind<arraySize and array1[ind]==array1[ind+1]):
                    count+=1
                    ind+=1;
                arrayTemp1.append((array1[ind][0],count))
                ind+=1

            #Igualar o temporario ao suposto array1
            array1 = arrayTemp1

            #Remover repetidos que estao seguidos do array2
            arraySize = len(array2)-1
            ind = 0
            while ind <= arraySize:
                count = array2[ind][1]
                while(ind<arraySize and array2[ind]==array2[ind+1]):
                    count+=1
                    ind+=1;
                arrayTemp2.append((array2[ind][0],count))
                ind+=1

            #Igualar o temporario ao suposto array2
            array2 = arrayTemp2

            #Check which list is smallest (This code doesnt look very good, think abut it later)
            n = len(array1)
            m = len(array2)

            if n>=m:
                smallArray = array2
                bigArray = array1
            else:
                smallArray = array1
                bigArray = array2

            #Go through both arrays to join them into one
            for word,count in smallArray:
                #Este binary search vai retornar
                count2 = self.binarySearch(bigArray, 0 , len(bigArray)-1, word)
                countFinal = count2 + count

                arrayFinal.append((word,countFinal))

            #Append the rest to the array
            arrayFinal.extend(bigArray)

            #Sort them with mergesort with locale PT
            self.mergeSort(arrayFinal) 

            array_of_arrays.append(arrayFinal)	

        #Indice 0 para nao retornar como array de arrays    
        return array_of_arrays[0]

    def binarySearch (self, arr, left, right, x): 
        # Check base case 
        if right >= left: 
            mid = left + (right - left)//2
            # If element is present at the middle itself 
            if arr[mid][0] == x: 
                ret = arr[mid][1]
                arr.pop(mid)
                return ret

            # If element is smaller than mid, then it  
            # can only be present in left subarray 
            elif locale.strcoll(arr[mid][0], x) > 0: 
                return self.binarySearch(arr, left, mid-1, x) 
    
            # Else the element can only be present  
            # in right subarray 
            else: 
                return self.binarySearch(arr, mid + 1, right, x) 
        else: 
            # Element is not present in the array 
            return 0

    def mergeSort(self,arr): 
        if len(arr) >1: 
            mid = len(arr)//2 #Finding the mid of the array 
            L = arr[:mid] # Dividing the array elements  
            R = arr[mid:] # into 2 halves 
    
            self.mergeSort(L) # Sorting the first half 
            self.mergeSort(R) # Sorting the second half 
    
            i = j = k = 0
            
            # Copy data to temp arrays L[] and R[] 
            while i < len(L) and j < len(R): 
                if locale.strcoll(L[i][0],R[j][0]) <0: 
                    arr[k] = L[i] 
                    i+=1
                else: 
                    arr[k] = R[j] 
                    j+=1
                k+=1
            
            # Checking if any element was left 
            while i < len(L): 
                arr[k] = L[i] 
                i+=1
                k+=1
            
            while j < len(R): 
                arr[k] = R[j] 
                j+=1
                k+=1
