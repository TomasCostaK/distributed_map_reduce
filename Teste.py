from Mapper import Mapper
from Reducer import Reducer

def main():
	mapper = Mapper()
	reducer = Reducer()
	arrayMap = mapper.map("Esta frase tem, numeros., , numeros 2 e \nwhite. frase")
	arrayMap2 = mapper.map("Este cena, numeros oo \nwho. frase")
	mapFinal = reducer.reduce(arrayMap,arrayMap2)
	print(mapFinal)

main()