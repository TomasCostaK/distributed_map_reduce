from Mapper import Mapper
from Reducer import Reducer

def main():
	mapper = Mapper()
	reducer = Reducer()
	arrayMap = mapper.map("Esta frase tem, numeros numeros 2 e \nwhite. frase")
	mapFinal = reducer.reduce(arrayMap)
	print(mapFinal)

main()