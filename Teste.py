from Mapper import Mapper
from Reducer import Reducer

def main():
	mapper = Mapper()
	reducer = Reducer()
	arrayMap = mapper.map("Esta Frase fRASe frase")
	arrayMap2 = mapper.map("Este cena\n. frase")
	mapFinal = reducer.reduce(arrayMap,arrayMap2)
	print(mapFinal)

main()