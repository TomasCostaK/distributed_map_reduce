from Mapper import Mapper
from Reducer import Reducer

def main():
	mapper = Mapper()
	reducer = Reducer()
	arrayMap = mapper.map("Esta Frase fRASe frase")
	arrayMap2 = mapper.map("Este cena\n. frase")
	mapFinal = reducer.reduce([arrayMap,arrayMap2])
	print("Reduced",mapFinal)
	print("\n")
	mapFinal2 = reducer.reduce([mapFinal,arrayMap2])
	print("Reduced",mapFinal2)
	print("\n")

	print(mapFinal2)

main()