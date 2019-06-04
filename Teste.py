from Mapper import Mapper
from Reducer import Reducer

def main():
	mapper = Mapper()
	reducer = Reducer()
	arrayMap = mapper.map("Esta Frase fRASe frase esta cenas")
	arrayMap2 = mapper.map("Este cena\n. frase")
	arrayMap3 = mapper.map("Esta\n comida!")
	mapFinal = reducer.reduce([arrayMap,arrayMap2,arrayMap3])

	print("Reduced: ",mapFinal)

main()