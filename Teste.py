from Mapper import Mapper
from Reducer import Reducer

def main():
	mapper = Mapper()
	reducer = Reducer()
	arrayMap = mapper.map("Esta à Frase, fRASe tomas frase esta unica única")
	arrayMap2 = mapper.map("única Este cena\n. frase única")
	arrayMap3 = mapper.map("à frase à")
	mapFinal = reducer.reduce([arrayMap,[]])

	print("Reduced: ",mapFinal)

main()