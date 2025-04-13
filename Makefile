all:
	ghc MapReduce.hs -package hashable -package random -threaded
run:
	./MapReduce  +RTS -N4
