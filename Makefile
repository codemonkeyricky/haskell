all:
	ghc Server.hs -package hashable -package random -threaded
run:
	./Server  +RTS -N4
