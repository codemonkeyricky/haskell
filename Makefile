all:
	ghc Server.hs -package hashable -package random -threaded
	ghc Client.hs -package hashable -package random -threaded
run:
	./Server 3000 2 3000  +RTS -N4
