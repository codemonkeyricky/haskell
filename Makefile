all:
	ghc Server.hs -package hashable -package random -threaded
	ghc Client.hs -package hashable -package random -threaded
run:
	./Server 3000 1 3000 &
	./Server 3001 1 3000 & 
	./Server 3002 1 3000 &
	./Server 3003 1 3000 &
