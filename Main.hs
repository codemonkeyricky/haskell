module Main where

import           Control.Monad.Fix (fix)

-- import           Control.Concurrent
import           Network.Socket
import           System.IO

main :: IO ()
main = do
  sock <- socket AF_INET Stream defaultProtocol
  setSocketOption sock ReuseAddr 1
  bind sock (SockAddrInet 3000 0)
  listen sock 5
  (sock2, _) <- accept sock
  -- create handle
  handle <- socketToHandle sock2 ReadWriteMode
  hSetBuffering handle NoBuffering
  hPutStrLn handle "Hi, what's your name?"
  -- wait for a line
  fix $ \loop -> do
    line <- fmap init (hGetLine handle)
    print "received something!"
    print line
    >> loop
  print "hello"
