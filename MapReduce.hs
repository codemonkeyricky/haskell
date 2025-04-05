{-# LANGUAGE DeriveAnyClass #-}

module Main where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad             (forever, when)
import           Control.Monad.Fix         (fix)
import           Data.Aeson                (FromJSON, ToJSON, decode, encode)
import qualified Data.ByteString           as DB
import qualified Data.ByteString.Lazy      as DBL
import           GHC.Generics              (Generic)
import           Network.Socket
import           Network.Socket.ByteString
import           System.IO

data Server = Server
  { address :: String
  , asset   :: [String]
  , version :: Integer
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data Gossip = Gossip
  { servers :: [Server]
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

serialize :: Gossip -> DBL.ByteString
serialize = encode

-- deserialize :: ByteString -> Maybe Gossip
-- deserialize = decode
sampleGossip :: Gossip
sampleGossip =
  Gossip
    { servers =
        [ Server
            {address = "127.0.0.1:8000", asset = ["BTC", "ETH"], version = 1}
        , Server
            {address = "127.0.0.1:8001", asset = ["XRP", "LTC"], version = 2}
        ]
    }

mainLoop :: Socket -> Chan Gossip -> Int -> IO ()
mainLoop sock chan msgNum = do
  conn <- accept sock
  forkIO (runConn conn chan msgNum)
  mainLoop sock chan $! msgNum + 1

-- sendAll :: Socket -> ByteString -> IO ()
-- sendAll sock bs = do
--   sent <- send sock bs
--   if sent < Data.ByteString.length bs
--     then sendAll sock (Data.ByteString.drop sent bs)
--     else return ()
runConn :: (Socket, SockAddr) -> Chan Gossip -> Int -> IO ()
runConn (sock, _) chan msgNum = do
  sendAll sock (DB.concat $ DBL.toChunks $ encode sampleGossip)
  print "done"

--   hdl <- socketToHandle sock ReadWriteMode
--   hSetBuffering hdl NoBuffering
--   hPutStrLn hdl "whatever"
main :: IO ()
main = do
  -- create, bind and listen on socket
  sock <- socket AF_INET Stream defaultProtocol
  bind sock (SockAddrInet 3000 0)
  listen sock 5
  -- create new channel
  chan <- newChan
  -- enter main loop
  mainLoop sock chan 0
  print "hello"
