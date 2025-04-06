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
import           System.Environment
import           System.Exit
import           System.IO

data Server = Server
  { address :: String
  , asset   :: [String]
  , version :: Integer
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data Gossip = Gossip
  { servers :: [Server]
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data Message
  = NewConnection Int
  | ReceivedGossip Int Gossip
  deriving (Show)

serialize :: Gossip -> DBL.ByteString
serialize = encode

deserialize :: DBL.ByteString -> Maybe Gossip
deserialize = decode

sampleGossip :: Gossip
sampleGossip =
  Gossip
    { servers =
        [ Server
            {address = "localhost:3000", asset = ["BTC", "ETH"], version = 1}
        ]
    }

mainLoop :: Socket -> Chan Message -> Int -> IO ()
mainLoop sock chan msgNum = do
  -- main event loop
  _ <-
    forkIO
      $ forever
      $ do
          msg <- readChan chan
          case msg of
            NewConnection id         -> print "test"
            ReceivedGossip id gossip -> print "test"
  -- connection forker
  forever $ do
    conn <- accept sock
    forkIO (connHandler conn chan msgNum)
    mainLoop sock chan $! msgNum + 1

connHandler :: (Socket, SockAddr) -> Chan Message -> Int -> IO ()
connHandler (sock, _) chan msgNum = do
  handle (\(SomeException _) -> return ())
    $ fix
    $ \loop -> do
        msg <- recv sock 4096
        when (DB.null msg) $ return () -- connection terminated
        case deserialize (DBL.fromStrict msg) of
          Just gossip -> do
            writeChan chan (ReceivedGossip msgNum gossip)
          Nothing -> print "Invalid message!"
        loop

main :: IO ()
main = do
--   args <- getArgs
--   port <-
--     case args of
--       [portStr] ->
--         case reads portStr :: [(Int, String)] of
--           [(port, "")] -> return port
--       _ -> do
--         print "require port"
--         exitFailure
--   print port
  -- create, bind and listen on socket
  sock <- socket AF_INET Stream defaultProtocol
  bind sock (SockAddrInet 3000 0)
  listen sock 5
  -- create new channel
  chan <- newChan
  -- enter main loop
  mainLoop sock chan 0
  print "hello"
