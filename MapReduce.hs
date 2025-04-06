{-# LANGUAGE DeriveAnyClass #-}

module Main where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad             (forever, void, when)
import           Control.Monad.Fix         (fix)
import           Data.Aeson                (FromJSON, ToJSON, decode, encode)
import qualified Data.ByteString           as DB
import qualified Data.ByteString.Lazy      as DBL
import           Data.Function             (on)
import           Data.List                 (nubBy, sortOn)
import           Data.Ord                  (Down (..))
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
  | GossipType Gossip
  | Ping
  | Pong
  | AddNode
  deriving (Show, Eq, Generic, ToJSON, FromJSON)

merge :: Gossip -> Gossip -> Gossip
merge (Gossip a) (Gossip b) =
  let c = a ++ b
      sorted = sortOn (Down . version) c -- sort by descending version
      merged = nubBy (on (==) address) sorted -- keep first occurence
   in Gossip merged

serialize :: Message -> DBL.ByteString
serialize = encode

deserialize :: DBL.ByteString -> Maybe Message
deserialize = decode

sampleGossip :: Gossip
sampleGossip =
  Gossip
    { servers =
        [ Server
            {address = "localhost:3000", asset = ["BTC", "ETH"], version = 1}
        ]
    }

node :: PortNumber -> Int -> IO ()
node port msgNum = do
  let eventLoop port chan =
        forever $ do
          (sock, msg) <- readChan chan
          case msg of
            NewConnection id -> print "test"
            GossipType gossip -> print "test"
            Ping -> do
              sendAll sock (DBL.toStrict $ serialize Pong)
            AddNode -> do
              void $ forkIO $ node (port + 1) 0
              eventLoop (port + 1) chan
  -- create socket and channel
  chan <- newChan
  sock <- socket AF_INET Stream defaultProtocol
  bind sock (SockAddrInet port 0)
  listen sock 5
  -- main event loop
  _ <- forkIO $ eventLoop port chan
  -- connection forker
  forever $ do
    conn <- accept sock
    forkIO (connHandler conn chan msgNum)

connHandler :: (Socket, SockAddr) -> Chan (Socket, Message) -> Int -> IO ()
connHandler (sock, _) chan msgNum = do
  handle (\(SomeException _) -> return ())
    $ fix
    $ \loop -> do
        msg <- recv sock 4096
        if DB.null msg
          then return ()
          else do
            case deserialize (DBL.fromStrict msg) of
              Just event -> writeChan chan (sock, event)
              Nothing    -> print "Invalid message!"
            loop

main :: IO ()
main = do
  DBL.putStr (serialize (Ping))
  node 3000 0
  print "hello"
