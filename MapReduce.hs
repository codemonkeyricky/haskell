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
  { port    :: Integer
  , version :: Integer
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data Cluster = Cluster
  { servers :: [Server]
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data Message
  = NewConnection Int
  | GossipRequest Cluster
  | GossipReply Cluster
  | Heartbeat
  | Ping
  | Pong
  | AddNode
  deriving (Show, Eq, Generic, ToJSON, FromJSON)

merge :: Cluster -> Cluster -> Cluster
merge (Cluster a) (Cluster b) =
  let c = a ++ b
      sorted = sortOn (Down . version) c -- sort by descending version
      merged = nubBy (on (==) port) sorted -- keep first occurence
   in Cluster merged

filterByPort :: Cluster -> Integer -> Cluster
filterByPort (Cluster servers) to_remove =
  Cluster $ filter (\server -> port server /= to_remove) servers

serialize :: Message -> DBL.ByteString
serialize = encode

deserialize :: DBL.ByteString -> Maybe Message
deserialize = decode

-- port / peer
node :: Integer -> Cluster -> IO ()
node my_port cluster = do
  let exchange_gossip rx peer cluster = do
        -- create socket
        sock <- socket AF_INET Stream defaultProtocol
        connect sock (SockAddrInet peer 0)
        -- send
        sendAll sock (DBL.toStrict $ serialize (GossipRequest cluster))
        -- receive
        forkIO (rxPacket sock rx)
  let eventLoop port rx cluster =
        forever $ do
          (maybe_tx, msg) <- readChan rx
          case msg of
            NewConnection id -> print "test"
            GossipRequest cluster -> print "GossipRequest!"
            Heartbeat -> do
              let peers = filterByPort cluster port
              print peers
              case servers peers of
                (Server {port = p}:_) ->
                  void $ exchange_gossip rx (fromIntegral p) cluster
                _ -> print "empty"
                  -- exchange_gossip rx (fromIntegral peer) cluster
              print "heartbeat"
            Ping ->
              case maybe_tx of
                Just tx -> writeChan tx Pong
                Nothing -> return ()
  let connAcceptor sock rx =
        forever $ do
          (conn, _) <- accept sock
          forkIO (rxPacket conn rx)
  let timerHeartbeat rx =
        forever $ do
          threadDelay 1000000
          writeChan rx (Nothing, Heartbeat)
  -- create socket and channel
  rx <- newChan
  sock <- socket AF_INET Stream defaultProtocol
  bind sock (SockAddrInet (fromIntegral my_port) 0)
  listen sock 5
  -- Define cluster with just me
  let cluster' =
        merge cluster Cluster {servers = [Server {port = my_port, version = 1}]}
  -- main event loop
  _ <- forkIO $ eventLoop my_port rx cluster'
  -- connection forker
  _ <- forkIO $ connAcceptor sock rx
  _ <- forkIO $ timerHeartbeat rx
  print "node create complete"

rxEvent :: Socket -> Chan (Maybe (Chan Message), Message) -> Message -> IO ()
rxEvent sock tx msg = do
  rx <- newChan
  writeChan tx (Just rx, msg)
  to_send <- readChan rx
  sendAll sock (DBL.toStrict $ serialize to_send)

rxPacket :: Socket -> Chan (Maybe (Chan Message), Message) -> IO ()
rxPacket sock tx = do
  handle (\(SomeException _) -> return ())
    $ fix
    $ \loop -> do
        msg <- recv sock 4096
        if DB.null msg
          then return ()
          else do
            case deserialize (DBL.fromStrict msg) of
              Just evt -> void $ forkIO (rxEvent sock tx evt)
              Nothing  -> print "Invalid message!"
            loop

main :: IO ()
main = do
  DBL.putStr (serialize (Ping))
  node 3000 Cluster {servers = []}
  node 3001 Cluster {servers = [Server {port = 3000, version = 1}]}
  forever $ threadDelay maxBound
  print "hello"
