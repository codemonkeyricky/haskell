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
import           Data.List
import           Data.Ord                  (Down (..))
import           GHC.Generics              (Generic)
import           Network.Socket
import           Network.Socket.ByteString
import           System.Environment
import           System.Exit
import           System.IO

data PersistState = PersistState
  { currentTerm :: Integer
  , votedFor    :: Maybe String
  , log         :: [String]
  }

data VolatileState = VolatileState
  { commitIndex :: Integer
  , lastApplied :: Integer
  -- leader only
  , nextIndex   :: [Integer]
  , matchIndex  :: [Integer]
  }

data Server = Server
  { port    :: Integer
  , version :: Integer
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data Cluster = Cluster
  { servers :: [Server]
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data AppendEntriesReq = AppendEntriesReq
  { term_ae      :: Integer
  , leaderId     :: Integer
  , prevLogIndex :: Integer
  , prevLogTerm  :: Integer
  , entries      :: [String]
  , leaderCommit :: Integer
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data AppendEntriesReply = AppendEntriesReply
  { term_ae' :: Integer
  , success  :: Bool
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data RequestVoteReq = RequestVoteReq
  { term_rv       :: Integer
  , candidateId   :: Integer
  , lasteLogIndex :: Integer
  , lastLogTerm   :: Integer
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data RequestVoteReply = RequestVoteReply
  { term_rv'    :: Integer
  , voteGranted :: Bool
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data Message
  = NewConnection Int
  | MAppendEntriesRequest AppendEntriesReq
  | MAppendEntriesReply AppendEntriesReply
  | MRequestVoteReq RequestVoteReq
  | MRequestVoteReply RequestVoteReply
  | GossipRequest Cluster
  | GossipReply Cluster
  | Heartbeat
  | Ping
  | Pong
  | AddNode
  | RegisterWorker Integer
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
  let eventLoop port rx cluster workers =
        forever $ do
          (maybe_tx, msg) <- readChan rx
          case msg of
            NewConnection id -> print "test"
            GossipRequest cluster' -> do
              let cluster'' = merge cluster cluster'
              -- print cluster''
              -- assert False $ return ()
              eventLoop port rx cluster'' workers
            Heartbeat -> do
              print cluster
              let peers = filterByPort cluster port
              -- print peers
              case servers peers of
                (Server {port = p}:_) ->
                  void $ exchange_gossip rx (fromIntegral p) cluster
                _ -> print "empty"
            RegisterWorker worker -> do
              eventLoop port rx cluster (nub $ sort $ worker : workers)
            Ping ->
              case maybe_tx of
                Just tx -> writeChan tx Pong
                Nothing -> return ()
                  -- exchange_gossip rx (fromIntegral peer) cluster
              -- print "heartbeat"
  let connAcceptor sock rx =
        forever $ do
          (conn, _) <- accept sock
          forkIO (rxPacket conn rx)
  let timerHeartbeat rx =
        forever $ do
          threadDelay 1000000
          writeChan rx (Nothing, Heartbeat)
  let cluster' =
        merge cluster Cluster {servers = [Server {port = my_port, version = 1}]}
  -- create socket and channel
  rx <- newChan
  sock <- socket AF_INET Stream defaultProtocol
  bind sock (SockAddrInet (fromIntegral my_port) 0)
  listen sock 5
  -- event loop, connection acceptor, timer heartbeat
  _ <- forkIO $ eventLoop my_port rx cluster' []
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
