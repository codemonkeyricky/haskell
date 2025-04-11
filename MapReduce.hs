{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}

module Main where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad             (forM_, forever, void, when)
import           Control.Monad.Fix         (fix)
import           Data.Aeson                (FromJSON, ToJSON, decode, encode)
import qualified Data.ByteString           as DB
import qualified Data.ByteString.Lazy      as DBL
import           Data.Function             (on)
import           Data.Hashable
import           Data.List
import           Data.Map
import           Data.Ord                  (Down (..))
import           GHC.Generics              (Generic)
import           Network.Socket
import           Network.Socket.ByteString
import           System.Environment
import           System.Exit
import           System.IO
import           System.Random             (randomRIO)
import           Text.Printf

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

data Status
  = Joining
  | Online
  | Leaving
  | Offline
  deriving (Generic, Enum, Eq, Show, ToJSON, FromJSON)

data Server = Server
  { port    :: Integer
  , status  :: Status
  , tokens  :: [Integer]
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

data ReadOp = ReadOp
  { r_key :: String
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data ReadOp' = ReadOp'
  { r'_status :: Bool
  , r'_value  :: Maybe String
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data WriteOp = WriteOp
  { w_key   :: String
  , w_value :: String
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data WriteOp' = WriteOp'
  { w'_status :: Bool
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data Message
  = GossipRequest Cluster
  | GossipReply Cluster
  | Heartbeat
  | DispatchJob Integer
  deriving (Show, Eq, Generic, ToJSON, FromJSON)

merge :: Cluster -> Cluster -> Cluster
merge (Cluster a) (Cluster b) =
  let c = a ++ b
      sorted = sortOn (Down . version) c -- sort by descending version
      merged = nubBy (on (==) port) sorted -- keep first occurence
      sorted' = sortOn port merged
   in Cluster sorted'

excludePort :: Cluster -> Integer -> Cluster
excludePort (Cluster servers) to_remove =
  Cluster $ Data.List.filter (\server -> port server /= to_remove) servers

serialize :: Message -> DBL.ByteString
serialize = encode

deserialize :: DBL.ByteString -> Maybe Message
deserialize = decode

getRing :: Cluster -> Data.Map.Map Integer Integer
getRing (Cluster a) =
  Data.Map.fromList
    $ concatMap (\server -> [(token, port server) | token <- tokens server]) a

-- port / peer
node :: Integer -> Cluster -> IO ()
node my_port cluster = do
  let exchange_gossip rx peer cluster = do
        -- create socket
        sock <- socket AF_INET Stream defaultProtocol
        setSocketOption sock ReuseAddr 1
        setSocketOption sock ReusePort 1
        -- connect with exception handling
        connectResult <-
          try $ connect sock (SockAddrInet peer 0) :: IO (Either IOException ())
        case connectResult of
          Left err -> do
            putStrLn $ "Failed to connect to peer: " ++ show err
            close sock
            return False
          Right _ -> do
            sendAll sock (DBL.toStrict $ serialize (GossipRequest cluster))
            forkIO (rxPacket sock rx)
            return True
  let eventLoop listeningPort rx cluster workers db q =
        forever $ do
          let ring = getRing cluster
          (maybe_tx, msg) <- readChan rx
          case msg of
            DispatchJob k -> do
              let q' = q ++ [k]
              print "x"
            GossipRequest cluster' -> do
              let cluster'' = merge cluster cluster'
              case maybe_tx of
                Just tx -> do
                  let gossip' = (GossipReply cluster'')
                  writeChan tx gossip'
                Nothing -> return ()
              eventLoop listeningPort rx cluster'' workers db q
            GossipReply cluster' -> do
              let cluster'' = merge cluster cluster'
              printf "%d:" listeningPort
              print cluster''
              eventLoop listeningPort rx cluster'' workers db q
            Heartbeat -> do
              print "heartbeat"
              let peers = excludePort cluster listeningPort
              let list = servers peers
              case list of
                [] -> print "empty"
                ll -> do
                  k <- randomRIO (0, length ll - 1)
                  let p = port $ ll !! k
                  success <- exchange_gossip rx (fromIntegral p) cluster
                  when (not success) $ do
                    -- remove node failed to connect
                    let cluster' = excludePort cluster p
                    eventLoop listeningPort rx cluster' workers db q
  let rxConn sock rx =
        forever $ do
          (conn, _) <- accept sock
          forkIO (rxPacket conn rx)
  let timerHeartbeat rx =
        forever $ do
          threadDelay 100000
          writeChan rx (Nothing, Heartbeat)
  let cluster' =
        merge
          cluster
          Cluster
            { servers =
                [ Server
                    { port = my_port
                    , status = Online
                    , tokens = [1, 2]
                    , version = 1
                    }
                ]
            }
  -- create socket and channel
  rx <- newChan
  sock <- socket AF_INET Stream defaultProtocol
  bind sock (SockAddrInet (fromIntegral my_port) 0)
  listen sock 5
  -- event loop, connection acceptor, timer heartbeat
  _ <- forkIO $ eventLoop my_port rx cluster' [] Data.Map.empty []
  _ <- forkIO $ rxConn sock rx
  _ <- forkIO $ timerHeartbeat rx
  print "node create complete"

rxEvent :: Socket -> Chan (Maybe (Chan Message), Message) -> Message -> IO ()
rxEvent sock tx msg = do
  rx <- newChan
  writeChan tx (Just rx, msg)
  to_send <- readChan rx
  sendAll sock (DBL.toStrict $ serialize to_send)
  close sock

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
        close sock

main :: IO ()
main = do
  let we = "whatever"
  let h = hash we
  print h
  -- seed
  node 3000 Cluster {servers = []}
  forM_ [1 .. 50] $ \i -> do
    forkIO
      $ node
          (3000 + i)
          Cluster
            { servers =
                [ Server
                    {port = 3000, status = Online, tokens = [], version = 0}
                ]
            }
  forever $ threadDelay maxBound
  print "hello"
