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
  , version :: Integer
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data Cluster = Cluster
  { servers :: [Server]
  } deriving (Show, Eq, Generic, ToJSON, FromJSON)

data Message
  = NewConnection Int
  | Gossip Cluster
  | Ping
  | Pong
  | AddNode
  deriving (Show, Eq, Generic, ToJSON, FromJSON)

merge :: Cluster -> Cluster -> Cluster
merge (Cluster a) (Cluster b) =
  let c = a ++ b
      sorted = sortOn (Down . version) c -- sort by descending version
      merged = nubBy (on (==) address) sorted -- keep first occurence
   in Cluster merged

serialize :: Message -> DBL.ByteString
serialize = encode

deserialize :: DBL.ByteString -> Maybe Message
deserialize = decode

-- port / peer
node :: PortNumber -> IO ()
node port = do
  let eventLoop port chan =
        forever $ do
          (sock, msg) <- readChan chan
          case msg of
            NewConnection id -> print "test"
            Gossip cluster -> print "test"
            Ping -> do
              sendAll sock (DBL.toStrict $ serialize Pong)
            AddNode -> do
              void $ forkIO $ node (port + 1)
              eventLoop (port + 1) chan
  -- create socket and channel
  chan <- newChan
  sock <- socket AF_INET Stream defaultProtocol
  bind sock (SockAddrInet port 0)
  listen sock 5
  -- Define cluster with just me
  let cluster =
        Cluster
          {servers = [Server {address = "localhost" ++ show port, version = 1}]}
  -- main event loop
  _ <- forkIO $ eventLoop port chan
  -- connection forker
  forever $ do
    conn <- accept sock
    forkIO (connHandler conn chan)

connHandler :: (Socket, SockAddr) -> Chan (Socket, Message) -> IO ()
connHandler (sock, _) chan = do
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
  node 3000
  print "hello"
