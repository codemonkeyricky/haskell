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
  | GossipRequest Cluster
  | GossipReply Cluster
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
node :: PortNumber -> String -> IO ()
node port peer = do
  let eventLoop port rx =
        forever $ do
          (tx, msg) <- readChan rx
          case msg of
            NewConnection id      -> print "test"
            GossipRequest cluster -> print "test"
            Ping                  -> writeChan tx Pong
  let connAcceptor sock rx =
        forever $ do
          conn <- accept sock
          forkIO (rxPacket conn rx)
  -- create socket and channel
  rx <- newChan
  sock <- socket AF_INET Stream defaultProtocol
  bind sock (SockAddrInet port 0)
  listen sock 5
  -- Define cluster with just me
  let cluster =
        Cluster
          {servers = [Server {address = "localhost" ++ show port, version = 1}]}
  -- main event loop
  _ <- forkIO $ eventLoop port rx
  -- connection forker
  _ <- forkIO $ connAcceptor sock rx
  print "node create complete"

rxEvent :: Socket -> Chan (Chan Message, Message) -> Message -> IO ()
rxEvent sock tx msg = do
  rx <- newChan
  writeChan tx (rx, msg)
  to_send <- readChan rx
  sendAll sock (DBL.toStrict $ serialize to_send)

rxPacket :: (Socket, SockAddr) -> Chan (Chan Message, Message) -> IO ()
rxPacket (sock, _) tx = do
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
  node 3000 ""
  node 3001 "localhost:3000"
  print "hello"
