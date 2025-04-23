{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}

module Main where

import           Common
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
import           System.Random             (StdGen, mkStdGen, randomRIO,
                                            randomRs)
import           Text.Printf

fib :: Integer -> Integer
fib 0 = 0
fib 1 = 1
fib n = do
  fib (n - 1) + fib (n - 2) -- Intentionally inefficient recursive version

-- port / peer
node :: Integer -> Cluster -> IO ()
node my_port cluster = do
  let timerHeartbeat rx = do
        threadDelay 100000
        writeChan rx (Nothing, Heartbeat)
  let exchange_gossip rx peer cluster = do
        -- create socket
        sock <- socket AF_INET Stream defaultProtocol
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
  let jobProcessor q = do
        forever $ do
          (rx, work) <- readChan q
          -- print "job started..."
          -- _ <- return $ busyWork 1000000000000
          let x = fib 35
          print x
          writeChan rx $ Just (CompletedJob True)
  let eventLoop listeningPort rx cluster workers db q =
        forever $ do
          let ring = getRing cluster
          -- print cluster
          (maybe_tx, msg) <- readChan rx
          case msg of
            SubmitJob k -> do
              case maybe_tx of
                Just tx -> do
                  writeChan q (tx, k)
                Nothing -> return ()
            GossipRequest cluster' -> do
              let cluster'' = merge cluster cluster'
              case maybe_tx of
                Just tx -> do
                  let gossip' = (GossipReply cluster'')
                  writeChan tx $ Just gossip'
                Nothing -> return ()
              eventLoop listeningPort rx cluster'' workers db q
            GossipReply cluster' -> do
              let cluster'' = merge cluster cluster'
              -- printf "%d:" listeningPort
              -- print cluster''
              case maybe_tx of
                Just tx -> do
                  -- writing nothing closes the socket
                  writeChan tx Nothing
                Nothing -> return ()
              forkIO $ timerHeartbeat rx
              eventLoop listeningPort rx cluster'' workers db q
            Heartbeat -> do
              let tryPeers cl = do
                    let all_peers =
                          excludeOffline $ excludePort cl listeningPort
                    let all_servers = servers all_peers
                    shuffled <- shuffle all_servers
                    case shuffled of
                      [] -> eventLoop listeningPort rx cl workers db q
                      (k:ks) -> do
                        let p = port k
                        success <- exchange_gossip rx (fromIntegral p) cl
                        if success
                          then do
                            eventLoop listeningPort rx cl workers db q
                          else do
                            let updatedPeers =
                                  Data.List.map
                                    (\s ->
                                       if port s == p
                                         then s
                                                { status = Offline
                                                , version = version s + 1
                                                }
                                         else s)
                                    (servers cl)
                            let cl' = Cluster {servers = updatedPeers}
                            print cl'
                            tryPeers cl'
              tryPeers cluster
  let rxConn sock rx =
        forever $ do
          (conn, _) <- accept sock
          forkIO (rxPacket conn rx)
  let cluster' =
        merge
          cluster
          Cluster
            { servers =
                [ Server
                    { port = my_port
                    , status = Online
                    , tokens = randomIndices my_port 1
                    , version = 1
                    }
                ]
            }
  -- create socket and channel
  rx <- newChan
  sock <- socket AF_INET Stream defaultProtocol
  setSocketOption sock ReuseAddr 1
  setSocketOption sock ReusePort 1
  bind sock (SockAddrInet (fromIntegral my_port) 0)
  listen sock 5
  -- job queue
  q <- newChan
  -- event loop, connection acceptor, timer heartbeat
  _ <- forkIO $ eventLoop my_port rx cluster' [] Data.Map.empty q
  _ <- forkIO $ jobProcessor q
  _ <- forkIO $ rxConn sock rx
  _ <- forkIO $ timerHeartbeat rx
  print "node create complete"

rxEvent ::
     Socket -> Chan (Maybe (Chan (Maybe Message)), Message) -> Message -> IO ()
rxEvent sock tx msg = do
  rx <- newChan
  writeChan tx (Just rx, msg)
  maybe_send <- readChan rx
  case maybe_send of
    Just send -> sendAll sock (DBL.toStrict $ serialize send)
    Nothing   -> close sock
  return ()

rxPacket :: Socket -> Chan (Maybe (Chan (Maybe Message)), Message) -> IO ()
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

dispatchJob :: Int -> IO ()
dispatchJob port = do
  sock <- socket AF_INET Stream defaultProtocol
  connectResult <-
    try $ connect sock (SockAddrInet (fromIntegral port) 0) :: IO
      (Either IOException ())
  case connectResult of
    Left err -> do
      putStrLn $ "Failed to connect to peer: " ++ show err
      close sock
      return ()
    Right _ -> do
      sendAll sock (DBL.toStrict $ serialize (SubmitJob 10))
      print "job dispatched..."
      msg <- recv sock 4096
      if DB.null msg
        then return ()
        else do
          case deserialize (DBL.fromStrict msg) of
            Just evt -> print "job completed!"
            Nothing  -> print "Invalid message!"
      close sock

singleExchange :: Integer -> Message -> IO (Maybe Message)
singleExchange port msg = do
  sock <- socket AF_INET Stream defaultProtocol
  connectResult <-
    try $ connect sock (SockAddrInet (fromIntegral port) 0) :: IO
      (Either IOException ())
  case connectResult of
    Left err -> do
      putStrLn $ "Failed to connect to peer: " ++ show err
      close sock
      return Nothing
    Right _ -> do
      sendAll sock (DBL.toStrict $ serialize $ msg)
      msg <- recv sock 4096
      close sock
      if DB.null msg
        then return Nothing
        else do
          case deserialize (DBL.fromStrict msg) of
            Just evt -> return $ Just evt
            Nothing  -> return Nothing

findServer :: Integer -> Data.Map.Map Integer Integer -> Integer
findServer hash ring =
  case Data.Map.lookupGE hash ring of
    Just (_, port) -> port
    Nothing        -> snd (Data.Map.findMin ring)

main :: IO ()
main = do
  args <- getArgs
  case args of
    [startPortStr, numNodesStr, seedPortStr] -> do
      let startPort = read startPortStr :: Integer
          numNodes = read numNodesStr :: Integer
          seedPort = read seedPortStr :: Integer
      let seed =
            Cluster
              { servers =
                  [ Server
                      { port = seedPort
                      , status = Online
                      , tokens = []
                      , version = 1
                      }
                  ]
              }
      -- forM_ [0 .. numNodes - 1] $ \i -> do
      --   forkIO $ node (startPort + i) seed
      forever $ threadDelay 1000000
    _ -> putStrLn "Usage: ./MapReduce <startPort> <numNodes> <seedPort>"
