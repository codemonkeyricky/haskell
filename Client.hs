{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}

module Main where

import           Common
import           Control.Concurrent
import           Control.Exception
import           Control.Monad             (forM_, forever, void, when)
import           Control.Monad.Fix         (fix)
import           Control.Monad.IO.Class    (liftIO)
import           Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT)
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

fib :: Integer -> Integer
fib 0 = 0
fib 1 = 1
fib n = do
    -- print "x"
  fib (n - 1) + fib (n - 2) -- Intentionally inefficient recursive version

-- port / peer
node :: Integer -> Cluster -> IO ()
node my_port cluster = do
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
          let x = fib 33
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
              eventLoop listeningPort rx cluster'' workers db q
            Heartbeat -> do
              -- print "heartbeat"
              let peers = excludePort cluster listeningPort
              let list = servers peers
              case list of
                [] -> pure ()
                ll -> do
                  k <- randomRIO (0, length ll - 1)
                  let p = port $ ll !! k
                  success <- exchange_gossip rx (fromIntegral p) cluster
                  when (not success) $ do
                    -- remove node failed to connect
                    let cluster' = excludePort cluster p
                    eventLoop listeningPort rx cluster' workers db q
            -- CompletedJob msg -> do
            --   case maybe_tx of
            --     Just tx -> do
            --     Nothing -> return ()
                  -- forkIO $ txEvent tx msg
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
                    , tokens = randomIndices my_port 3
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

-- connectToPeer :: Integer -> Maybe Sock
connectToPeer :: Integer -> MaybeT IO Socket
connectToPeer port = do
  MaybeT $ do
    sock <- socket AF_INET Stream defaultProtocol
    rv <-
      try $ connect sock (SockAddrInet (fromIntegral port) 0) :: IO
        (Either IOException ())
    case rv of
      Right _ -> return (Just sock)
      Left _ -> do
        close sock
        return Nothing

dispatch :: Socket -> Message -> MaybeT IO Message
dispatch sock tx = do
  MaybeT $ do
    sendAll sock (DBL.toStrict $ (serialize tx))
    msg <- recv sock 4096
    if DB.null msg
      then return Nothing
      else do
        close sock
        case deserialize (DBL.fromStrict msg) of
          Just evt -> return (Just evt)
          Nothing  -> return Nothing

-- dispatch :: Sock -> ByteString -> Maybe ByteString
singleExchange :: Integer -> Message -> MaybeT IO (Message)
singleExchange port job = do
  sock <- connectToPeer port
  msg <- dispatch sock job
  return msg

findServer :: Integer -> Data.Map.Map Integer Integer -> Integer
findServer hash ring =
  case Data.Map.lookupGE hash ring of
    Just (_, port) -> port
    Nothing        -> snd (Data.Map.findMin ring)

getCluster :: Integer -> Cluster -> MaybeT IO Cluster
getCluster port seed = do
  msg <- singleExchange port (GossipRequest seed)
  case msg of
    GossipReply cluster -> return cluster
    _                   -> MaybeT $ return Nothing

main :: IO ()
main = do
  args <- getArgs
  case args of
    [startPortStr, jobCountStr] -> do
      let startPort = read startPortStr :: Integer
          jobCount = read jobCountStr :: Integer
      let seed =
            Cluster
              { servers =
                  [ Server
                      { port = startPort
                      , status = Online
                      , tokens = []
                      , version = 0
                      }
                  ]
              }
      void
        $ runMaybeT
        $ do
            cluster <- getCluster startPort seed
            let ring = getRing . excludeOffline $ cluster
            completionSignal <- liftIO newEmptyMVar
            liftIO
              $ forM_ [1 .. jobCount]
              $ \i -> do
                  forkIO $ do
                    let hh = (hash (show i)) `mod` 65536
                    print hh
                    let k = findServer (toInteger hh) ring
                    let kk = show k
                    print kk
                    void
                      $ runMaybeT
                      $ singleExchange (fromIntegral k)
                      $ SubmitJob 10
                    liftIO $ putMVar completionSignal () -- Signal completion
            liftIO $ forM_ [1 .. jobCount] $ \_ -> takeMVar completionSignal
            return ()
    _ -> putStrLn "Usage: ./MapReduce <peerPort> <jobCount>"
