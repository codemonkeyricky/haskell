{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}

module Common where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Control.Monad             (forM_, forever, void, when)
import           Control.Monad.Fix         (fix)
import           Data.Aeson                (FromJSON, ToJSON, decode, encode)
import           Data.Array.IO
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
import           System.Random
import           System.Random             (StdGen, mkStdGen, randomRIO,
                                            randomRs)
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
  | SubmitJob Integer
  | CompletedJob Bool
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

excludeOffline :: Cluster -> Cluster
excludeOffline (Cluster servers) =
  Cluster $ Data.List.filter (\server -> status server /= Offline) servers

serialize :: Message -> DBL.ByteString
serialize = encode

deserialize :: DBL.ByteString -> Maybe Message
deserialize = decode

getRing :: Cluster -> Data.Map.Map Integer Integer
getRing (Cluster a) =
  Data.Map.fromList
    $ concatMap (\server -> [(token, port server) | token <- tokens server]) a
    -- Helper to generate n distinct random indices

randomIndices :: Integer -> Int -> [Integer] -- Takes a seed as input
randomIndices seed n =
  Data.List.take n $ nub $ randomRs (0, 65536) (mkStdGen (fromInteger seed))

-- | Randomly shuffle a list
--   /O(N)/
shuffle :: [a] -> IO [a]
shuffle xs = do
  ar <- newArray n xs
  forM [1 .. n] $ \i -> do
    j <- randomRIO (i, n)
    vi <- readArray ar i
    vj <- readArray ar j
    writeArray ar j vi
    return vj
  where
    n = length xs
    newArray :: Int -> [a] -> IO (IOArray Int a)
    newArray n xs = newListArray (1, n) xs
