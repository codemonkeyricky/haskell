{-# LANGUAGE OverloadedStrings #-}

module WGet where

import           Control.Exception         (SomeException, try)
import           Control.Monad             (when)
import qualified Data.ByteString.Lazy      as L
import           Network.HTTP.Client
import           Network.HTTP.Client.TLS   (tlsManagerSettings)
import           Network.HTTP.Types.Status (statusCode)
import           System.Environment        (getArgs)
import           System.Exit               (exitFailure)

-- | Main entry point
main :: IO ()
main = do
  args <- getArgs
  when (length args /= 1) $ do
    putStrLn "Usage: hwget <URL>"
    exitFailure
  let url = head args
  result <- try (downloadFile url) :: IO (Either SomeException ())
  case result of
    Left err -> do
      putStrLn $ "Error: " ++ show err
      exitFailure
    Right _ -> putStrLn "Download completed successfully."

-- | Download a file from a given URL
downloadFile :: String -> IO ()
downloadFile url = do
    -- Create a manager with TLS support
  manager <- newManager tlsManagerSettings
    -- Parse the URL and create a request
  request <- parseRequest url
    -- Execute the request
  response <- httpLbs request manager
    -- Check if the request was successful (status code 2xx)
  let status = statusCode $ responseStatus response
  if status >= 200 && status < 300
    then do
            -- Extract filename from URL if possible
      let filename =
            case reverse (takeWhile (/= '/') (reverse url)) of
              ""   -> "index.html"
              name -> name
            -- Save the response body to a file
      L.writeFile filename (responseBody response)
      putStrLn $ "Saved to: " ++ filename
    else do
      putStrLn $ "HTTP error: " ++ show status
      exitFailure
