module Main where

import           Data.List (intercalate)
import           System.IO

-- Transformation functions
-- 1. Normalize: remove newline characters from each line
normalize :: [String] -> [String]
normalize = map (filter (/= '\n'))

-- 2. Tokenize: split each line into tokens delimited by spaces
tokenize :: [String] -> [[String]]
tokenize = map words

-- 3. Replace: replace all "apple" tokens with "banana"
replace :: [[String]] -> [[String]]
replace =
  map
    (map
       (\word ->
          if word == "apple"
            then "banana"
            else word))

-- Combine all transformations
transform :: [String] -> [[String]]
transform = replace . tokenize . normalize

-- Pretty print the result for display
prettyPrint :: [[String]] -> String
prettyPrint = intercalate "\n" . map (intercalate " ")

-- Main program: read input, transform, and print output
main :: IO ()
main = do
  putStrLn "Enter lines of text (press Ctrl-D to end input):"
  input <- getContents
  let output = transform (lines input)
  putStrLn "\nTransformed output:"
  putStrLn (prettyPrint output)
