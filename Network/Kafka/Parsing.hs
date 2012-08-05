module Kafka.Parsing where
import Data.ByteString.Char8
import Data.Serialize.Get

forceEither :: (Show a) => String -> Either a r -> r
forceEither _ (Right res) = res
forceEither source (Left res) = error $ "error parsing from " ++ source ++ " with: " ++ show res

runGet' :: ByteString -> Get a -> Either String a
runGet' = flip runGet
