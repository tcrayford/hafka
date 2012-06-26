module Kafka.Parsing where
import Data.ByteString.Char8
import Data.Serialize.Get

forceEither :: (Show a) => ByteString -> Either a r -> r
forceEither _ (Right res) = res
forceEither raw (Left res) = error $ "error parsing " ++ show raw ++ "with: " ++ show res

runGet' :: ByteString -> Get a -> Either String a
runGet' = flip runGet
