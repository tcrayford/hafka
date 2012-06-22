{-# LANGUAGE OverloadedStrings #-}
module Kafka.Consumer where
import Kafka.Types
import Network
import Data.ByteString.Char8(ByteString)
import qualified Data.ByteString.Char8 as B
import System.IO
import Data.Serialize.Put
import Data.Serialize.Get

data ConsumerSettings = ConsumerSettings Topic Partition

consumeFirst :: ConsumerSettings -> IO Message
consumeFirst a = do
  h <- connectTo "localhost" $ PortNumber 9092
  B.hPut h $ consumeRequest a
  hFlush h
  result <- readDataResponse h
  return . Prelude.last $ parseMessageSet result

consumeRequest ::  ConsumerSettings -> ByteString
consumeRequest a = runPut $ do
  encodeRequestSize a
  encodeRequest a

encodeRequestSize :: ConsumerSettings -> Put
encodeRequestSize (ConsumerSettings (Topic topic) _) = putWord32be $ fromIntegral requestSize
  where requestSize = 2 + 2 + B.length topic + 4 + 8 + 4

encodeRequest ::  ConsumerSettings -> Put
encodeRequest a = do
  putRequestType
  putTopic a
  putPartition a
  putOffset
  putMaxSize

fetchRequestType :: Int
fetchRequestType = 1

putRequestType :: Put
putRequestType = putWord16be $ fromIntegral fetchRequestType

putTopic ::  ConsumerSettings -> Put
putTopic (ConsumerSettings (Topic t) _)  = do
  putWord16be . fromIntegral $ B.length t
  putByteString t

putPartition ::  ConsumerSettings -> Put
putPartition (ConsumerSettings _ (Partition p)) = putWord32be $ fromIntegral p

putOffset :: Put
putOffset = putWord64be 0

putMaxSize :: Put
putMaxSize = putWord32be 1048576 -- 1 MB

readDataResponse :: Handle -> IO ByteString
readDataResponse h = do
  rawLength <- B.hGet h 4
  let (Right dataLength) = runGet getDataLength rawLength
  rawMessageSet <- B.hGet h dataLength
  return $ B.drop 2 rawMessageSet

getDataLength :: Get Int
getDataLength = do
  raw <- getWord32be
  return $ fromIntegral raw

parseMessageSet :: ByteString -> [Message]
parseMessageSet a = parseMessageSet' a [] 0 startingLength
  where startingLength = B.length a - 4

parseMessageSet' :: ByteString -> [Message] -> Int -> Int -> [Message]
parseMessageSet' a messages processed totalLength
  | processed <= totalLength = parseMessageSet' a (messages ++ [parsed]) (processed + 4 + messageSize) totalLength
  | otherwise = messages
  where messageSize = getMessageSize processed a
        parsed = parseMessage $ bSplice a processed (messageSize + 4)

getMessageSize :: Int -> ByteString -> Int
getMessageSize processed raw = fromIntegral $ forceEither $ runGet' raw $ do
                                                skip processed
                                                getWord32be

bSplice :: ByteString -> Int -> Int -> ByteString
bSplice a start end = B.take end (B.drop start a)

forceEither :: (Show a) => Either a r -> r
forceEither (Right res) = res
forceEither (Left res) = error $ show res

runGet' ::  ByteString -> Get a -> Either String a
runGet' = flip runGet

parseMessage :: ByteString -> Message
parseMessage raw = Message $ forceEither $ runGet' raw $ do
  size <- getWord32be
  _ <- getWord8
  _ <- getWord32be
  getByteString $ (fromIntegral size :: Int) - 5
