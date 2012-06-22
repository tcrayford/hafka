{-# LANGUAGE OverloadedStrings #-}
module Consumer where
import Network
import Data.ByteString.Char8
import qualified Data.ByteString.Char8 as B
import System.IO
import qualified Data.ByteString.Char8 as B
import Data.Serialize.Put
import Data.Digest.CRC32
import Data.Serialize.Get

data ConsumerSettings = ConsumerSettings ByteString Int

consumeFirst :: ConsumerSettings -> IO ByteString
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
encodeRequestSize (ConsumerSettings topic _) = putWord32be $ fromIntegral requestSize
  where requestSize = 2 + 2 + (B.length topic) + 4 + 8 + 4

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
putTopic (ConsumerSettings t _)  = do
  putWord16be . fromIntegral $ B.length t
  putByteString t

putPartition ::  ConsumerSettings -> Put
putPartition (ConsumerSettings _ p) = putWord32be $ fromIntegral p

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
  
parseMessageSet :: ByteString -> [ByteString]
parseMessageSet a = parseMessageSet' a [] 0 startingLength
  where startingLength = B.length a - 4 

parseMessageSet' :: ByteString -> [ByteString] -> Int -> Int -> [ByteString]
parseMessageSet' a messages processed length
  | processed <= length = parseMessageSet' a (messages ++ [parsed]) (processed + 4 + messageSize) length
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

runGet' = flip runGet

parseMessage :: ByteString -> ByteString
parseMessage raw = forceEither $ runGet' raw $ do
  size <- getWord32be
  magic <- getWord8
  checksum <- getWord32be
  payload <- getByteString $ ((fromIntegral size) :: Int) - 5
  return payload
