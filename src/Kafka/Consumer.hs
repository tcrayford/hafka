{-# LANGUAGE OverloadedStrings #-}
module Kafka.Consumer where
import Control.Concurrent(threadDelay)
import Data.ByteString.Char8(ByteString)
import Data.Serialize.Get
import Data.Serialize.Put
import Kafka.Consumer.ByteReader
import Kafka.Parsing
import Kafka.Response
import Kafka.Types
import Network
import System.IO
import qualified Data.ByteString.Char8 as B

data BasicConsumer = BasicConsumer {
    cStream :: Stream
  , cOffset :: Offset
  }

class Consumer c where
  consume :: c -> IO ([Message], c)
  getOffset :: c -> Offset
  getStream :: c -> Stream
  increaseOffsetBy :: c -> Int -> c

instance Consumer BasicConsumer where
  consume c = do
    result <- getFetchData c
    case result of
      (Right r) -> return $! parseMessageSet r c
      (Left r) -> do 
        print ("error parsing response: " ++ show r)
        return ([], c)
  getOffset (BasicConsumer _ o) = o
  getStream (BasicConsumer s _) = s
  increaseOffsetBy settings increment = settings { cOffset = newOffset }
    where newOffset = Offset (current + increment)
          (Offset current) = cOffset settings

consumeLoop :: (Consumer c) => c -> (Message -> IO b) -> IO ()
consumeLoop a f = do
  (messages, newSettings) <- consume a
  mapM_ f messages
  threadDelay 2000
  consumeLoop newSettings f

getFetchData :: (Consumer c) => c -> IO Response
getFetchData a = do
  h <- connectTo "localhost" $ PortNumber 9092
  B.hPut h $ consumeRequest a
  hFlush h
  res <- readDataResponse (handleByteReader h) dropErrorCode
  hClose h
  return res

consumeRequest :: (Consumer c) => c -> ByteString
consumeRequest a = runPut $ do
  encodeRequestSize a
  encodeRequest a

encodeRequestSize :: (Consumer c) => c -> Put
encodeRequestSize c = putWord32be $ fromIntegral requestSize
  where requestSize = 2 + 2 + B.length topic + 4 + 8 + 4
        (Topic topic) = getTopic c

getTopic :: (Consumer c) => c -> Topic
getTopic c = sTopic $ getStream c

getPartition :: (Consumer c) => c -> Partition
getPartition c = sPartition $ getStream c

encodeRequest :: (Consumer c) => c -> Put
encodeRequest a = do
  putRequestType
  putTopic a
  putPartition a
  putOffset a
  putMaxSize

putRequestType :: Put
putRequestType = putWord16be $ fromIntegral raw
  where (RequestType raw) = fetchRequestType

putTopic :: (Consumer c) => c -> Put
putTopic c  = do
  putWord16be . fromIntegral $ B.length t
  putByteString t
  where (Topic t) = getTopic c

putPartition :: (Consumer c) => c -> Put
putPartition c = putWord32be $ fromIntegral p
  where (Partition p) = getPartition c

putOffset :: (Consumer c) => c -> Put
putOffset c = putWord64be $ fromIntegral offset
  where (Offset offset) = getOffset c

putMaxSize :: Put
putMaxSize = putWord32be 1048576 -- 1 MB

type RawConsumeResponseHandler = (ErrorCode -> ByteString -> IO Response)

readDataResponse :: ByteReader -> RawConsumeResponseHandler -> IO Response
readDataResponse h handler = do
  rawLength <- h 4
  let (Right dataLength) = runGet getDataLength rawLength
  rawResponse <- h dataLength
  let x = parseErrorCode rawResponse
  handler x rawResponse

type Response = Either ErrorCode ByteString

dropErrorCode :: RawConsumeResponseHandler
dropErrorCode x rawResponse = case x of
    Success -> return $! Right $ B.drop 2 rawResponse
    e -> return $! Left e

getDataLength :: Get Int
getDataLength = do
  raw <- getWord32be
  return $ fromIntegral raw

parseMessageSet :: (Consumer c) => ByteString -> c -> ([Message], c)
parseMessageSet a = parseMessageSet' a [] 0 startingLength
  where startingLength = B.length a - 4

parseMessageSet' :: (Consumer c) => ByteString -> [Message] -> Int -> Int -> c -> ([Message], c)
parseMessageSet' a messages processed totalLength settings
  | processed <= totalLength = parseMessageSet' a newMessages newProcessed totalLength newSettings
  | otherwise = (messages, settings)
  where messageSize = parseMessageSize processed a
        parsed = parseMessage $ bSplice a processed (messageSize + 4)
        newSettings = increaseOffsetBy settings processed
        newMessages = messages ++ [parsed]
        newProcessed = processed + 4 + messageSize

parseMessageSize :: Int -> ByteString -> Int
parseMessageSize processed raw = fromIntegral $ forceEither "parseMessageSize" $ runGet' raw $ do
                                                skip processed
                                                getWord32be

bSplice :: ByteString -> Int -> Int -> ByteString
bSplice a start end = B.take end (B.drop start a)

parseMessage :: ByteString -> Message
parseMessage raw = Message $ forceEither "parseMessage" $ runGet' raw $ do
  size <- getWord32be
  _ <- getWord8
  _ <- getWord32be
  getByteString $ fromIntegral size - 5

