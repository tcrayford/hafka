{-# LANGUAGE OverloadedStrings #-}
module Kafka.Consumer where
import Kafka.Types
import Kafka.Parsing
import Kafka.Response
import Network
import Data.ByteString.Char8(ByteString)
import qualified Data.ByteString.Char8 as B
import System.IO
import Data.Serialize.Put
import Data.Serialize.Get
import Control.Concurrent(threadDelay)

data Consumer = Consumer {
    cStream :: Stream
  , cOffset :: Offset
  }

consumeLoop :: Consumer -> (Message -> IO b) -> IO ()
consumeLoop a f = do
  (messages, newSettings) <- consume a
  mapM_ f messages
  threadDelay 2000
  consumeLoop newSettings f

consume :: Consumer -> IO ([Message], Consumer)
consume a = do
  result <- getFetchData a
  case result of
    (Right r) -> return $! parseMessageSet r a
    (Left r) -> do 
      print ("error parsing response: " ++ show r)
      return ([], a)

getFetchData :: Consumer -> IO (Either ErrorCode ByteString)
getFetchData a = do
  h <- connectTo "localhost" $ PortNumber 9092
  B.hPut h $ consumeRequest a
  hFlush h
  res <- readDataResponse h
  hClose h
  return res

consumeRequest :: Consumer -> ByteString
consumeRequest a = runPut $ do
  encodeRequestSize a
  encodeRequest a

encodeRequestSize :: Consumer -> Put
encodeRequestSize (Consumer (Stream (Topic topic) _) _) = putWord32be $ fromIntegral requestSize
  where requestSize = 2 + 2 + B.length topic + 4 + 8 + 4

encodeRequest :: Consumer -> Put
encodeRequest a = do
  putRequestType
  putTopic a
  putPartition a
  putOffset a
  putMaxSize

putRequestType :: Put
putRequestType = putWord16be $ fromIntegral raw
  where (RequestType raw) = fetchRequestType

putTopic :: Consumer -> Put
putTopic (Consumer (Stream (Topic t) _) _)  = do
  putWord16be . fromIntegral $ B.length t
  putByteString t

putPartition :: Consumer -> Put
putPartition (Consumer (Stream _ (Partition p)) _) = putWord32be $ fromIntegral p

putOffset :: Consumer -> Put
putOffset (Consumer _ (Offset offset)) = putWord64be $ fromIntegral offset

putMaxSize :: Put
putMaxSize = putWord32be 1048576 -- 1 MB

readDataResponse :: Handle -> IO (Either ErrorCode ByteString)
readDataResponse h = do
  rawLength <- B.hGet h 4
  let (Right dataLength) = runGet getDataLength rawLength
  rawResponse <- B.hGet h dataLength
  let x = parseErrorCode rawResponse
  case x of
    Success -> return $! Right $ B.drop 2 rawResponse
    e -> return $! Left e

getDataLength :: Get Int
getDataLength = do
  raw <- getWord32be
  return $ fromIntegral raw

parseMessageSet :: ByteString -> Consumer -> ([Message], Consumer)
parseMessageSet a = parseMessageSet' a [] 0 startingLength
  where startingLength = B.length a - 4

parseMessageSet' :: ByteString -> [Message] -> Int -> Int -> Consumer -> ([Message], Consumer)
parseMessageSet' a messages processed totalLength settings
  | processed <= totalLength = parseMessageSet' a newMessages newProcessed totalLength newSettings
  | otherwise = (messages, settings)
  where messageSize = parseMessageSize processed a
        parsed = parseMessage $ bSplice a processed (messageSize + 4)
        newSettings = increaseOffsetBy settings processed
        newMessages = (messages ++ [parsed])
        newProcessed = processed + 4 + messageSize

increaseOffsetBy :: Consumer -> Int -> Consumer
increaseOffsetBy settings increment = settings { cOffset = newOffset }
  where newOffset = Offset (current + increment)
        (Offset current) = cOffset settings

parseMessageSize :: Int -> ByteString -> Int
parseMessageSize processed raw = fromIntegral $ forceEither raw $ runGet' raw $ do
                                                skip processed
                                                getWord32be

bSplice :: ByteString -> Int -> Int -> ByteString
bSplice a start end = B.take end (B.drop start a)

parseMessage :: ByteString -> Message
parseMessage raw = Message $ forceEither raw $ runGet' raw $ do
  size <- getWord32be
  _ <- getWord8
  _ <- getWord32be
  getByteString $ fromIntegral size - 5

