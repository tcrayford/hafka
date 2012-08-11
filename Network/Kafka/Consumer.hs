{-# LANGUAGE OverloadedStrings #-}
module Network.Kafka.Consumer where
import Control.Concurrent(threadDelay)
import Data.ByteString.Char8(ByteString)
import Data.Serialize.Get
import Data.Serialize.Put
import Network.Kafka.Consumer.ByteReader
import Network.Kafka.Parsing
import Network.Kafka.Response
import Network.Kafka.Request
import Network.Kafka.Types
import qualified Data.ByteString.Char8 as B

class Consumer c where
  consume :: c -> IO ([Message], c)
  getOffset :: c -> Offset
  getStream :: c -> Stream
  increaseOffsetBy :: c -> Int -> c

consumeLoop :: (Consumer c) => c -> (Message -> IO b) -> IO ()
consumeLoop a f = do
  (messages, newSettings) <- consume a
  mapM_ f messages
  threadDelay 2000
  consumeLoop newSettings f

consumeRequest :: (Consumer c) => c -> ByteString
consumeRequest a = runPut $ do
  encodeRequestSize a
  encodeRequest a

encodeRequestSize :: (Consumer c) => c -> Put
encodeRequestSize c = putWord32be . fromIntegral $ requestSize (getTopic c)

requestSize :: Topic -> Int
requestSize (Topic topic) = 2 + 2 + B.length topic + 4 + 8 + 4

getTopic :: (Consumer c) => c -> Topic
getTopic c = sTopic $ getStream c

getPartition :: (Consumer c) => c -> Partition
getPartition c = sPartition $ getStream c

encodeRequest :: (Consumer c) => c -> Put
encodeRequest a = do
  putRequestType
  putStream (getStream a)
  putOffset a
  putMaxSize

putRequestType :: Put
putRequestType = putWord16be $ fromIntegral raw
  where (RequestType raw) = fetchRequestType

putOffset :: (Consumer c) => c -> Put
putOffset c = putWord64be $ fromIntegral offset
  where (Offset offset) = getOffset c

putMaxSize :: Put
putMaxSize = putWord32be 1048576 -- 1 MB

type RawConsumeResponseHandler = (ErrorCode -> ByteString -> Response)

readDataResponse :: ByteReader -> RawConsumeResponseHandler -> IO Response
readDataResponse h handler = do
  rawLength <- h 4
  let (Right dataLength) = runGet getDataLength rawLength
  rawResponse <- h dataLength
  let x = parseErrorCode rawResponse
  return $! handler x rawResponse

type Response = Either ErrorCode ByteString

dropErrorCode :: RawConsumeResponseHandler
dropErrorCode x rawResponse = case x of
    Success -> Right $ B.drop 2 rawResponse
    e -> Left e

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
        parsed = parseMessage $ bSplice processed (messageSize + 4) a
        newSettings = increaseOffsetBy settings newProcessed
        newMessages = messages ++ [parsed]
        newProcessed = processed + 4 + messageSize

parseMessageSize :: Int -> ByteString -> Int
parseMessageSize processed raw = fromIntegral $ forceEither "parseMessageSize" $ runGet' raw $ do
                                                skip processed
                                                getWord32be

bSplice :: Int -> Int -> ByteString -> ByteString
bSplice start end a = B.take end (B.drop start a)

parseMessage :: ByteString -> Message
parseMessage raw = Message $ forceEither "parseMessage" $ runGet' raw $ do
  size <- getWord32be
  _ <- getWord8
  _ <- getWord32be
  getByteString $ fromIntegral size - 5

