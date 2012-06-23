{-# LANGUAGE OverloadedStrings #-}
module Kafka.Consumer where
import Kafka.Types
import Network
import Data.ByteString.Char8(ByteString)
import qualified Data.ByteString.Char8 as B
import System.IO
import Data.Serialize.Put
import Data.Serialize.Get
import Control.Monad(forever)
import Control.Concurrent(threadDelay)

data ConsumerSettings = ConsumerSettings {
    cTopic :: Topic
  , cPartition :: Partition
  , cOffset :: Offset
  }

consumeFirst :: ConsumerSettings -> IO Message
consumeFirst a = do
  h <- connectTo "localhost" $ PortNumber 9092
  B.hPut h $ consumeRequest a
  hFlush h
  result <- readDataResponse h
  return . Prelude.last $ fst $ parseMessageSet result a

consumeLoop :: ConsumerSettings -> (Message -> IO b) -> IO ()
consumeLoop a f = do
  (messages, newSettings) <- consume a
  mapM f messages
  threadDelay 2000
  consumeLoop newSettings f

consume :: ConsumerSettings -> IO ([Message], ConsumerSettings)
consume a = do
  h <- connectTo "localhost" $ PortNumber 9092
  B.hPut h $ consumeRequest a
  hFlush h
  result <- readDataResponse h
  return $ parseMessageSet result a

consumeRequest ::  ConsumerSettings -> ByteString
consumeRequest a = runPut $ do
  encodeRequestSize a
  encodeRequest a

encodeRequestSize :: ConsumerSettings -> Put
encodeRequestSize (ConsumerSettings (Topic topic) _ _) = putWord32be $ fromIntegral requestSize
  where requestSize = 2 + 2 + B.length topic + 4 + 8 + 4

encodeRequest ::  ConsumerSettings -> Put
encodeRequest a = do
  putRequestType
  putTopic a
  putPartition a
  putOffset a
  putMaxSize

putRequestType :: Put
putRequestType = putWord16be $ fromIntegral raw
  where (RequestType raw) = fetchRequestType

putTopic ::  ConsumerSettings -> Put
putTopic (ConsumerSettings (Topic t) _ _)  = do
  putWord16be . fromIntegral $ B.length t
  putByteString t

putPartition ::  ConsumerSettings -> Put
putPartition (ConsumerSettings _ (Partition p) _) = putWord32be $ fromIntegral p

putOffset :: ConsumerSettings -> Put
putOffset (ConsumerSettings _ _ (Offset offset)) = putWord64be $ fromIntegral offset

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

parseMessageSet :: ByteString -> ConsumerSettings -> ([Message], ConsumerSettings)
parseMessageSet a settings = parseMessageSet' a [] 0 startingLength settings
  where startingLength = B.length a - 4

parseMessageSet' :: ByteString -> [Message] -> Int -> Int -> ConsumerSettings -> ([Message], ConsumerSettings)
parseMessageSet' a messages processed totalLength settings
  | processed <= totalLength = parseMessageSet' a (messages ++ [parsed]) (processed + 4 + messageSize) totalLength newSettings
  | otherwise = (messages, settings)
  where messageSize = getMessageSize processed a
        parsed = parseMessage $ bSplice a processed (messageSize + 4)
        newSettings = increaseOffsetBy settings processed

increaseOffsetBy :: ConsumerSettings -> Int -> ConsumerSettings
increaseOffsetBy settings increment = settings { cOffset = newOffset }
  where newOffset = Offset (current + increment)
        (Offset current) = cOffset settings

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
