{-# LANGUAGE OverloadedStrings #-}
module Network.Kafka.Producer where
import Network.Kafka.Request
import Network.Kafka.Types
import Data.ByteString.Char8
import qualified Data.ByteString.Char8 as B
import Network
import Data.Serialize.Put
import Data.Digest.CRC32
import System.IO

data ProducerSettings = ProducerSettings Stream

class Producer a where
  produce :: a -> [Message] -> IO ()

instance Producer ProducerSettings where
  produce settings messages = do
    h <- connectTo "localhost" $ PortNumber 9092
    B.hPut h $ fullProduceRequest settings messages
    hFlush h
    hClose h

fullProduceRequest :: ProducerSettings -> [Message] -> ByteString
fullProduceRequest settings messages = runPut $ do
  putWord32be $ fromIntegral $ produceRequestLength messages settings
  produceRequest settings messages

produceRequest :: ProducerSettings -> [Message] -> Put
produceRequest (ProducerSettings s) m = do
  putProduceRequestType
  putStream s
  putMessages m

putProduceRequestType :: Put
putProduceRequestType = putWord16be $ fromIntegral raw
  where (RequestType raw) = produceRequestType

putMessages :: [Message] -> Put
putMessages messages = do
  putWord32be $ fromIntegral $ messageSetLength messages
  mapM_ putMessage messages

putMessage :: Message -> Put
putMessage (Message message) = do
  putWord32be $ fromIntegral (5 + B.length message)
  putMessageMagic
  putWord32be (crc32 message)
  putByteString message

produceRequestLength :: [Message] -> ProducerSettings -> Int
produceRequestLength messages (ProducerSettings stream) = requestType + streamLength stream + messageSet + messageSetLength messages
  where requestType = 2
        messageSet = 4

streamLength :: Stream -> Int
streamLength (Stream (Topic t) (Partition _)) = topicLength + B.length t + partition
  where topicLength = 2
        partition = 4

putMessageMagic :: Put
putMessageMagic = putWord8 0

messageSetLength :: [Message] -> Int
messageSetLength = sum . Prelude.map mLength

mLength :: Message -> Int
mLength (Message m) = messageMagic + rawLength + crc + B.length m
  where messageMagic = 1
        rawLength = 4
        crc = 4

