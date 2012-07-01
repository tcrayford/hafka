{-# LANGUAGE OverloadedStrings #-}
module Kafka.Producer where
import Kafka.Types
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
  putWord32be $ fromIntegral (B.length body)
  putByteString body
  where
    body = produceRequest settings messages

produceRequest :: ProducerSettings -> [Message] -> ByteString
produceRequest settings m = runPut $ do
  putProduceRequestType
  putTopic settings
  putPartition settings
  putMessages m

putProduceRequestType :: Put
putProduceRequestType = putWord16be $ fromIntegral raw
  where (RequestType raw) = produceRequestType

putTopic :: ProducerSettings -> Put
putTopic (ProducerSettings (Stream (Topic t) _)) = do
  putWord16be $ fromIntegral (B.length t)
  putByteString t

putPartition :: ProducerSettings -> Put
putPartition (ProducerSettings (Stream _ (Partition p))) = putWord32be $ fromIntegral p

putMessages :: [Message] -> Put
putMessages messages = do
  putWord32be $ fromIntegral (B.length encoded)
  putByteString encoded
  where encoded = B.concat $ Prelude.map putMessage messages

putMessage :: Message -> ByteString
putMessage message = runPut $ do
  let encoded = encode message
  putWord32be $ fromIntegral (B.length encoded)
  putByteString encoded

encode :: Message -> ByteString
encode (Message message) = runPut $ do
  putMessageMagic
  putWord32be (crc32 message)
  putByteString message
  where putMessageMagic = putWord8 0

