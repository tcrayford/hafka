{-# LANGUAGE OverloadedStrings #-}
module Specs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Data.ByteString.Char8
import qualified Data.ByteString.Char8 as B
import Network
import Data.Serialize.Put
import Data.Digest.CRC32
import System.IO

produceRequestID = 0

data ProducerSettings = ProducerSettings ByteString Int
data ConsumerSettings = ConsumerSettings

encode :: ByteString -> ByteString
encode message = runPut $ do
  putMessageMagic
  putWord32be (crc32 message)
  putByteString message
  where putMessageMagic = putWord8 0

putMessage :: ByteString -> Put
putMessage message = do
  let encoded = encode message
  putWord32be $ fromIntegral (B.length encoded)
  putByteString encoded

produce :: ProducerSettings -> ByteString -> IO ()
produce settings message = do
  h <- connectTo "localhost" $ PortNumber 9092
  B.hPut h req
  hFlush h
  hClose h
  --error (show (Prelude.map show (B.unpack req)))
  where
    m = runPut $ putMessage message
    body = runPut $ produceRequest settings m
    req = runPut $ do
      putWord32be $ fromIntegral (B.length body)
      putByteString body

produceRequest settings m = do
  putWord16be $ fromIntegral 0
  putTopic settings
  putPartition settings
  putMessages m

putTopic (ProducerSettings t _) = do
  putWord16be $ fromIntegral (B.length t)
  putByteString t

putPartition (ProducerSettings _ p) = do
  putWord32be $ fromIntegral p
  
putMessages m = do
  putWord32be $ fromIntegral (B.length m)
  putByteString m

consumeOne :: ConsumerSettings -> IO ByteString
consumeOne a = return "hello from hafka"

main = hspecX $
  describe "pushing and consuming a message" $ do
    let testProducer = (ProducerSettings "test" 0)
        testConsumer = undefined

    it "should eventually pop the same message" $ do
      produce testProducer "hello from hafka"
      result <- consumeOne testConsumer
      ("hello from hafka" :: ByteString) @?= result

-- TODO:
-- produce multiple results on the socket
