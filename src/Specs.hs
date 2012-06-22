{-# LANGUAGE OverloadedStrings #-}
module Specs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Data.ByteString.Char8(ByteString)
import Kafka.Producer
import Kafka.Consumer
import Control.Concurrent(threadDelay)

main :: IO ()
main = hspecX $
  describe "pushing and consuming a message" $ do
    let testProducer = ProducerSettings "test" 0
        testConsumer = ConsumerSettings "test" 0

    it "should eventually pop the same message" $ do
      produce testProducer "hello from hafka"
      threadDelay 10000
      result <- consumeFirst testConsumer
      ("hello from hafka" :: ByteString) @=? result

-- TODO:
-- produce multiple produce requests on the same socket
-- restart closed sockets automatically
-- produce multiple messages
-- consume in a delayed loop
-- request type as an enum
-- newtype all the things
-- handle error response codes on the consume response
-- handle failing to parse a message
-- introduce a Message type
-- QC parse and serialize inverting each other
-- higher level api? typeclasses for Produceable/Consumable?
-- remove duplication with message headers
-- do polling to make tests faster
-- more tests
