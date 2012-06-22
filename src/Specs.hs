{-# LANGUAGE OverloadedStrings #-}
module Specs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Kafka.Producer
import Kafka.Consumer
import Kafka.Types
import Control.Concurrent(threadDelay)

main :: IO ()
main = hspecX $
  describe "pushing and consuming a message" $ do
    let testProducer = ProducerSettings (Topic "test") (Partition 0)
        testConsumer = ConsumerSettings (Topic "test") (Partition 0)

    it "should eventually pop the same message" $ do
      produce testProducer (Message "hello from hafka")
      threadDelay 10000
      result <- consumeFirst testConsumer
      Message "hello from hafka" @=? result

-- TODO:
-- produce multiple produce requests on the same socket
--  use Control.Concurrent.Chan?
--  restart closed sockets automatically
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
-- asString for topic
-- asInt or whatever for partition
-- wrapper type for (Foo Topic Parition)
