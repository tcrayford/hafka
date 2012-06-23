{-# LANGUAGE OverloadedStrings #-}
module Specs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Kafka.Producer
import Kafka.Consumer
import Kafka.Types
import Control.Concurrent(threadDelay, forkIO)
import Control.Concurrent.MVar
import System.Timeout
import System.IO.Unsafe

main :: IO ()
main = hspecX $
  describe "pushing and consuming a message" $ do
    let testProducer = ProducerSettings (Topic "test") (Partition 0)
        testConsumer = ConsumerSettings (Topic "test") (Partition 0) (Offset 0)

    it "should eventually pop the same message" $ do
      produce testProducer (Message "hello from hafka")
      threadDelay 10000
      result <- consumeFirst testConsumer
      Message "hello from hafka" @=? result

    it "pops the message in a loop" $ do
      result <- newEmptyMVar
      produce testProducer (Message "hello to the loop")
      forkIO $ consumeLoop testConsumer (\message ->
        if message == Message "hello to the loop" then
          putMVar result message
        else
          return ())
      waitFor result (\found ->
        Message "hello to the loop" @=? found)

waitFor :: MVar a -> (a -> b) -> b
waitFor result success = do
  let f = unsafePerformIO $ timeout 1000 $ takeMVar result
  case f of
    (Just found) -> success found
    Nothing -> error "timed out whilst waiting for the message"

-- TODO:
-- produce multiple produce requests on the same socket
--  use Control.Concurrent.Chan?
--  restart closed sockets automatically
-- produce multiple messages
-- consume in a delayed loop
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
-- introduce a MessageSet type
-- pop two messages in a loop
-- randomize the messages put on
-- keep the socket alive whilst consuming forever
-- restart closed sockets when consuming forever
-- grep for "::  "
