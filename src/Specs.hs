{-# LANGUAGE OverloadedStrings #-}
module Specs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.Hspec.QuickCheck
import Test.HUnit
import Test.QuickCheck
import Kafka.Producer
import Kafka.Consumer
import Kafka.Types
import Control.Concurrent(threadDelay, forkIO)
import Control.Concurrent.MVar
import System.Timeout
import System.IO.Unsafe
import System.Random
import qualified Data.ByteString.Char8 as B
import Control.Monad(when)

main :: IO ()
main = hspecX $
  describe "hafka" $ do
    integrationTest
    qcProperties

integrationTest = 
  describe "the integrated producer -> consumer loop" $
    it "can pop and push a message" $ do
      partition <- getStdRandom $ randomR (0, 5)
      rawTopic <- getStdRandom $ randomR ('a', 'Z')
      rawMessageChar <- getStdRandom $ randomR ('a', 'Z')

      let topic = B.pack [rawTopic, rawTopic, rawTopic]
          testProducer = ProducerSettings (Topic topic) (Partition partition)
          testConsumer = ConsumerSettings (Topic topic) (Partition partition) (Offset 0)
          rawMessage = B.pack [rawMessageChar, rawMessageChar, rawMessageChar]
      result <- newEmptyMVar
      produce testProducer (Message rawMessage)
      forkIO $ consumeLoop testConsumer (\message ->
        when (message == Message rawMessage) $
          putMVar result message)
      waitFor result (\found ->
        Message rawMessage@=? found)

waitFor :: MVar a -> (a -> b) -> b
waitFor result success = do
  let f = unsafePerformIO $ timeout 10000 $ takeMVar result
  case f of
    (Just found) -> success found
    Nothing -> error "timed out whilst waiting for the message"

instance Arbitrary Message where
  arbitrary = do
    a <-  arbitrary
    return $ Message (B.pack a)

qcProperties = describe "the client" $
  prop "serialize -> deserialize is id" $
    \message -> parseMessage (putMessage message) == message

-- TODO:
-- produce multiple produce requests on the same socket
--  use Control.Concurrent.Chan?
--  restart closed sockets automatically
-- produce multiple messages
-- consume in a delayed loop
-- handle error response codes on the consume response
-- handle failing to parse a message
-- introduce a Message type
-- higher level api? typeclasses for Produceable/Consumable?
-- remove duplication with message headers
-- do polling to make tests faster
-- introduce a MessageSet type
-- pop two messages in a loop
-- randomize the messages put on
-- keep the socket alive whilst consuming forever
-- restart closed sockets when consuming forever
-- rename ConsumerSettings to Consumer
-- broker should be setup with host/port
-- write a test for the offset increasing after parsing a message set
