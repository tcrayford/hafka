{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Specs where
import Test.Hspec.Monadic
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Kafka.Producer
import Kafka.Consumer
import Kafka.Types
import Control.Concurrent(forkIO)
import Control.Concurrent.MVar
import System.Timeout
import System.Random
import qualified Data.ByteString.Char8 as B
import Control.Monad(when, void)
import Test.QuickCheck.Monadic

main :: IO ()
main = hspecX $
  describe "hafka" $ do
    integrationTest
    qcProperties

integrationTest :: Specs
integrationTest = 
  describe "the integrated producer -> consumer loop" $
    prop "can pop and push a message" $ integrated


integrated partition topic message = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer topic partition
      result <- run newEmptyMVar

      run $ produce testProducer message
      run $ recordMatching testConsumer message result

      waitFor result (\found ->
        assert (message == found)
        ) ("timed out waiting for " ++ show message ++ " to be delivered")

instance Arbitrary Partition where
  arbitrary = do
    a <- elements [0..5]
    return $ Partition a

instance Arbitrary Topic where
  arbitrary = do
    a <- suchThat (listOf $ elements ['a'..'z']) (not . null)
    return $ Topic $ B.pack a

instance Arbitrary Message where
  arbitrary = do
    a <- suchThat (listOf $ elements ['a'..'z']) (not . null)
    return $ Message (B.pack a)

coupledProducerConsumer :: Topic -> Partition -> (ProducerSettings, Consumer)
coupledProducerConsumer t p = (ProducerSettings t p, Consumer t p $ Offset 0)

recordMatching :: Consumer -> Message -> MVar Message -> IO ()
recordMatching c original r = do
  _ <- forkIO $ consumeLoop c (\message ->
    when (original == message) $
      putMVar r message)
  return ()

waitFor :: MVar a -> (a -> PropertyM IO ()) -> String -> PropertyM IO ()
waitFor result success message = do
  f <- run $ timeout 1000000 $ takeMVar result
  case f of
    (Just found) -> void $ success found
    Nothing -> error message

qcProperties :: Specs
qcProperties = describe "the client" $ do
  prop "serialize -> deserialize is id" $
    \message -> parseMessage (putMessage message) == message

  prop "serialized message length is 1 + 4 + n" $
    \message@(Message raw) -> parseMessageSize 0 (putMessage message) == 1 + 4 + B.length raw

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
-- broker should be setup with host/port
-- write a test for the offset increasing after parsing a message set
