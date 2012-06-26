{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Specs where
import Test.Hspec.Monadic
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Kafka.Producer
import Kafka.Consumer
import Kafka.Types
import Control.Concurrent.MVar
import qualified Data.ByteString.Char8 as B
import Test.QuickCheck.Monadic
import Specs.IntegrationHelper
import Control.Concurrent(forkIO)
import Control.Monad(when)

main :: IO ()
main = hspec $
  describe "hafka" $ do
    integrationTest
    messageProperties

integrationTest :: Spec
integrationTest = 
  describe "the integrated producer -> consumer loop" $
    prop "can pop and push a message" produceToConsume

produceToConsume :: Partition -> Topic -> Message -> Property
produceToConsume partition topic message = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer topic partition
      result <- run newEmptyMVar

      run $ produce testProducer message
      run $ recordMatching testConsumer message result

      run $ waitFor result ("timed out waiting for " ++ show message ++ " to be delivered")

recordMatching :: Consumer -> Message -> MVar Message -> IO ()
recordMatching c original r = do
  _ <- forkIO $ consumeLoop c go
  return ()

  where
    go :: Message -> IO ()
    go message = when (original == message) $ finish message
    finish :: Message -> IO ()
    finish message = do
              putMVar r message
              killCurrent

messageProperties :: Spec
messageProperties = describe "the client" $ do
  prop "serialize -> deserialize is id" $
    \message -> parseMessage (putMessage message) == message

  prop "serialized message length is 1 + 4 + n" $
    \message@(Message raw) -> parseMessageSize 0 (putMessage message) == 1 + 4 + B.length raw

instance Arbitrary Partition where
  arbitrary = do
    a <- elements [0..5]
    return $ Partition a

instance Arbitrary Topic where
  arbitrary = do
    a <- nonEmptyString
    return $ Topic $ B.pack a

instance Arbitrary Message where
  arbitrary = do
    a <- nonEmptyString
    return $ Message (B.pack a)

nonEmptyString :: Gen String
nonEmptyString = suchThat (listOf $ elements ['a'..'z']) (not . null)

-- higher level api? typeclasses for Produceable, Consumeable?
