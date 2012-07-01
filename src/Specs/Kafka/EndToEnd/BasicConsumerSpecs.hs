{-# LANGUAGE OverloadedStrings #-}
module Specs.Kafka.EndToEnd.BasicConsumerSpecs where
import Control.Concurrent.MVar
import Kafka.Producer
import Kafka.Types
import Specs.IntegrationHelper
import Test.Hspec.Monadic
import Test.QuickCheck
import Test.QuickCheck.Monadic
import Test.HUnit
import Test.Hspec.Monadic

produceToConsume :: Stream -> Message -> Property
produceToConsume stream message = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      run $ produce testProducer [message]
      run $ recordMatching testConsumer message result

      run $ waitFor result message (return ())

deliversWhenProducingMultipleMessages :: Spec
deliversWhenProducingMultipleMessages = it "delivers multiple messages" $ do
      let stream = Stream (Topic "basic_consumer_delivers_multiple_messages") (Partition 0)
          (testProducer, testConsumer) = coupledProducerConsumer stream
          (m1, m2) = (Message "m1", Message "m2")
      result <- newEmptyMVar

      produce testProducer [m1, m2]
      recordMatching testConsumer m2 result

      waitFor result m2 (return ())

