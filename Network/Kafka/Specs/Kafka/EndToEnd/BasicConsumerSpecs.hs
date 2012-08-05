{-# LANGUAGE OverloadedStrings #-}
module Network.Kafka.Specs.Kafka.EndToEnd.BasicConsumerSpecs where
import Control.Concurrent.MVar
import Network.Kafka.Producer
import Network.Kafka.Types
import Network.Kafka.Specs.IntegrationHelper
import Test.Hspec.Monadic

produceToConsume :: Spec
produceToConsume = it "can push -> pop an message" $ do
      let stream = Stream (Topic "produceToConsume") (Partition 4)
          (testProducer, testConsumer) = coupledProducerConsumer stream
          message = Message "produceToConsume"
      result <- newEmptyMVar

      produce testProducer [message]
      recordMatching testConsumer message result

      waitFor result message (return ())

deliversWhenProducingMultipleMessages :: Spec
deliversWhenProducingMultipleMessages = it "delivers multiple messages" $ do
      let stream = Stream (Topic "basic_consumer_delivers_multiple_messages") (Partition 0)
          (testProducer, testConsumer) = coupledProducerConsumer stream
          (m1, m2) = (Message "m1", Message "m2")
      result <- newEmptyMVar

      produce testProducer [m1, m2]
      recordMatching testConsumer m2 result

      waitFor result m2 (return ())

