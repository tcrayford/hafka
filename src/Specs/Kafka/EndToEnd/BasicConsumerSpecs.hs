module Specs.Kafka.EndToEnd.BasicConsumerSpecs where
import Control.Concurrent.MVar
import Kafka.Producer
import Kafka.Types
import Specs.IntegrationHelper
import Test.Hspec.Monadic
import Test.QuickCheck
import Test.QuickCheck.Monadic

produceToConsume :: Stream -> Message -> Property
produceToConsume stream message = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      run $ produce testProducer [message]
      run $ recordMatching testConsumer message result

      run $ waitFor result message (return ())

deliversWhenProducingMultipleMessages :: Stream -> Message -> Message -> Property
deliversWhenProducingMultipleMessages stream m1 m2 = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      run $ produce testProducer [m1, m2]
      run $ recordMatching testConsumer m2 result

      run $ waitFor result m2 (return ())

