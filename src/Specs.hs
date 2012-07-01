{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Specs where
import Control.Concurrent.MVar
import Kafka.Consumer.KeepAlive
import Kafka.Network
import Kafka.Producer
import Kafka.Types
import Specs.IntegrationHelper
import Specs.Kafka.Arbitrary()
import Specs.Kafka.EndToEnd.BasicConsumerSpecs
import Specs.Kafka.EndToEnd.KeepAliveSpecs
import Specs.Kafka.KeepAlive
import Specs.Kafka.ParsingSpecs
import Specs.Kafka.Unit.ConsumerSpecs
import Specs.Kafka.Unit.KeepAliveSpecs
import Test.HUnit
import Test.Hspec.Monadic
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Test.QuickCheck.Monadic

main :: IO ()
main = hspec $
  describe "hafka" $ do
    parsingErrorCode
    reconnectingToClosedSocket
    parseConsumptionTest
    consumerSpecs
    messageProperties
    integrationTests

integrationTests :: Spec
integrationTests =
  describe "the integrated producer -> consumer loop" $ do
    describe "basic consumer" $ do
      prop "can pop and push a message" produceToConsume
      prop "can produce multiple messages" deliversWhenProducingMultipleMessages
    describe "keep alive consumer" $ do
      keepAliveReconectsToClosedSockets
      prop "can consume with a keepalive" consumesWithKeepAlive
      prop "keepAlive consumes multiple messages" keepAliveConsumesMultipleMessages 

consumesWithKeepAlive :: Stream -> Message -> Property
consumesWithKeepAlive stream message = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      run $ produce testProducer [message]
      c <- run (keepAlive testConsumer)
      run $ recordMatching c message result

      run $ waitFor result message (killSocket c)


-- higher level api? typeclasses for Produceable, Consumeable?
-- put the basic consumer in Kafka.Consumer.Basic
