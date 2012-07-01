{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Specs where
import Control.Concurrent.MVar
import Kafka.Consumer.KeepAlive
import Kafka.Producer
import Kafka.Types
import Specs.IntegrationHelper
import Specs.Kafka.Arbitrary()
import Specs.Kafka.EndToEnd.BasicConsumerSpecs
import Specs.Kafka.EndToEnd.KeepAliveSpecs
import Specs.Kafka.EndToEnd.KeepAliveProducerSpecs
import Specs.Kafka.KeepAlive
import Specs.Kafka.ParsingSpecs
import Specs.Kafka.Unit.ConsumerSpecs
import Specs.Kafka.Unit.KeepAliveSpecs
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
      deliversWhenProducingMultipleMessages
      prop "can pop and push a message" produceToConsume

    describe "keep alive consumer" $ do
      keepAliveReconectsToClosedSockets
      keepAliveConsumesMultipleMessages 
      prop "can consume with a keepalive" consumesWithKeepAlive

    describe "keep alive producer" $
      prop "can produce with keep alive" keepAliveProducerProduces

-- higher level api? typeclasses for Produceable, Consumeable?
-- put the basic consumer in Kafka.Consumer.Basic
