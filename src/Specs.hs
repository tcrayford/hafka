{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Specs where
import Specs.Kafka.Arbitrary()
import Specs.Kafka.EndToEnd.BasicConsumerSpecs
import Specs.Kafka.EndToEnd.KeepAliveProducerSpecs
import Specs.Kafka.EndToEnd.KeepAliveSpecs
import Specs.Kafka.KeepAlive
import Specs.Kafka.ParsingSpecs
import Specs.Kafka.Unit.ConsumerSpecs
import Specs.Kafka.Unit.KeepAliveSpecs
import Test.Hspec.Monadic
import Test.Hspec.QuickCheck
import qualified Bench.Benchmarks as Bench

main :: IO ()
main = hspec $
  describe "hafka" $ do
    parsingErrorCode
    reconnectingToClosedSocket
    parseConsumptionTest
    consumerSpecs
    messageProperties
    benchSpecs
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

    describe "keep alive producer" $ do
      prop "can produce with keep alive" keepAliveProducerProduces
      keepAliveProducerReconnects

benchSpecs :: Spec
benchSpecs = describe "the benchmarks" $ do
  it "roundtripBasicConsumer" Bench.roundtripBasicConsumer
  it "roundtripKeepAliveConsumer" Bench.roundtripKeepAliveConsumer
  it "roundtripBasicProducer" Bench.roundtripBasicProducer
  it "roundtripKeepAliveProducer" Bench.roundtripKeepAliveProducer

-- higher level api? typeclasses for Produceable, Consumeable?
-- put the basic consumer in Kafka.Consumer.Basic
