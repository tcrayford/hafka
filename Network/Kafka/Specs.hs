{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Network.Kafka.Specs where
import Network.Kafka.Specs.Kafka.Arbitrary()
import Network.Kafka.Specs.Kafka.EndToEnd.BasicConsumerSpecs
import Network.Kafka.Specs.Kafka.EndToEnd.KeepAliveProducerSpecs
import Network.Kafka.Specs.Kafka.EndToEnd.KeepAliveSpecs
import Network.Kafka.Specs.Kafka.KeepAlive
import Network.Kafka.Specs.Kafka.ParsingSpecs
import Network.Kafka.Specs.Kafka.Unit.ConsumerSpecs
import Network.Kafka.Specs.Kafka.Unit.KeepAliveSpecs
import Network.Kafka.Specs.Kafka.Unit.ProducerSpecs
import Test.Hspec.Monadic
import qualified Network.Kafka.Bench.Benchmarks as Bench

main :: IO ()
main = hspec $
  describe "hafka" $ do
    parsingErrorCode
    reconnectingToClosedSocket
    parseConsumptionTest
    consumerSpecs
    producerSpecs
    messageProperties
    benchSpecs
    integrationTests

integrationTests :: Spec
integrationTests =
  describe "the integrated producer -> consumer loop" $ do
    describe "basic consumer" $ do
      deliversWhenProducingMultipleMessages
      produceToConsume

    describe "keep alive consumer" $ do
      keepAliveReconectsToClosedSockets
      keepAliveConsumesMultipleMessages 
      consumesWithKeepAlive

    describe "keep alive producer" $ do
      keepAliveProducerProduces
      keepAliveProducerReconnects

benchSpecs :: Spec
benchSpecs = describe "the benchmarks" $ do
  it "roundtripBasicConsumer" Bench.roundtripBasicConsumer
  it "roundtripKeepAliveConsumer" Bench.roundtripKeepAliveConsumer
  it "roundtripBasicProducer" Bench.roundtripBasicProducer
  it "roundtripKeepAliveProducer" Bench.roundtripKeepAliveProducer

-- higher level api? typeclasses for Produceable, Consumeable?
-- put the basic consumer in Kafka.Consumer.Basic
