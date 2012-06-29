{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Specs where
import Control.Concurrent.MVar
import Kafka.Consumer.KeepAlive
import Kafka.Network
import Kafka.Producer
import Kafka.Types
import Network.Socket(sClose, sIsConnected)
import Specs.IntegrationHelper
import Specs.Kafka.Arbitrary()
import Specs.Kafka.EndToEnd.KeepAliveSpecs
import Specs.Kafka.KeepAlive
import Specs.Kafka.ParsingSpecs
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
    messageProperties
    integrationTests

integrationTests :: Spec
integrationTests =
  describe "the integrated producer -> consumer loop" $ do
    prop "can pop and push a message" produceToConsume
    prop "can produce multiple messages" deliversWhenProducingMultipleMessages
    prop "can consume with a keepalive" consumesWithKeepAlive
    prop "can reconnect to closed sockets" keepAliveReconectsToClosedSockets
    prop "keepAlive consumes multiple messages" keepAliveConsumesMultipleMessages 

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

consumesWithKeepAlive :: Stream -> Message -> Property
consumesWithKeepAlive stream message = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      run $ produce testProducer [message]
      c <- run (keepAlive testConsumer)
      run $ recordMatching c message result

      run $ waitFor result message (killSocket c)

reconnectingToClosedSocket :: Spec
reconnectingToClosedSocket = describe "reconnectSocket" $
  it "reconnects a closed socket" $ do
    s <- connectTo "localhost" $ PortNumber 9092
    sClose s
    s2 <- reconnectSocket s
    c <- sIsConnected s2
    c @?= True

-- higher level api? typeclasses for Produceable, Consumeable?
-- put the basic consumer in Kafka.Consumer.Basic
