{-# LANGUAGE OverloadedStrings #-}
module Network.Kafka.Specs.Kafka.EndToEnd.KeepAliveSpecs where
import Control.Concurrent.MVar
import Control.Monad
import Network.Kafka.Consumer
import Network.Kafka.Consumer.KeepAlive
import Network.Kafka.Producer
import Network.Kafka.Types
import Network.Kafka.Specs.IntegrationHelper
import System.IO
import System.Timeout
import Test.HUnit
import Test.Hspec.HUnit
import Test.Hspec.Monadic

consumesWithKeepAlive :: Spec
consumesWithKeepAlive = it "consumes with keep alive" $ do
      let stream = Stream (Topic "consumesWithKeepAlive") (Partition 1)
          message = Message "consumesWithKeepAlive"
          (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- newEmptyMVar

      produce testProducer [message]
      c <- keepAlive testConsumer
      recordMatching c message result

      waitFor result message (killSocket c)


keepAliveReconectsToClosedSockets :: Spec
keepAliveReconectsToClosedSockets = it "reconnects to closed sockets" $ do

      let stream = Stream (Topic "keep_alive_reconnects_to_closed_sockets") (Partition 0)
          message = Message "keep alive should receive this message after closing the socket"
          (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- newEmptyMVar
      c <- keepAlive testConsumer

      recordMatching c message result
      killSocket c
      produce testProducer [message]

      waitFor result message (killSocket c)

keepAliveConsumesMultipleMessages :: Spec
keepAliveConsumesMultipleMessages = it "consumes multiple messages" $ do
      let stream = Stream (Topic "keep_alive_consumes_multiple_messages") (Partition 0)
          (testProducer, testConsumer) = coupledProducerConsumer stream
          (m1, m2) = (Message "m1", Message "m2")
          
      result <- newEmptyMVar
      c <- keepAlive testConsumer

      produce testProducer [m1, m2]
      recordMatching c m2 result

      waitFor result m2 (return ())

killSocket :: KeepAliveConsumer -> IO ()
killSocket c = do
  r <- timeout 100000 $ takeMVar (kaSocket c)
  case r of
    (Just s) -> do
      hClose s
      putMVar (kaSocket c) s
    Nothing -> error "timed out whilst trying to kill the socket"

