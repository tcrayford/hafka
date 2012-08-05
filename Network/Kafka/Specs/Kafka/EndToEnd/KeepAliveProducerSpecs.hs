{-# LANGUAGE OverloadedStrings #-}
module Network.Kafka.Specs.Kafka.EndToEnd.KeepAliveProducerSpecs where
import Control.Concurrent.MVar
import Network.Kafka.Producer
import Network.Kafka.Producer.KeepAlive
import Network.Kafka.Types
import Network.Kafka.Specs.IntegrationHelper
import System.IO
import System.Timeout
import Test.Hspec.Monadic
import Test.Hspec.HUnit()

keepAliveProducerProduces :: Spec
keepAliveProducerProduces = it "keep alive producer produces" $ do
      let stream = Stream (Topic "keepAliveProducerProduces") (Partition 0)
          (testProducer, testConsumer) = coupledProducerConsumer stream
          message = Message "keepAliveProducerProduces"
      result <- newEmptyMVar

      p <- keepAliveProducer testProducer
      produce p [message]
      recordMatching testConsumer message result

      waitFor result message (killSocket' p)

keepAliveProducerReconnects :: Spec
keepAliveProducerReconnects = it "reconnects after the socket is closed" $ do
      let stream = Stream (Topic "keep_alive_producer_reconnects_to_closed_sockets") (Partition 0)
          message = Message "keep_alive_producer_should_receive_this_after_closing_socket"
          (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- newEmptyMVar
      p <- keepAliveProducer testProducer

      killSocket' p

      recordMatching testConsumer message result
      produce p [message]

      waitFor result message (killSocket' p)


killSocket' :: KeepAliveProducer -> IO ()
killSocket' p = do
  r <- timeout 100000 $ takeMVar (kapSocket p)
  case r of
    (Just h) -> do
      hClose h
      putMVar (kapSocket p) h
    Nothing -> error "timed out whilst trying to kill the socket"

