{-# LANGUAGE OverloadedStrings #-}
module Specs.Kafka.EndToEnd.KeepAliveSpecs where
import Control.Concurrent.MVar
import Control.Monad
import Kafka.Consumer
import Kafka.Consumer.KeepAlive
import Kafka.Producer
import Kafka.Types
import Network.Socket(sClose)
import Specs.IntegrationHelper
import System.Timeout
import Test.HUnit
import Test.Hspec.Monadic
import Test.QuickCheck
import Test.QuickCheck.Monadic

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

keepAliveConsumesMultipleMessages :: Stream -> Message -> Message -> Property
keepAliveConsumesMultipleMessages stream m1 m2 = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar
      c <- run (keepAlive testConsumer)

      run $ produce testProducer [m1, m2]
      run $ recordMatching c m2 result

      run $ waitFor result m2 (return ())

recordMatching :: (Consumer c) => c -> Message -> MVar Message -> IO ()
recordMatching c original r = forkIO' $ consumeLoop c go
  where
    go :: Message -> IO ()
    go message = when (original == message) $ finish message
    finish :: Message -> IO ()
    finish message = do
              putMVar r message
              killCurrent

killSocket :: KeepAliveConsumer -> IO ()
killSocket c = do
  r <- timeout 100000 $ takeMVar (kaSocket c)
  case r of
    (Just s) -> do
      sClose s
      putMVar (kaSocket c) s
    Nothing -> error "timed out whilst trying to kill the socket"

