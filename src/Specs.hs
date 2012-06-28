{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Specs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Kafka.Producer
import Kafka.Consumer
import Kafka.Consumer.KeepAlive
import Kafka.Types
import Kafka.Response
import Control.Concurrent.MVar
import qualified Data.ByteString.Char8 as B
import Test.QuickCheck.Monadic
import Specs.IntegrationHelper
import Specs.Kafka.KeepAlive
import Specs.Kafka.ParsingSpecs
import Control.Concurrent(forkIO)
import Data.Serialize.Put
import Network.Socket(sClose, sIsConnected)
import Kafka.Network
import Control.Monad
import System.Timeout
import Specs.Kafka.Arbitrary

main :: IO ()
main = hspec $
  describe "hafka" $ do
    parsingErrorCode
    reconnectingToClosedSocket
    parseConsumptionTest
    messageProperties
    integrationTest

integrationTest :: Spec
integrationTest = 
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

      run $ waitFor result ("timed out waiting for " ++ show message ++ " to be delivered") (return ())

deliversWhenProducingMultipleMessages :: Stream -> Message -> Message -> Property
deliversWhenProducingMultipleMessages stream m1 m2 = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      run $ produce testProducer [m1, m2]
      run $ recordMatching testConsumer m2 result

      run $ waitFor result ("timed out waiting for " ++ show m2 ++ " to be delivered") (return ())

consumesWithKeepAlive :: Stream -> Message -> Property
consumesWithKeepAlive stream message = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      run $ produce testProducer [message]
      c <- run (keepAlive testConsumer)
      run $ recordMatching c message result

      run $ waitFor result ("timed out waiting for " ++ show message ++ " to be delivered") (killSocket c)

keepAliveReconectsToClosedSockets :: Stream -> Message -> Property
keepAliveReconectsToClosedSockets stream message = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      c <- run (keepAlive testConsumer)
      run $ recordMatching c message result
      run $ killSocket c

      run $ produce testProducer [message]

      run $ waitFor result ("timed out waiting for " ++ show message ++ " to be delivered") (killSocket c)

keepAliveConsumesMultipleMessages :: Stream -> Message -> Message -> Property
keepAliveConsumesMultipleMessages stream m1 m2 = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      c <- run (keepAlive testConsumer)
      run $ produce testProducer [m1, m2]
      run $ recordMatching c m2 result

      run $ waitFor result ("timed out waiting for " ++ show m2 ++ " to be delivered") (return ())

killSocket :: KeepAliveConsumer -> IO ()
killSocket c = do
  r <- timeout 100000 $ takeMVar (kaSocket c)
  case r of
    (Just s) -> do
      sClose s
      putMVar (kaSocket c) s
    Nothing -> error "timed out whilst trying to kill the socket"


recordMatching :: (Consumer c) => c -> Message -> MVar Message -> IO ()
recordMatching c original r = do
  _ <- forkIO $ consumeLoop c go
  return ()

  where
    go :: Message -> IO ()
    go message = when (original == message) $ finish message
    finish :: Message -> IO ()
    finish message = do
              putMVar r message
              killCurrent

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
