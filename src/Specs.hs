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
import Control.Concurrent(forkIO)
import Control.Monad(when)
import Data.Serialize.Put
import Network.Socket(sClose, sIsConnected)
import Kafka.Network
import Control.Monad

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

produceToConsume :: Stream -> Message -> Property
produceToConsume stream message = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      run $ produce testProducer [message]
      run $ recordMatching testConsumer message result

      run $ waitFor result ("timed out waiting for " ++ show message ++ " to be delivered")

deliversWhenProducingMultipleMessages :: Stream -> Message -> Message -> Property
deliversWhenProducingMultipleMessages stream m1 m2 = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      run $ produce testProducer [m1, m2]
      run $ recordMatching testConsumer m2 result

      run $ waitFor result ("timed out waiting for " ++ show m2 ++ " to be delivered")

consumesWithKeepAlive :: Stream -> Message -> Property
consumesWithKeepAlive stream message = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      run $ produce testProducer [message]
      c <- run (keepAlive testConsumer)
      run $ recordMatching c message result

      run $ waitFor result ("timed out waiting for " ++ show message ++ " to be delivered")

keepAliveReconectsToClosedSockets :: Stream -> Message -> Property
keepAliveReconectsToClosedSockets stream message = monadicIO $ do
      let (testProducer, testConsumer) = coupledProducerConsumer stream
      result <- run newEmptyMVar

      c <- run (keepAlive testConsumer)
      run $ recordMatching c message result
      s <- run $ takeMVar (kaSocket c)
      run $ sClose s
      run $ putMVar (kaSocket c) s

      run $ produce testProducer [message]

      run $ waitFor result ("timed out waiting for " ++ show message ++ " to be delivered")

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

messageProperties :: Spec
messageProperties = describe "the client" $ do
  prop "serialize -> deserialize is id" $
    \message -> parseMessage (putMessage message) == message

  prop "serialized message length is 1 + 4 + n" $
    \message@(Message raw) -> parseMessageSize 0 (putMessage message) == 1 + 4 + B.length raw

parsingErrorCode :: Spec
parsingErrorCode = describe "the client" $
  it "parses an error code" $ do
    let b = putErrorCode 4
    parseErrorCode b @?= InvalidFetchSize

putErrorCode :: Int -> B.ByteString
putErrorCode code = runPut $ putWord16be $ fromIntegral code

reconnectingToClosedSocket :: Spec
reconnectingToClosedSocket = describe "reconnectSocket" $ do
  it "reconnects a closed socket" $ do
    s <- connectTo "localhost" $ PortNumber 9092
    sClose s
    s2 <- reconnectSocket s
    c <- sIsConnected s2
    (c @?= True)

parseConsumptionTest :: Spec
parseConsumptionTest = describe "parseConsumption" $ do
  let parser bs c = ([Message bs], c)
  it "finds no messages when there is a parse error" $ do
    c <- aKeepAliveConsumer
    (r, a) <- parseConsumption (Left Unknown) c parser
    r @?= []

  it "parses the found messages if parse succeeds" $ do
    c <- aKeepAliveConsumer
    (r, a) <- parseConsumption (Right "an message") c parser
    r @?= [Message "an message"]

aKeepAliveConsumer = do
  let c = BasicConsumer (Stream (Topic "test") (Partition 0)) (Offset 0)
  keepAlive c

instance Arbitrary Partition where
  arbitrary = do
    a <- elements [0..5]
    return $ Partition a

instance Arbitrary Topic where
  arbitrary = do
    a <- nonEmptyString
    return $ Topic $ B.pack a

instance Arbitrary Stream where
  arbitrary = do
    t <- arbitrary
    p <- arbitrary
    return $! Stream t p

instance Arbitrary Message where
  arbitrary = do
    a <- nonEmptyString
    return $ Message (B.pack a)

nonEmptyString :: Gen String
nonEmptyString = suchThat (listOf $ elements ['a'..'z']) (not . null)

-- higher level api? typeclasses for Produceable, Consumeable?
-- put the basic consumer in Kafka.Consumer.Basic
