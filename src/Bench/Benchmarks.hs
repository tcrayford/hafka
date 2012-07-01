{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Bench.Benchmarks where
import Control.Concurrent.MVar
import Control.DeepSeq
import Control.Exception(evaluate)
import Control.Monad.Trans(liftIO)
import Control.Monad(forM_)
import Criterion.Config(defaultConfig)
import Criterion.Main
import Kafka.Consumer
import Kafka.Consumer.Basic
import Kafka.Consumer.KeepAlive
import Kafka.Producer
import Kafka.Producer.KeepAlive
import Kafka.Types
import Specs.IntegrationHelper
import Specs.Kafka.EndToEnd.KeepAliveSpecs
import Specs.Kafka.EndToEnd.KeepAliveProducerSpecs
import qualified Data.ByteString.Char8 as B

instance NFData B.ByteString

main :: IO ()
main = do
  let p10 = rawMessage 10
      p100 = rawMessage 100
      c10 = rawMessageSet 10
      c100 = rawMessageSet 100
      producerSettings = ProducerSettings (Stream (Topic "test") (Partition 0))
      consumerSettings = BasicConsumer (Stream (Topic "test") (Partition 0)) (Offset 0)
      eval = liftIO . evaluate $ rnf [p10, p100, c10, c100]

  defaultMainWith defaultConfig eval [
      bgroup "produce" [
          bench "10p" $ whnf (fullProduceRequest producerSettings) [Message p10]
        , bench "100p" $ whnf (fullProduceRequest producerSettings) [Message p100]
      ],
      bgroup "consume" [
          bench "10c" $ whnf (parseMessageSet c10) consumerSettings
        , bench "100c" $ whnf (parseMessageSet c100) consumerSettings
      ],

      bgroup "consumerRoundTrip" [
          bench "roundtripBasicConsumer" roundtripBasicConsumer
        , bench "roundtripKeepAliveConsumer" roundtripKeepAliveConsumer
      ],

      bgroup "producerRoundTrip" [
          bench "roundtripBasicProducer" roundtripBasicProducer
        , bench "roundtripKeepAliveProducer" roundtripKeepAliveProducer
      ]
    ]


rawMessage :: Int -> B.ByteString
rawMessage n = B.pack . take n $ repeat 'a'

rawMessageSet :: Int -> B.ByteString
rawMessageSet n = putMessage message
  where message = Message $! B.pack . take n $ repeat 'a'

roundtripBasicConsumer :: IO ()
roundtripBasicConsumer = roundtripBasics "bench_basic_consumer_roundtrip"

roundtripKeepAliveConsumer :: IO ()
roundtripKeepAliveConsumer = do
  let stream = Stream (Topic "bench_keep_alive_consumer_roundtrip") (Partition 0)
      (testProducer, testConsumer) = coupledProducerConsumer stream
      messages = messagesWithPrefix "benchKeepAliveConsumerRoundTrip"

  c <- keepAlive testConsumer
  result <- newEmptyMVar
  forM_ messages (\m -> produce testProducer [m])
  recordMatching c (last messages) result

  waitFor result (last messages) (killSocket c)

roundtripBasicProducer :: IO ()
roundtripBasicProducer = roundtripBasics "bench_basic_producer_roundtrip"

roundtripBasics :: B.ByteString -> IO ()
roundtripBasics t = do
  let stream = Stream (Topic t) (Partition 0)
      (testProducer, testConsumer) = coupledProducerConsumer stream
      messages = messagesWithPrefix t

  result <- newEmptyMVar
  forM_ messages (\m -> produce testProducer [m])
  recordMatching testConsumer (last messages) result

  waitFor result (last messages) (return ())

roundtripKeepAliveProducer :: IO ()
roundtripKeepAliveProducer = do
  let stream = Stream (Topic "bench_roundtrip_keep_alive_producer") (Partition 0)
      (testProducer, testConsumer) = coupledProducerConsumer stream
      messages = messagesWithPrefix "benchKeepAliveProducerRoundTrip"

  p <- keepAliveProducer testProducer
  result <- newEmptyMVar
  forM_ messages (\m -> produce p [m])
  recordMatching testConsumer (last messages) result

  waitFor result (last messages) (killSocket' p)

messagesWithPrefix :: B.ByteString -> [Message]
messagesWithPrefix prefix = map f . take 2 $ range
  where f n = Message (prefix `B.append` B.pack (show n))
        range :: [Int]
        range = [0..]

