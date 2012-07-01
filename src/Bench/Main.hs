{-# LANGUAGE OverloadedStrings #-}
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
import Kafka.Types
import Specs.IntegrationHelper
import Specs.Kafka.EndToEnd.KeepAliveSpecs
import qualified Data.ByteString.Char8 as B

instance NFData B.ByteString

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
          bench "10p" $ whnf (fullProduceRequest producerSettings) $ [Message p10]
        , bench "100p" $ whnf (fullProduceRequest producerSettings) $ [Message p100]
      ],
      bgroup "consume" [
          bench "10c" $ whnf (parseMessageSet c10) consumerSettings
        , bench "100c" $ whnf (parseMessageSet c100) consumerSettings
      ],

      bgroup "roundtrip" [
          bench "roundtripBasicConsumer" $ roundtripBasicConsumer
        , bench "roundtripKeepAliveConsumer" $ roundtripKeepAliveConsumer
      ]
    ]

rawMessage n = B.pack . take n $ repeat 'a'

rawMessageSet n = putMessage message
  where message = Message $! B.pack . take n $ repeat 'a'

roundtripBasicConsumer :: IO ()
roundtripBasicConsumer = do
  let stream = Stream (Topic "bench_basic_consumer_roundtrip") (Partition 0)
      (testProducer, testConsumer) = coupledProducerConsumer stream
      messages = messagesWithPrefix "benchBasicConsumerRoundTrip"

  result <- newEmptyMVar
  forM_ messages (\m -> produce testProducer [m])
  recordMatching testConsumer (last messages) result

  waitFor result (last messages) (return ())

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

messagesWithPrefix :: B.ByteString -> [Message]
messagesWithPrefix prefix = map f . take 100 $ [0..]
  where f n = Message (prefix `B.append` (B.pack $ show n))

