{-# LANGUAGE OverloadedStrings #-}
import Criterion.Main
import qualified Data.ByteString.Char8 as B
import Kafka.Producer
import Kafka.Consumer
import Kafka.Consumer.Basic
import Kafka.Types
import Control.Exception(evaluate)
import Control.Monad.Trans(liftIO)
import Criterion.Config(defaultConfig)
import Control.DeepSeq

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
      ]
    ]

rawMessage n = B.pack . take n $ repeat 'a'

rawMessageSet n = putMessage message
  where message = Message $! B.pack . take n $ repeat 'a'
