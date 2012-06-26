{-# LANGUAGE OverloadedStrings #-}
module Bench where
import Criterion.Main
import qualified Data.ByteString.Char8 as B
import Kafka.Producer
import Kafka.Types

main = defaultMain [
    bgroup "produce" [
      bench "10" $ whnf (fullProduceRequest producerSettings) (Message $ rawMessage 10)
      , bench "100" $ whnf (fullProduceRequest producerSettings) (Message $ rawMessage 100)
    ]
  ]
  where
    producerSettings = ProducerSettings (Topic "test") (Partition 0)

rawMessage n = B.pack . take n $ repeat 'a'
