{-# LANGUAGE OverloadedStrings #-}
module Specs.Kafka.Unit.ProducerSpecs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Kafka.Types
import Kafka.Producer

producerSpecs = describe "producer" $ do
  it "the stream length is always 6 + length of topic" $ do
    6 + 4 @?= streamLength (Stream (Topic "1234") (Partition 0))
  
