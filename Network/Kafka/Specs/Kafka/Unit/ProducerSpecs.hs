{-# LANGUAGE OverloadedStrings #-}
module Network.Kafka.Specs.Kafka.Unit.ProducerSpecs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Network.Kafka.Types
import Network.Kafka.Producer

producerSpecs ::Spec
producerSpecs = describe "producer" $
  it "the stream length is always 6 + length of topic" $
    6 + 4 @?= streamLength (Stream (Topic "1234") (Partition 0))
  
