{-# LANGUAGE OverloadedStrings #-}
module Specs.Kafka.KeepAlive where
import Kafka.Consumer
import Kafka.Consumer.Basic
import Kafka.Consumer.KeepAlive
import Kafka.Types
import Test.HUnit
import Test.Hspec.HUnit()
import Test.Hspec.Monadic

parseConsumptionTest :: Spec
parseConsumptionTest = describe "parseConsumption" $ do
  let parser bs c = ([Message bs], c)
  it "finds no messages when there is a parse error" $ do
    c <- aKeepAliveConsumer
    (r, _) <- parseConsumption (Left Unknown) c parser
    r @?= []

  it "parses the found messages if parse succeeds" $ do
    c <- aKeepAliveConsumer
    (r, _) <- parseConsumption (Right "an message") c parser
    r @?= [Message "an message"]

aKeepAliveConsumer :: IO KeepAliveConsumer
aKeepAliveConsumer = do
  let c = BasicConsumer (Stream (Topic "test") (Partition 0)) (Offset 0)
  keepAlive c

