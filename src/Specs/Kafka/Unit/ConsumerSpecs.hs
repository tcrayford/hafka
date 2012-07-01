{-# LANGUAGE OverloadedStrings #-}
module Specs.Kafka.Unit.ConsumerSpecs where
import Kafka.Consumer
import Kafka.Types
import Specs.Kafka.Arbitrary()
import Test.HUnit
import Test.Hspec.Monadic
import Test.Hspec.QuickCheck
import qualified Data.ByteString.Char8 as B

consumerSpecs = describe "general purpose consumers" $ do
  describe "dropErrorCode" $ do
    it "drops the code if it is successful" $
      dropErrorCode Success "12remaining" @=? Right "remaining"

    it "throws the code if the response is bad" $
      dropErrorCode Unknown "1234" @=? Left Unknown
      
  describe "bSplice" $
    prop "bSplice with 0 and the length is id" $
      \m -> bSplice 0 (B.length m) m == m

