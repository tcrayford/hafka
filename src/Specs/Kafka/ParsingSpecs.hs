{-# LANGUAGE OverloadedStrings #-}
module Specs.Kafka.ParsingSpecs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Kafka.Producer
import Kafka.Types
import Kafka.Response
import Kafka.Consumer
import qualified Data.ByteString.Char8 as B
import Data.Serialize.Put
import Specs.Kafka.Arbitrary

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
