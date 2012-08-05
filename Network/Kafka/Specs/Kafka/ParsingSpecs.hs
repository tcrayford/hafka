{-# LANGUAGE OverloadedStrings #-}
module Network.Kafka.Specs.Kafka.ParsingSpecs where
import Data.Serialize.Put
import Network.Kafka.Consumer
import Network.Kafka.Consumer.Basic
import Network.Kafka.Producer
import Network.Kafka.Response
import Network.Kafka.Types
import Network.Kafka.Specs.Kafka.Arbitrary()
import Test.HUnit
import Test.Hspec.HUnit()
import Test.Hspec.Monadic
import Test.Hspec.QuickCheck
import qualified Data.ByteString.Char8 as B

messageProperties :: Spec
messageProperties = describe "the client" $ do
  let stream = Stream (Topic "ignored topic") (Partition 0)
      c = BasicConsumer stream (Offset 0)
  prop "serialize -> deserialize is id" $
    \message -> parseMessage (runPut $ putMessage message) == message

  it "parsing empty message set gives empty list" $
    fst (parseMessageSet "" c) @?= []

  it "parsing the empty message set does not change the offset" $
    getOffset (snd $ parseMessageSet "" c) @?= Offset 0

  prop "serialized message length is 1 + 4 + n" $
    \message@(Message raw) -> parseMessageSize 0 (runPut $ putMessage message) == 1 + 4 + B.length raw

parsingErrorCode :: Spec
parsingErrorCode = describe "the client" $
  it "parses an error code" $ do
    let b = putErrorCode 4
    parseErrorCode b @?= InvalidFetchSize

putErrorCode :: Int -> B.ByteString
putErrorCode code = runPut $ putWord16be $ fromIntegral code
