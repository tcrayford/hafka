module Kafka.Types where
import Data.ByteString.Char8(ByteString)

newtype Topic = Topic ByteString
newtype Partition = Partition Int
newtype Offset = Offset Int
