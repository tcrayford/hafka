module Kafka.Types where
import Data.ByteString.Char8(ByteString)

newtype Topic = Topic ByteString
newtype Partition = Partition Int
newtype Offset = Offset Int

newtype Message = Message ByteString deriving (Show, Eq)

newtype RequestType = RequestType Int

produceRequestType, fetchRequestType :: RequestType
produceRequestType = RequestType 0
fetchRequestType = RequestType 1
