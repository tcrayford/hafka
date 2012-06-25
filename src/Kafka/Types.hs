module Kafka.Types where
import Data.ByteString.Char8(ByteString)

newtype Topic = Topic ByteString deriving (Show, Eq)
newtype Partition = Partition Int deriving (Show, Eq)
newtype Offset = Offset Int deriving (Show, Eq)

newtype Message = Message ByteString deriving (Show, Eq)

newtype RequestType = RequestType Int

produceRequestType, fetchRequestType :: RequestType
produceRequestType = RequestType 0
fetchRequestType = RequestType 1

data ErrorCode = Unknown | Success | OffsetOutOfRange | InvalidMessage | WrongPartition | InvalidFetchSize
