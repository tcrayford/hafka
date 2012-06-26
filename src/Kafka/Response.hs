module Kafka.Response where
import Kafka.Types
import Data.ByteString.Char8
import Data.Serialize.Get
import Data.List(lookup)
import Kafka.Parsing

parseErrorCode :: ByteString -> ErrorCode
parseErrorCode raw = forceEither raw $ runGet' raw $ do
  c <- getWord16be
  return $! lookupErrorCode c

forceMaybe (Just a) = a
forceMaybe Nothing = Unknown

lookupErrorCode c = forceMaybe $ lookup c [
    (-1, Unknown),
    (0, Success),
    (1, OffsetOutOfRange),
    (2, InvalidMessage),
    (3, WrongPartition),
    (4, InvalidFetchSize)
  ]

