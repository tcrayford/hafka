module Network.Kafka.Response where
import Network.Kafka.Types
import Data.Serialize.Get
import Network.Kafka.Parsing
import Data.ByteString.Char8(ByteString)

parseErrorCode :: ByteString -> ErrorCode
parseErrorCode raw = forceEither "parseErrorCode" $ runGet' raw $ do
  c <- getWord16be
  return $! lookupErrorCode c

forceMaybe :: Maybe ErrorCode -> ErrorCode
forceMaybe (Just a) = a
forceMaybe Nothing = Unknown

lookupErrorCode :: (Eq a, Num a) => a -> ErrorCode
lookupErrorCode c = forceMaybe $ lookup c [
    (-1, Unknown),
    (0, Success),
    (1, OffsetOutOfRange),
    (2, InvalidMessage),
    (3, WrongPartition),
    (4, InvalidFetchSize)
  ]

