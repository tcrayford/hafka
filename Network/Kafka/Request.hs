module Kafka.Request where
import Data.Serialize.Put
import Kafka.Types
import qualified Data.ByteString.Char8 as B

putStream :: Stream -> Put
putStream (Stream t p) = do
  putTopic t
  putPartition p
  
putTopic :: Topic -> Put
putTopic (Topic t)  = do
  putWord16be . fromIntegral $ B.length t
  putByteString t

putPartition :: Partition -> Put
putPartition (Partition p) = putWord32be $ fromIntegral p
