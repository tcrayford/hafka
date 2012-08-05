module Network.Kafka.Consumer.ByteReader where
import Data.ByteString.Char8(ByteString)
import qualified Data.ByteString.Char8 as B
import System.IO(Handle)

type ByteReader = (Int -> IO ByteString)

handleByteReader :: Handle -> ByteReader
handleByteReader = B.hGet

