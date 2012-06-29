module Kafka.Consumer.ByteReader where
import Network.Socket(Socket)
import Network.Socket.ByteString(recvFrom)
import Data.ByteString.Char8(ByteString)
import qualified Data.ByteString.Char8 as B
import System.IO(Handle)

type ByteReader = (Int -> IO ByteString)

socketByteReader :: Socket -> ByteReader
socketByteReader = recvFrom'

handleByteReader :: Handle -> ByteReader
handleByteReader = B.hGet

recvFrom' :: Socket -> Int -> IO ByteString
recvFrom' s n = do
  (d, _) <- recvFrom s n
  return d

