module Network.Kafka.Connection(
    N.PortID(..)
  , N.connectTo
  , connectToKafka
  , reconnectSocket
  ) where
import Network.BSD
import qualified Network as N
import System.IO
import qualified Control.Exception as Exception

connectToKafka :: IO Handle
connectToKafka = N.connectTo "localhost" $ N.PortNumber 9092

reconnectSocket :: Handle -> IO Handle
reconnectSocket h = do
  c <- hIsOpen h
  if c then
    return h
  else
    connectToKafka

