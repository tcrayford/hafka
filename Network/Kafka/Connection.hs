{-# LANGUAGE DoAndIfThenElse #-}
module Network.Kafka.Connection(
    N.PortID(..)
  , N.connectTo
  , connectToKafka
  , reconnectSocket
  ) where
import qualified Network as N
import System.IO

connectToKafka :: IO Handle
connectToKafka = N.connectTo "localhost" $ N.PortNumber 9092

reconnectSocket :: Handle -> IO Handle
reconnectSocket h = do
  c <- hIsOpen h
  if c then
    return h
  else
    connectToKafka

