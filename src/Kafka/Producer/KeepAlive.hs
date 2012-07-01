module Kafka.Producer.KeepAlive where
import Kafka.Producer
import Kafka.Network
import Control.Concurrent.MVar
import System.IO
import qualified Data.ByteString.Char8 as B

data KeepAliveProducer = KeepAliveProducer {
      kapSocket :: MVar Handle
    , kapSettings :: ProducerSettings
  }

instance Producer KeepAliveProducer where
  produce p messages = do
    newP <- maybeReconnect p
    h <- takeMVar (kapSocket newP)
    B.hPut h $ fullProduceRequest (kapSettings newP) messages
    hFlush h
    putMVar (kapSocket newP) h

maybeReconnect :: KeepAliveProducer -> IO KeepAliveProducer
maybeReconnect p = do
  s <- takeMVar (kapSocket p)
  isConnected <- hIsOpen s
  if isConnected then do
      putMVar (kapSocket p) s
      return $! p
    else do
      s' <- reconnectSocket s
      putMVar (kapSocket p) s'
      return $! p

keepAliveProducer :: ProducerSettings -> IO KeepAliveProducer
keepAliveProducer p = do
  s <- connectToKafka >>= newMVar
  return $! KeepAliveProducer s p

