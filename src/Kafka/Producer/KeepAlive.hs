module Kafka.Producer.KeepAlive where
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import Kafka.Producer
import Kafka.Network
import Control.Concurrent.MVar

data KeepAliveProducer = KeepAliveProducer {
      kapSocket :: MVar Socket
    , kapSettings :: ProducerSettings
  }

instance Producer KeepAliveProducer where
  produce p messages = do
    newP <- maybeReconnect p
    s <- takeMVar (kapSocket newP)
    send s $ fullProduceRequest (kapSettings newP) messages
    putMVar (kapSocket newP) s

maybeReconnect :: KeepAliveProducer -> IO KeepAliveProducer
maybeReconnect p = do
  s <- takeMVar (kapSocket p)
  isConnected <- sIsConnected s
  case isConnected of
    True -> do
      putMVar (kapSocket p) s
      return $! p
    False -> do
      s' <- reconnectSocket s
      putMVar (kapSocket p) s'
      return $! p

keepAliveProducer :: ProducerSettings -> IO KeepAliveProducer
keepAliveProducer p = do
  s <- connectToKafka >>= newMVar
  return $! KeepAliveProducer s p

