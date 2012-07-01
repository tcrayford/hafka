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
    s <- takeMVar (kapSocket p)
    send s $ fullProduceRequest (kapSettings p) messages
    putMVar (kapSocket p) s

keepAliveProducer :: ProducerSettings -> IO KeepAliveProducer
keepAliveProducer p = do
  s <- connectToKafka >>= newMVar
  return $! KeepAliveProducer s p
