module Kafka.Consumer.KeepAlive where
import Kafka.Consumer
import Kafka.Network
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import System.IO

data KeepAliveConsumer = KeepAliveConsumer {
    kaConsumer :: BasicConsumer
  , kaSocket :: Socket
  }

instance Consumer KeepAliveConsumer where
  consume c = do
    let s = kaSocket c
    result <- readDataResponse h
    case result of
      (Right r) -> return $! parseMessageSet r c
      (Left r) -> do 
        print ("error parsing response: " ++ show r)
        return ([], c)
    
  getOffset c = getOffset $ kaConsumer c
  getStream c = getStream $ kaConsumer c
  increaseOffsetBy c n = c { kaConsumer = newC }
    where newC = increaseOffsetBy (kaConsumer c) n

keepAlive :: BasicConsumer -> IO KeepAliveConsumer
keepAlive c = do
  s <- connectTo "localhost" $ PortNumber 9092
  return $! KeepAliveConsumer c s

