module Kafka.Consumer.KeepAlive where
import Control.Concurrent.MVar
import Data.ByteString.Char8(ByteString)
import Kafka.Consumer
import Kafka.Consumer.ByteReader
import Kafka.Consumer.Basic
import Kafka.Network
import Kafka.Types
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString

data KeepAliveConsumer = KeepAliveConsumer {
    kaConsumer :: BasicConsumer
  , kaSocket :: MVar Socket
  }

instance Consumer KeepAliveConsumer where
  consume c = do
    newC <- withReconnected c
    result <- withSocket newC (\s -> do
        _ <- send s $ consumeRequest newC
        readDataResponse (socketByteReader s) dropErrorCode
      )
    parseConsumption result newC parseMessageSet

  getOffset c = getOffset $ kaConsumer c
  getStream c = getStream $ kaConsumer c

  increaseOffsetBy c n = c { kaConsumer = newC }
    where newC = increaseOffsetBy (kaConsumer c) n

type MessageSetParser = (ByteString -> KeepAliveConsumer -> ([Message], KeepAliveConsumer))

parseConsumption :: Response -> KeepAliveConsumer -> MessageSetParser -> IO ([Message], KeepAliveConsumer)
parseConsumption result newC f = case result of
    (Right r) -> return $! f r newC
    (Left r) -> do
      print ("error parsing response: " ++ show r)
      return ([], newC)

withSocket :: KeepAliveConsumer -> (Socket -> IO a) -> IO a
withSocket c = withMVar (kaSocket c)

withReconnected :: KeepAliveConsumer -> IO KeepAliveConsumer
withReconnected c = do
  s <- takeMVar $ kaSocket c
  s' <- reconnectSocket s
  putMVar (kaSocket c) s'
  return $! c

keepAlive :: BasicConsumer -> IO KeepAliveConsumer
keepAlive c = do
  s <- connectToKafka >>= newMVar
  return $! KeepAliveConsumer c s

