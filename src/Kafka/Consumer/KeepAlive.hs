module Kafka.Consumer.KeepAlive where
import Kafka.Consumer
import Kafka.Parsing
import Kafka.Response
import Kafka.Types
import Kafka.Network
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import System.IO
import qualified Data.ByteString.Char8 as B
import Data.ByteString.Char8(ByteString)
import Data.Serialize.Get

data KeepAliveConsumer = KeepAliveConsumer {
    kaConsumer :: BasicConsumer
  , kaSocket :: Socket
  }

instance Consumer KeepAliveConsumer where
  consume c = do
    newC <- withReconnected c
    let s = kaSocket c
    send s $ consumeRequest newC
    result <- readDataResponse' s
    case result of
      (Right r) -> return $! parseMessageSet r newC
      (Left r) -> do 
        print ("error parsing response: " ++ show r)
        return ([], newC)

  getOffset c = getOffset $ kaConsumer c
  getStream c = getStream $ kaConsumer c

  increaseOffsetBy c n = c { kaConsumer = newC }
    where newC = increaseOffsetBy (kaConsumer c) n

withReconnected :: KeepAliveConsumer -> IO KeepAliveConsumer
withReconnected c = do
  s <- reconnectSocket $ kaSocket c
  return $! c { kaSocket = s }

reconnectSocket :: Socket -> IO Socket
reconnectSocket s = do
  c <- sIsConnected s
  if c then
    return s
  else
    connectToKafka

connectToKafka :: IO Socket
connectToKafka = connectTo "localhost" $ PortNumber 9092

readDataResponse' :: Socket -> IO (Either ErrorCode ByteString)
readDataResponse' s = do
  d <- recv s 4096
  let (Right dataLength) = runGet getDataLength d
      rawResponse = B.take dataLength (B.drop 4 d)
      x = parseErrorCode rawResponse
  case x of
    Success -> return $! Right $ B.drop 2 rawResponse
    e -> return $! Left e

keepAlive :: BasicConsumer -> IO KeepAliveConsumer
keepAlive c = do
  s <- connectToKafka
  return $! KeepAliveConsumer c s

