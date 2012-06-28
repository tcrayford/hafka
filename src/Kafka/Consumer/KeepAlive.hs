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
import Control.Concurrent.MVar
import Control.Monad(liftM)
import Debug.Trace

data KeepAliveConsumer = KeepAliveConsumer {
    kaConsumer :: BasicConsumer
  , kaSocket :: MVar Socket
  }

instance Consumer KeepAliveConsumer where
  consume c = do
    newC <- withReconnected c
    result <- withSocket newC (\s -> do
        send s $ consumeRequest newC
        readDataResponse' s
      )
    parseConsumption result newC parseMessageSet

  getOffset c = getOffset $ kaConsumer c
  getStream c = getStream $ kaConsumer c

  increaseOffsetBy c n = c { kaConsumer = newC }
    where newC = increaseOffsetBy (kaConsumer c) n

parseConsumption :: (Either ErrorCode ByteString) -> KeepAliveConsumer -> (ByteString -> KeepAliveConsumer -> ([Message], KeepAliveConsumer)) -> IO ([Message], KeepAliveConsumer)
parseConsumption result newC f = do
  case result of
    (Right r) -> return $! f r newC
    (Left r) -> do
      print ("error parsing response: " ++ show r)
      return ([], newC)

withSocket :: KeepAliveConsumer -> (Socket -> IO a) -> IO a
withSocket c f = withMVar (kaSocket c) f

withReconnected :: KeepAliveConsumer -> IO KeepAliveConsumer
withReconnected c = do
  s <- takeMVar $ kaSocket c
  s' <- reconnectSocket s
  putMVar (kaSocket c) s'
  return $! c

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
  d <- recvFrom' s 4
  let (Right dataLength) = runGet getDataLength d
  rawResponse <- recvFrom' s dataLength
  let x = parseErrorCode rawResponse
  case x of
    Success -> return $! Right $ B.drop 2 rawResponse
    e -> return $! Left e

recvFrom' :: Socket -> Int -> IO ByteString
recvFrom' s n = do 
  (d, _) <- recvFrom s n
  return d

keepAlive :: BasicConsumer -> IO KeepAliveConsumer
keepAlive c = do
  s <- connectToKafka >>= newMVar
  return $! KeepAliveConsumer c s

