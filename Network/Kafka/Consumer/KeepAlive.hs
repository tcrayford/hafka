module Network.Kafka.Consumer.KeepAlive where
import Control.Concurrent.MVar
import Data.ByteString.Char8(ByteString)
import qualified Data.ByteString.Char8 as B
import Network.Kafka.Consumer
import Network.Kafka.Consumer.ByteReader
import Network.Kafka.Consumer.Basic
import Network.Kafka.Connection
import Network.Kafka.Types
import System.IO

data KeepAliveConsumer = KeepAliveConsumer {
    kaConsumer :: BasicConsumer
  , kaSocket :: MVar Handle
  }

instance Consumer KeepAliveConsumer where
  consume c = do
    newC <- withReconnected c
    result <- withSocket newC (\h -> do
        _ <- B.hPut h $ consumeRequest newC
        hFlush h
        readDataResponse (handleByteReader h) dropErrorCode
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

withSocket :: KeepAliveConsumer -> (Handle -> IO a) -> IO a
withSocket c = withMVar (kaSocket c)

withReconnected :: KeepAliveConsumer -> IO KeepAliveConsumer
withReconnected c = do
  s <- takeMVar $ kaSocket c
  x <- hIsOpen s
  if x then do
    putMVar (kaSocket c) s
    return $! c
  else do
    s' <- reconnectSocket s
    putMVar (kaSocket c) s'
    return $! c

keepAlive :: BasicConsumer -> IO KeepAliveConsumer
keepAlive c = do
  s <- connectToKafka >>= newMVar
  return $! KeepAliveConsumer c s

