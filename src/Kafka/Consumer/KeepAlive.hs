module Kafka.Consumer.KeepAlive where
import Kafka.Consumer
import Kafka.Parsing
import Kafka.Response
import Kafka.Types
import Kafka.Network
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import qualified Data.ByteString.Char8 as B
import Data.ByteString.Char8(ByteString)
import Data.Serialize.Get
import Control.Concurrent.MVar

data KeepAliveConsumer = KeepAliveConsumer {
    kaConsumer :: BasicConsumer
  , kaSocket :: MVar Socket
  }

instance Consumer KeepAliveConsumer where
  consume c = do
    newC <- withReconnected c
    result <- withSocket newC (\s -> do
        _ <- send s $ consumeRequest newC
        readDataResponse' s dropErrorCode
      )
    parseConsumption result newC parseMessageSet

  getOffset c = getOffset $ kaConsumer c
  getStream c = getStream $ kaConsumer c

  increaseOffsetBy c n = c { kaConsumer = newC }
    where newC = increaseOffsetBy (kaConsumer c) n

type Response = Either ErrorCode ByteString
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

reconnectSocket :: Socket -> IO Socket
reconnectSocket s = do
  c <- sIsConnected s
  if c then
    return s
  else
    connectToKafka

connectToKafka :: IO Socket
connectToKafka = connectTo "localhost" $ PortNumber 9092

type RawConsumeResponseHandler = (ErrorCode -> ByteString -> IO (Either ErrorCode ByteString))

type ByteReader = (Int -> IO ByteString)

socketByteReader :: Socket -> Int -> IO ByteString
socketByteReader = recvFrom'

readDataResponse' :: Socket -> RawConsumeResponseHandler  -> IO (Either ErrorCode ByteString)
readDataResponse' s handler = do
  d <- recvFrom' s 4

  let dataLength = getDataLength' d
  rawResponse <- recvFrom' s dataLength
  let x = parseErrorCode rawResponse
  handler x rawResponse

getDataLength' :: ByteString -> Int
getDataLength' d = forceEither "getDataLength'" $ runGet getDataLength d

dropErrorCode :: RawConsumeResponseHandler
dropErrorCode x rawResponse = case x of
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

