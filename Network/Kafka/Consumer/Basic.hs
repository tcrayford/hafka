module Network.Kafka.Consumer.Basic where
import Network.Kafka.Consumer
import Network.Kafka.Consumer.ByteReader
import Network.Kafka.Types
import Network
import System.IO
import qualified Data.ByteString.Char8 as B

data BasicConsumer = BasicConsumer {
    cStream :: Stream
  , cOffset :: Offset
  }

instance Consumer BasicConsumer where
  consume c = do
    result <- getFetchData c
    case result of
      (Right r) -> return $! parseMessageSet r c
      (Left r) -> do 
        print ("error parsing response: " ++ show r)
        return ([], c)
  getOffset (BasicConsumer _ o) = o
  getStream (BasicConsumer s _) = s
  increaseOffsetBy settings increment = settings { cOffset = newOffset }
    where newOffset = Offset (current + increment)
          (Offset current) = cOffset settings

getFetchData :: (Consumer c) => c -> IO Response
getFetchData a = do
  h <- connectTo "localhost" $ PortNumber 9092
  B.hPut h $ consumeRequest a
  hFlush h
  res <- readDataResponse (handleByteReader h) dropErrorCode
  hClose h
  return res

