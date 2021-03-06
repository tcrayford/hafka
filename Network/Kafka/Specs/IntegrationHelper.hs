module Network.Kafka.Specs.IntegrationHelper where
import Network.Kafka.Producer
import Network.Kafka.Consumer
import Network.Kafka.Consumer.Basic
import Network.Kafka.Types
import Control.Concurrent.MVar
import System.Timeout
import Control.Concurrent(forkIO, killThread, myThreadId)
import Control.Monad(void, when)

coupledProducerConsumer :: Stream -> (ProducerSettings, BasicConsumer)
coupledProducerConsumer s = (ProducerSettings s, BasicConsumer s $ Offset 0)

waitFor :: (Show a) => MVar a -> a -> IO () -> IO ()
waitFor result message finalizer = do
  f <- timeout 1000000 $ takeMVar result
  finalizer
  case f of
    (Just _) -> return ()
    Nothing -> error $ "timed out waiting for " ++ show message ++ "to be delivered"

killCurrent :: IO ()
killCurrent = myThreadId >>= killThread

forkIO' :: IO () -> IO ()
forkIO' a = void $ forkIO a

recordMatching :: (Consumer c) => c -> Message -> MVar Message -> IO ()
recordMatching c original r = forkIO' $ consumeLoop c go
  where
    go :: Message -> IO ()
    go message = when (original == message) $ finish message
    finish :: Message -> IO ()
    finish message = do
              putMVar r message
              killCurrent
