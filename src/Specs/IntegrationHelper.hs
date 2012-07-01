module Specs.IntegrationHelper where
import Kafka.Producer
import Kafka.Consumer
import Kafka.Consumer.Basic
import Kafka.Types
import Control.Concurrent.MVar
import System.Timeout
import Control.Concurrent(forkIO, killThread, myThreadId)
import Control.Monad(void, when)

coupledProducerConsumer :: Stream -> (ProducerSettings, BasicConsumer)
coupledProducerConsumer s = (ProducerSettings s, BasicConsumer s $ Offset 0)

waitFor :: (Show a) => MVar a -> a -> IO () -> IO ()
waitFor result message finalizer = do
  f <- timeout 100000 $ takeMVar result
  finalizer
  case f of
    (Just _) -> return ()
    Nothing -> error $ "timed out waiting for " ++ show message ++ "to be delivered"

killCurrent :: IO ()
killCurrent = myThreadId >>= killThread

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
