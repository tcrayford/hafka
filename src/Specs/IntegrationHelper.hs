module Specs.IntegrationHelper where
import Kafka.Producer
import Kafka.Consumer
import Kafka.Consumer.Basic
import Kafka.Types
import Control.Concurrent.MVar
import System.Timeout
import Control.Concurrent(forkIO, killThread, myThreadId)
import Control.Monad(void)

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

forkIO' a = void $ forkIO a

