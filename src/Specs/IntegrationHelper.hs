module Specs.IntegrationHelper where
import Kafka.Producer
import Kafka.Consumer
import Kafka.Types
import Control.Concurrent.MVar
import System.Timeout
import Control.Concurrent(killThread, myThreadId)

coupledProducerConsumer :: Stream -> (ProducerSettings, BasicConsumer)
coupledProducerConsumer s = (ProducerSettings s, BasicConsumer s $ Offset 0)

waitFor :: MVar a -> String -> IO () -> IO ()
waitFor result message finalizer = do
  f <- timeout 1000000 $ takeMVar result
  finalizer
  case f of
    (Just _) -> return ()
    Nothing -> error message

killCurrent :: IO ()
killCurrent = myThreadId >>= killThread

