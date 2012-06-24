module Specs.IntegrationHelper where
import Kafka.Producer
import Kafka.Consumer
import Kafka.Types
import Control.Concurrent.MVar
import System.Timeout
import Control.Concurrent(killThread, myThreadId)

coupledProducerConsumer :: Topic -> Partition -> (ProducerSettings, Consumer)
coupledProducerConsumer t p = (ProducerSettings t p, Consumer t p $ Offset 0)

waitFor :: MVar a -> String -> IO ()
waitFor result message = do
  f <- timeout 1000000 $ takeMVar result
  case f of
    (Just _) -> return ()
    Nothing -> error message

killCurrent :: IO ()
killCurrent = myThreadId >>= killThread

