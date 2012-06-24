module Specs.IntegrationHelper where
import Kafka.Producer
import Kafka.Consumer
import Kafka.Types
import Control.Concurrent.MVar
import System.Timeout
import Control.Monad(void)
import Test.QuickCheck.Monadic

coupledProducerConsumer :: Topic -> Partition -> (ProducerSettings, Consumer)
coupledProducerConsumer t p = (ProducerSettings t p, Consumer t p $ Offset 0)

waitFor :: MVar a -> String -> IO ()
waitFor result message = do
  f <- timeout 1000000 $ takeMVar result
  case f of
    (Just found) -> return ()
    Nothing -> error message

