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

waitFor :: MVar a -> (a -> PropertyM IO ()) -> String -> PropertyM IO ()
waitFor result success message = do
  f <- run $ timeout 1000000 $ takeMVar result
  case f of
    (Just found) -> void $ success found
    Nothing -> error message

