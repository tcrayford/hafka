module Network.Kafka.Specs.Kafka.Unit.KeepAliveSpecs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Network.Kafka.Connection
import System.IO

reconnectingToClosedSocket :: Spec
reconnectingToClosedSocket = describe "reconnectSocket" $
  it "reconnects a closed socket" $ do
    h <- connectTo "localhost" $ PortNumber 9092
    hClose h
    h2 <- reconnectSocket h
    c <- hIsOpen h2
    c @?= True
