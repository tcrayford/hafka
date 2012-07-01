module Specs.Kafka.Unit.KeepAliveSpecs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit
import Test.HUnit
import Kafka.Network
import Kafka.Consumer.KeepAlive
import Network.Socket(sClose, sIsConnected)

reconnectingToClosedSocket :: Spec
reconnectingToClosedSocket = describe "reconnectSocket" $
  it "reconnects a closed socket" $ do
    s <- connectTo "localhost" $ PortNumber 9092
    sClose s
    s2 <- reconnectSocket s
    c <- sIsConnected s2
    c @?= True
