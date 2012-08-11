{-# LANGUAGE OverloadedStrings #-}
import Network.Kafka.Consumer
import Network.Kafka.Consumer.Basic
import Network.Kafka.Types
import qualified Data.ByteString as B


main = do
  let settings = BasicConsumer (Stream (Topic "hafka-examples") (Partition 0)) (Offset 0)
  consumeLoop settings (\(Message x) -> do
    B.putStrLn "received message: "
    B.putStrLn x)
