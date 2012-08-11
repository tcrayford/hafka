{-# LANGUAGE OverloadedStrings #-}
import Network.Kafka.Producer
import Network.Kafka.Types
import qualified Data.ByteString as B

main = do
  putStrLn "ready for Message"
  message <- B.getLine
  let settings = ProducerSettings (Stream (Topic "hafka-examples") (Partition 0))
  produce settings [Message message]
  main
  
