import qualified Network.Kafka.Bench.Benchmarks as B

main :: IO ()
main = B.roundtripKeepAliveConsumer

