module Kafka.Network(
    N.PortID(..)
  , connectTo
  , socketToHandle
  , IOMode(..)
  ) where
import Network.Socket
import Network.BSD
import qualified Network as N
import System.IO
import qualified Control.Exception as Exception

connectTo :: HostName -> N.PortID -> IO Socket
connectTo hostname (N.PortNumber port) = connect' hostname (show port)

connect' :: HostName -> ServiceName -> IO Socket
connect' host serv = do
    proto <- getProtocolNumber "tcp"
    let hints = defaultHints { addrFlags = [AI_ADDRCONFIG]
                             , addrProtocol = proto
                             , addrSocketType = Stream }
    addrs <- getAddrInfo (Just hints) (Just host) (Just serv)
    firstSuccessful $ map tryToConnect addrs
  where
  tryToConnect addr =
    Exception.bracketOnError
        (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
        (sClose)  -- only done if there's an error
        (\sock -> do
          connect sock (addrAddress addr)
          return sock
        )

firstSuccessful :: [IO a] -> IO a
firstSuccessful [] = error "firstSuccessful: empty list"
firstSuccessful (p:ps) = catchIO p $ \e ->
    case ps of
        [] -> Exception.throw e
        _  -> firstSuccessful ps

catchIO :: IO a -> (Exception.IOException -> IO a) -> IO a
catchIO = Exception.catch

