module Specs where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit

main = hspecX $
  describe "" $ do
    it "" $ do
      2 @?= 1
