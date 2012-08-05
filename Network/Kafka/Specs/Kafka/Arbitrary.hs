{-# LANGUAGE OverloadedStrings #-}
module Specs.Kafka.Arbitrary where
import qualified Data.ByteString.Char8 as B
import Test.QuickCheck
import Kafka.Types

instance Arbitrary Topic where
  arbitrary = do
    a <- nonEmptyString
    return $ Topic $ B.pack a

instance Arbitrary Stream where
  arbitrary = do
    t <- arbitrary
    p <- arbitrary
    return $! Stream t p

instance Arbitrary Message where
  arbitrary = do
    a <- nonEmptyString
    return $ Message (B.pack a)

instance Arbitrary B.ByteString where
  arbitrary = do
    a <- arbitrary
    return $! B.pack a

nonEmptyString :: Gen String
nonEmptyString = suchThat (listOf $ elements ['a'..'z']) (not . null)

instance Arbitrary Partition where
  arbitrary = do
    a <- elements [0..5]
    return $ Partition a
