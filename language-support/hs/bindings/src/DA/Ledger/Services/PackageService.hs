-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.PackageService (
    listPackages,
    getPackage, Package(..),
    getPackageStatus, PackageStatus(..),
    ) where

import Com.Daml.Ledger.Api.V2.PackageService
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import Proto3.Suite.Types(Enumerated(..))
import qualified Data.Vector as Vector
import Data.ByteString(ByteString)

listPackages :: LedgerService [PackageId]
listPackages =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- packageServiceClient client
        let PackageService {packageServiceListPackages=rpc} = service
        response <- rpc (ClientNormalRequest ListPackagesRequest timeout mdm)
        ListPackagesResponse xs <- unwrap response
        return $ map PackageId $ Vector.toList xs

newtype Package = Package ByteString deriving (Eq,Ord,Show)

getPackage :: PackageId -> LedgerService (Maybe Package)
getPackage pid =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- packageServiceClient client
        let PackageService {packageServiceGetPackage=rpc} = service
        let request = GetPackageRequest (unPackageId pid)
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrapWithNotFound
            >>= \case
            Nothing ->
                return Nothing
            Just (GetPackageResponse _ bs _) ->
                return $ Just $ Package bs

getPackageStatus :: PackageId -> LedgerService PackageStatus
getPackageStatus pid =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- packageServiceClient client
        let PackageService {packageServiceGetPackageStatus=rpc} = service
        let request = GetPackageStatusRequest (unPackageId pid)
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrap
            >>= \case
            GetPackageStatusResponse (Enumerated (Left n)) ->
                fail $ "unexpected PackageStatus enum = " <> show n
            GetPackageStatusResponse (Enumerated (Right status)) ->
                return status
