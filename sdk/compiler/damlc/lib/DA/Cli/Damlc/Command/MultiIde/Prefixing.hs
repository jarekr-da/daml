-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

-- This module handles prefixing of identifiers generated by the SubIdes, so that they are unique to the client, but are returned in the same form to the servers.
-- It has been implemented to be stateless for simplicity
module DA.Cli.Damlc.Command.MultiIde.Prefixing (
  addProgressTokenPrefixToClientMessage,
  addProgressTokenPrefixToServerMessage,
  addLspPrefixToServerMessage,
  stripLspPrefix,
  stripWorkDoneProgressCancelTokenPrefix,
) where

import Control.Lens
import Control.Monad
import qualified Data.Text as T
import Data.Tuple (swap)
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import DA.Cli.Damlc.Command.MultiIde.Types

-- ProgressToken Prefixing
-- Progress tokens can be created on both client and subIdes. They are then reported on by a subIde, and cancelled by the client.
--   We need to avoid collisions between tokens created by different subIdes.
-- Server created progress tokens use the SWindowWorkDoneProgressCreate notification. We prefix these tokens uniquely to the subIde that created them.
--   Handled in addProgressTokenPrefixToServerMessage
-- Client created progress tokens are created alongside requests, `getProgressLenses` creates lenses for these tokens. We prefix these with `client`
--   All prefixed tokens also include their original type, so the transformation can be safely reversed.
--   Handled in addProgressTokenPrefixToClientMessage
-- Progress messages
--   Sent from subIdes, these are always forwarded to the client. However, depending on what created the Token, the prefix will need changing.
--   If the subIde created the token, the progress message token will be unprefixed.
--     We check the first character of the token. Neither server or client can create tokens starting with non-hex character. Our prefixing starts with `i` or `t`
--     If there is no prefix, we add the prefix of the sending subIde. No subIde should ever give progress reports for tokens created by other subIdes.
--   If the client created the token, it will have the client prefix. We detect this and remove it, so the client sees the exact name it created.
--   Handled in addProgressTokenPrefixToServerMessage
-- Cancel messages
--   Sent by the client, forwarded to either all or a specific subIde.
--   If the token was created by the client, it will have no prefix. We add the prefix and broadcast to all subIdes
--     This is safe because it is impossible for a subIde to generate a token matching the prefixed client token, so the correct subIde will delete the token, and the rest will ignore
--   If the token was created by a subIde, it will have the subIde's unique prefix. We strip this from the message, and return it to MultiIde.hs.
--     The message handling there will lookup the IDE matching this prefix and send the message to it. If no IDE exists, we can safely drop the message, as the IDE has been removed.

-- | Convenience type for prefixes
data ProgressTokenPrefix
  = SubIdePrefix T.Text
  | ClientPrefix

progressTokenPrefixToText :: ProgressTokenPrefix -> T.Text
progressTokenPrefixToText (SubIdePrefix t) = t
progressTokenPrefixToText ClientPrefix = "client"

progressTokenPrefixFromMaybe :: Maybe T.Text -> ProgressTokenPrefix
progressTokenPrefixFromMaybe = maybe ClientPrefix SubIdePrefix

-- | Reversible ProgressToken prefixing
-- Given ProgressTokens can be int or text, we encode them as text as well as a tag to say if the original was an int
-- Such that ProgressNumericToken 10   -> ProgressTextToken "iPREFIX-10"
--       and ProgressTextToken "hello" -> ProgressTextToken "tPREFIX-hello"
addProgressTokenPrefix :: ProgressTokenPrefix -> LSP.ProgressToken -> LSP.ProgressToken
addProgressTokenPrefix prefix (LSP.ProgressNumericToken t) = LSP.ProgressTextToken $ "i" <> progressTokenPrefixToText prefix <> "-" <> T.pack (show t)
addProgressTokenPrefix prefix (LSP.ProgressTextToken t) = LSP.ProgressTextToken $ "t" <> progressTokenPrefixToText prefix <> "-" <> t

progressTokenSplitPrefix :: T.Text -> (T.Text, Maybe T.Text)
progressTokenSplitPrefix = bimap T.tail (mfilter (/="client") . Just) . swap . T.breakOn "-"

-- Removes prefix, returns the subIde prefix if the token was created by a subIde
stripProgressTokenPrefix :: LSP.ProgressToken -> (LSP.ProgressToken, Maybe ProgressTokenPrefix)
stripProgressTokenPrefix (LSP.ProgressTextToken (T.uncons -> Just ('i', rest))) =
  bimap (LSP.ProgressNumericToken . read . T.unpack) (Just . progressTokenPrefixFromMaybe) $ progressTokenSplitPrefix rest
stripProgressTokenPrefix (LSP.ProgressTextToken (T.uncons -> Just ('t', rest))) =
  bimap LSP.ProgressTextToken (Just . progressTokenPrefixFromMaybe) $ progressTokenSplitPrefix rest
stripProgressTokenPrefix t = (t, Nothing)

-- Prefixes the SWindowWorkDoneProgressCreate and SProgress messages from subIde. Rest are unchanged.
addProgressTokenPrefixToServerMessage :: T.Text -> LSP.FromServerMessage -> LSP.FromServerMessage
addProgressTokenPrefixToServerMessage prefix (LSP.FromServerMess LSP.SWindowWorkDoneProgressCreate req) =
  LSP.FromServerMess LSP.SWindowWorkDoneProgressCreate $ req & LSP.params . LSP.token %~ addProgressTokenPrefix (SubIdePrefix prefix)
addProgressTokenPrefixToServerMessage prefix (LSP.FromServerMess LSP.SProgress notif) =
  case stripProgressTokenPrefix $ notif ^. LSP.params . LSP.token of
    -- ProgressToken was created by this subIde, add its usual prefix
    (unprefixedToken, Nothing) ->
      let prefixedToken = addProgressTokenPrefix (SubIdePrefix prefix) unprefixedToken
       in LSP.FromServerMess LSP.SProgress $ notif & LSP.params . LSP.token .~ prefixedToken
    -- ProgressToken was created by client, send back the unprefixed token
    (unprefixedToken, Just ClientPrefix) -> LSP.FromServerMess LSP.SProgress $ notif & LSP.params . LSP.token .~ unprefixedToken
    (_, Just (SubIdePrefix t)) -> error $ "SubIde with prefix " <> T.unpack t <> " is somehow aware of its own prefixing. Something is very wrong."
addProgressTokenPrefixToServerMessage _ msg = msg

-- Prefixes client created progress tokens for all requests that can create them.
addProgressTokenPrefixToClientMessage :: LSP.FromClientMessage' a -> LSP.FromClientMessage' a
addProgressTokenPrefixToClientMessage = \case
  mess@(LSP.FromClientMess method params) ->
    case LSP.splitClientMethod method of
      LSP.IsClientReq -> do
        let progressLenses = getProgressLenses method
            doAddProgressTokenPrefix
              :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
              .  Maybe (ReifiedLens' (LSP.RequestMessage m) (Maybe LSP.ProgressToken))
              -> LSP.RequestMessage m
              -> LSP.RequestMessage m
            doAddProgressTokenPrefix = maybe id $ \lens -> runLens lens . mapped %~ addProgressTokenPrefix ClientPrefix
            params' = doAddProgressTokenPrefix (workDoneLens progressLenses) $ doAddProgressTokenPrefix (partialResultLens progressLenses) params
         in LSP.FromClientMess method params'
      _ -> mess
  rsp@LSP.FromClientRsp {} -> rsp

-- Convenience wrapper for the reified progress token lenses.
-- Note that there are 2 types, workDone and partialResult.
-- Most messages with progress have both, but ssome only have workDone
data ProgressLenses (m :: LSP.Method 'LSP.FromClient 'LSP.Request) = ProgressLenses
  { workDoneLens :: Maybe (ReifiedLens' (LSP.RequestMessage m) (Maybe LSP.ProgressToken))
  , partialResultLens :: Maybe (ReifiedLens' (LSP.RequestMessage m) (Maybe LSP.ProgressToken))
  }

getProgressLenses :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request). LSP.SMethod m -> ProgressLenses m
getProgressLenses = \case
  LSP.SInitialize -> workDone
  LSP.SWorkspaceSymbol -> both
  LSP.SWorkspaceExecuteCommand -> workDone
  LSP.STextDocumentCompletion -> both
  LSP.STextDocumentHover -> workDone
  LSP.STextDocumentDeclaration -> both
  LSP.STextDocumentDefinition -> both
  LSP.STextDocumentTypeDefinition -> both
  LSP.STextDocumentImplementation -> both
  LSP.STextDocumentReferences -> both
  LSP.STextDocumentDocumentHighlight -> both
  LSP.STextDocumentDocumentSymbol -> both
  LSP.STextDocumentCodeAction -> both
  LSP.STextDocumentCodeLens -> both
  LSP.STextDocumentDocumentLink -> both
  LSP.STextDocumentDocumentColor -> both
  LSP.STextDocumentColorPresentation -> both
  LSP.STextDocumentFormatting -> workDone
  LSP.STextDocumentRangeFormatting -> workDone
  LSP.STextDocumentRename -> workDone
  LSP.STextDocumentFoldingRange -> both
  LSP.STextDocumentSelectionRange -> both
  LSP.STextDocumentPrepareCallHierarchy -> workDone
  LSP.SCallHierarchyIncomingCalls -> both
  LSP.SCallHierarchyOutgoingCalls -> both
  LSP.STextDocumentSemanticTokensFull -> both
  LSP.STextDocumentSemanticTokensFullDelta -> both
  LSP.STextDocumentSemanticTokensRange -> both
  _ -> ProgressLenses Nothing Nothing
  where
    workDone
      :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
      .  LSP.HasWorkDoneToken (LSP.MessageParams m) (Maybe LSP.ProgressToken)
      => ProgressLenses m
    workDone = ProgressLenses (Just $ Lens $ LSP.params . LSP.workDoneToken) Nothing
    both
      :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
      .  ( LSP.HasWorkDoneToken (LSP.MessageParams m) (Maybe LSP.ProgressToken)
         , LSP.HasPartialResultToken (LSP.MessageParams m) (Maybe LSP.ProgressToken)
         )
      => ProgressLenses m
    both = ProgressLenses (Just $ Lens $ LSP.params . LSP.workDoneToken) (Just $ Lens $ LSP.params . LSP.partialResultToken)

-- strips and returns the subIde prefix from cancel messages. Gives Nothing for client created tokens
stripWorkDoneProgressCancelTokenPrefix
  :: LSP.NotificationMessage 'LSP.WindowWorkDoneProgressCancel
  -> (LSP.NotificationMessage 'LSP.WindowWorkDoneProgressCancel, Maybe T.Text)
stripWorkDoneProgressCancelTokenPrefix notif =
  case stripProgressTokenPrefix $ notif ^. LSP.params . LSP.token of
    -- Token was created by the client, add the client prefix and broadcast to all subIdes
    (unprefixedToken, Nothing) ->
      let prefixedToken = addProgressTokenPrefix ClientPrefix unprefixedToken
       in (notif & LSP.params . LSP.token .~ prefixedToken, Nothing)
    -- Created by subIde, strip the prefix and send to the specific subIde that created it.
    (unprefixedToken, Just (SubIdePrefix prefix)) -> (notif & LSP.params . LSP.token .~ unprefixedToken, Just prefix)
    (_, Just ClientPrefix) -> error "Client attempted to cancel a ProgressToken with the client prefix, which it should not be aware of. Something went wrong."

-- LspId Prefixing

-- We need to ensure all IDs from different subIdes are unique to the client, so we prefix them.
-- Given IDs can be int or text, we encode them as text as well as a tag to say if the original was an int
-- Such that IdInt 10       -> IdString "iPREFIX-10"
--       and IdString "hello" -> IdString "tPREFIX-hello"
addLspPrefix
  :: forall (f :: LSP.From) (m :: LSP.Method f 'LSP.Request)
  .  T.Text
  -> LSP.LspId m
  -> LSP.LspId m
addLspPrefix prefix (LSP.IdInt t) = LSP.IdString $ "i" <> prefix <> "-" <> T.pack (show t)
addLspPrefix prefix (LSP.IdString t) = LSP.IdString $ "t" <> prefix <> "-" <> t

stripLspPrefix
  :: forall (f :: LSP.From) (m :: LSP.Method f 'LSP.Request)
  .  LSP.LspId m
  -> LSP.LspId m
stripLspPrefix (LSP.IdString (T.unpack -> ('i':rest))) = LSP.IdInt $ read $ tail $ dropWhile (/='-') rest
stripLspPrefix (LSP.IdString (T.uncons -> Just ('t', rest))) = LSP.IdString $ T.tail $ T.dropWhile (/='-') rest
-- Maybe this should error? This method should only be called on LspIds that we know have been prefixed
stripLspPrefix t = t

-- Prefixes applied to builtin and custom requests. Notifications do not have ids, responses do not need this logic.
addLspPrefixToServerMessage :: SubIdeInstance -> LSP.FromServerMessage -> LSP.FromServerMessage
addLspPrefixToServerMessage _ res@(LSP.FromServerRsp _ _) = res
addLspPrefixToServerMessage ide res@(LSP.FromServerMess method params) =
  case LSP.splitServerMethod method of
    LSP.IsServerReq -> LSP.FromServerMess method $ params & LSP.id %~ addLspPrefix (ideMessageIdPrefix ide)
    LSP.IsServerNot -> res
    LSP.IsServerEither ->
      case params of
        LSP.ReqMess params' -> LSP.FromServerMess method $ LSP.ReqMess $ params' & LSP.id %~ addLspPrefix (ideMessageIdPrefix ide)
        LSP.NotMess _ -> res
