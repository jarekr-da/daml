-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GADTs #-}

-- This module handles prefixing of identifiers generated by the SubIDEs, so that they are unique to the client, but are returned in the same form to the servers.
module DA.Cli.Damlc.Command.MultiIde.Prefixing (
  addProgressTokenPrefixToClientMessage,
  addProgressTokenPrefixToServerMessage,
  addLspPrefixToServerMessage,
  removeLspPrefix,
  removeWorkDoneProgressCancelTokenPrefix,
) where

import Control.Applicative ((<|>))
import Control.Concurrent.MVar
import Control.Lens
import Data.Foldable (find)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import DA.Cli.Damlc.Command.MultiIde.Types

-- ProgressToken Prefixing

-- Progress tokens are created in 2 ways:
--   The subIDE sends a CreateToken
--     In this case, we need to prefix the token, as multiple subIDEs might use the same name 
--   The client sends any message with a progress token attached
--     In this case, we can assume the token is unique to the client, and not modify it
-- There are then 2 messages that can use these tokens:
--   Progress message from the subIDE
--     This includes the token id, but we don't know if said token id was created by the subIDE (so needs prefixing) or client
--     Therefore, whenever we see a token created by the server, we store it in a var (along with the subIde it came from)
--     Then when we see a progress message with a token, if the token is in the var, we know it needs the prefix, otherwise its unmodified
--   Cancel message from the client
--     This has a token id, but again we dont know where it came from, so if it needs its prefix removed
--     We again lookup in the map, if we find it, we remove the prefix, and send it to the subIDE that created it
--     If we don't find it, we know its unique to the client, and only exists on one of the SubIDEs
--     We are then safe to broadcast the cancel message to all IDEs, as those that don't have it will ignore the message
-- TODO: Verify what happens if the client creates a token and sends to SUbIDE1, then subIDE tries to create one with the same name
--   This gets prefixed and submitted to client
--   after this, the client tries to cancel the token it created for SubIDE1, which gets submitted to all SubIDEs
--   will this cancel the one subIDE2 made - yes
--   So, can the server and client create clashing token names? Likely :(


-- GHCIDE generates essentially counting numbers from 1
-- vscode generates full UUIDs
-- Neither will clash if we prefix with `client`
-- All requests from client with the workdone or partial result token will get prefixed with `client` (using addProgressTokenPrefix)
-- On cancel request, if we dont have the token in our mapping, we prefix with client and broadcast as usual
-- on Progress notification from server, if we dont have the token, we drop the client prefix and forward it - implying the prefix is reversible, which it isn't
--   might instead want to store the prefixing, with the FilePath changed to a Maybe FilePath, Nothing = from client?
-- Also, listing all the methods with the HasToken thing will be annoying and potentially wrong, check if we can achieve it with overlapping instances (and look up if thats safe)

-- Adds the prefix, doesn't need to be reversible as we'll keep a backwards lookup
-- Just needs to be unique
addProgressTokenPrefix :: T.Text -> LSP.ProgressToken -> LSP.ProgressToken
addProgressTokenPrefix prefix (LSP.ProgressNumericToken t) = LSP.ProgressTextToken $ prefix <> "-" <> T.pack (show t)
addProgressTokenPrefix prefix (LSP.ProgressTextToken t) = LSP.ProgressTextToken $ prefix <> "-" <> t

-- Added to WindowWorkDoneProgressCreate and Progress
-- If its create, add the prefix and store the change (and source IDE) in the mapping var
-- if its progress, attempt to read the correct prefixed name from the mapping var, on failure, call back to the initial token id
--   Safe as we assume this was created by client, so is already unique.
addProgressTokenPrefixToServerMessage :: ProgressTokenPrefixesVar -> FilePath -> T.Text -> LSP.FromServerMessage -> IO LSP.FromServerMessage
addProgressTokenPrefixToServerMessage tokensVar home prefix (LSP.FromServerMess LSP.SWindowWorkDoneProgressCreate req) = do
  let unPrefixedToken = req ^. LSP.params . LSP.token
      prefixedToken = addProgressTokenPrefix prefix unPrefixedToken
  modifyMVar_ tokensVar (pure . Map.insert (unPrefixedToken, Just home) prefixedToken)
  pure $ LSP.FromServerMess LSP.SWindowWorkDoneProgressCreate $ req & LSP.params . LSP.token .~ prefixedToken
addProgressTokenPrefixToServerMessage tokensVar home _ (LSP.FromServerMess LSP.SProgress notif) = do
  let unPrefixedToken = notif ^. LSP.params . LSP.token
  -- for lookup and delete, try no home then given home, abstract as a function. This ensures the progress lookup still works for client created tokens
  prefixedToken <- fromMaybe unPrefixedToken . lookupHomeAndNot (unPrefixedToken, home) <$> readMVar tokensVar
  case notif ^. LSP.params . LSP.value of
    LSP.End _ -> modifyMVar_ tokensVar (pure . Map.delete (unPrefixedToken, Just home) . Map.delete (unPrefixedToken, Nothing))
    _ -> pure ()
  pure $ LSP.FromServerMess LSP.SProgress $ notif & LSP.params . LSP.token .~ prefixedToken
addProgressTokenPrefixToServerMessage _ _ _ msg = pure msg

lookupHomeAndNot :: (LSP.ProgressToken, FilePath) -> ProgressTokenPrefixes -> Maybe LSP.ProgressToken
lookupHomeAndNot (t, home) m = Map.lookup (t, Nothing) m <|> Map.lookup (t, Just home) m

-- Adds the "client" prefix to all workDone and partial result Progress tokens
addProgressTokenPrefixToClientMessage :: ProgressTokenPrefixesVar -> LSP.FromClientMessage' a -> IO (LSP.FromClientMessage' a)
addProgressTokenPrefixToClientMessage tokensVar = \case
  mess@(LSP.FromClientMess method params) ->
    case LSP.splitClientMethod method of
      LSP.IsClientReq -> do
        let progressLenses = getProgressLenses method
        params' <- 
          maybe pure (\lens -> runLens lens $ traverse addClientProgressTokenPrefix) (workDoneLens progressLenses) params
            >>= maybe pure (\lens -> runLens lens $ traverse addClientProgressTokenPrefix) (partialResultLens progressLenses)
        pure $ LSP.FromClientMess method params'
      _ -> pure mess
  rsp@LSP.FromClientRsp {} -> pure rsp
  where
    addClientProgressTokenPrefix :: LSP.ProgressToken -> IO LSP.ProgressToken
    addClientProgressTokenPrefix unPrefixedToken = do
      let prefixedToken = addProgressTokenPrefix "client" unPrefixedToken
      -- We insert to the mapping backwards, as the mapping goes from server token to client token, and for client -> server messages, this should add, not remove, the prefix.
      modifyMVar_ tokensVar (pure . Map.insert (prefixedToken, Nothing) unPrefixedToken)
      pure prefixedToken

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

findKeyByValue :: Eq v => v -> Map.Map k v -> Maybe k
findKeyByValue val = fmap fst . find ((==val) . snd) . Map.toList

-- Remove the prefix from WindowWorkDoneProgressCancel, and return the source SubIDE if we knew it.
removeWorkDoneProgressCancelTokenPrefix
  :: ProgressTokenPrefixesVar
  -> LSP.NotificationMessage 'LSP.WindowWorkDoneProgressCancel
  -> IO (LSP.NotificationMessage 'LSP.WindowWorkDoneProgressCancel, Maybe FilePath)
removeWorkDoneProgressCancelTokenPrefix tokensVar notif = do
  let token = notif ^. LSP.params . LSP.token
  mKey <- findKeyByValue token <$> readMVar tokensVar
  case mKey of
    Nothing -> error "Got a cancel request for a ProgressToken that was never created."
    Just key@(unPrefixedToken, mHome) -> do
      modifyMVar_ tokensVar (pure . Map.delete key)
      pure (notif & LSP.params . LSP.token .~ unPrefixedToken, mHome)

-- LspId Prefixing

-- We need to ensure all IDs from different subIDEs are unique to the client, so we prefix them.
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

removeLspPrefix
  :: forall (f :: LSP.From) (m :: LSP.Method f 'LSP.Request)
  .  LSP.LspId m
  -> LSP.LspId m
removeLspPrefix (LSP.IdString (T.unpack -> ('i':rest))) = LSP.IdInt $ read $ tail $ dropWhile (/='-') rest
removeLspPrefix (LSP.IdString (T.uncons -> Just ('t', rest))) = LSP.IdString $ T.tail $ T.dropWhile (/='-') rest
-- Maybe this should error? This method should only be called on LspIds that we know have been prefixed
removeLspPrefix t = t

addLspPrefixToServerMessage :: SubIDE -> LSP.FromServerMessage -> LSP.FromServerMessage
addLspPrefixToServerMessage _ res@(LSP.FromServerRsp _ _) = res
addLspPrefixToServerMessage ide res@(LSP.FromServerMess method params) =
  case LSP.splitServerMethod method of
    LSP.IsServerReq -> LSP.FromServerMess method $ params & LSP.id %~ addLspPrefix (ideMessageIdPrefix ide)
    LSP.IsServerNot -> res
    LSP.IsServerEither ->
      case params of
        LSP.ReqMess params' -> LSP.FromServerMess method $ LSP.ReqMess $ params' & LSP.id %~ addLspPrefix (ideMessageIdPrefix ide)
        LSP.NotMess _ -> res
