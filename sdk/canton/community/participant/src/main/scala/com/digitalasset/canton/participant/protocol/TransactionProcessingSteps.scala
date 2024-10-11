// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NamedLoggingContext}
import com.digitalasset.canton.metrics.*
import com.digitalasset.canton.participant.metrics.TransactionProcessingMetrics
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.ProcessingSteps.ParsedRequest
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.{
  MalformedPayload,
  NoMediatorError,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.*
import com.digitalasset.canton.participant.protocol.TransactionProcessor.*
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.{
  ContractAuthenticationFailed,
  DomainWithoutMediatorError,
  SequencerRequest,
  SubmissionDuringShutdown,
  SubmissionInternalError,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessResult,
  ActivenessSet,
  CommitSet,
}
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.{
  SubmissionAlreadyInFlight,
  TimeoutTooLow,
  UnknownDomain,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory.*
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  ContractLookupError,
  SerializableContractOfId,
  UnknownPackageError,
}
import com.digitalasset.canton.participant.protocol.validation.*
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.{
  ErrorWithInternalConsistencyCheck,
  alertingPartyLookup,
}
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.ErrorWithSubTransaction
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.TimeCheckFailure
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithSuffixesAndMerged,
  WithoutSuffixes,
}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.computeRandomnessLength
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.{ConfirmationRequestSessionKeyStore, SessionKeyStore}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, LfTransactionUtil}
import com.digitalasset.canton.{
  LedgerSubmissionId,
  LfKeyResolver,
  LfPartyId,
  RequestCounter,
  SequencerCounter,
  WorkflowId,
  checked,
}
import com.digitalasset.daml.lf.data.ImmArray
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.PLens

import java.util.concurrent.ConcurrentHashMap
import scala.annotation.nowarn
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

/** The transaction processor that coordinates the Canton transaction protocol.
  *
  * @param participantId    The participant id hosting the transaction processor.
  */
@nowarn("msg=dead code following this construct")
class TransactionProcessingSteps(
    domainId: DomainId,
    participantId: ParticipantId,
    confirmationRequestFactory: TransactionConfirmationRequestFactory,
    confirmationResponseFactory: TransactionConfirmationResponseFactory,
    modelConformanceChecker: ModelConformanceChecker,
    staticDomainParameters: StaticDomainParameters,
    crypto: DomainSyncCryptoClient,
    contractStore: ContractStore,
    metrics: TransactionProcessingMetrics,
    serializableContractAuthenticator: SerializableContractAuthenticator,
    authenticationValidator: AuthenticationValidator,
    authorizationValidator: AuthorizationValidator,
    internalConsistencyChecker: InternalConsistencyChecker,
    tracker: CommandProgressTracker,
    protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit val ec: ExecutionContext)
    extends ProcessingSteps[
      SubmissionParam,
      TransactionSubmissionResult,
      TransactionViewType,
      TransactionSubmissionError,
    ]
    with NamedLogging {
  private def protocolVersion = staticDomainParameters.protocolVersion

  override type SubmissionSendError = TransactionProcessor.SubmissionErrors.SequencerRequest.Error
  override type PendingSubmissions = Unit
  override type PendingSubmissionId = Unit
  override type PendingSubmissionData = Nothing

  override type SubmissionResultArgs = Unit

  override type ParsedRequestType = ParsedTransactionRequest

  override type RejectionArgs = TransactionProcessingSteps.RejectionArgs

  override type RequestError = TransactionProcessorError
  override type ResultError = TransactionProcessorError

  override type RequestType = ProcessingSteps.RequestType.Transaction
  override val requestType: RequestType = ProcessingSteps.RequestType.Transaction

  override def pendingSubmissions(state: SyncDomainEphemeralState): Unit = ()

  override def requestKind: String = "Transaction"

  override def submissionDescription(param: SubmissionParam): String =
    show"submitters ${param.submitterInfo.actAs}, command-id ${param.submitterInfo.commandId}"

  override def submissionIdOfPendingRequest(pendingData: PendingTransaction): Unit = ()

  override def removePendingSubmission(
      pendingSubmissions: Unit,
      pendingSubmissionId: Unit,
  ): Option[Nothing] = None

  override def createSubmission(
      submissionParam: SubmissionParam,
      mediator: MediatorGroupRecipient,
      ephemeralState: SyncDomainEphemeralStateLookup,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionSubmissionError, Submission] = {
    val SubmissionParam(
      submitterInfo,
      transactionMeta,
      keyResolver,
      wfTransaction,
      disclosedContracts,
    ) = submissionParam

    val tracked = new TrackedTransactionSubmission(
      submitterInfo,
      transactionMeta,
      keyResolver,
      wfTransaction,
      mediator,
      recentSnapshot,
      ephemeralState.contractLookup,
      disclosedContracts,
    )

    EitherT.rightT[FutureUnlessShutdown, TransactionSubmissionError](tracked)
  }

  override def embedNoMediatorError(error: NoMediatorError): TransactionSubmissionError =
    DomainWithoutMediatorError.Error(error.topologySnapshotTimestamp, domainId)

  override def getSubmitterInformation(
      views: Seq[DecryptedView]
  ): (Option[ViewSubmitterMetadata], Option[SubmissionTracker.SubmissionData]) = {
    val submitterMetadataO =
      views.map(_.tree.submitterMetadata.unwrap).collectFirst { case Right(meta) => meta }
    val submissionDataForTrackerO = submitterMetadataO.map(meta =>
      SubmissionTracker.SubmissionData(meta.submittingParticipant, meta.maxSequencingTime)
    )

    (submitterMetadataO, submissionDataForTrackerO)
  }

  private class TrackedTransactionSubmission(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      mediator: MediatorGroupRecipient,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
      contractLookup: ContractLookup,
      disclosedContracts: Map[LfContractId, SerializableContract],
  )(implicit traceContext: TraceContext)
      extends TrackedSubmission {

    private def changeId: ChangeId = submitterInfo.changeId

    override val changeIdHash: ChangeIdHash = ChangeIdHash(changeId)

    override def specifiedDeduplicationPeriod: DeduplicationPeriod =
      submitterInfo.deduplicationPeriod

    override def commandDeduplicationFailure(
        failure: DeduplicationFailed
    ): TransactionSubmissionTrackingData = {
      // If the deduplication period is not supported, we report the empty deduplication period to be on the safe side
      // Ideally, we'd report the offset that is being assigned to the completion event,
      // but that is not supported in our current architecture as the indexer assigns the global offset at a later stage
      // of processing.
      lazy val emptyDeduplicationPeriod =
        DeduplicationPeriod.DeduplicationDuration(java.time.Duration.ZERO)

      val (error, dedupInfo): (TransactionError, DeduplicationPeriod) = failure match {
        case CommandDeduplicator.AlreadyExists(completionOffset, accepted, submissionId) =>
          CommandDeduplicationError.DuplicateCommandReject(
            changeId,
            UpstreamOffsetConvert.fromGlobalOffset(completionOffset).toHexString,
            accepted,
            submissionId,
          ) ->
            DeduplicationPeriod.DeduplicationOffset(
              // Extend the reported deduplication period to include the conflicting submission,
              // as deduplication offsets are exclusive
              UpstreamOffsetConvert.fromGlobalOffset(completionOffset.toLong - 1L)
            )
        case CommandDeduplicator.DeduplicationPeriodTooEarly(requested, supported) =>
          val error: TransactionError = supported match {
            case DeduplicationPeriod.DeduplicationDuration(longestDuration) =>
              CommandDeduplicationError.DeduplicationPeriodStartsTooEarlyErrorWithDuration(
                changeId,
                requested,
                longestDuration.toString,
              )
            case DeduplicationPeriod.DeduplicationOffset(earliestOffset) =>
              CommandDeduplicationError.DeduplicationPeriodStartsTooEarlyErrorWithOffset(
                changeId,
                requested,
                earliestOffset.toHexString,
              )
          }
          error -> emptyDeduplicationPeriod
        case CommandDeduplicator.MalformedOffset(error) =>
          CommandDeduplicationError.MalformedDeduplicationOffset.Error(
            error
          ) -> emptyDeduplicationPeriod
      }
      mkTransactionSubmissionTrackingData(
        error,
        submitterInfo.toCompletionInfo.copy(optDeduplicationPeriod = dedupInfo.some),
      )
    }

    private def mkTransactionSubmissionTrackingData(
        error: TransactionError,
        completionInfo: CompletionInfo,
    ): TransactionSubmissionTrackingData =
      TransactionSubmissionTrackingData(
        completionInfo,
        TransactionSubmissionTrackingData.CauseWithTemplate(error),
        domainId,
        protocolVersion,
      )

    override def submissionId: Option[LedgerSubmissionId] = submitterInfo.submissionId

    override def maxSequencingTimeO: OptionT[Future, CantonTimestamp] = OptionT.liftF(
      recentSnapshot.ipsSnapshot.findDynamicDomainParametersOrDefault(protocolVersion).map {
        domainParameters =>
          CantonTimestamp(transactionMeta.ledgerEffectiveTime)
            .add(domainParameters.ledgerTimeRecordTimeTolerance.unwrap)
      }
    )

    override def prepareBatch(
        actualDeduplicationOffset: DeduplicationPeriod.DeduplicationOffset,
        maxSequencingTime: CantonTimestamp,
        sessionKeyStore: SessionKeyStore,
    ): EitherT[FutureUnlessShutdown, SubmissionTrackingData, PreparedBatch] = {
      logger.debug("Preparing batch for transaction submission")
      val submitterInfoWithDedupPeriod =
        submitterInfo.copy(deduplicationPeriod = actualDeduplicationOffset)

      def causeWithTemplate(message: String, reason: TransactionConfirmationRequestCreationError) =
        TransactionSubmissionTrackingData.CauseWithTemplate(
          SubmissionErrors.MalformedRequest.Error(message, reason)
        )

      // TODO(#19999): Move this check to the protocol processor's pre-submission validation
      def check(
          transaction: LfVersionedTransaction,
          topologySnapshot: TopologySnapshot,
      ): Future[Boolean] = {

        val actionNodes = transaction.nodes.values.collect { case an: LfActionNode => an }

        val informeesCheckPartiesPerNode = actionNodes.map { node =>
          node.informeesOfNode & LfTransactionUtil.stateKnownTo(node)
        }
        val signatoriesCheckPartiesPerNode = actionNodes.map { node =>
          LfTransactionUtil.signatoriesOrMaintainers(node) | LfTransactionUtil.actingParties(node)
        }
        val allParties =
          (informeesCheckPartiesPerNode.flatten ++ signatoriesCheckPartiesPerNode.flatten).toSet
        val eligibleParticipantsF =
          topologySnapshot.activeParticipantsOfPartiesWithInfo(allParties.toSeq).map { result =>
            result.map { case (party, info) =>
              (party, info.participants.values.exists(_.permission.canConfirm))
            }
          }

        eligibleParticipantsF.map { eligibleParticipants =>
          signatoriesCheckPartiesPerNode.forall { signatoriesForNode =>
            signatoriesForNode.nonEmpty && signatoriesForNode.forall(eligibleParticipants(_))
          }
        }
      }

      val result = for {
        _ <- EitherT(
          check(wfTransaction.unwrap, recentSnapshot.ipsSnapshot)
            .map(
              Either.cond(
                _,
                (),
                causeWithTemplate(
                  "Incompatible Domain",
                  MalformedLfTransaction(
                    s"No confirmation policy applicable (snapshot at ${recentSnapshot.ipsSnapshot.timestamp})"
                  ),
                ),
              )
            )
        ).mapK(FutureUnlessShutdown.outcomeK)

        _ <- submitterInfo.actAs
          .parTraverse(rawSubmitter =>
            EitherT
              .fromEither[FutureUnlessShutdown](LfPartyId.fromString(rawSubmitter))
              .leftMap[TransactionSubmissionTrackingData.RejectionCause](msg =>
                causeWithTemplate(msg, MalformedSubmitter(rawSubmitter))
              )
          )

        lookupContractsWithDisclosed: SerializableContractOfId =
          (contractId: LfContractId) =>
            disclosedContracts
              .get(contractId)
              .map(contract =>
                EitherT.rightT[Future, TransactionTreeFactory.ContractLookupError](contract)
              )
              .getOrElse(
                TransactionTreeFactory
                  .contractInstanceLookup(contractLookup)(implicitly, implicitly)(
                    contractId
                  )
              )

        confirmationRequestTimer = metrics.protocolMessages.confirmationRequestCreation
        // Perform phase 1 of the protocol that produces a transaction confirmation request
        request <- confirmationRequestTimer.timeEitherFUS(
          confirmationRequestFactory
            .createConfirmationRequest(
              wfTransaction,
              submitterInfoWithDedupPeriod,
              transactionMeta.workflowId.map(WorkflowId(_)),
              keyResolver,
              mediator,
              recentSnapshot,
              sessionKeyStore,
              lookupContractsWithDisclosed,
              maxSequencingTime,
              protocolVersion,
            )
            .leftMap[TransactionSubmissionTrackingData.RejectionCause] {
              case TransactionTreeFactoryError(UnknownPackageError(unknownTo)) =>
                TransactionSubmissionTrackingData
                  .CauseWithTemplate(SubmissionErrors.PackageNotVettedByRecipients.Error(unknownTo))
              case TransactionTreeFactoryError(ContractLookupError(contractId, _)) =>
                TransactionSubmissionTrackingData
                  .CauseWithTemplate(SubmissionErrors.UnknownContractDomain.Error(contractId))
              case creationError =>
                causeWithTemplate("Transaction confirmation request creation failed", creationError)
            }
        )
        _ = logger.debug(s"Generated requestUuid=${request.informeeMessage.requestUuid}")
        batch <- EitherT
          .right[TransactionSubmissionTrackingData.RejectionCause](
            request.asBatch(recentSnapshot.ipsSnapshot)
          )
          .mapK(FutureUnlessShutdown.outcomeK)
      } yield {
        val batchSize = batch.toProtoVersioned.serializedSize
        val numRecipients = batch.allRecipients.size
        val numEnvelopes = batch.envelopesCount
        tracker
          .findHandle(
            submitterInfoWithDedupPeriod.commandId,
            submitterInfoWithDedupPeriod.applicationId,
            submitterInfoWithDedupPeriod.actAs,
            submitterInfoWithDedupPeriod.submissionId,
          )
          .recordEnvelopeSizes(batchSize, numRecipients, numEnvelopes)

        metrics.protocolMessages.confirmationRequestSize.update(batchSize)(MetricsContext.Empty)

        new PreparedTransactionBatch(
          batch,
          request.rootHash,
          submitterInfoWithDedupPeriod.toCompletionInfo,
        ): PreparedBatch
      }

      def mkError(
          rejectionCause: TransactionSubmissionTrackingData.RejectionCause
      ): Success[Outcome[Either[SubmissionTrackingData, PreparedBatch]]] = {
        val trackingData = TransactionSubmissionTrackingData(
          submitterInfoWithDedupPeriod.toCompletionInfo,
          rejectionCause,
          domainId,
          protocolVersion,
        )
        Success(Outcome(Left(trackingData)))
      }

      // Make sure that we don't throw an error
      EitherT(result.value.transform {
        case Success(Outcome(Right(preparedBatch))) => Success(Outcome(Right(preparedBatch)))
        case Success(Outcome(Left(rejectionCause))) => mkError(rejectionCause)
        case Success(AbortedDueToShutdown) => Success(AbortedDueToShutdown)
        case Failure(PassiveInstanceException(_reason)) =>
          val rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(
            SyncServiceInjectionError.PassiveReplica.Error(
              applicationId = submitterInfo.applicationId,
              commandId = submitterInfo.commandId,
            )
          )
          mkError(rejectionCause)
        case Failure(exception) =>
          val rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(
            SyncServiceInjectionError.InjectionFailure.Failure(exception)
          )
          mkError(rejectionCause)
      })
    }

    override def submissionTimeoutTrackingData: SubmissionTrackingData =
      TransactionSubmissionTrackingData(
        submitterInfo.toCompletionInfo.copy(optDeduplicationPeriod = None),
        TransactionSubmissionTrackingData.TimeoutCause,
        domainId,
        protocolVersion,
      )

    override def embedInFlightSubmissionTrackerError(
        error: InFlightSubmissionTracker.InFlightSubmissionTrackerError
    ): TransactionSubmissionError = error match {
      case SubmissionAlreadyInFlight(_newSubmission, existingSubmission) =>
        TransactionProcessor.SubmissionErrors.SubmissionAlreadyInFlight(
          changeId,
          existingSubmission.submissionId,
          existingSubmission.submissionDomain,
        )
      case UnknownDomain(domainId) =>
        TransactionRoutingError.ConfigurationErrors.SubmissionDomainNotReady.Error(domainId)
      case TimeoutTooLow(_submission, lowerBound) =>
        TransactionProcessor.SubmissionErrors.TimeoutError.Error(lowerBound)
    }

    override def embedSequencerRequestError(
        error: ProtocolProcessor.SequencerRequestError
    ): SubmissionSendError =
      TransactionProcessor.SubmissionErrors.SequencerRequest.Error(error.sendError)

    override def shutdownDuringInFlightRegistration: TransactionSubmissionError =
      TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown.Rejection()

    override def onDefinitiveFailure: TransactionSubmissionResult = TransactionSubmissionFailure

    override def definiteFailureTrackingData(
        failure: UnlessShutdown[Throwable]
    ): SubmissionTrackingData = {
      val error = (failure match {
        case UnlessShutdown.AbortedDueToShutdown =>
          SubmissionDuringShutdown.Rejection()
        case UnlessShutdown.Outcome(exception) =>
          SubmissionInternalError.Failure(exception)
      }): TransactionError
      mkTransactionSubmissionTrackingData(error, submitterInfo.toCompletionInfo)
    }

    override def onPotentialFailure(
        maxSequencingTime: CantonTimestamp
    ): TransactionSubmissionResult =
      TransactionSubmissionUnknown(maxSequencingTime)
  }

  private class PreparedTransactionBatch(
      override val batch: Batch[DefaultOpenEnvelope],
      override val rootHash: RootHash,
      completionInfo: CompletionInfo,
  ) extends PreparedBatch {
    override def pendingSubmissionId: Unit = ()

    override def embedSequencerRequestError(
        error: ProtocolProcessor.SequencerRequestError
    ): SequencerRequest.Error =
      TransactionProcessor.SubmissionErrors.SequencerRequest.Error(error.sendError)

    override def submissionErrorTrackingData(
        error: SubmissionSendError
    )(implicit traceContext: TraceContext): TransactionSubmissionTrackingData = {
      val errorCode: TransactionError = error.sendError match {
        case SendAsyncClientError.RequestRefused(SendAsyncError.Overloaded(_)) =>
          TransactionProcessor.SubmissionErrors.DomainBackpressure.Rejection(error.toString)
        case otherSendError =>
          TransactionProcessor.SubmissionErrors.SequencerRequest.Error(otherSendError)
      }
      val rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(errorCode)
      TransactionSubmissionTrackingData(
        completionInfo,
        rejectionCause,
        domainId,
        protocolVersion,
      )
    }
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: Unit,
      submissionParam: SubmissionParam,
      pendingSubmissionId: PendingSubmissionId,
  ): EitherT[Future, SubmissionSendError, SubmissionResultArgs] =
    EitherT.pure(())

  override def createSubmissionResult(
      deliver: Deliver[Envelope[?]],
      unit: Unit,
  ): TransactionSubmitted =
    TransactionSubmitted

  // TODO(#8057) extract the decryption into a helper class that can be unit-tested.
  override def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType]]]],
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionProcessorError, DecryptedViews] =
    metrics.protocolMessages.transactionMessageReceipt.timeEitherFUS {
      // even if we encounter errors, we process the good views as normal
      // such that the validation is available if the transaction confirmation request gets approved nevertheless.

      val pureCrypto = snapshot.pureCrypto

      def lightTransactionViewTreeDeserializer(
          bytes: ByteString
      ): Either[DefaultDeserializationError, LightTransactionViewTree] =
        LightTransactionViewTree
          .fromByteString((pureCrypto, computeRandomnessLength(pureCrypto)), protocolVersion)(
            bytes
          )
          .leftMap(err => DefaultDeserializationError(err.message))

      def decryptTree(
          vt: TransactionViewMessage,
          optRandomness: Option[SecureRandomness],
      ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, LightTransactionViewTree] =
        EncryptedViewMessage.decryptFor(
          staticDomainParameters,
          snapshot,
          sessionKeyStore,
          vt,
          participantId,
          optRandomness,
        )(
          lightTransactionViewTreeDeserializer
        )

      // To recover parallel processing to the largest possible extent, we'll associate a promise to each received
      // view. The promise gets fulfilled once the randomness for that view is computed - either directly by decryption,
      // because the participant is an informee of the view, or indirectly, because the participant is an informee on an
      // ancestor view and so it contains that view's randomness.

      // TODO(i12911): a malicious submitter can send a bogus view whose randomness cannot be decrypted/derived,
      //  crashing the SyncDomain
      val randomnessMap =
        batch.foldLeft(Map.empty[ViewHash, PromiseUnlessShutdown[SecureRandomness]]) {
          case (m, evt) =>
            m + (evt.protocolMessage.viewHash -> new PromiseUnlessShutdown[SecureRandomness](
              "secure-randomness",
              futureSupervisor,
            ))
        }

      // We keep track of all randomnesses used for the views, both the one used to
      // encrypt the view and the one that is sent as part of the ancestor view, and check for mismatches at the end.
      val allRandomnessMap = new ConcurrentHashMap[ViewHash, Seq[SecureRandomness]]()

      def addRandomnessToMap(viewHash: ViewHash, toAdd: SecureRandomness): SecureRandomness = {
        allRandomnessMap.compute(
          viewHash,
          (_, existing) => {
            val updatedList =
              if (existing == null || existing.isEmpty) Seq(toAdd)
              else existing :+ toAdd
            updatedList
          },
        )
        toAdd
      }

      def checkRandomnessMap(): Unit =
        allRandomnessMap.asScala.find { case (_, listRandomness) =>
          listRandomness.distinct.size >= 2
        } match {
          case Some((viewHash, _)) =>
            ErrorUtil.internalError(
              new IllegalArgumentException(
                s"View $viewHash has different encryption keys associated with it."
              )
            )
          case None => ()
        }

      def extractRandomnessFromView(
          transactionViewEnvelope: OpenEnvelope[TransactionViewMessage]
      ) = {
        def completeRandomnessPromise(): FutureUnlessShutdown[SecureRandomness] = {
          val message = transactionViewEnvelope.protocolMessage
          val randomnessF = EncryptedViewMessage
            .decryptRandomness(
              staticDomainParameters.requiredEncryptionSpecs,
              snapshot,
              sessionKeyStore,
              message,
              participantId,
            )
            .map(addRandomnessToMap(message.viewHash, _))
            .valueOr { e =>
              ErrorUtil.internalError(
                new IllegalArgumentException(
                  s"Can't decrypt the randomness of the view with hash ${message.viewHash} " +
                    s"where I'm allegedly an informee. $e"
                )
              )
            }
          checked(randomnessMap(transactionViewEnvelope.protocolMessage.viewHash))
            .completeWith(randomnessF)
            .discard
          randomnessF
        }

        if (
          transactionViewEnvelope.recipients.leafRecipients.contains(MemberRecipient(participantId))
        ) completeRandomnessPromise().map(_ => ())
        else FutureUnlessShutdown.unit
      }

      def decryptViewWithRandomness(
          viewMessage: TransactionViewMessage,
          randomness: SecureRandomness,
      ): EitherT[
        FutureUnlessShutdown,
        EncryptedViewMessageError,
        (DecryptedView, Option[Signature]),
      ] =
        for {
          ltvt <- decryptTree(viewMessage, Some(randomness))
          _ = ltvt.subviewHashesAndKeys
            .map { case ViewHashAndKey(subviewHash, subviewKey) =>
              randomnessMap.get(subviewHash) match {
                case Some(promise) =>
                  promise.outcome(addRandomnessToMap(subviewHash, subviewKey))
                case None =>
                  // TODO(i12911): make sure to not approve the request
                  SyncServiceAlarm
                    .Warn(
                      s"View ${viewMessage.viewHash} lists a subview with hash $subviewHash, but " +
                        s"I haven't received any views for this hash"
                    )
                    .report()
              }
            }
        } yield (ltvt, viewMessage.submittingParticipantSignature)

      def decryptView(
          transactionViewEnvelope: OpenEnvelope[TransactionViewMessage]
      ): FutureUnlessShutdown[Either[
        EncryptedViewMessageError,
        (WithRecipients[DecryptedView], Option[Signature]),
      ]] = {
        val extractRandomnessFromViewF = extractRandomnessFromView(transactionViewEnvelope)
        for {
          randomness <- randomnessMap(transactionViewEnvelope.protocolMessage.viewHash).futureUS
          lightViewTreeE <- decryptViewWithRandomness(
            transactionViewEnvelope.protocolMessage,
            randomness,
          ).value
          _ <- extractRandomnessFromViewF
        } yield lightViewTreeE.map { case (view, signature) =>
          (WithRecipients(view, transactionViewEnvelope.recipients), signature)
        }
      }

      EitherT.right {
        for {
          decryptionResult <- batch.toNEF.parTraverse(decryptView)
          _ = checkRandomnessMap()
        } yield DecryptedViews(decryptionResult)
      }
    }

  override def computeFullViews(
      decryptedViewsWithSignatures: Seq[(WithRecipients[DecryptedView], Option[Signature])]
  ): (Seq[(WithRecipients[FullView], Option[Signature])], Seq[MalformedPayload]) = {

    val lens = PLens[
      (WithRecipients[LightTransactionViewTree], Option[Signature]),
      (WithRecipients[FullTransactionViewTree], Option[Signature]),
      LightTransactionViewTree,
      FullTransactionViewTree,
    ](_._1.unwrap)(tvt => { case (WithRecipients(_, rec), sig) =>
      (WithRecipients(tvt, rec), sig)
    })

    val (fullViews, incompleteLightViewTrees, duplicateLightViewTrees) =
      LightTransactionViewTree.toFullViewTrees(
        lens,
        protocolVersion,
        crypto.pureCrypto,
        topLevelOnly = true,
      )(decryptedViewsWithSignatures)

    val incompleteLightViewTreeErrors = incompleteLightViewTrees.map {
      case (WithRecipients(vt, _), _) =>
        ProtocolProcessor.IncompleteLightViewTree(vt.viewPosition)
    }

    val duplicateLightViewTreeErrors = duplicateLightViewTrees.map {
      case (WithRecipients(vt, _), _) =>
        ProtocolProcessor.DuplicateLightViewTree(vt.viewPosition)
    }

    (fullViews, incompleteLightViewTreeErrors ++ duplicateLightViewTreeErrors)
  }

  override def computeParsedRequest(
      rc: RequestCounter,
      ts: CantonTimestamp,
      sc: SequencerCounter,
      rootViewsWithMetadata: NonEmpty[
        Seq[(WithRecipients[FullTransactionViewTree], Option[Signature])]
      ],
      submitterMetadataO: Option[SubmitterMetadata],
      isFreshOwnTimelyRequest: Boolean,
      malformedPayloads: Seq[MalformedPayload],
      mediator: MediatorGroupRecipient,
      snapshot: DomainSnapshotSyncCryptoApi,
      domainParameters: DynamicDomainParametersWithValidity,
  )(implicit traceContext: TraceContext): Future[ParsedTransactionRequest] = {
    val rootViewTrees = rootViewsWithMetadata.map { case (WithRecipients(view, _), _) => view }
    for {
      usedAndCreated <- ExtractUsedAndCreated(
        participantId,
        staticDomainParameters,
        rootViewTrees.map(_.view),
        snapshot.ipsSnapshot,
        loggerFactory,
      )
    } yield ParsedTransactionRequest(
      rc,
      ts,
      sc,
      rootViewsWithMetadata,
      submitterMetadataO,
      isFreshOwnTimelyRequest,
      malformedPayloads,
      mediator,
      usedAndCreated,
      rootViewTrees.head1.workflowIdO,
      snapshot,
      domainParameters,
    )
  }

  override def computeActivenessSet(
      parsedRequest: ParsedTransactionRequest
  )(implicit
      traceContext: TraceContext
  ): Either[TransactionProcessorError, ActivenessSet] =
    // TODO(i12911): check that all non-root lightweight trees can be decrypted with the expected (derived) randomness
    //   Also, check that all the view's informees received the derived randomness
    Right(parsedRequest.usedAndCreated.activenessSet)

  def authenticateInputContracts(
      parsedRequest: ParsedTransactionRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, Unit] =
    authenticateInputContractsInternal(
      parsedRequest.usedAndCreated.contracts.used
    )

  override def constructPendingDataAndResponse(
      parsedRequest: ParsedTransactionRequest,
      reassignmentLookup: ReassignmentLookup,
      activenessResultFuture: FutureUnlessShutdown[ActivenessResult],
      engineController: EngineController,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionProcessorError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {

    val ParsedTransactionRequest(
      rc,
      requestTimestamp,
      sc,
      _,
      _,
      freshOwnTimelyTx,
      malformedPayloads,
      mediator,
      _,
      _,
      snapshot,
      _,
    ) = parsedRequest

    val ipsSnapshot = snapshot.ipsSnapshot
    val requestId = RequestId(requestTimestamp)

    def checkReplayedTransaction: Option[String] =
      Option.when(!freshOwnTimelyTx)("View %s belongs to a replayed transaction")

    def doParallelChecks(): Future[ParallelChecksResult] = {
      val ledgerTime = parsedRequest.ledgerTime

      for {
        authenticationResult <- authenticationValidator.verifyViewSignatures(parsedRequest)

        consistencyResultE = ContractConsistencyChecker
          .assertInputContractsInPast(
            parsedRequest.usedAndCreated.contracts.used.toList,
            ledgerTime,
          )

        domainParameters <- ipsSnapshot.findDynamicDomainParametersOrDefault(protocolVersion)

        // `tryCommonData` should never throw here because all views have the same root hash
        // which already commits to the ParticipantMetadata and CommonMetadata
        commonData = checked(tryCommonData(parsedRequest.rootViewTrees))
        amSubmitter = parsedRequest.submitterMetadataO.exists(
          _.submittingParticipant == participantId
        )
        timeValidationE = TimeValidator.checkTimestamps(
          commonData,
          requestTimestamp,
          domainParameters.ledgerTimeRecordTimeTolerance,
          amSubmitter,
          logger,
        )

        replayCheckResult = if (amSubmitter) checkReplayedTransaction else None

        authorizationResult <- authorizationValidator.checkAuthorization(parsedRequest)

        // We run the model conformance check asynchronously so that we can complete the pending request
        // in the Phase37Synchronizer without waiting for it, thereby allowing us to concurrently receive a
        // mediator verdict.
        conformanceResultET = modelConformanceChecker
          .check(
            parsedRequest.rootViewTrees,
            keyResolverFor(_),
            rc,
            ipsSnapshot,
            commonData,
            getEngineAbortStatus = () => engineController.abortStatus,
          )

        globalKeyHostedParties <- InternalConsistencyChecker.hostedGlobalKeyParties(
          parsedRequest.rootViewTrees,
          participantId,
          snapshot.ipsSnapshot,
        )

        internalConsistencyResultE = internalConsistencyChecker.check(
          parsedRequest.rootViewTrees,
          alertingPartyLookup(globalKeyHostedParties),
        )

      } yield ParallelChecksResult(
        authenticationResult,
        consistencyResultE,
        authorizationResult,
        conformanceResultET,
        internalConsistencyResultE,
        timeValidationE,
        replayCheckResult,
      )
    }

    def awaitActivenessResult: FutureUnlessShutdown[ActivenessResult] = activenessResultFuture.map {
      activenessResult =>
        val contractResult = activenessResult.contracts

        if (contractResult.notFree.nonEmpty)
          throw new RuntimeException(
            s"Activeness result for a transaction confirmation request contains already non-free contracts ${contractResult.notFree}"
          )
        if (activenessResult.inactiveReassignments.nonEmpty)
          throw new RuntimeException(
            s"Activeness result for a transaction confirmation request contains inactive reassignments ${activenessResult.inactiveReassignments}"
          )
        activenessResult
    }

    def computeValidationResult(
        parsedRequest: ParsedTransactionRequest,
        parallelChecksResult: ParallelChecksResult,
        activenessResult: ActivenessResult,
    ): TransactionValidationResult = {
      val viewResults = SortedMap.newBuilder[ViewPosition, ViewValidationResult](
        ViewPosition.orderViewPosition.toOrdering
      )

      parsedRequest.rootViewTrees.forgetNE
        .flatMap(v => v.view.allSubviewsWithPosition(v.viewPosition))
        .foreach { case (view, viewPosition) =>
          val participantView = ParticipantTransactionView.tryCreate(view)
          val viewParticipantData = participantView.viewParticipantData
          val createdCore = viewParticipantData.createdCore.map(_.contract.contractId).toSet
          /* Since `viewParticipantData.coreInputs` contains all input contracts (archivals and usage only),
           * it suffices to check for `coreInputs` here.
           * We don't check for `viewParticipantData.createdInSubviewArchivedInCore` in this view
           * because it suffices to check them in the subview where the contract is created.
           */
          val coreInputs = viewParticipantData.coreInputs.keySet

          // No need to check for created contracts being locked because then they'd be reported as existing.
          val contractResult = activenessResult.contracts
          val alreadyLocked = contractResult.alreadyLocked intersect coreInputs
          val existing = contractResult.notFresh.intersect(createdCore)
          val unknown = contractResult.unknown intersect coreInputs
          val notActive = contractResult.notActive.keySet intersect coreInputs
          val inactive = unknown ++ notActive

          val viewActivenessResult = ViewActivenessResult(
            inactiveContracts = inactive,
            alreadyLockedContracts = alreadyLocked,
            existingContracts = existing,
          )

          viewResults += (viewPosition -> ViewValidationResult(
            participantView,
            viewActivenessResult,
          ))
        }

      val usedAndCreated = parsedRequest.usedAndCreated
      validation.TransactionValidationResult(
        transactionId = parsedRequest.transactionId,
        submitterMetadataO = parsedRequest.submitterMetadataO,
        workflowIdO = parsedRequest.workflowIdO,
        contractConsistencyResultE = parallelChecksResult.consistencyResultE,
        authenticationResult = parallelChecksResult.authenticationResult,
        authorizationResult = parallelChecksResult.authorizationResult,
        modelConformanceResultET = parallelChecksResult.conformanceResultET,
        internalConsistencyResultE = parallelChecksResult.internalConsistencyResultE,
        consumedInputsOfHostedParties = usedAndCreated.contracts.consumedInputsOfHostedStakeholders,
        witnessed = usedAndCreated.contracts.witnessed,
        divulged = usedAndCreated.contracts.divulged,
        createdContracts = usedAndCreated.contracts.created,
        transient = usedAndCreated.contracts.transient,
        activenessResult = activenessResult,
        viewValidationResults = viewResults.result(),
        timeValidationResultE = parallelChecksResult.timeValidationResultE,
        hostedWitnesses = usedAndCreated.hostedWitnesses,
        replayCheckResult = parallelChecksResult.replayCheckResult,
      )
    }

    val mediatorRecipients = Recipients.cc(mediator)

    val result =
      for {
        parallelChecksResult <- FutureUnlessShutdown.outcomeF(doParallelChecks())
        activenessResult <- awaitActivenessResult
      } yield {
        val transactionValidationResult = computeValidationResult(
          parsedRequest,
          parallelChecksResult,
          activenessResult,
        )
        // The responses depend on the result of the model conformance check, and are therefore also delayed.
        val responsesF =
          confirmationResponseFactory.createConfirmationResponses(
            requestId,
            malformedPayloads,
            transactionValidationResult,
            ipsSnapshot,
          )

        val pendingTransaction =
          createPendingTransaction(
            requestId,
            responsesF,
            transactionValidationResult,
            rc,
            sc,
            mediator,
            freshOwnTimelyTx,
            engineController,
          )
        StorePendingDataAndSendResponseAndCreateTimeout(
          pendingTransaction,
          EitherT.right(responsesF.map(_.map(_ -> mediatorRecipients))),
          RejectionArgs(
            pendingTransaction,
            LocalRejectError.TimeRejects.LocalTimeout.Reject(),
          ),
        )
      }
    EitherT.right(result)
  }

  override def constructResponsesForMalformedPayloads(
      requestId: RequestId,
      rootHash: RootHash,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit
      traceContext: TraceContext
  ): Seq[ConfirmationResponse] =
    Seq(
      confirmationResponseFactory.createConfirmationResponsesForMalformedPayloads(
        requestId,
        rootHash,
        malformedPayloads,
      )
    )

  override def eventAndSubmissionIdForRejectedCommand(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      submitterMetadata: ViewSubmitterMetadata,
      _rootHash: RootHash,
      freshOwnTimelyTx: Boolean,
      error: TransactionError,
  )(implicit
      traceContext: TraceContext
  ): (Option[Traced[Update]], Option[PendingSubmissionId]) = {
    val rejection = Update.CommandRejected.FinalReason(error.rpcStatus())
    completionInfoFromSubmitterMetadataO(submitterMetadata, freshOwnTimelyTx).map {
      completionInfo =>
        Traced(
          Update.CommandRejected(
            ts.toLf,
            completionInfo,
            rejection,
            domainId,
            Some(
              DomainIndex.of(
                RequestIndex(
                  counter = rc,
                  sequencerCounter = Some(sc),
                  timestamp = ts,
                )
              )
            ),
          )
        )
    } -> None // Transaction processing doesn't use pending submissions
  }

  override def postProcessSubmissionRejectedCommand(
      error: TransactionError,
      pendingSubmission: Nothing,
  )(implicit
      traceContext: TraceContext
  ): Unit = ()

  override def createRejectionEvent(rejectionArgs: TransactionProcessingSteps.RejectionArgs)(
      implicit traceContext: TraceContext
  ): Either[TransactionProcessorError, Option[Traced[Update]]] = {

    val RejectionArgs(pendingTransaction, rejectionReason) = rejectionArgs
    val PendingTransaction(
      freshOwnTimelyTx,
      requestTime,
      requestCounter,
      requestSequencerCounter,
      transactionValidationResult,
      _,
      _locallyRejected,
      _engineController,
      _abortedF,
    ) =
      pendingTransaction
    val submitterMetaO = transactionValidationResult.submitterMetadataO
    val completionInfoO =
      submitterMetaO.flatMap(completionInfoFromSubmitterMetadataO(_, freshOwnTimelyTx))

    rejectionReason.logWithContext(Map("requestId" -> pendingTransaction.requestId.toString))
    val rejection = Update.CommandRejected.FinalReason(rejectionReason.reason())

    val updateO = completionInfoO.map(info =>
      Traced(
        Update.CommandRejected(
          requestTime.toLf,
          info,
          rejection,
          domainId,
          Some(
            DomainIndex.of(
              RequestIndex(
                counter = requestCounter,
                sequencerCounter = Some(requestSequencerCounter),
                timestamp = requestTime,
              )
            )
          ),
        )
      )
    )
    Right(updateO)
  }

  @VisibleForTesting
  private[protocol] def authenticateInputContractsInternal(
      inputContracts: Map[LfContractId, SerializableContract]
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, Unit] =
    EitherT.fromEither(
      inputContracts.toList
        .traverse_ { case (contractId, contract) =>
          serializableContractAuthenticator
            .authenticate(contract)
            .leftMap(message => ContractAuthenticationFailed.Error(contractId, message).reported())
        }
    )

  private def completionInfoFromSubmitterMetadataO(
      meta: SubmitterMetadata,
      freshOwnTimelyTx: Boolean,
  ): Option[CompletionInfo] = {
    lazy val completionInfo = CompletionInfo(
      meta.actAs.toList,
      meta.applicationId.unwrap,
      meta.commandId.unwrap,
      Some(meta.dedupPeriod),
      meta.submissionId,
      messageUuid = None,
    )

    Option.when(freshOwnTimelyTx)(completionInfo)
  }

  private[this] def createPendingTransaction(
      id: RequestId,
      responsesF: FutureUnlessShutdown[Seq[ConfirmationResponse]],
      transactionValidationResult: TransactionValidationResult,
      rc: RequestCounter,
      sc: SequencerCounter,
      mediator: MediatorGroupRecipient,
      freshOwnTimelyTx: Boolean,
      engineController: EngineController,
  )(implicit traceContext: TraceContext): PendingTransaction = {
    // We consider that we rejected if at least one of the responses is not "approve'
    val locallyRejectedF = responsesF.map(_.exists(response => !response.localVerdict.isApprove))

    // The request was aborted if the model conformance check ended with an abort error, due to either a timeout
    // or a negative mediator verdict concurrently received in Phase 7
    val engineAbortStatusF = transactionValidationResult.modelConformanceResultET.value.map {
      case Left(error) => error.engineAbortStatus
      case _ => EngineAbortStatus.notAborted
    }

    validation.PendingTransaction(
      freshOwnTimelyTx,
      id.unwrap,
      rc,
      sc,
      transactionValidationResult,
      mediator,
      locallyRejectedF,
      engineController.abort,
      engineAbortStatusF,
    )
  }

  private def getCommitSetAndContractsToBeStoredAndEventApproveConform(
      pendingRequestData: RequestType#PendingRequestData,
      completionInfoO: Option[CompletionInfo],
      modelConformanceResult: ModelConformanceChecker.Result,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val txValidationResult = pendingRequestData.transactionValidationResult
    val commitSet = txValidationResult.commitSet(pendingRequestData.requestId)

    computeCommitAndContractsAndEvent(
      requestTime = pendingRequestData.requestTime,
      requestCounter = pendingRequestData.requestCounter,
      txId = txValidationResult.transactionId,
      workflowIdO = txValidationResult.workflowIdO,
      requestSequencerCounter = pendingRequestData.requestSequencerCounter,
      commitSet = commitSet,
      createdContracts = txValidationResult.createdContracts,
      witnessed = txValidationResult.witnessed,
      divulged = txValidationResult.divulged,
      hostedWitnesses = txValidationResult.hostedWitnesses,
      completionInfoO = completionInfoO,
      lfTx = modelConformanceResult.suffixedTransaction,
    )
  }

  private def computeCommitAndContractsAndEvent(
      requestTime: CantonTimestamp,
      requestCounter: RequestCounter,
      txId: TransactionId,
      workflowIdO: Option[WorkflowId],
      requestSequencerCounter: SequencerCounter,
      commitSet: CommitSet,
      createdContracts: Map[LfContractId, SerializableContract],
      witnessed: Map[LfContractId, SerializableContract],
      divulged: Map[LfContractId, SerializableContract],
      hostedWitnesses: Set[LfPartyId],
      completionInfoO: Option[CompletionInfo],
      lfTx: WellFormedTransaction[WithSuffixesAndMerged],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val commitSetF = Future.successful(commitSet)
    val contractsToBeStored = createdContracts.values.toSeq.map(WithTransactionId(_, txId))

    val witnessedAndDivulged = witnessed ++ divulged
    def storeDivulgedContracts: Future[Unit] =
      contractStore
        .storeDivulgedContracts(
          requestCounter,
          witnessedAndDivulged.values.toSeq,
        )

    for {
      // Store the divulged contracts in the contract store
      _ <- EitherT.right(storeDivulgedContracts)

      lfTxId <- EitherT
        .fromEither[Future](txId.asLedgerTransactionId)
        .leftMap[TransactionProcessorError](FieldConversionError("Transaction Id", _))

      contractMetadata =
        // We deliberately do not forward the driver metadata
        // for divulged contracts since they are not visible on the Ledger API
        (createdContracts ++ witnessed).view.collect {
          case (contractId, SerializableContract(_, _, _, _, Some(salt))) =>
            contractId -> DriverContractMetadata(salt).toLfBytes(protocolVersion)
        }.toMap

      acceptedEvent = Traced(
        Update.TransactionAccepted(
          completionInfoO = completionInfoO,
          transactionMeta = TransactionMeta(
            ledgerEffectiveTime = lfTx.metadata.ledgerTime.toLf,
            workflowId = workflowIdO.map(_.unwrap),
            submissionTime = lfTx.metadata.submissionTime.toLf,
            // Set the submission seed to zeros one (None no longer accepted) because it is pointless for projected
            // transactions and it leaks the structure of the omitted parts of the transaction.
            submissionSeed = Update.noOpSeed,
            optUsedPackages = None,
            optNodeSeeds = Some(lfTx.metadata.seeds.to(ImmArray)),
            optByKeyNodes = None, // optByKeyNodes is unused by the indexer
          ),
          transaction = LfCommittedTransaction(lfTx.unwrap),
          transactionId = lfTxId,
          recordTime = requestTime.toLf,
          hostedWitnesses = hostedWitnesses.toList,
          contractMetadata = contractMetadata,
          domainId = domainId,
          domainIndex = Some(
            DomainIndex.of(
              RequestIndex(
                counter = requestCounter,
                sequencerCounter = Some(requestSequencerCounter),
                timestamp = requestTime,
              )
            )
          ),
        )
      )
    } yield CommitAndStoreContractsAndPublishEvent(
      Some(commitSetF),
      contractsToBeStored,
      Some(acceptedEvent),
    )
  }

  private def getCommitSetAndContractsToBeStoredAndEventApprovePartlyConform(
      pendingRequestData: RequestType#PendingRequestData,
      completionInfoO: Option[CompletionInfo],
      validSubTransaction: WellFormedTransaction[WithSuffixesAndMerged],
      validSubViewsNE: NonEmpty[Seq[TransactionView]],
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] =
    for {
      usedAndCreated <- EitherT.right(
        ExtractUsedAndCreated(
          participantId,
          staticDomainParameters,
          validSubViewsNE,
          topologySnapshot,
          loggerFactory,
        )
      )

      createdContracts = usedAndCreated.contracts.created

      commitSet = CommitSet.createForTransaction(
        activenessResult = pendingRequestData.transactionValidationResult.activenessResult,
        requestId = pendingRequestData.requestId,
        consumedInputsOfHostedParties = usedAndCreated.contracts.consumedInputsOfHostedStakeholders,
        transient = usedAndCreated.contracts.transient,
        createdContracts = createdContracts,
      )

      commitAndContractsAndEvent <- computeCommitAndContractsAndEvent(
        requestTime = pendingRequestData.requestTime,
        requestCounter = pendingRequestData.requestCounter,
        txId = pendingRequestData.transactionValidationResult.transactionId,
        workflowIdO = pendingRequestData.transactionValidationResult.workflowIdO,
        requestSequencerCounter = pendingRequestData.requestSequencerCounter,
        commitSet = commitSet,
        createdContracts = createdContracts,
        witnessed = usedAndCreated.contracts.witnessed,
        divulged = usedAndCreated.contracts.divulged,
        hostedWitnesses = usedAndCreated.hostedWitnesses,
        completionInfoO = completionInfoO,
        lfTx = validSubTransaction,
      )
    } yield commitAndContractsAndEvent

  override def getCommitSetAndContractsToBeStoredAndEvent(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      verdict: Verdict,
      pendingRequestData: RequestType#PendingRequestData,
      pendingSubmissionMap: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionProcessorError,
    CommitAndStoreContractsAndPublishEvent,
  ] = {
    val ts = event.event.content.timestamp
    val submitterMetaO = pendingRequestData.transactionValidationResult.submitterMetadataO
    val completionInfoO = submitterMetaO.flatMap(
      completionInfoFromSubmitterMetadataO(_, pendingRequestData.freshOwnTimelyTx)
    )

    def getCommitSetAndContractsToBeStoredAndEvent(
        topologySnapshot: TopologySnapshot
    ): EitherT[
      FutureUnlessShutdown,
      TransactionProcessorError,
      CommitAndStoreContractsAndPublishEvent,
    ] = {
      val resultFE = for {
        modelConformanceResultE <-
          pendingRequestData.transactionValidationResult.modelConformanceResultET.value

        resultET = (verdict, modelConformanceResultE) match {

          // Positive verdict: we commit
          case (_: Verdict.Approve, _) => handleApprovedVerdict(topologySnapshot)

          // Model conformance check error:
          // - if the error is an abort, it means the model conformance check was still running while we received
          //   a negative verdict; we then reject with the verdict, as it is the best information we have
          // - otherwise, we reject with the actual error
          case (reasons: Verdict.ParticipantReject, Left(error)) =>
            if (error.engineAbortStatus.isAborted)
              rejected(reasons.keyEvent)
            else rejectedWithModelConformanceError(error)

          case (reject: Verdict.MediatorReject, Left(error)) =>
            if (error.engineAbortStatus.isAborted)
              rejected(reject)
            else rejectedWithModelConformanceError(error)

          // No model conformance check error: we reject with the verdict
          case (reasons: Verdict.ParticipantReject, _) =>
            rejected(reasons.keyEvent)

          case (reject: Verdict.MediatorReject, _) =>
            rejected(reject)

        }
        result <- resultET.value
      } yield result

      EitherT(resultFE)
    }

    def handleApprovedVerdict(topologySnapshot: TopologySnapshot)(implicit
        traceContext: TraceContext
    ): EitherT[
      FutureUnlessShutdown,
      TransactionProcessorError,
      CommitAndStoreContractsAndPublishEvent,
    ] =
      pendingRequestData.transactionValidationResult.modelConformanceResultET.biflatMap(
        {
          case ErrorWithSubTransaction(
                _errors,
                Some(validSubTransaction),
                NonEmpty(validSubViewsNE),
              ) =>
            getCommitSetAndContractsToBeStoredAndEventApprovePartlyConform(
              pendingRequestData,
              completionInfoO,
              validSubTransaction,
              validSubViewsNE,
              topologySnapshot,
            ).mapK(FutureUnlessShutdown.outcomeK)

          case error =>
            // There is no valid subview
            //   -> we can reject as no participant will commit a subtransaction and violate transparency.
            rejectedWithModelConformanceError(error)
        },
        modelConformanceResult =>
          getCommitSetAndContractsToBeStoredAndEventApproveConform(
            pendingRequestData,
            completionInfoO,
            modelConformanceResult,
          ).mapK(FutureUnlessShutdown.outcomeK),
      )

    def rejectedWithModelConformanceError(error: ErrorWithSubTransaction) =
      rejected(
        LocalRejectError.MalformedRejects.ModelConformance
          .Reject(error.errors.head1.toString)
          .toLocalReject(protocolVersion)
      )

    def rejected(
        rejection: TransactionRejection
    ): EitherT[
      FutureUnlessShutdown,
      TransactionProcessorError,
      CommitAndStoreContractsAndPublishEvent,
    ] =
      (for {
        event <- EitherT.fromEither[Future](
          createRejectionEvent(RejectionArgs(pendingRequestData, rejection))
        )
      } yield CommitAndStoreContractsAndPublishEvent(None, Seq(), event))
        .mapK(FutureUnlessShutdown.outcomeK)

    for {
      topologySnapshot <- EitherT
        .right[TransactionProcessorError](
          crypto.ips.awaitSnapshotUS(pendingRequestData.requestTime)
        )

      maxDecisionTime <- ProcessingSteps
        .getDecisionTime(topologySnapshot, pendingRequestData.requestTime)
        .leftMap(DomainParametersError(domainId, _))
        .mapK(FutureUnlessShutdown.outcomeK)

      _ <-
        (if (ts <= maxDecisionTime) EitherT.pure[Future, TransactionProcessorError](())
         else
           EitherT.right[TransactionProcessorError](
             Future.failed(new IllegalArgumentException("Timeout message after decision time"))
           )).mapK(FutureUnlessShutdown.outcomeK)

      resultTopologySnapshot <- EitherT
        .right[TransactionProcessorError](
          crypto.ips.awaitSnapshotUS(ts)
        )

      mediatorActiveAtResultTs <- EitherT
        .right[TransactionProcessorError](
          resultTopologySnapshot.isMediatorActive(pendingRequestData.mediator)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      res <-
        if (mediatorActiveAtResultTs) getCommitSetAndContractsToBeStoredAndEvent(topologySnapshot)
        else {
          // Additional validation requested during security audit as DIA-003-013.
          // Activeness of the mediator already gets checked in Phase 3,
          // this additional validation covers the case that the mediator gets deactivated between Phase 3 and Phase 7.
          rejected(
            LocalRejectError.MalformedRejects.MalformedRequest
              .Reject(
                s"The mediator ${pendingRequestData.mediator} has been deactivated while processing the request. Rolling back."
              )
              .toLocalReject(protocolVersion)
          )
        }
    } yield res
  }

  override def postProcessResult(verdict: Verdict, pendingSubmission: Nothing)(implicit
      traceContext: TraceContext
  ): Unit = ()

  override def embedRequestError(
      err: ProtocolProcessor.RequestProcessingError
  ): TransactionProcessorError =
    GenericStepsError(err)

  override def embedResultError(
      err: ProtocolProcessor.ResultProcessingError
  ): TransactionProcessorError =
    GenericStepsError(err)
}

object TransactionProcessingSteps {

  final case class SubmissionParam(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: WellFormedTransaction[WithoutSuffixes],
      disclosedContracts: Map[LfContractId, SerializableContract],
  )

  final case class ParsedTransactionRequest(
      override val rc: RequestCounter,
      override val requestTimestamp: CantonTimestamp,
      override val sc: SequencerCounter,
      rootViewTreesWithMetadata: NonEmpty[
        Seq[(WithRecipients[FullTransactionViewTree], Option[Signature])]
      ],
      override val submitterMetadataO: Option[SubmitterMetadata],
      override val isFreshOwnTimelyRequest: Boolean,
      override val malformedPayloads: Seq[MalformedPayload],
      override val mediator: MediatorGroupRecipient,
      usedAndCreated: UsedAndCreated,
      workflowIdO: Option[WorkflowId],
      override val snapshot: DomainSnapshotSyncCryptoApi,
      override val domainParameters: DynamicDomainParametersWithValidity,
  ) extends ParsedRequest[SubmitterMetadata] {

    lazy val rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]] = rootViewTreesWithMetadata.map {
      case (WithRecipients(rootViewTree, _), _) => rootViewTree
    }

    lazy val rootViewTreesWithSignatures: NonEmpty[
      Seq[(FullTransactionViewTree, Option[Signature])]
    ] = rootViewTreesWithMetadata.map { case (WithRecipients(rootViewTree, _), signature) =>
      (rootViewTree, signature)
    }

    override def rootHash: RootHash = rootViewTrees.head1.rootHash

    def transactionId: TransactionId = rootViewTrees.head1.transactionId

    def ledgerTime: CantonTimestamp = rootViewTrees.head1.ledgerTime
  }

  private final case class ParallelChecksResult(
      authenticationResult: Map[ViewPosition, String],
      consistencyResultE: Either[List[ReferenceToFutureContractError], Unit],
      authorizationResult: Map[ViewPosition, String],
      conformanceResultET: EitherT[
        FutureUnlessShutdown,
        ModelConformanceChecker.ErrorWithSubTransaction,
        ModelConformanceChecker.Result,
      ],
      internalConsistencyResultE: Either[ErrorWithInternalConsistencyCheck, Unit],
      timeValidationResultE: Either[TimeCheckFailure, Unit],
      replayCheckResult: Option[String],
  )

  final case class RejectionArgs(
      pendingTransaction: PendingTransaction,
      error: TransactionRejection,
  )

  def keyResolverFor(
      rootView: TransactionView
  )(implicit loggingContext: NamedLoggingContext): LfKeyResolver =
    rootView.globalKeyInputs.fmap(_.unversioned.resolution)

  /** @throws java.lang.IllegalArgumentException if `receivedViewTrees` contains views with different transaction root hashes
    */
  def tryCommonData(receivedViewTrees: NonEmpty[Seq[FullTransactionViewTree]]): CommonData = {
    val distinctCommonData = receivedViewTrees
      .map(v => CommonData(v.transactionId, v.ledgerTime, v.submissionTime))
      .distinct
    if (distinctCommonData.lengthCompare(1) == 0) distinctCommonData.head1
    else
      throw new IllegalArgumentException(
        s"Found several different transaction IDs, LETs or confirmation policies: $distinctCommonData"
      )
  }

  final case class CommonData(
      transactionId: TransactionId,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
  )
}
