// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, NonEmptyChain}
import cats.implicits.catsStdInstancesForFuture
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.{
  DomainSnapshotSyncCryptoApi,
  DomainSyncCryptoClient,
  Signature,
}
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition, ViewTree, ViewType}
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.Phase37Synchronizer.RequestOutcome
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  PendingRequestData,
  WrapsProcessorError,
}
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker.TimeoutResult
import com.digitalasset.canton.participant.protocol.conflictdetection.{CommitSet, RequestTracker}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmission,
  InFlightSubmissionTracker,
  SequencedSubmission,
  UnsequencedSubmission,
}
import com.digitalasset.canton.participant.protocol.validation.{
  PendingTransaction,
  RecipientsValidator,
}
import com.digitalasset.canton.participant.store
import com.digitalasset.canton.participant.store.{StoredContract, SyncDomainEphemeralState}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.messages.Verdict.Approve
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{AsyncResult, HandlerResult}
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, MediatorRef, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.{condUnitET, ifThenET}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, LfPartyId, RequestCounter, SequencerCounter, checked}
import com.google.common.annotations.VisibleForTesting

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** The [[ProtocolProcessor]] combines [[ProcessingSteps]] specific to a particular kind of request
  * with the common processing steps and wires them up with the state updates and synchronization.
  *
  * @param steps The specific processing steps
  * @tparam SubmissionParam  The bundled submission parameters
  * @tparam SubmissionResult The bundled submission results
  * @tparam RequestViewType     The type of view trees used by the request
  * @tparam Result           The specific type of the result message
  * @tparam SubmissionError  The type of errors that occur during submission processing
  */
abstract class ProtocolProcessor[
    SubmissionParam,
    SubmissionResult,
    RequestViewType <: ViewType,
    Result <: MediatorResult with SignedProtocolMessageContent,
    SubmissionError <: WrapsProcessorError,
](
    private[protocol] val steps: ProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      RequestViewType,
      Result,
      SubmissionError,
    ],
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    ephemeral: SyncDomainEphemeralState,
    crypto: DomainSyncCryptoClient,
    sequencerClient: SequencerClient,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    skipRecipientsCheck: Boolean,
)(implicit
    ec: ExecutionContext,
    resultCast: SignedMessageContentCast[Result],
) extends AbstractMessageProcessor(
      ephemeral,
      crypto,
      sequencerClient,
      protocolVersion,
    )
    with RequestProcessor[RequestViewType] {

  import ProtocolProcessor.*
  import com.digitalasset.canton.util.ShowUtil.*

  def participantId: ParticipantId

  private val recipientsValidator
      : RecipientsValidator[(WithRecipients[steps.DecryptedView], Option[Signature])] =
    new RecipientsValidator(_._1.unwrap, _._1.recipients, loggerFactory)

  private[this] def withKind(message: String): String = s"${steps.requestKind}: $message"

  /** Stores a counter for the submissions.
    * Incremented whenever we pick a mediator for a submission
    * so that we use mediators round-robin.
    *
    * Every processor picks the mediators independently,
    * so it may be that the participant picks the same mediator several times in a row,
    * but for different kinds of requests.
    */
  private val submissionCounter: AtomicInteger = new AtomicInteger(0)

  /** Submits the request to the sequencer, using a recent topology snapshot and the current persisted state
    * as an approximation to the future state at the assigned request timestamp.
    *
    * @param submissionParam The bundled submission parameters
    * @return The submission error or a future with the submission result.
    *         With submission tracking, the outer future completes after the submission is registered as in-flight,
    *         and the inner future after the submission has been sequenced or if it will never be sequenced.
    *         Without submission tracking, both futures complete after the submission has been sequenced
    *         or if it will not be sequenced.
    */
  def submit(submissionParam: SubmissionParam)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SubmissionError, FutureUnlessShutdown[SubmissionResult]] = {
    logger.debug(withKind(s"Preparing request ${steps.submissionDescription(submissionParam)}"))

    val recentSnapshot = crypto.currentSnapshotApproximation
    for {
      mediator <- chooseMediator(recentSnapshot.ipsSnapshot)
        .leftMap(steps.embedNoMediatorError)
        .mapK(FutureUnlessShutdown.outcomeK)
      submission <- steps.prepareSubmission(submissionParam, mediator, ephemeral, recentSnapshot)
      result <- {
        submission match {
          case untracked: steps.UntrackedSubmission =>
            submitWithoutTracking(submissionParam, untracked)
          case tracked: steps.TrackedSubmission => submitWithTracking(submissionParam, tracked)
        }
      }
    } yield result
  }

  private def chooseMediator(
      recentSnapshot: TopologySnapshot
  )(implicit traceContext: TraceContext): EitherT[Future, NoMediatorError, MediatorRef] = {
    val fut = for {
      allMediatorGroups <- recentSnapshot.mediatorGroups()
      allActiveMediatorGroups = allMediatorGroups.filter(_.isActive)
    } yield {
      val mediatorCount = allActiveMediatorGroups.size
      if (mediatorCount == 0) {
        Left(NoMediatorError(recentSnapshot.timestamp))
      } else {
        // Pick the next by incrementing the counter and selecting the mediator modulo the number of all mediators.
        // When the number of mediators changes, this strategy may result in the same mediator being picked twice in a row.
        // This is acceptable as mediator changes are rare.
        //
        // This selection strategy assumes that the `mediators` method in the `MediatorDomainStateClient`
        // returns the mediators in a consistent order. This assumption holds mostly because the cache
        // usually returns the fixed `Seq` in the cache.
        val newSubmissionCounter = submissionCounter.incrementAndGet()
        val chosenIndex = {
          val mod = newSubmissionCounter % mediatorCount
          // The submissionCounter overflows after Int.MAX_VALUE submissions
          // and then the modulo is negative. We must ensure that it's positive!
          if (mod < 0) mod + mediatorCount else mod
        }
        val mediator = checked(allActiveMediatorGroups(chosenIndex))
        val chosen = MediatorRef(mediator)
        logger.debug(s"Chose the mediator $chosen")
        Right(chosen)
      }
    }
    EitherT(fut)
  }

  /** Submits the batch without registering as in-flight and reports send errors as [[scala.Left$]] */
  private def submitWithoutTracking(
      submissionParam: SubmissionParam,
      untracked: steps.UntrackedSubmission,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SubmissionError, FutureUnlessShutdown[SubmissionResult]] = {
    val result = for {

      maxSequencingTime <- EitherT
        .right(
          untracked.maxSequencingTimeO
            .getOrElse(sequencerClient.generateMaxSequencingTime)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      sendResultAndResultArgs <- submitInternal(
        submissionParam,
        untracked.pendingSubmissionId,
        MessageId.randomMessageId(),
        untracked.batch,
        maxSequencingTime,
        untracked.embedSubmissionError,
      )
      (sendResult, resultArgs) = sendResultAndResultArgs
      result <- EitherT.fromEither[FutureUnlessShutdown](sendResult match {
        case SendResult.Success(deliver) => Right(steps.createSubmissionResult(deliver, resultArgs))
        case SendResult.Error(error) =>
          Left(untracked.embedSubmissionError(SequencerDeliverError(error)))
        case SendResult.Timeout(sequencerTime) =>
          Left(untracked.embedSubmissionError(SequencerTimeoutError(sequencerTime)))
      })
    } yield result
    result.bimap(untracked.toSubmissionError, FutureUnlessShutdown.pure)
  }

  /** Register the submission as in-flight, deduplicate it, and submit it.
    * Errors after the registration are reported asynchronously only and return a [[scala.Right$]].
    * This ensures that every submission generates at most one rejection reason, namely through the
    * timely rejection mechanism. In-flight tracking may concurrently remove the submission at any time
    * and publish the timely rejection event instead of the actual error.
    */
  def submitWithTracking(submissionParam: SubmissionParam, tracked: steps.TrackedSubmission)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SubmissionError, FutureUnlessShutdown[SubmissionResult]] = {
    val maxSequencingTimeF =
      tracked.maxSequencingTimeO
        .mapK(FutureUnlessShutdown.outcomeK)
        .getOrElse(sequencerClient.generateMaxSequencingTime)

    EitherT.right(maxSequencingTimeF).flatMap(submitWithTracking(submissionParam, tracked, _))
  }

  private def submitWithTracking(
      submissionParam: SubmissionParam,
      tracked: steps.TrackedSubmission,
      maxSequencingTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SubmissionError, FutureUnlessShutdown[SubmissionResult]] = {
    val messageUuid = UUID.randomUUID()
    val inFlightSubmission = InFlightSubmission(
      changeIdHash = tracked.changeIdHash,
      submissionId = tracked.submissionId,
      submissionDomain = domainId,
      messageUuid = messageUuid,
      rootHashO = None,
      sequencingInfo =
        UnsequencedSubmission(maxSequencingTime, tracked.submissionTimeoutTrackingData),
      submissionTraceContext = traceContext,
    )
    val messageId = inFlightSubmission.messageId
    val specifiedDeduplicationPeriod = tracked.specifiedDeduplicationPeriod
    logger.debug(s"Registering the submission as in-flight")

    val registeredF = inFlightSubmissionTracker
      .register(inFlightSubmission, specifiedDeduplicationPeriod)
      .leftMap(tracked.embedInFlightSubmissionTrackerError)
      .onShutdown {
        // If we abort due to a shutdown, we don't know whether the submission was registered.
        // The SyncDomain should guard this method call with a performUnlessClosing,
        // so we should see a shutdown here only if the SyncDomain close timeout was exceeded.
        // Therefore, WARN makes sense as a logging level.
        logger.warn(s"Shutdown while registering the submission as in-flight.")
        Left(tracked.shutdownDuringInFlightRegistration)
      }

    def observeSubmissionError(
        newUnsequencedSubmission: UnsequencedSubmission
    ): Future[SubmissionResult] = {
      // cap the new timeout by the max sequencing time
      // so that the timeout field can move only backwards
      val newUnsequencedSubmissionWithCappedTimeout =
        if (newUnsequencedSubmission.timeout > maxSequencingTime)
          newUnsequencedSubmission.copy(timeout = maxSequencingTime)
        else newUnsequencedSubmission
      for {
        _unit <- inFlightSubmissionTracker.observeSubmissionError(
          tracked.changeIdHash,
          domainId,
          messageId,
          newUnsequencedSubmissionWithCappedTimeout,
        )
      } yield tracked.onFailure
    }

    // After in-flight registration, Make sure that all errors get a chance to update the tracking data and
    // instead return a `SubmissionResult` so that the submission will be acknowledged over the ledger API.
    def unlessError[A](eitherT: EitherT[FutureUnlessShutdown, UnsequencedSubmission, A])(
        continuation: A => FutureUnlessShutdown[SubmissionResult]
    ): FutureUnlessShutdown[SubmissionResult] = {
      eitherT.value.transformWith {
        case Success(UnlessShutdown.Outcome(Right(a))) => continuation(a)
        case Success(UnlessShutdown.Outcome(Left(newUnsequencedSubmission))) =>
          FutureUnlessShutdown.outcomeF(observeSubmissionError(newUnsequencedSubmission))
        case Success(UnlessShutdown.AbortedDueToShutdown) =>
          logger.debug(s"Failed to process submission due to shutdown")
          FutureUnlessShutdown.pure(tracked.onFailure)
        case Failure(exception) =>
          // We merely log an error and rely on the maxSequencingTimeout to produce a rejection event eventually.
          // It is not clear whether we managed to send the submission.
          logger.error(s"Failed to submit submission", exception)
          FutureUnlessShutdown.pure(tracked.onFailure)
      }
    }

    def afterRegistration(
        deduplicationResult: Either[DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset]
    ): FutureUnlessShutdown[SubmissionResult] = deduplicationResult match {
      case Left(failed) =>
        FutureUnlessShutdown.outcomeF(
          observeSubmissionError(tracked.commandDeduplicationFailure(failed))
        )
      case Right(actualDeduplicationOffset) =>
        def sendBatch(
            preparedBatch: steps.PreparedBatch
        ): FutureUnlessShutdown[SubmissionResult] = {
          val submittedEF = submitInternal(
            submissionParam,
            preparedBatch.pendingSubmissionId,
            messageId,
            preparedBatch.batch,
            maxSequencingTime,
            preparedBatch.embedSequencerRequestError,
          ).leftMap { submissionError =>
            logger.warn(s"Failed to submit submission due to $submissionError")
            preparedBatch.submissionErrorTrackingData(submissionError)
          }

          // As the `SendTracker` does not persist its state,
          // we would observe the sequencing here only if the participant has not crashed.
          // We therefore delegate observing the sequencing to the MessageDispatcher,
          // which can rely on the SequencedEventStore for persistence.
          unlessError(submittedEF) { case (sendResult, resultArgs) =>
            val submissionResult = sendResult match {
              case SendResult.Success(deliver) =>
                steps.createSubmissionResult(deliver, resultArgs)
              case _: SendResult.NotSequenced => tracked.onFailure
            }
            FutureUnlessShutdown.pure(submissionResult)
          }
        }

        unlessError {
          val batchF = for {
            batch <- tracked.prepareBatch(actualDeduplicationOffset, maxSequencingTime)
            _ <- EitherT.right[UnsequencedSubmission](
              inFlightSubmissionTracker.updateRegistration(inFlightSubmission, batch.rootHash)
            )
          } yield batch

          batchF.mapK(FutureUnlessShutdown.outcomeK)
        }(sendBatch)
    }

    registeredF.mapK(FutureUnlessShutdown.outcomeK).map(afterRegistration)
  }

  /** Submit the batch and return the [[com.digitalasset.canton.sequencing.client.SendResult]]
    * and the [[com.digitalasset.canton.participant.protocol.ProcessingSteps#SubmissionResultArgs]].
    */
  private def submitInternal(
      submissionParam: SubmissionParam,
      submissionId: steps.PendingSubmissionId,
      messageId: MessageId,
      batch: Batch[DefaultOpenEnvelope],
      maxSequencingTime: CantonTimestamp,
      embedSubmissionError: SequencerRequestError => steps.SubmissionSendError,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    steps.SubmissionSendError,
    (SendResult, steps.SubmissionResultArgs),
  ] = {
    def removePendingSubmission(): Unit = {
      steps
        .removePendingSubmission(steps.pendingSubmissions(ephemeral), submissionId)
        .discard[Option[steps.PendingSubmissionData]]
    }

    for {
      // The pending submission must be registered before the request is sent, to avoid races
      resultArgs <- steps
        .updatePendingSubmissions(
          steps.pendingSubmissions(ephemeral),
          submissionParam,
          submissionId,
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      _ = logger.debug(
        withKind(
          s"Sending batch with id $submissionId for request ${steps.submissionDescription(submissionParam)}"
        )
      )

      // use the send callback and a promise to capture the eventual sequenced event read by the submitter
      sendResultP = new PromiseUnlessShutdown[SendResult](
        "sequenced-event-send-result",
        futureSupervisor,
      )

      _ <- sequencerClient
        .sendAsync(
          batch,
          SendType.ConfirmationRequest,
          callback = sendResultP.success,
          maxSequencingTime = maxSequencingTime,
          messageId = messageId,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .leftMap { err =>
          removePendingSubmission()
          embedSubmissionError(SequencerRequestError(err))
        }

      // If we're shutting down, the sendResult below won't complete successfully (because it's wrapped in a `FutureUnlessShutdown`)
      // We still want to clean up pending submissions in that case though so make sure we do that by adding a callback on
      // the promise here
      _ = sendResultP.future.onComplete {
        case Success(UnlessShutdown.AbortedDueToShutdown) =>
          logger.debug(s"Submission $submissionId aborted due to shutdown")
          removePendingSubmission()
        case _ =>
      }

      sendResult <- EitherT.right(FutureUnlessShutdown(sendResultP.future))
    } yield {
      SendResult.log("Submission", logger)(UnlessShutdown.Outcome(sendResult))

      sendResult match {
        case SendResult.Success(deliver) =>
          schedulePendingSubmissionRemoval(deliver.timestamp, submissionId)
        case SendResult.Error(_) | SendResult.Timeout(_) =>
          removePendingSubmission()
      }

      sendResult -> resultArgs
    }
  }

  /** Removes the pending submission once the request tracker has advanced to the decision time.
    * This happens if the request times out (w.r.t. the submission timestamp) or the sequencer never sent a request.
    */
  private def schedulePendingSubmissionRemoval(
      submissionTimestamp: CantonTimestamp,
      submissionId: steps.PendingSubmissionId,
  )(implicit traceContext: TraceContext): Unit = {

    val removeF = for {
      domainParameters <- crypto.ips
        .awaitSnapshot(submissionTimestamp)
        .flatMap(_.findDynamicDomainParameters())
        .flatMap(_.toFuture(new RuntimeException(_)))

      decisionTime <- domainParameters.decisionTimeForF(submissionTimestamp)
      _ = ephemeral.timeTracker.requestTick(decisionTime)
      _ <- ephemeral.requestTracker.awaitTimestamp(decisionTime).getOrElse(Future.unit).map { _ =>
        steps.removePendingSubmission(steps.pendingSubmissions(ephemeral), submissionId).foreach {
          submissionData =>
            logger.debug(s"Removing sent submission $submissionId without a result.")
            steps.postProcessResult(
              Verdict.ParticipantReject(
                NonEmpty(
                  List,
                  Set.empty[LfPartyId] ->
                    LocalReject.TimeRejects.LocalTimeout.Reject(protocolVersion),
                ),
                protocolVersion,
              ),
              submissionData,
            )
        }
      }
    } yield ()

    FutureUtil.doNotAwait(removeF, s"Failed to remove the pending submission $submissionId")
  }

  private def toHandlerRequest(
      ts: CantonTimestamp,
      result: EitherT[
        FutureUnlessShutdown,
        steps.RequestError,
        EitherT[FutureUnlessShutdown, steps.RequestError, Unit],
      ],
  )(implicit traceContext: TraceContext): HandlerResult = {
    // We discard the lefts because they are logged by `logRequestWarnings`
    logRequestWarnings(ts, result)
      .map(innerAsync => AsyncResult(innerAsync.getOrElse(())))
      .getOrElse(AsyncResult.immediate)
  }

  private[this] def logRequestWarnings(
      resultTimestamp: CantonTimestamp,
      result: EitherT[
        FutureUnlessShutdown,
        steps.RequestError,
        EitherT[FutureUnlessShutdown, steps.RequestError, Unit],
      ],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, steps.RequestError, EitherT[
    FutureUnlessShutdown,
    steps.RequestError,
    Unit,
  ]] = {

    def logRequestWarnings[T](
        result: EitherT[FutureUnlessShutdown, steps.RequestError, T],
        default: T,
    ): EitherT[FutureUnlessShutdown, steps.RequestError, T] = {
      val warningsLogged = EitherTUtil.leftSubflatMap(result) { processorError =>
        processorError.underlyingProcessorError() match {
          case err => Left(processorError)
        }
      }
      EitherTUtil.logOnErrorU(
        warningsLogged,
        s"${steps.requestKind} ${RequestId(resultTimestamp)}: Failed to process request",
      )
    }

    logRequestWarnings(
      result.map(logRequestWarnings(_, ())),
      EitherT.pure[FutureUnlessShutdown, steps.RequestError](()),
    )
  }

  override def processRequest(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      batch: steps.RequestBatch,
  )(implicit traceContext: TraceContext): HandlerResult = {
    val RequestAndRootHashMessage(viewMessages, rootHashMessage, mediatorId, _isReceipt) = batch
    val requestId = RequestId(ts)

    if (precedesCleanReplay(requestId)) {
      // The `MessageDispatcher` should not call this method for requests before the clean replay starting point
      HandlerResult.synchronous(
        ErrorUtil.internalErrorAsyncShutdown(
          new IllegalArgumentException(
            s"Request with timestamp $ts precedes the clean replay starting point"
          )
        )
      )
    } else {
      logger.info(show"Processing ${steps.requestKind.unquoted} request at $requestId.")

      val rootHash = batch.rootHashMessage.rootHash
      val freshOwnTimelyTxF = ephemeral.submissionTracker.register(rootHash, requestId)

      val processedET = performUnlessClosingEitherU(
        s"ProtocolProcess.processRequest(rc=$rc, sc=$sc, traceId=${traceContext.traceId})"
      ) {
        // registering the request has to be done synchronously
        EitherT
          .rightT[Future, ProtocolProcessor.this.steps.RequestError](
            ephemeral.phase37Synchronizer
              .registerRequest(steps.requestType)(RequestId(ts))
          )
          .map { handleRequestData =>
            performRequestProcessing(ts, rc, sc, handleRequestData, batch, freshOwnTimelyTxF)
          }
      }
      toHandlerRequest(ts, processedET)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  @VisibleForTesting
  private[protocol] def performRequestProcessing(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      handleRequestData: Phase37Synchronizer.PendingRequestDataHandle[
        steps.requestType.PendingRequestData
      ],
      batch: steps.RequestBatch,
      freshOwnTimelyTxF: FutureUnlessShutdown[Boolean],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, steps.RequestError, Unit] = {
    val RequestAndRootHashMessage(viewMessages, rootHashMessage, mediator, isReceipt) = batch
    val requestId = RequestId(ts)
    val rootHash = rootHashMessage.rootHash

    def checkRootHash(
        decryptedViews: Seq[(WithRecipients[steps.DecryptedView], Option[Signature])]
    ): (Seq[MalformedPayload], Seq[(WithRecipients[steps.DecryptedView], Option[Signature])]) = {

      val correctRootHash = rootHashMessage.rootHash
      val (viewsWithCorrectRootHash, viewsWithWrongRootHash) =
        decryptedViews.partition { case (view, _) => view.unwrap.rootHash == correctRootHash }
      val malformedPayloads: Seq[MalformedPayload] =
        viewsWithWrongRootHash.map { case (viewTree, _) =>
          ProtocolProcessor.WrongRootHash(viewTree.unwrap, correctRootHash)
        }

      (malformedPayloads, viewsWithCorrectRootHash)
    }

    def observeSequencedRootHash(amSubmitter: Boolean): Future[Unit] =
      if (amSubmitter && !isReceipt) {
        // We are the submitting participant and yet the request does not have a message ID.
        // This looks like a preplay attack, and we mark the request as sequenced in the in-flight
        // submission tracker to avoid the situation that our original submission never gets sequenced
        // and gets picked up by a timely rejection, which would emit a duplicate command completion.
        val sequenced = SequencedSubmission(sc, ts)
        inFlightSubmissionTracker.observeSequencedRootHash(
          rootHash,
          sequenced,
        )
      } else Future.unit

    performUnlessClosingEitherUSF(
      s"$functionFullName(rc=$rc, sc=$sc, traceId=${traceContext.traceId})"
    ) {
      val preliminaryChecksET = for {
        snapshot <- EitherT.right(
          crypto.awaitSnapshotUSSupervised(s"await crypto snapshot $ts")(ts)
        )
        domainParameters <- EitherT(
          snapshot.ipsSnapshot
            .findDynamicDomainParameters()
            .map(
              _.leftMap(_ =>
                steps.embedRequestError(
                  UnableToGetDynamicDomainParameters(
                    snapshot.domainId,
                    snapshot.ipsSnapshot.timestamp,
                  )
                )
              )
            )
        ).mapK(FutureUnlessShutdown.outcomeK)

        decisionTime <- EitherT.fromEither[FutureUnlessShutdown](
          steps.decisionTimeFor(domainParameters, ts)
        )

        decryptedViews <- steps
          .decryptViews(viewMessages, snapshot)
          .mapK(FutureUnlessShutdown.outcomeK)
      } yield (snapshot, decisionTime, decryptedViews)

      for {
        preliminaryChecks <- preliminaryChecksET.leftMap { err =>
          ephemeral.submissionTracker.cancelRegistration(rootHash, requestId)
          err
        }
        (snapshot, decisionTime, decryptedViews) = preliminaryChecks

        steps.DecryptedViews(decryptedViewsWithSignatures, rawDecryptionErrors) = decryptedViews
        _ = rawDecryptionErrors.foreach { decryptionError =>
          logger.warn(s"Request $rc: Decryption error: $decryptionError")
        }
        decryptionErrors = rawDecryptionErrors.map(ViewMessageError(_))

        (incorrectRootHashes, viewsWithCorrectRootHash) = checkRootHash(
          decryptedViewsWithSignatures
        )
        _ = incorrectRootHashes.foreach { incorrectRootHash =>
          logger.warn(s"Request $rc: Found malformed payload: $incorrectRootHash")
        }

        // TODO(i12643): Remove this flag when no longer needed
        checkRecipientsResult <- EitherT.right(
          if (skipRecipientsCheck) FutureUnlessShutdown.pure((Seq.empty, viewsWithCorrectRootHash))
          else
            FutureUnlessShutdown.outcomeF(
              recipientsValidator.retainInputsWithValidRecipients(
                requestId,
                viewsWithCorrectRootHash,
                snapshot.ipsSnapshot,
              )
            )
        )
        (incorrectRecipients, viewsWithCorrectRootHashAndRecipients) = checkRecipientsResult

        (fullViewsWithCorrectRootHashAndRecipients, incorrectDecryptedViews) =
          steps.computeFullViews(viewsWithCorrectRootHashAndRecipients)

        malformedPayloads =
          decryptionErrors ++ incorrectRootHashes ++ incorrectRecipients ++ incorrectDecryptedViews

        _ <- NonEmpty.from(fullViewsWithCorrectRootHashAndRecipients) match {
          case None =>
            ephemeral.submissionTracker.cancelRegistration(rootHash, requestId)
            trackAndSendResponsesMalformed(
              rc,
              sc,
              ts,
              handleRequestData,
              decisionTime,
              mediator,
              snapshot,
              malformedPayloads,
            ).mapK(FutureUnlessShutdown.outcomeK)

          case Some(goodViewsWithSignatures) =>
            // All views with the same correct root hash declare the same mediator, so it's enough to look at the head
            val (firstView, _) = goodViewsWithSignatures.head1

            val views = goodViewsWithSignatures.forgetNE.map { case (v, _) => v.unwrap }

            val observeF = steps.getSubmissionDataForTracker(views) match {
              case Some(submissionData) =>
                ephemeral.submissionTracker.provideSubmissionData(
                  rootHash,
                  requestId,
                  submissionData,
                )

                observeSequencedRootHash(submissionData.submitterParticipant == participantId)
              case None =>
                // There are no root views
                ephemeral.submissionTracker.cancelRegistration(
                  rootHash,
                  requestId,
                )

                Future.unit
            }

            val declaredMediator = firstView.unwrap.mediator
            // Lazy so as to prevent this running concurrently with `observeF`
            lazy val processF = if (declaredMediator == mediator) {
              processRequestWithGoodViews(
                ts,
                rc,
                sc,
                handleRequestData,
                decisionTime,
                snapshot,
                mediator,
                goodViewsWithSignatures,
                malformedPayloads,
                freshOwnTimelyTxF,
              )
            } else {
              // When the mediator `mediatorId` receives the root hash message,
              // it will either lack the informee tree or find the wrong mediator ID in it.
              // The submitting participant is malicious (unless the sequencer is), so it is not this participant
              // and therefore we don't have to output a completion event
              logger.error(
                s"Mediator $declaredMediator declared in views is not the recipient $mediator of the root hash message"
              )
              EitherT.right[steps.RequestError](
                prepareForMediatorResultOfBadRequest(rc, sc, ts)
              )
            }

            for {
              _ <- EitherT.right(observeF).mapK(FutureUnlessShutdown.outcomeK)
              _ <- processF
            } yield ()
        }
      } yield ()
    }
  }

  private def processRequestWithGoodViews(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      handleRequestData: Phase37Synchronizer.PendingRequestDataHandle[
        steps.requestType.PendingRequestData
      ],
      decisionTime: CantonTimestamp,
      snapshot: DomainSnapshotSyncCryptoApi,
      mediator: MediatorRef,
      viewsWithSignatures: NonEmpty[
        Seq[(WithRecipients[steps.FullView], Option[Signature])]
      ],
      malformedPayloads: Seq[MalformedPayload],
      freshOwnTimelyTxF: FutureUnlessShutdown[Boolean],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ProtocolProcessor.this.steps.RequestError, Unit] = {
    val views = viewsWithSignatures.map { case (view, _) => view }

    def continueProcessing(freshOwnTimelyTx: Boolean) =
      for {
        activenessAndPending <- steps
          .computeActivenessSetAndPendingContracts(
            ts,
            rc,
            sc,
            viewsWithSignatures,
            malformedPayloads,
            snapshot,
            mediator,
          )
          .mapK(FutureUnlessShutdown.outcomeK)
        _ <- trackAndSendResponses(
          rc,
          sc,
          ts,
          handleRequestData,
          mediator,
          snapshot,
          decisionTime,
          activenessAndPending,
          freshOwnTimelyTx,
        )
      } yield ()

    def stopProcessing(freshOwnTimelyTx: Boolean) = {
      SyncServiceAlarm
        .Warn(
          s"Request $rc: Chosen mediator $mediator is inactive at $ts. Skipping this request."
        )
        .report()

      // The chosen mediator may have become inactive between submission and sequencing.
      // All honest participants and the mediator will ignore the request,
      // but the submitting participant still must produce a completion event.
      val (eventO, submissionIdO) =
        steps.eventAndSubmissionIdForInactiveMediator(ts, rc, sc, views, freshOwnTimelyTx)

      for {
        _ <- EitherT.right(
          FutureUnlessShutdown.outcomeF(
            unlessCleanReplay(rc)(
              ephemeral.recordOrderPublisher.schedulePublication(sc, rc, ts, eventO)
            )
          )
        )
        submissionDataO = submissionIdO.flatMap(submissionId =>
          // This removal does not interleave with `schedulePendingSubmissionRemoval`
          // as the sequencer respects the max sequencing time of the request.
          // TODO(M99) Gracefully handle the case that the sequencer does not respect the max sequencing time.
          steps.removePendingSubmission(
            steps.pendingSubmissions(ephemeral),
            submissionId,
          )
        )
        _ = submissionDataO.foreach(
          steps.postProcessSubmissionForInactiveMediator(mediator, ts, _)
        )
        _ <- EitherT.right[steps.RequestError] {
          handleRequestData.complete(None)
          invalidRequest(rc, sc, ts)
        }
      } yield ()
    }

    // Check whether the declared mediator is still an active mediator.
    for {
      mediatorIsActive <- EitherT
        .right(snapshot.ipsSnapshot.isMediatorActive(mediator))
        .mapK(FutureUnlessShutdown.outcomeK)
      freshOwnTimelyTx <- EitherT.right(freshOwnTimelyTxF)
      _ <-
        if (mediatorIsActive)
          continueProcessing(freshOwnTimelyTx)
        else
          stopProcessing(freshOwnTimelyTx)
    } yield ()
  }

  private def trackAndSendResponses(
      rc: RequestCounter,
      sc: SequencerCounter,
      ts: CantonTimestamp,
      handleRequestData: Phase37Synchronizer.PendingRequestDataHandle[
        steps.requestType.PendingRequestData
      ],
      mediator: MediatorRef,
      snapshot: DomainSnapshotSyncCryptoApi,
      decisionTime: CantonTimestamp,
      contractsAndContinue: steps.CheckActivenessAndWritePendingContracts,
      freshOwnTimelyTx: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, steps.RequestError, Unit] = {
    val requestId = RequestId(ts)

    val steps.CheckActivenessAndWritePendingContracts(
      activenessSet,
      pendingContracts,
      pendingDataAndResponseArgs,
    ) = contractsAndContinue

    for {
      requestFuturesF <- EitherT
        .fromEither[FutureUnlessShutdown](
          ephemeral.requestTracker
            .addRequest(rc, sc, ts, ts, decisionTime, activenessSet)
        )
        .leftMap(err => steps.embedRequestError(RequestTrackerError(err)))

      _ <- steps
        .authenticateInputContracts(pendingDataAndResponseArgs)
        .mapK(FutureUnlessShutdown.outcomeK)

      conflictingContracts <- EitherT.right(
        FutureUnlessShutdown.outcomeF(
          ephemeral.storedContractManager.addPendingContracts(rc, pendingContracts)
        )
      )
      // TODO(i12908): This check may evaluate differently during a replay. Should not cause a hard failure
      //  E.g., if the contract has been written to the store in between with different contract data or metadata.
      _ <- condUnitET[FutureUnlessShutdown](
        conflictingContracts.isEmpty,
        steps.embedRequestError(ConflictingContractData(conflictingContracts, pendingContracts)),
      )

      pendingContractIds = pendingContracts.map(_.unwrap.contractId).toSet

      pendingDataAndResponsesAndTimeoutEvent <-
        if (isCleanReplay(rc)) {
          val pendingData = CleanReplayData(rc, sc, pendingContractIds, mediator)
          val responses = Seq.empty[(MediatorResponse, Recipients)]
          val timeoutEvent = Either.right(Option.empty[TimestampedEvent])
          EitherT.pure[FutureUnlessShutdown, steps.RequestError](
            (pendingData, responses, () => timeoutEvent)
          )
        } else {
          for {
            pendingCursor <- EitherT.right(
              FutureUnlessShutdown.outcomeF(ephemeral.requestJournal.insert(rc, ts))
            )

            pendingDataAndResponses <- steps.constructPendingDataAndResponse(
              pendingDataAndResponseArgs,
              ephemeral.transferCache,
              ephemeral.storedContractManager,
              requestFuturesF.flatMap(_.activenessResult),
              pendingCursor,
              mediator,
              freshOwnTimelyTx,
            )

            steps.StorePendingDataAndSendResponseAndCreateTimeout(
              pendingData,
              responses,
              rejectionArgs,
            ) = pendingDataAndResponses
            PendingRequestData(
              pendingRequestCounter,
              pendingSequencerCounter,
              pendingContractIds2,
              _,
            ) = pendingData
            _ = if (
              pendingRequestCounter != rc
              || pendingSequencerCounter != sc
              || pendingContractIds2 != pendingContractIds
            )
              throw new RuntimeException("Pending result data inconsistent with request")

          } yield (
            WrappedPendingRequestData(pendingData),
            responses,
            () => steps.createRejectionEvent(rejectionArgs),
          )
        }

      (
        pendingData,
        responsesTo,
        timeoutEvent,
      ) =
        pendingDataAndResponsesAndTimeoutEvent

      // Make sure activeness result finished
      requestFutures <- EitherT.right[steps.RequestError](requestFuturesF)
      _activenessResult <- EitherT.right[steps.RequestError](requestFutures.activenessResult)

      _ <- EitherT.right[steps.RequestError](
        FutureUnlessShutdown.outcomeF(
          unlessCleanReplay(rc)(
            ephemeral.requestJournal.transit(rc, ts, RequestState.Pending, RequestState.Confirmed)
          )
        )
      )

      _ = handleRequestData.complete(Some(pendingData))
      timeoutET = EitherT
        .right(requestFutures.timeoutResult)
        .flatMap(
          handleTimeout(
            requestId,
            rc,
            sc,
            pendingData.pendingContracts,
            decisionTime,
            timeoutEvent(),
          )
        )
      _ = EitherTUtil.doNotAwait(timeoutET, "Handling timeout failed")

      signedResponsesTo <- EitherT.right(responsesTo.parTraverse { case (response, recipients) =>
        FutureUnlessShutdown.outcomeF(
          signResponse(snapshot, response).map(_ -> recipients)
        )
      })

      _ <- sendResponses(requestId, rc, signedResponsesTo)
        .leftMap(err => steps.embedRequestError(SequencerRequestError(err)))
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield ()

  }

  private def trackAndSendResponsesMalformed(
      rc: RequestCounter,
      sc: SequencerCounter,
      ts: CantonTimestamp,
      handleRequestData: Phase37Synchronizer.PendingRequestDataHandle[
        steps.requestType.PendingRequestData
      ],
      decisionTime: CantonTimestamp,
      mediatorRef: MediatorRef,
      snapshot: DomainSnapshotSyncCryptoApi,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit traceContext: TraceContext): EitherT[Future, steps.RequestError, Unit] = {

    val requestId = RequestId(ts)

    if (isCleanReplay(rc)) {
      ephemeral.requestTracker.tick(sc, ts)
      EitherT.rightT(())
    } else {
      for {
        pendingCursor <- EitherT.right(ephemeral.requestJournal.insert(rc, ts))
        _ <- EitherT.right(pendingCursor)

        _ = ephemeral.requestTracker.tick(sc, ts)

        responses = steps.constructResponsesForMalformedPayloads(requestId, malformedPayloads)
        recipients = Recipients.cc(mediatorRef.toRecipient)
        messages <- EitherT.right(responses.parTraverse { response =>
          signResponse(snapshot, response).map(_ -> recipients)
        })

        _ <- EitherT.right[steps.RequestError](
          ephemeral.requestJournal.transit(rc, ts, RequestState.Pending, RequestState.Confirmed)
        )

        _ <- sendResponses(requestId, rc, messages)
          .leftMap(err => steps.embedRequestError(SequencerRequestError(err)))

        _ = handleRequestData.complete(None)

        _ <- EitherT.right[steps.RequestError](terminateRequest(rc, sc, ts, ts))
      } yield ()
    }
  }

  override def processMalformedMediatorRequestResult(
      timestamp: CantonTimestamp,
      sequencerCounter: SequencerCounter,
      signedResultBatch: Either[EventWithErrors[Deliver[DefaultOpenEnvelope]], SignedContent[
        Deliver[DefaultOpenEnvelope]
      ]],
  )(implicit traceContext: TraceContext): HandlerResult = {
    val content = signedResultBatch.fold(_.content, _.content)
    val ts = content.timestamp

    val processedET = performUnlessClosingEitherU(functionFullName) {
      val malformedMediatorRequestEnvelopes = content.batch.envelopes
        .mapFilter(ProtocolMessage.select[SignedProtocolMessage[MalformedMediatorRequestResult]])
      require(
        malformedMediatorRequestEnvelopes.sizeCompare(1) == 0,
        steps.requestKind + " result contains multiple malformed mediator request envelopes",
      )
      val malformedMediatorRequest = malformedMediatorRequestEnvelopes(0).protocolMessage
      val requestId = malformedMediatorRequest.message.requestId
      val sc = content.counter

      logger.info(
        show"Got malformed mediator result for ${steps.requestKind.unquoted} request at $requestId."
      )

      performResultProcessing(
        signedResultBatch,
        Left(malformedMediatorRequest),
        requestId,
        ts,
        sc,
      )
    }

    toHandlerResult(ts, processedET)
  }

  private def toHandlerResult(
      ts: CantonTimestamp,
      result: EitherT[
        FutureUnlessShutdown,
        steps.ResultError,
        EitherT[FutureUnlessShutdown, steps.ResultError, Unit],
      ],
  )(implicit traceContext: TraceContext): HandlerResult = {
    // We discard the lefts because they are logged by `logResultWarnings`
    logResultWarnings(ts, result)
      .map(innerAsync => AsyncResult(innerAsync.getOrElse(())))
      .getOrElse(AsyncResult.immediate)
  }

  override def processResult(
      signedResultBatchE: Either[
        EventWithErrors[Deliver[DefaultOpenEnvelope]],
        SignedContent[Deliver[DefaultOpenEnvelope]],
      ]
  )(implicit traceContext: TraceContext): HandlerResult = {
    val content = signedResultBatchE.fold(_.content, _.content)
    val ts = content.timestamp
    val sc = content.counter

    val processedET = performUnlessClosingEitherU(
      s"ProtocolProcess.processResult(sc=$sc, traceId=${traceContext.traceId}"
    ) {
      val resultEnvelopes =
        content.batch.envelopes
          .mapFilter(ProtocolMessage.select[SignedProtocolMessage[Result]])
      ErrorUtil.requireArgument(
        resultEnvelopes.sizeCompare(1) == 0,
        steps.requestKind + " result contains multiple such messages",
      )

      val result = resultEnvelopes(0).protocolMessage
      val requestId = result.message.requestId

      logger.debug(
        show"Got result for ${steps.requestKind.unquoted} request at $requestId: $resultEnvelopes"
      )

      performResultProcessing(signedResultBatchE, Right(result), requestId, ts, sc)
    }

    toHandlerResult(ts, processedET)
  }

  @VisibleForTesting
  private[protocol] def performResultProcessing(
      signedResultBatchE: Either[
        EventWithErrors[Deliver[DefaultOpenEnvelope]],
        SignedContent[Deliver[DefaultOpenEnvelope]],
      ],
      resultE: Either[SignedProtocolMessage[MalformedMediatorRequestResult], SignedProtocolMessage[
        Result
      ]],
      requestId: RequestId,
      resultTs: CantonTimestamp,
      sc: SequencerCounter,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, steps.ResultError, EitherT[FutureUnlessShutdown, steps.ResultError, Unit]] = {
    ephemeral.recordOrderPublisher.tick(sc, resultTs)

    val snapshotTs =
      if (protocolVersion >= ProtocolVersion.v5) requestId.unwrap
      else {
        // Keeping legacy behavior to enforce a consistent behavior in old protocol versions.
        // If different participants pick different values, this could result in a ledger fork.
        resultTs
      }

    for {
      snapshot <- EitherT.right(
        crypto.ips.awaitSnapshotSupervised(s"await crypto snapshot $resultTs")(snapshotTs)
      )

      domainParameters <- EitherT(
        snapshot
          .findDynamicDomainParameters()
          .map(
            _.leftMap(_ =>
              steps.embedResultError(
                UnableToGetDynamicDomainParameters(
                  domainId,
                  requestId.unwrap,
                )
              )
            )
          )
      )

      decisionTime <- EitherT.fromEither[Future](
        steps.decisionTimeFor(domainParameters, requestId.unwrap)
      )

      participantDeadline <- EitherT.fromEither[Future](
        steps.participantResponseDeadlineFor(domainParameters, requestId.unwrap)
      )

      _ <- condUnitET[Future](
        resultTs <= decisionTime, {
          ephemeral.requestTracker.tick(sc, resultTs)
          steps.embedResultError(DecisionTimeElapsed(requestId, resultTs))
          /* We must not evict the request from `pendingRequestData` or `pendingSubmissionMap`
           * because this will have been taken care of by `handleTimeout`
           * when the request tracker progresses to the decision time.
           */
        },
      )
      _ <- EitherT.cond[Future](
        resultTs > participantDeadline || !resultE.merge.message.verdict.isTimeoutDeterminedByMediator,
        (), {
          SyncServiceAlarm
            .Warn(
              s"Received mediator timeout message at $resultTs before response deadline for request $requestId."
            )
            .report()

          ephemeral.requestTracker.tick(sc, resultTs)
          steps.embedResultError(TimeoutResultTooEarly(requestId))
        },
      )
      asyncResult <-
        if (!precedesCleanReplay(requestId))
          performResultProcessing2(
            signedResultBatchE,
            resultE,
            requestId,
            resultTs,
            sc,
            domainParameters,
          )
        else
          EitherT.pure[Future, steps.ResultError](
            EitherT.pure[FutureUnlessShutdown, steps.ResultError](())
          )
    } yield asyncResult
  }

  /** This processing step corresponds to the end of the synchronous part of the processing
    * of mediator result.
    * The inner `EitherT` corresponds to the subsequent async stage.
    */
  private[this] def performResultProcessing2(
      signedResultBatchE: Either[
        EventWithErrors[Deliver[DefaultOpenEnvelope]],
        SignedContent[Deliver[DefaultOpenEnvelope]],
      ],
      resultE: Either[
        SignedProtocolMessage[MalformedMediatorRequestResult],
        SignedProtocolMessage[Result],
      ],
      requestId: RequestId,
      resultTs: CantonTimestamp,
      sc: SequencerCounter,
      domainParameters: DynamicDomainParametersWithValidity,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, steps.ResultError, EitherT[FutureUnlessShutdown, steps.ResultError, Unit]] = {
    val unsignedResultE = resultE.fold(x => Left(x.message), y => Right(y.message))

    def filterInvalidSignature(
        pendingRequestData: PendingRequestDataOrReplayData[steps.requestType.PendingRequestData]
    ): Future[Boolean] =
      for {
        snapshot <- crypto.awaitSnapshot(requestId.unwrap)
        res <- {
          pendingRequestData.mediator match {
            case MediatorRef.Single(mediatorId) =>
              resultE.merge
                .verifySignature(
                  snapshot,
                  mediatorId,
                )
                .value
            case MediatorRef.Group(MediatorsOfDomain(mediatorGroupIndex: MediatorGroupIndex)) =>
              (for {
                signatureCheck <- resultE.merge.verifySignature(snapshot, mediatorGroupIndex)
              } yield signatureCheck).value
          }
        }
      } yield {
        res match {
          case Left(err) =>
            SyncServiceAlarm
              .Warn(
                s"Received a mediator result at $resultTs for $requestId " +
                  s"with an invalid signature for ${pendingRequestData.mediator}. Discarding message... Details: $err"
              )
              .report()
            false
          case Right(()) =>
            true
        }
      }

    def filterInvalidRootHash(
        pendingRequestDataOrReplayData: PendingRequestDataOrReplayData[
          steps.requestType.PendingRequestData
        ]
    ): Future[Boolean] = Future.successful {
      val invalidO = for {
        case TransactionResultMessage(
          requestId,
          _verdict,
          resultRootHash,
          _domainId,
          _,
        ) <- unsignedResultE.toOption
        case WrappedPendingRequestData(pendingRequestData) <- Some(pendingRequestDataOrReplayData)
        case PendingTransaction(txId, _, _, _, _, requestTime, _, _, _, _) <- Some(
          pendingRequestData
        )

        txRootHash = txId.toRootHash
        if resultRootHash != txRootHash
      } yield {
        val cause =
          s"Received a transaction result message at $requestTime from ${pendingRequestData.mediator} " +
            s"for $requestId with an invalid root hash $resultRootHash instead of $txRootHash. Discarding message..."
        SyncServiceAlarm.Warn(cause).report()
      }

      invalidO.isEmpty
    }

    val combinedFilter =
      (prd: PendingRequestDataOrReplayData[steps.requestType.PendingRequestData]) =>
        MonadUtil
          .foldLeftM(true, Seq(filterInvalidSignature _, filterInvalidRootHash _))((acc, x) =>
            if (acc) x(prd) else Future.successful(acc)
          )

    // Wait until we have processed the corresponding request
    //
    // This may deadlock if we haven't received the `requestId` as a request.
    // For example, if there never was a request with the given timestamp,
    // then the phase 3-7 synchronizer waits until the all requests until `requestId`'s timestamp
    // and the next request have reached `Confirmed`.
    // However, if there was no request between `requestId` and `ts`,
    // then the next request will not reach `Confirmed`
    // because the request tracker will not progress beyond `ts` as the `tick` for `ts` comes only after this point.
    // Accordingly, time proofs will not trigger a timeout either.
    //
    // We don't know whether any protocol processor has ever seen the request with `requestId`;
    // it might be that the message dispatcher already decided that the request is malformed and should not be processed.
    // In this case, the message dispatcher has assigned a request counter to the request if it expects to get a mediator result
    // and the BadRootHashMessagesRequestProcessor moved the request counter to `Confirmed`.
    // So the deadlock should happen only if the mediator or sequencer are dishonest.
    //
    // TODO(M99) This argument relies on the mediator sending a MalformedMediatorRequest only to participants
    //  that have also received a message with the request.
    //  A dishonest sequencer or mediator could break this assumption.

    /* This part is still run as part of the synchronous processing, because we want
     * only the first call to awaitConfirmed to get
     * a non-empty value.
     *
     * Some more synchronization is done in the Phase37Synchronizer.
     */

    val res = performUnlessClosingEitherUSF(
      s"$functionFullName(sc=$sc, traceId=${traceContext.traceId})"
    )(
      EitherT(
        ephemeral.phase37Synchronizer
          .awaitConfirmed(steps.requestType)(requestId, combinedFilter)
          .map {
            case RequestOutcome.Success(pendingRequestData) =>
              Right(pendingRequestData)
            case RequestOutcome.AlreadyServedOrTimeout =>
              ephemeral.requestTracker.tick(sc, resultTs)
              Left(steps.embedResultError(UnknownPendingRequest(requestId)))
            case RequestOutcome.Invalid =>
              ephemeral.requestTracker.tick(sc, resultTs)
              Left(steps.embedResultError(InvalidPendingRequest(requestId)))
          }
      ).mapK(FutureUnlessShutdown.outcomeK).flatMap { pendingRequestDataOrReplayData =>
        performResultProcessing3(
          signedResultBatchE,
          unsignedResultE,
          requestId,
          resultTs,
          sc,
          domainParameters,
          pendingRequestDataOrReplayData,
        )
      }
    )

    // This is now lifted to the asynchronous part of the processing.
    EitherT.pure(res)
  }

  private[this] def performResultProcessing3(
      signedResultBatchE: Either[
        EventWithErrors[Deliver[DefaultOpenEnvelope]],
        SignedContent[Deliver[DefaultOpenEnvelope]],
      ],
      resultE: Either[MalformedMediatorRequestResult, Result],
      requestId: RequestId,
      resultTs: CantonTimestamp,
      sc: SequencerCounter,
      domainParameters: DynamicDomainParametersWithValidity,
      pendingRequestDataOrReplayData: PendingRequestDataOrReplayData[
        steps.requestType.PendingRequestData
      ],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, steps.ResultError, Unit] = {
    val verdict = resultE.merge.verdict

    val PendingRequestData(requestCounter, requestSequencerCounter, pendingContracts, _) =
      pendingRequestDataOrReplayData
    val cleanReplay = isCleanReplay(requestCounter, pendingRequestDataOrReplayData)
    val pendingSubmissionDataO = pendingSubmissionDataForRequest(pendingRequestDataOrReplayData)

    for {
      commitAndEvent <- pendingRequestDataOrReplayData match {
        case WrappedPendingRequestData(pendingRequestData) =>
          for {
            commitSetAndContractsAndEvent <- steps
              .getCommitSetAndContractsToBeStoredAndEvent(
                signedResultBatchE,
                resultE,
                pendingRequestData,
                steps.pendingSubmissions(ephemeral),
                crypto.pureCrypto,
              )
              .mapK(FutureUnlessShutdown.outcomeK)
          } yield {
            val steps.CommitAndStoreContractsAndPublishEvent(
              commitSetOF,
              contractsToBeStored,
              eventO,
            ) = commitSetAndContractsAndEvent

            val isApproval = verdict match {
              case _: Approve => true
              case _ => false
            }

            if (!isApproval && commitSetOF.isDefined)
              throw new RuntimeException("Negative verdicts entail an empty commit set")
            if (!contractsToBeStored.subsetOf(pendingContracts))
              throw new RuntimeException("All contracts to be stored should be pending")

            (commitSetOF, contractsToBeStored, eventO)
          }
        case _: CleanReplayData =>
          val commitSetOF = verdict match {
            case _: Approve => Some(Future.successful(CommitSet.empty))
            case _ => None
          }

          val contractsToBeStored = Set.empty[LfContractId]
          val eventO = None

          EitherT.pure[FutureUnlessShutdown, steps.ResultError](
            (commitSetOF, contractsToBeStored, eventO)
          )
      }
      (commitSetOF, contractsToBeStored, eventO) = commitAndEvent

      commitTime = resultTs
      commitSetF <- signalResultToRequestTracker(
        requestCounter,
        sc,
        requestId,
        resultTs,
        commitTime,
        commitSetOF,
        domainParameters,
      ).leftMap(err => steps.embedResultError(RequestTrackerError(err)))
        .mapK(FutureUnlessShutdown.outcomeK)

      contractStoreUpdate = pendingContracts
        .map(contractId => (contractId, contractsToBeStored.contains(contractId)))
        .toMap

      _ <- EitherT.right(
        FutureUnlessShutdown.outcomeF(
          ephemeral.storedContractManager.commitIfPending(requestCounter, contractStoreUpdate)
        )
      )

      _ <- ifThenET(!cleanReplay) {
        for {
          _unit <- {
            logger.info(
              show"Finalizing ${steps.requestKind.unquoted} request at $requestId with event ${eventO}."
            )
            // Schedule publication of the event with the associated causality update.
            // Note that both fields are optional.
            // Some events (such as rejection events) are not associated with causality updates.
            // Additionally, we may process a causality update without an associated event (this happens on transfer-in)
            EitherT.right[steps.ResultError](
              FutureUnlessShutdown.outcomeF(
                ephemeral.recordOrderPublisher
                  .schedulePublication(
                    requestSequencerCounter,
                    requestCounter,
                    requestId.unwrap,
                    eventO,
                  )
              )
            )
          }

          commitSet <- EitherT.right[steps.ResultError](commitSetF)
          _ = ephemeral.recordOrderPublisher.scheduleAcsChangePublication(
            requestSequencerCounter,
            requestId.unwrap,
            requestCounter,
            commitSet,
          )
          requestTimestamp = requestId.unwrap
          _unit <- EitherT.right[steps.ResultError](
            FutureUnlessShutdown.outcomeF(
              terminateRequest(
                requestCounter,
                requestSequencerCounter,
                requestTimestamp,
                commitTime,
              )
            )
          )
        } yield pendingSubmissionDataO.foreach(steps.postProcessResult(verdict, _))
      }
    } yield ()
  }

  private[this] def logResultWarnings(
      resultTimestamp: CantonTimestamp,
      result: EitherT[
        FutureUnlessShutdown,
        steps.ResultError,
        EitherT[FutureUnlessShutdown, steps.ResultError, Unit],
      ],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, steps.ResultError, EitherT[
    FutureUnlessShutdown,
    steps.ResultError,
    Unit,
  ]] = {

    def logResultWarnings[T](
        result: EitherT[FutureUnlessShutdown, steps.ResultError, T],
        default: T,
    ): EitherT[FutureUnlessShutdown, steps.ResultError, T] = {
      val warningsLogged = EitherTUtil.leftSubflatMap(result) { processorError =>
        processorError.underlyingProcessorError() match {
          case Some(DecisionTimeElapsed(requestId, _)) =>
            logger.warn(
              show"${steps.requestKind.unquoted} request at $requestId: Result arrived after the decision time (arrived at $resultTimestamp)"
            )
            Right(default)
          case Some(UnknownPendingRequest(requestId)) =>
            // the mediator can send duplicate transaction results during crash recovery and fail over, triggering this error
            logger.info(
              show"${steps.requestKind.unquoted} request at $requestId: Received event at $resultTimestamp for request that is not pending"
            )
            Right(default)
          case Some(InvalidPendingRequest(requestId)) =>
            logger.info(
              show"${steps.requestKind.unquoted} request at $requestId: Received event at $resultTimestamp for request that is invalid"
            )
            Right(default)
          case err => Left(processorError)
        }
      }

      EitherTUtil.logOnErrorU(warningsLogged, s"${steps.requestKind}: Failed to process result")
    }

    logResultWarnings(
      result.map(logResultWarnings(_, ())),
      EitherT.pure[FutureUnlessShutdown, steps.ResultError](()),
    )
  }

  private[this] def pendingSubmissionDataForRequest(
      pendingRequestDataOrReplayData: PendingRequestDataOrReplayData[
        steps.requestType.PendingRequestData
      ]
  ): Option[steps.PendingSubmissionData] =
    for {
      pendingRequestData <- maybePendingRequestData(pendingRequestDataOrReplayData)
      submissionId = steps.submissionIdOfPendingRequest(pendingRequestData)
      submissionData <- steps.removePendingSubmission(
        steps.pendingSubmissions(ephemeral),
        submissionId,
      )
    } yield submissionData

  private def maybePendingRequestData[A <: PendingRequestData](
      pendingRequestDataOrReplayData: PendingRequestDataOrReplayData[A]
  ): Option[A] =
    pendingRequestDataOrReplayData match {
      case WrappedPendingRequestData(pendingRequestData) => Some(pendingRequestData)
      case _: CleanReplayData => None
    }

  private def signalResultToRequestTracker(
      rc: RequestCounter,
      sc: SequencerCounter,
      requestId: RequestId,
      resultTimestamp: CantonTimestamp,
      commitTime: CantonTimestamp,
      commitSetOF: Option[Future[CommitSet]],
      domainParameters: DynamicDomainParametersWithValidity,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, RequestTracker.RequestTrackerError, FutureUnlessShutdown[CommitSet]] = {

    def withRc(rc: RequestCounter, msg: String): String = s"Request $rc: $msg"

    val requestTimestamp = requestId.unwrap

    ErrorUtil.requireArgument(
      resultTimestamp <= domainParameters
        .decisionTimeFor(requestTimestamp)
        .valueOr(e =>
          throw new IllegalStateException(s"Cannot enforce decision time constraint: $e")
        ),
      withRc(rc, "Result message after decision time"),
    )

    for {
      _ <- EitherT
        .fromEither[Future](ephemeral.requestTracker.addResult(rc, sc, resultTimestamp, commitTime))
        .leftMap(e => {
          SyncServiceAlarm.Warn(s"Failed to add result for $requestId. $e").report()
          e
        })
      commitSetF = commitSetOF.getOrElse(Future.successful(CommitSet.empty))
      commitSetT <- EitherT.right(commitSetF.transform(Success(_)))
      commitFuture <- EitherT
        .fromEither[Future](ephemeral.requestTracker.addCommitSet(rc, commitSetT))
        .leftMap(e => {
          SyncServiceAlarm.Warn(s"Unexpected mediator result message for $requestId. $e").report()
          e: RequestTracker.RequestTrackerError
        })
    } yield {
      commitFuture
        .valueOr(e =>
          SyncServiceAlarm
            .Warn(withRc(rc, s"An error occurred while persisting commit set: $e"))
            .report()
        )
        .flatMap(_ => FutureUnlessShutdown.fromTry(commitSetT))
    }
  }

  private def handleTimeout(
      requestId: RequestId,
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      pendingContracts: Set[LfContractId],
      decisionTime: CantonTimestamp,
      timeoutEvent: => Either[steps.ResultError, Option[TimestampedEvent]],
  )(
      result: TimeoutResult
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, steps.ResultError, Unit] =
    if (result.timedOut) {
      logger.info(
        show"${steps.requestKind.unquoted} request at $requestId timed out without a transaction result message."
      )

      def publishEvent(): EitherT[Future, steps.ResultError, Unit] = {
        for {
          maybeEvent <- EitherT.fromEither[Future](timeoutEvent)
          _ <- EitherT.liftF(
            ephemeral.recordOrderPublisher
              .schedulePublication(
                sequencerCounter,
                requestCounter,
                requestId.unwrap,
                maybeEvent,
              )
          )
          requestTimestamp = requestId.unwrap
          _unit <- EitherT.right[steps.ResultError](
            terminateRequest(requestCounter, sequencerCounter, requestTimestamp, decisionTime)
          )
        } yield ()
      }

      for {
        pendingRequestDataOrReplayData <- EitherT.liftF(
          ephemeral.phase37Synchronizer
            .awaitConfirmed(steps.requestType)(requestId)
            .map {
              case RequestOutcome.Success(pendingRequestData) => pendingRequestData
              case RequestOutcome.AlreadyServedOrTimeout =>
                throw new IllegalStateException(s"Unknown pending request $requestId at timeout.")
              case RequestOutcome.Invalid =>
                throw new IllegalStateException(s"Invalid pending request $requestId.")
            }
        )

        // No need to clean up the pending submissions because this is handled (concurrently) by schedulePendingSubmissionRemoval
        cleanReplay = isCleanReplay(requestCounter, pendingRequestDataOrReplayData)

        _ <- EitherT.right[steps.ResultError](
          ephemeral.storedContractManager.deleteIfPending(requestCounter, pendingContracts)
        )

        _ <- ifThenET(!cleanReplay)(publishEvent())
      } yield ()
    } else EitherT.pure[Future, steps.ResultError](())

  private[this] def isCleanReplay(
      requestCounter: RequestCounter,
      pendingData: PendingRequestDataOrReplayData[_ <: PendingRequestData],
  ): Boolean = {
    val cleanReplay = isCleanReplay(requestCounter)
    if (cleanReplay != pendingData.isCleanReplay)
      throw new IllegalStateException(
        s"Request $requestCounter is before the starting point at ${ephemeral.startingPoints.processing.nextRequestCounter}, but not a replay"
      )
    cleanReplay
  }

  /** A request precedes the clean replay if it came before the
    * [[com.digitalasset.canton.participant.store.SyncDomainEphemeralState.startingPoints]]'s
    * [[com.digitalasset.canton.participant.store.SyncDomainEphemeralStateFactory.StartingPoints.cleanReplay]].
    */
  private[this] def precedesCleanReplay(requestId: RequestId): Boolean =
    requestId.unwrap <= ephemeral.startingPoints.cleanReplay.prenextTimestamp
}

object ProtocolProcessor {

  sealed trait PendingRequestDataOrReplayData[+A <: PendingRequestData]
      extends PendingRequestData
      with Product
      with Serializable {
    def isCleanReplay: Boolean
  }

  final case class WrappedPendingRequestData[+A <: PendingRequestData](unwrap: A)
      extends PendingRequestDataOrReplayData[A] {
    override def requestCounter: RequestCounter = unwrap.requestCounter
    override def requestSequencerCounter: SequencerCounter = unwrap.requestSequencerCounter
    override def pendingContracts: Set[LfContractId] = unwrap.pendingContracts
    override def isCleanReplay: Boolean = false
    override def mediator: MediatorRef = unwrap.mediator
  }

  final case class CleanReplayData(
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      override val pendingContracts: Set[LfContractId],
      override val mediator: MediatorRef,
  ) extends PendingRequestDataOrReplayData[Nothing] {
    override def isCleanReplay: Boolean = true
  }

  sealed trait ProcessorError extends Product with Serializable with PrettyPrinting

  sealed trait SubmissionProcessingError extends ProcessorError

  sealed trait RequestProcessingError extends ProcessorError

  sealed trait ResultProcessingError extends ProcessorError

  /** We were unable to send the request to the sequencer */
  final case class SequencerRequestError(sendError: SendAsyncClientError)
      extends SubmissionProcessingError
      with RequestProcessingError {
    override def pretty: Pretty[SequencerRequestError] = prettyOfParam(_.sendError)
  }

  /** The sequencer refused to sequence the batch for delivery */
  final case class SequencerDeliverError(deliverError: DeliverError)
      extends SubmissionProcessingError
      with RequestProcessingError {
    override def pretty: Pretty[SequencerDeliverError] = prettyOfParam(_.deliverError)
  }

  /** The identity snapshot does not list a mediator, so we cannot pick one. */
  final case class NoMediatorError(topologySnapshotTimestamp: CantonTimestamp)
      extends SubmissionProcessingError {
    override def pretty: Pretty[NoMediatorError] = prettyOfClass(
      param("topology snapshot timestamp", _.topologySnapshotTimestamp)
    )
  }

  /** The sequencer did not sequence our event within the allotted time
    * @param timestamp sequencer time when the timeout occurred
    */
  final case class SequencerTimeoutError(timestamp: CantonTimestamp)
      extends SubmissionProcessingError
      with RequestProcessingError {
    override def pretty: Pretty[SequencerTimeoutError] = prettyOfClass(unnamedParam(_.timestamp))
  }

  final case class UnableToGetDynamicDomainParameters(domainId: DomainId, ts: CantonTimestamp)
      extends RequestProcessingError
      with ResultProcessingError {
    override def pretty: Pretty[UnableToGetDynamicDomainParameters] = prettyOfClass(
      param("domain id", _.domainId),
      param("timestamp", _.ts),
    )
  }

  final case class RequestTrackerError(error: RequestTracker.RequestTrackerError)
      extends RequestProcessingError
      with ResultProcessingError {
    override def pretty: Pretty[RequestTrackerError] = prettyOfParam(_.error)
  }

  final case class ConflictingContractData(
      existing: Set[StoredContract],
      newContracts: Seq[WithTransactionId[SerializableContract]],
  ) extends RequestProcessingError {
    override def pretty: Pretty[ConflictingContractData] = prettyOfClass(
      param("existing", _.existing),
      param("new contracts", _.newContracts),
    )
  }

  final case class ContractStoreError(error: NonEmptyChain[store.ContractStoreError])
      extends ResultProcessingError {
    override def pretty: Pretty[ContractStoreError] = prettyOfParam(_.error.toChain.toList)
  }

  final case class DecisionTimeElapsed(requestId: RequestId, timestamp: CantonTimestamp)
      extends ResultProcessingError {
    override def pretty: Pretty[DecisionTimeElapsed] = prettyOfClass(
      param("request id", _.requestId),
      param("timestamp", _.timestamp),
    )
  }

  final case class UnknownPendingRequest(requestId: RequestId) extends ResultProcessingError {
    override def pretty: Pretty[UnknownPendingRequest] = prettyOfClass(unnamedParam(_.requestId))
  }

  final case class InvalidPendingRequest(requestId: RequestId) extends ResultProcessingError {
    override def pretty: Pretty[InvalidPendingRequest] = prettyOfClass(unnamedParam(_.requestId))
  }

  final case class TimeoutResultTooEarly(requestId: RequestId) extends ResultProcessingError {
    override def pretty: Pretty[TimeoutResultTooEarly] = prettyOfClass(unnamedParam(_.requestId))
  }

  final case class DomainParametersError(domainId: DomainId, context: String)
      extends ProcessorError {
    override def pretty: Pretty[DomainParametersError] = prettyOfClass(
      param("domain", _.domainId),
      param("context", _.context.unquoted),
    )
  }

  sealed trait MalformedPayload extends Product with Serializable with PrettyPrinting

  final case class ViewMessageError[VT <: ViewType](
      error: EncryptedViewMessageError[VT]
  ) extends MalformedPayload {
    override def pretty: Pretty[ViewMessageError.this.type] = prettyOfParam(_.error)
  }

  final case class WrongRootHash(viewTree: ViewTree, expectedRootHash: RootHash)
      extends MalformedPayload {
    override def pretty: Pretty[WrongRootHash] = prettyOfClass(
      param("view tree", _.viewTree),
      param("expected root hash", _.expectedRootHash),
    )
  }

  final case class WrongRecipients(viewTree: ViewTree) extends MalformedPayload {

    override def pretty: Pretty[WrongRecipients] =
      prettyOfClass(
        param("viewHash", _.viewTree.viewHash),
        param("viewPosition", _.viewTree.viewPosition),
      )
  }

  final case class IncompleteLightViewTree(
      position: ViewPosition
  ) extends MalformedPayload {

    override def pretty: Pretty[IncompleteLightViewTree] =
      prettyOfClass(param("position", _.position))
  }

  final case class DuplicateLightViewTree(
      position: ViewPosition
  ) extends MalformedPayload {

    override def pretty: Pretty[DuplicateLightViewTree] =
      prettyOfClass(param("position", _.position))
  }
}
