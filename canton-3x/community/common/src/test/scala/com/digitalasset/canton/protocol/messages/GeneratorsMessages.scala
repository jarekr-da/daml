// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{GeneratorsCrypto, Signature}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond, ViewPosition, ViewType}
import com.digitalasset.canton.protocol.messages.LocalReject.ConsistencyRejections.{
  DuplicateKey,
  InactiveContracts,
  InconsistentKey,
  LockedContracts,
  LockedKeys,
}
import com.digitalasset.canton.protocol.messages.LocalReject.MalformedRejects.{
  BadRootHashMessages,
  CreatesExistingContracts,
  ModelConformance,
  Payloads,
}
import com.digitalasset.canton.protocol.messages.LocalReject.TimeRejects.{
  LedgerTime,
  LocalTimeout,
  SubmissionTime,
}
import com.digitalasset.canton.protocol.messages.LocalReject.TransferInRejects.{
  AlreadyCompleted,
  ContractAlreadyActive,
  ContractAlreadyArchived,
  ContractIsLocked,
}
import com.digitalasset.canton.protocol.messages.LocalReject.TransferOutRejects.ActivenessCheckFailed
import com.digitalasset.canton.protocol.{RequestId, RootHash, TransferDomainId}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.GeneratorsTransaction
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

object GeneratorsMessages {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsData.*
  import com.digitalasset.canton.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.version.GeneratorsVersion.*

  @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
  /*
   Execution context is needed for crypto operations. Since wiring a proper ec would be
   too complex here, using the global one.
   */
  private implicit val ec: ExecutionContext = ExecutionContext.global

  implicit val acsCommitmentArb: Arbitrary[AcsCommitment] = Arbitrary(
    for {
      domainId <- Arbitrary.arbitrary[DomainId]
      sender <- Arbitrary.arbitrary[ParticipantId]
      counterParticipant <- Arbitrary.arbitrary[ParticipantId]

      periodFrom <- Arbitrary.arbitrary[CantonTimestampSecond]
      periodDuration <- Gen.choose(1, 86400L).map(PositiveSeconds.tryOfSeconds)
      period = CommitmentPeriod(periodFrom, periodDuration)

      commitment <- byteStringArb.arbitrary
      protocolVersion <- representativeProtocolVersionGen(AcsCommitment)
    } yield AcsCommitment.create(
      domainId,
      sender,
      counterParticipant,
      period,
      commitment,
      protocolVersion.representative,
    )
  )

  // TODO(#14515): move elsewhere?
  implicit val protoMediatorRejectionCodeArb
      : Arbitrary[com.digitalasset.canton.protocol.v0.MediatorRejection.Code] = genArbitrary

  object GeneratorsVerdict {
    def mediatorRejectGen(
        pv: RepresentativeProtocolVersion[Verdict.type]
    ): Gen[Verdict.MediatorReject] =
      // TODO(#14515): do we want randomness here?
      Gen.const {
        val status = com.google.rpc.status.Status(com.google.rpc.Code.CANCELLED_VALUE)
        Verdict.MediatorReject.tryCreate(status, pv.representative)
      }

    // TODO(#14515) Check that the generator is exhaustive
    implicit val mediatorRejectArb: Arbitrary[Verdict.MediatorReject] =
      Arbitrary(
        representativeProtocolVersionGen(Verdict).flatMap(mediatorRejectGen)
      )

    private val verdictApproveArb: Arbitrary[Verdict.Approve] = Arbitrary(
      representativeProtocolVersionGen(Verdict).map(Verdict.Approve())
    )

    private def participantRejectGenFor(pv: ProtocolVersion): Gen[Verdict.ParticipantReject] =
      nonEmptyListGen[(Set[LfPartyId], LocalReject)](
        GeneratorsLocalVerdict.participantRejectReasonArbFor(pv)
      ).map { reasons =>
        Verdict.ParticipantReject(reasons)(Verdict.protocolVersionRepresentativeFor(pv))
      }

    // If this pattern match is not exhaustive anymore, update the generator below
    {
      ((_: Verdict) match {
        case _: Verdict.Approve => ()
        case _: Verdict.MediatorReject => ()
        case _: Verdict.ParticipantReject => ()
      }).discard
    }
    def verdictGenFor(pv: ProtocolVersion): Gen[Verdict] = {
      val rpv = Verdict.protocolVersionRepresentativeFor(pv)
      Gen.oneOf(
        verdictApproveArb.arbitrary,
        mediatorRejectGen(rpv),
        participantRejectGenFor(pv),
      )
    }

    implicit val verdictArb: Arbitrary[Verdict] = Arbitrary(
      Arbitrary.arbitrary[ProtocolVersion].flatMap(verdictGenFor)
    )
  }

  object GeneratorsLocalVerdict {
    // TODO(#14515) Check that the generator is exhaustive
    private def localRejectImplGen(pv: ProtocolVersion): Gen[LocalRejectImpl] = {
      val resources = List("resource1", "resource2")
      val details = "details"

      val builders: Seq[RepresentativeProtocolVersion[LocalVerdict.type] => LocalRejectImpl] = Seq(
        LockedContracts.Reject(resources),
        LockedKeys.Reject(resources),
        InactiveContracts.Reject(resources),
        DuplicateKey.Reject(resources),
        InconsistentKey.Reject(resources),
        LedgerTime.Reject(details),
        SubmissionTime.Reject(details),
        LocalTimeout.Reject(),
        ActivenessCheckFailed.Reject(details),
        ContractAlreadyArchived.Reject(details),
        ContractAlreadyActive.Reject(details),
        ContractIsLocked.Reject(details),
        AlreadyCompleted.Reject(details),
        /*
         GenericReject is intentionally excluded
         Reason: it should not be serialized.
         */
        // GenericReject("cause", details, resources, "SOME_ID", ErrorCategory.TransientServerFailure),
      )

      val rpv = LocalVerdict.protocolVersionRepresentativeFor(pv)
      Gen.oneOf(builders).map(_(rpv))
    }

    // TODO(#14515) Check that the generator is exhaustive
    private def localVerdictMalformedGen(pv: ProtocolVersion): Gen[Malformed] = {
      val resources = List("resource1", "resource2")
      val details = "details"

      val builders: Seq[RepresentativeProtocolVersion[LocalVerdict.type] => Malformed] = Seq(
        /*
          MalformedRequest.Reject is intentionally excluded
          The reason is for backward compatibility reason, its `v0.LocalReject.Code` does not correspond to the id
          (`v0.LocalReject.Code.MalformedPayloads` vs "LOCAL_VERDICT_MALFORMED_REQUEST")
         */
        // MalformedRequest.Reject(details),
        Payloads.Reject(details),
        ModelConformance.Reject(details),
        BadRootHashMessages.Reject(details),
        CreatesExistingContracts.Reject(resources),
      )

      val rpv = LocalVerdict.protocolVersionRepresentativeFor(pv)
      Gen.oneOf(builders).map(_(rpv))
    }

    // TODO(#14515) Check that the generator is exhaustive
    private def localRejectGenFor(pv: ProtocolVersion): Gen[LocalReject] =
      Gen.oneOf(localRejectImplGen(pv), localVerdictMalformedGen(pv))

    private def localApproveGenFor(pv: ProtocolVersion): Gen[LocalApprove] =
      Gen.const(LocalApprove(pv))

    // If this pattern match is not exhaustive anymore, update the generator below
    {
      ((_: LocalVerdict) match {
        case _: LocalApprove => ()
        case _: LocalReject => ()
      }).discard
    }

    def localVerdictArbFor(pv: ProtocolVersion): Gen[LocalVerdict] =
      Gen.oneOf(localApproveGenFor(pv), localRejectGenFor(pv))

    implicit val localVerdictArb: Arbitrary[LocalVerdict] = Arbitrary(
      for {
        rpv <- representativeProtocolVersionGen(LocalVerdict)
        localVerdict <- localVerdictArbFor(rpv.representative)
      } yield localVerdict
    )

    private[GeneratorsMessages] def participantRejectReasonArbFor(
        pv: ProtocolVersion
    ): Arbitrary[(Set[LfPartyId], LocalReject)] = Arbitrary(
      for {
        parties <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
        reject <- localRejectGenFor(pv)
      } yield (parties, reject)
    )
  }

  implicit val transferResultArb: Arbitrary[TransferResult[TransferDomainId]] = Arbitrary(for {
    pv <- Arbitrary.arbitrary[ProtocolVersion]
    requestId <- Arbitrary.arbitrary[RequestId]
    informees <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
    domain <- Arbitrary.arbitrary[TransferDomainId]
    verdict <- GeneratorsVerdict.verdictGenFor(pv)
  } yield TransferResult.create(requestId, informees, domain, verdict, pv))

  implicit val malformedMediatorRequestResultArb: Arbitrary[MalformedMediatorRequestResult] =
    Arbitrary(
      for {
        pv <- Arbitrary.arbitrary[ProtocolVersion]
        requestId <- Arbitrary.arbitrary[RequestId]
        domainId <- Arbitrary.arbitrary[DomainId]
        viewType <- Arbitrary.arbitrary[ViewType]
        mediatorReject <- GeneratorsVerdict.mediatorRejectGen(
          Verdict.protocolVersionRepresentativeFor(pv)
        )
      } yield MalformedMediatorRequestResult.tryCreate(
        requestId,
        domainId,
        viewType,
        mediatorReject,
        pv,
      )
    )

  implicit val transactionResultMessage: Arbitrary[TransactionResultMessage] = Arbitrary(for {
    pv <- Arbitrary.arbitrary[ProtocolVersion]

    verdict <- GeneratorsVerdict.verdictGenFor(pv)
    rootHash <- Arbitrary.arbitrary[RootHash]
    requestId <- Arbitrary.arbitrary[RequestId]
    domainId <- Arbitrary.arbitrary[DomainId]

    // TODO(#14241) Also generate instance that contains InformeeTree + make pv above cover all the values
  } yield TransactionResultMessage(requestId, verdict, rootHash, domainId, pv))

  implicit val mediatorResponseArb: Arbitrary[MediatorResponse] = Arbitrary(
    for {
      pv <- Arbitrary.arbitrary[ProtocolVersion]
      requestId <- Arbitrary.arbitrary[RequestId]
      sender <- Arbitrary.arbitrary[ParticipantId]
      localVerdict <- GeneratorsLocalVerdict.localVerdictArbFor(pv)

      domainId <- Arbitrary.arbitrary[DomainId]

      confirmingParties <- localVerdict match {
        case _: Malformed =>
          Gen.const(Set.empty[LfPartyId])
        case _: LocalApprove | _: LocalReject =>
          nonEmptySet(implicitly[Arbitrary[LfPartyId]]).arbitrary.map(_.forgetNE)
        case _ => Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      }

      rootHash <- localVerdict match {
        case _: LocalApprove | _: LocalReject => Gen.some(Arbitrary.arbitrary[RootHash])
        case _ => Gen.option(Arbitrary.arbitrary[RootHash])
      }

      viewPositionO <- localVerdict match {
        case _: LocalApprove | _: LocalReject =>
          Gen.some(Arbitrary.arbitrary[ViewPosition])
        case _ => Gen.option(Arbitrary.arbitrary[ViewPosition])
      }

    } yield MediatorResponse.tryCreate(
      requestId,
      sender,
      viewPositionO,
      localVerdict,
      rootHash,
      confirmingParties,
      domainId,
      pv,
    )
  )

  // TODO(#14515) Check that the generator is exhaustive
  implicit val mediatorResultArb: Arbitrary[MediatorResult] = Arbitrary(
    Gen.oneOf[MediatorResult](
      Arbitrary.arbitrary[MalformedMediatorRequestResult],
      Arbitrary.arbitrary[TransactionResultMessage],
      Arbitrary.arbitrary[TransferResult[TransferDomainId]],
    )
  )

  // TODO(#14515) Check that the generator is exhaustive
  implicit val signedProtocolMessageContentArb: Arbitrary[SignedProtocolMessageContent] = Arbitrary(
    Gen.oneOf(
      Arbitrary.arbitrary[AcsCommitment],
      Arbitrary.arbitrary[MalformedMediatorRequestResult],
      Arbitrary.arbitrary[MediatorResponse],
      Arbitrary.arbitrary[MediatorResult],
    )
  )

  implicit val typedSignedProtocolMessageContent
      : Arbitrary[TypedSignedProtocolMessageContent[SignedProtocolMessageContent]] = Arbitrary(for {
    pv <- representativeProtocolVersionGen(TypedSignedProtocolMessageContent)
    content <- Arbitrary.arbitrary[SignedProtocolMessageContent]
  } yield TypedSignedProtocolMessageContent(content, pv.representative))

  implicit val signedProtocolMessageArb
      : Arbitrary[SignedProtocolMessage[SignedProtocolMessageContent]] = Arbitrary(for {
    rpv <- representativeProtocolVersionGen(SignedProtocolMessage)
    typedMessage <- Arbitrary
      .arbitrary[TypedSignedProtocolMessageContent[SignedProtocolMessageContent]]

    signatures <- nonEmptyListGen(implicitly[Arbitrary[Signature]])
  } yield SignedProtocolMessage(typedMessage, signatures)(rpv))

  private implicit val emptyTraceContext: TraceContext = TraceContext.empty
  private lazy val syncCrypto = GeneratorsCrypto.cryptoFactory.headSnapshot

  private def domainTopologyTransactionMessageGenFor(
      pv: ProtocolVersion
  ): Gen[DomainTopologyTransactionMessage] =
    for {
      transactions <- Gen.listOf(GeneratorsTransaction.signedTopologyTransactionGenFor(pv))
      domainId <- Arbitrary.arbitrary[DomainId]
      notSequencedAfter <- Arbitrary.arbitrary[CantonTimestamp]
    } yield Await.result(
      DomainTopologyTransactionMessage.tryCreate(
        transactions,
        syncCrypto,
        domainId,
        notSequencedAfter,
        pv,
      ),
      10.seconds,
    )

  // TODO(#14241) Once we have more generators for merkle trees base classes, make these generators exhaustive
  private def unsignedProtocolMessageV4GenFor(pv: ProtocolVersion): Gen[UnsignedProtocolMessage] =
    domainTopologyTransactionMessageGenFor(pv)

  private def UnsignedProtocolMessage(pv: ProtocolVersion): Gen[UnsignedProtocolMessage] =
    domainTopologyTransactionMessageGenFor(pv)

  // TODO(#14515) Check that the generator is exhaustive
  // We don't include `protocolMessageV0GenFor` because we don't want
  // to test EnvelopeContentV0 that uses a legacy converter
  def protocolMessageGen(pv: ProtocolVersion): Gen[ProtocolMessage] = Gen.oneOf(
    unsignedProtocolMessageV4GenFor(pv),
    UnsignedProtocolMessage(pv),
  )

  // TODO(#14515) Check that the generator is exhaustive
  implicit val envelopeContentArb: Arbitrary[EnvelopeContent] = Arbitrary(for {
    pv <- Arbitrary.arbitrary[ProtocolVersion]
    rpv = EnvelopeContent.protocolVersionRepresentativeFor(pv)
    // We don't test EnvelopeContentV0 because it uses legacy converter which is incompatible with this test
    protocolMessageGen = unsignedProtocolMessageV4GenFor(pv)
    protocolMessage <- protocolMessageGen
  } yield EnvelopeContent.tryCreate(protocolMessage, pv))

}