// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.syntax.either.*
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.{AbsoluteOffset, CantonTimestamp, FullUnassignmentTree}
import com.digitalasset.canton.participant.protocol.reassignment.IncompleteReassignmentData.ReassignmentEventGlobalOffset
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import io.scalaland.chimney.dsl.*

/** Stores the data for a reassignment that is incomplete, i.e., for which only the assignment or the unassignment was
  * emitted on the multi-domain event log.
  *
  * If [[IncompleteReassignmentData.ReassignmentEventGlobalOffset]] is a [[IncompleteReassignmentData.UnassignmentEventGlobalOffset]],
  * it means that the unassignment event was emitted before or at `queryOffset` and that assigned event was not yet
  * emitted or at an offset greater than `queryOffset`.
  * The same holds symmetrically for a [[IncompleteReassignmentData.AssignmentEventGlobalOffset]].
  */
final case class IncompleteReassignmentData private (
    sourceProtocolVersion: Source[ProtocolVersion],
    unassignmentTs: CantonTimestamp,
    unassignmentRequestCounter: RequestCounter,
    unassignmentRequest: FullUnassignmentTree,
    unassignmentDecisionTime: CantonTimestamp,
    contract: SerializableContract,
    unassignmentResult: Option[DeliveredUnassignmentResult],
    reassignmentEventGlobalOffset: ReassignmentEventGlobalOffset,
    queryOffset: AbsoluteOffset,
) {

  def sourceDomain: Source[DomainId] = unassignmentRequest.sourceDomain
  def targetDomain: Target[DomainId] = unassignmentRequest.targetDomain

  def unassignmentGlobalOffset: Option[AbsoluteOffset] =
    reassignmentEventGlobalOffset.unassignmentGlobalOffset

  def assignmentGlobalOffset: Option[AbsoluteOffset] =
    reassignmentEventGlobalOffset.assignmentGlobalOffset

  require(
    contract.contractId == unassignmentRequest.contractId,
    s"Supplied contract with ID ${contract.contractId} differs from the ID ${unassignmentRequest.contractId} of the unassignment request.",
  )

  def toReassignmentData: ReassignmentData = this
    .into[ReassignmentData]
    .withFieldComputed(
      _.reassignmentGlobalOffset,
      _.reassignmentEventGlobalOffset match {
        case IncompleteReassignmentData.AssignmentEventGlobalOffset(globalOffset) =>
          Some(ReassignmentData.AssignmentGlobalOffset(globalOffset))
        case IncompleteReassignmentData.UnassignmentEventGlobalOffset(globalOffset) =>
          Some(ReassignmentData.UnassignmentGlobalOffset(globalOffset))
      },
    )
    .transform
}

object IncompleteReassignmentData {
  def create(
      reassignmentData: ReassignmentData,
      queryOffset: AbsoluteOffset,
  ): Either[String, IncompleteReassignmentData] = {
    val reassignmentEventGlobalOffsetE: Either[String, ReassignmentEventGlobalOffset] =
      ReassignmentEventGlobalOffset.create(
        queryOffset = queryOffset,
        unassignmentGlobalOffset = reassignmentData.unassignmentGlobalOffset,
        assignmentGlobalOffset = reassignmentData.assignmentGlobalOffset,
      )

    reassignmentEventGlobalOffsetE.map { reassignmentEventGlobalOffset =>
      reassignmentData
        .into[IncompleteReassignmentData]
        .withFieldConst(_.queryOffset, queryOffset)
        .withFieldConst(_.reassignmentEventGlobalOffset, reassignmentEventGlobalOffset)
        .withConstructor(IncompleteReassignmentData.apply _)
        .transform
    }
  }

  def tryCreate(
      reassignmentData: ReassignmentData,
      queryOffset: AbsoluteOffset,
  ): IncompleteReassignmentData =
    create(reassignmentData, queryOffset).valueOr(err =>
      throw new IllegalArgumentException(s"Unable to create IncompleteReassignmentData: $err")
    )

  sealed trait ReassignmentEventGlobalOffset {
    def globalOffset: AbsoluteOffset
    def unassignmentGlobalOffset: Option[AbsoluteOffset]
    def assignmentGlobalOffset: Option[AbsoluteOffset]
  }

  final case class AssignmentEventGlobalOffset(globalOffset: AbsoluteOffset)
      extends ReassignmentEventGlobalOffset {
    override def unassignmentGlobalOffset: Option[AbsoluteOffset] = None

    override def assignmentGlobalOffset: Option[AbsoluteOffset] = Some(globalOffset)
  }

  final case class UnassignmentEventGlobalOffset(globalOffset: AbsoluteOffset)
      extends ReassignmentEventGlobalOffset {
    override def unassignmentGlobalOffset: Option[AbsoluteOffset] = Some(globalOffset)

    override def assignmentGlobalOffset: Option[AbsoluteOffset] = None
  }

  object ReassignmentEventGlobalOffset {
    private[reassignment] def create(
        queryOffset: AbsoluteOffset,
        unassignmentGlobalOffset: Option[AbsoluteOffset],
        assignmentGlobalOffset: Option[AbsoluteOffset],
    ): Either[String, ReassignmentEventGlobalOffset] =
      (unassignmentGlobalOffset, assignmentGlobalOffset) match {
        case (Some(unassignment), None) if unassignment <= queryOffset =>
          Right(UnassignmentEventGlobalOffset(unassignment))

        case (None, Some(assignment)) if assignment <= queryOffset =>
          Right(AssignmentEventGlobalOffset(assignment))

        case (Some(unassignment), Some(assignment))
            if unassignment <= queryOffset && queryOffset < assignment =>
          Right(UnassignmentEventGlobalOffset(unassignment))
        case (Some(unassignment), Some(assignment))
            if assignment <= queryOffset && queryOffset < unassignment =>
          Right(AssignmentEventGlobalOffset(assignment))

        case _ =>
          Left(
            s"Expecting incomplete reassignment at offset $queryOffset, found unassignment=$unassignmentGlobalOffset and assignment=$assignmentGlobalOffset"
          )
      }
  }
}
