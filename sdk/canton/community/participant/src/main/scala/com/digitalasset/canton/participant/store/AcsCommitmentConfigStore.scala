// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.db.DbAcsCommitmentConfigStore
import com.digitalasset.canton.participant.store.memory.InMemoryAcsCommitmentConfigStore
import com.digitalasset.canton.pruning.{
  ConfigForDomainThresholds,
  ConfigForNoWaitCounterParticipants,
  ConfigForSlowCounterParticipants,
}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait AcsCommitmentSlowCounterParticipantConfigStore {

  /** fetches all slow counter participant configuration stored in the cache.
    */
  def fetchAllSlowCounterParticipantConfig()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(Seq[ConfigForSlowCounterParticipants], Seq[ConfigForDomainThresholds])]

  /** creates the configurations in the database, if they already exists they are instead updated with the new values.
    */
  def createOrUpdateCounterParticipantConfigs(
      configs: Seq[ConfigForSlowCounterParticipants],
      thresholds: Seq[ConfigForDomainThresholds],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** remove all slow configurations for the given domains.
    * if the sequence of domains is empty, then everything is purged.
    */
  def clearSlowCounterParticipants(domainIds: Seq[DomainId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
}

trait AcsCommitmentNoWaitCounterParticipantConfigStore {

  /** adds a given no wait configuration, if it already exists then it has no effect.
    */
  def addNoWaitCounterParticipant(configs: Seq[ConfigForNoWaitCounterParticipants])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** removes given domains and counter participants.
    * if domains are empty then given participants are removed from all domains.
    * if participants are empty then given domains have all values removed.
    * if both are empty then everything is removed.
    */
  def removeNoWaitCounterParticipant(domains: Seq[DomainId], participants: Seq[ParticipantId])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** get all active no wait configurations, with possibility to filter by domains and participants.
    * if no filter is applied all active no waits are returned.
    */
  def getAllActiveNoWaitCounterParticipants(
      filterDomains: Seq[DomainId],
      filterParticipants: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ConfigForNoWaitCounterParticipants]]
}

/** Read and write interface for ACS commitment configs. */
trait AcsCounterParticipantConfigStore
    extends AcsCommitmentSlowCounterParticipantConfigStore
    with AcsCommitmentNoWaitCounterParticipantConfigStore
    with AutoCloseable

object AcsCounterParticipantConfigStore {
  def create(
      metrics: ParticipantMetrics,
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): AcsCounterParticipantConfigStore =
    storage match {
      case _: MemoryStorage => new InMemoryAcsCommitmentConfigStore()
      case jdbc: DbStorage =>
        new DbAcsCommitmentConfigStore(metrics, jdbc, timeouts, loggerFactory)
    }
}
