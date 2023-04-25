// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package ledger

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.client.LedgerClient
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.daml.scalautil.Statement.discard

import scala.concurrent.Await

object LedgerApiClient {
  sealed abstract class Message extends Product with Serializable

  // Used by TriggerProcess
  private[process] final case class CommandSubmission(
      request: SubmitRequest
  ) extends Message

  def create(
      client: LedgerClient
  )(implicit config: TriggerSimulationConfig): Behavior[Message] = {
    Behaviors.receiveMessage { case CommandSubmission(request) =>
      discard(
        Await.result(
          client.commandClient.submitSingleCommand(request),
          config.ledgerSubmissionTimeout,
        )
      )
      Behaviors.same
    }
  }
}