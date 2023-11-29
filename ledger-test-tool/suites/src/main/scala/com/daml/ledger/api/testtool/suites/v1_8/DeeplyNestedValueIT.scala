// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.ErrorCode
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.Party
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, Update}
import com.daml.ledger.test.java.semantic.deeplynestedvalue._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

final class DeeplyNestedValueIT extends LedgerTestSuite {
  implicit val handlerCompanion
      : ContractCompanion.WithoutKey[Handler.Contract, Handler.ContractId, Handler] =
    Handler.COMPANION

  @tailrec
  private[this] def toNat(i: Long, acc: Nat = new nat.Z(javaapi.data.Unit.getInstance)): Nat =
    if (i == 0) acc else toNat(i - 1, new nat.S(acc))

  private[this] def waitForTransactionId(
      alpha: ParticipantTestContext,
      party: Party,
      command: Update[_],
  )(implicit
      ec: ExecutionContext
  ): Future[Either[Throwable, String]] =
    alpha
      .submitAndWaitForTransactionId(
        alpha.submitAndWaitRequest(party, command.commands)
      )
      .transform(x => Success(x.map(_.transactionId).toEither))

  private[this] def camlCase(s: String) =
    s.split(" ").iterator.map(_.capitalize).mkString("")

  List[Long](46, 100, 101, 110, 200).foreach { nesting =>
    val accepted = nesting <= 100
    val result = if (accepted) "Accept" else "Reject"

    // Once converted to Nat, `n` will have a nesting `nesting`.
    // Note that Nat.Z(()) has nesting 1.
    val n = nesting - 1

    // Choice argument are always wrapped in a record
    val nChoiceArgument = n - 1

    // The nesting of the payload of a `Contract` is one more than the nat it contains
    val nContract = n - 1

    // The nesting of the key of a `ContractWithKey` is one more than the nat it contains
    val nKey = n - 1

    def test[T](description: String, errorCodeIfExpected: ErrorCode)(
        update: ExecutionContext => (
            ParticipantTestContext,
            Party,
        ) => Future[Either[Throwable, T]]
    ): Unit =
      super.test(
        result + camlCase(description) + nesting.toString,
        s"${result.toLowerCase}s $description with a nesting of $nesting",
        allocate(SingleParty),
      )(implicit ec => { case Participants(Participant(alpha, party)) =>
        update(ec)(alpha, party).map {
          case Right(_) if accepted => ()
          case Left(err: Throwable) if !accepted =>
            assertGrpcError(
              err,
              errorCodeIfExpected,
              None,
              checkDefiniteAnswerMetadata = true,
            )
          case otherwise =>
            fail("Unexpected " + otherwise.fold(err => s"failure: $err", _ => "success"))
        }
      })

    test(
      "create command",
      LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
    ) { implicit ec => (alpha, party) =>
      waitForTransactionId(alpha, party, new Contract(party, nContract, toNat(nContract)).create)
    }

    test(
      "exercise command",
      LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <- waitForTransactionId(
          alpha,
          party,
          handler.exerciseDestruct(toNat(nChoiceArgument)),
        )
      } yield result
    }

    test(
      "create argument in CreateAndExercise command",
      LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
    ) { implicit ec => (alpha, party) =>
      waitForTransactionId(
        alpha,
        party,
        new Contract(party, nContract, toNat(nContract)).createAnd
          .exerciseArchive(),
      )
    }

    test(
      "choice argument in CreateAndExercise command",
      LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
    ) { implicit ec => (alpha, party) =>
      waitForTransactionId(
        alpha,
        party,
        new Handler(party).createAnd.exerciseDestruct(toNat(nChoiceArgument)),
      )
    }

    test(
      "exercise argument",
      LedgerApiErrors.CommandExecution.Interpreter.ValueNesting,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <-
          waitForTransactionId(
            alpha,
            party,
            handler.exerciseConstructThenDestruct(nChoiceArgument),
          )
      } yield result
    }

    test(
      "exercise output",
      LedgerApiErrors.CommandExecution.Interpreter.ValueNesting,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <-
          waitForTransactionId(alpha, party, handler.exerciseConstruct(n))
      } yield result
    }

    test(
      "create argument",
      LedgerApiErrors.CommandExecution.Interpreter.ValueNesting,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <- waitForTransactionId(alpha, party, handler.exerciseCreate(nContract))
      } yield result
    }

    test(
      "contract key",
      LedgerApiErrors.CommandExecution.Interpreter.ValueNesting,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <- waitForTransactionId(alpha, party, handler.exerciseCreateKey(nKey))
      } yield result
    }

    if (accepted) {
      // Because we cannot create contracts with nesting > 100,
      // it does not make sense to test fetch of those kinds of contracts.
      test(
        "fetch by key",
        LedgerApiErrors.CommandExecution.Interpreter.ValueNesting,
      ) { implicit ec => (alpha, party) =>
        for {
          handler: Handler.ContractId <- alpha.create(party, new Handler(party))
          _ <- alpha.exercise(party, handler.exerciseCreateKey(nKey))
          result <- waitForTransactionId(alpha, party, handler.exerciseFetchByKey(nKey))
        } yield result
      }
    }

    test(
      "failing lookup by key",
      LedgerApiErrors.CommandExecution.Interpreter.ValueNesting,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <- waitForTransactionId(alpha, party, handler.exerciseLookupByKey(nKey))
      } yield result
    }

    if (accepted) {
      // Because we cannot create contracts with key nesting > 100,
      // it does not make sens to test successful lookup for those keys.
      test(
        "successful lookup by key",
        LedgerApiErrors.CommandExecution.Interpreter.ValueNesting,
      ) { implicit ec => (alpha, party) =>
        for {
          handler: Handler.ContractId <- alpha.create(party, new Handler(party))
          _ <- alpha.exercise(party, handler.exerciseCreateKey(nKey))
          result <-
            waitForTransactionId(alpha, party, handler.exerciseLookupByKey(nKey))
        } yield result
      }
    }

  }
}
