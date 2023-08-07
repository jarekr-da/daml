// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package v2
package ledgerinteraction

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.{IdentityProviderId, ObjectMeta, PartyDetails, User, UserRight}
import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.interpretation.Error.ContractIdInContractKey
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast.PackageMetadata
import com.daml.lf.language.Ast.TTyCon
import com.daml.lf.language.{LookupError, PackageInterface, Reference}
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.lf.speedy.Speedy.Machine
import com.daml.lf.speedy.{Pretty, SValue, TraceLog, WarningLog, SError}
import com.daml.lf.transaction.{
  GlobalKey,
  IncompleteTransaction,
  Node,
  NodeId,
  Transaction,
  Versioned,
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.nonempty.NonEmpty
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.InMemoryUserManagementStore
import io.grpc.StatusRuntimeException
import scalaz.OneAnd
import scalaz.OneAnd._
import scalaz.std.set._
import scalaz.syntax.foldable._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

// Client for the script service.
class IdeLedgerClient(
    val originalCompiledPackages: PureCompiledPackages,
    traceLog: TraceLog,
    warningLog: WarningLog,
    canceled: () => Boolean,
) extends ScriptLedgerClient {
  override def transport = "script service"

  private val nextSeed: () => crypto.Hash =
    // We seeds to secureRandom with a fix seed to get deterministic sequences of seeds
    // across different runs of IdeLedgerClient.
    crypto.Hash.secureRandom(crypto.Hash.hashPrivateKey(s"script-service"))

  private var _currentSubmission: Option[ScenarioRunner.CurrentSubmission] = None

  def currentSubmission: Option[ScenarioRunner.CurrentSubmission] = _currentSubmission

  private[this] var compiledPackages = originalCompiledPackages

  private[this] var preprocessor = makePreprocessor

  private[this] var unvettedPackages: Set[PackageId] = Set.empty

  private[this] def makePreprocessor =
    new preprocessing.CommandPreprocessor(
      compiledPackages.pkgInterface,
      requireV1ContractIdSuffix = false,
    )

  private[this] def partialFunctionFilterNot[A](f: A => Boolean): PartialFunction[A, A] = {
    case x if !f(x) => x
  }

  // Given a set of disabled packages, filter out all definitions from those packages from the original compiled packages
  // Similar logic to Scenario-services' Context.scala, however here we make no changes on the module level, and never directly add new packages
  // We only maintain a subset of an original known package set.
  private[this] def updateCompiledPackages() = {
    compiledPackages =
      if (!unvettedPackages.isEmpty)(
        originalCompiledPackages.copy(
          // Remove packages from the set
          packageIds = originalCompiledPackages.packageIds -- unvettedPackages,
          // Filter out pkgId "key" to the partial function
          pkgInterface = new PackageInterface(
            partialFunctionFilterNot(
              unvettedPackages
            ) andThen originalCompiledPackages.pkgInterface.signatures
          ),
          // Filter out any defs in a disabled package
          defns = originalCompiledPackages.defns.view
            .filterKeys(sDefRef => !unvettedPackages(sDefRef.packageId))
            .toMap,
        ),
      )
      else originalCompiledPackages
    preprocessor = makePreprocessor
  }

  private var _ledger: ScenarioLedger = ScenarioLedger.initialLedger(Time.Timestamp.Epoch)
  def ledger: ScenarioLedger = _ledger

  private var allocatedParties: Map[String, PartyDetails] = Map()

  private val userManagementStore = new InMemoryUserManagementStore(createAdmin = false)

  override def query(parties: OneAnd[Set, Ref.Party], templateId: Identifier)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Seq[ScriptLedgerClient.ActiveContract]] = {
    val acs = ledger.query(
      view = ScenarioLedger.ParticipantView(Set(), Set(parties.toList: _*)),
      effectiveAt = ledger.currentTime,
    )
    val filtered = acs.collect {
      case ScenarioLedger.LookupOk(
            cid,
            Versioned(_, Value.ContractInstance(tpl, arg)),
            stakeholders,
          ) if tpl == templateId && parties.any(stakeholders.contains(_)) =>
        (cid, arg)
    }
    Future.successful(filtered.map { case (cid, c) =>
      ScriptLedgerClient.ActiveContract(templateId, cid, c)
    })
  }

  private def lookupContractInstance(
      parties: OneAnd[Set, Ref.Party],
      cid: ContractId,
  ): Option[Value.ContractInstance] = {

    ledger.lookupGlobalContract(
      view = ScenarioLedger.ParticipantView(Set(), Set(parties.toList: _*)),
      effectiveAt = ledger.currentTime,
      cid,
    ) match {
      case ScenarioLedger.LookupOk(
            _,
            Versioned(_, contractInstance),
            stakeholders,
          ) if parties.any(stakeholders.contains(_)) =>
        Some(contractInstance)
      case _ =>
        // Note that contrary to `fetch` in a scenario, we do not
        // abort on any of the error cases. This makes sense if you
        // consider this a wrapper around the ACS endpoint where
        // we cannot differentiate between visibility errors
        // and the contract not being active.
        None
    }
  }

  override def queryContractId(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      cid: ContractId,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]] = {
    Future.successful(
      lookupContractInstance(parties, cid).map { case Value.ContractInstance(_, arg) =>
        ScriptLedgerClient.ActiveContract(templateId, cid, arg)
      }
    )
  }

  private[this] def computeView(
      templateId: TypeConName,
      interfaceId: TypeConName,
      arg: Value,
  ): Option[Value] = {

    val valueTranslator = new ValueTranslator(
      pkgInterface = compiledPackages.pkgInterface,
      requireV1ContractIdSuffix = false,
    )

    valueTranslator.translateValue(TTyCon(templateId), arg) match {
      case Left(_) =>
        sys.error("computeView: translateValue failed")

      case Right(argument) =>
        val compiler: speedy.Compiler = compiledPackages.compiler
        val iview = speedy.InterfaceView(templateId, argument, interfaceId)
        val sexpr = compiler.unsafeCompileInterfaceView(iview)
        val machine = Machine.fromPureSExpr(compiledPackages, sexpr)(Script.DummyLoggingContext)

        machine.runPure() match {
          case Right(svalue) =>
            val version = machine.tmplId2TxVersion(templateId)
            Some(svalue.toNormalizedValue(version))

          case Left(_) =>
            None
        }
    }
  }

  private[this] def implements(templateId: TypeConName, interfaceId: TypeConName): Boolean = {
    compiledPackages.pkgInterface.lookupInterfaceInstance(interfaceId, templateId).isRight
  }

  override def queryInterface(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      viewType: Ast.Type,
  )(implicit ec: ExecutionContext, mat: Materializer): Future[Seq[(ContractId, Option[Value])]] = {

    val acs: Seq[ScenarioLedger.LookupOk] = ledger.query(
      view = ScenarioLedger.ParticipantView(Set(), Set(parties.toList: _*)),
      effectiveAt = ledger.currentTime,
    )
    val filtered: Seq[(ContractId, Value.ContractInstance)] = acs.collect {
      case ScenarioLedger.LookupOk(
            cid,
            Versioned(_, contractInstance @ Value.ContractInstance(templateId, _)),
            stakeholders,
          ) if implements(templateId, interfaceId) && parties.any(stakeholders.contains(_)) =>
        (cid, contractInstance)
    }
    val res: Seq[(ContractId, Option[Value])] = {
      filtered.map { case (cid, contractInstance) =>
        contractInstance match {
          case Value.ContractInstance(templateId, arg) =>
            val viewOpt = computeView(templateId, interfaceId, arg)
            (cid, viewOpt)
        }
      }
    }
    Future.successful(res)
  }

  override def queryInterfaceContractId(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      viewType: Ast.Type,
      cid: ContractId,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[Value]] = {

    lookupContractInstance(parties, cid) match {
      case None => Future.successful(None)
      case Some(Value.ContractInstance(templateId, arg)) =>
        val viewOpt = computeView(templateId, interfaceId, arg)
        Future.successful(viewOpt)
    }
  }

  override def queryContractKey(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      key: SValue,
      translateKey: (Identifier, Value) => Either[String, SValue],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]] = {
    val keyValue = key.toUnnormalizedValue
    def keyBuilderError(err: crypto.Hash.HashingError): Future[GlobalKey] =
      Future.failed(
        err match {
          case crypto.Hash.HashingError.ForbiddenContractId() =>
            SError.SErrorDamlException(ContractIdInContractKey(keyValue))
        }
      )
    GlobalKey
      .build(templateId, keyValue)
      .fold(keyBuilderError(_), Future.successful(_))
      .flatMap { gkey =>
        ledger.ledgerData.activeKeys.get(gkey) match {
          case None => Future.successful(None)
          case Some(cid) => queryContractId(parties, templateId, cid)
        }
      }
  }

  private def getTypeIdentifier(t: Ast.Type): Option[Identifier] =
    t match {
      case Ast.TTyCon(ty) => Some(ty)
      case _ => None
    }

  private def fromInterpretationError(err: interpretation.Error): SubmitError = {
    import interpretation.Error._
    err match {
      // TODO[SW]: This error isn't yet fully implemented, so remains unknown. Convert to a `dev` error once the feature is supported in IDELedger.
      case e: RejectedAuthorityRequest => SubmitError.UnknownError(e.toString)
      case ContractNotFound(cid) =>
        SubmitError.ContractNotFound(
          NonEmpty(Seq, cid),
          Some(SubmitError.ContractNotFound.AdditionalInfo.NotFound()),
        )
      case ContractKeyNotFound(key) => SubmitError.ContractKeyNotFound(key)
      case e: FailedAuthorization =>
        SubmitError.AuthorizationError(Pretty.prettyDamlException(e).renderWideStream.mkString)
      case ContractNotActive(cid, tid, _) => SubmitError.ContractNotActive(tid, cid)
      case DisclosedContractKeyHashingError(cid, key, hash) =>
        SubmitError.DisclosedContractKeyHashingError(cid, key, hash.toString)
      // Hide not visible as not found
      case ContractKeyNotVisible(_, key, _, _, _) =>
        SubmitError.ContractKeyNotFound(key)
      case DuplicateContractKey(key) => SubmitError.DuplicateContractKey(Some(key))
      case InconsistentContractKey(key) => SubmitError.InconsistentContractKey(key)
      // Only pass on the error if the type is a TTyCon
      case UnhandledException(ty, v) =>
        SubmitError.UnhandledException(getTypeIdentifier(ty).map(tyId => (tyId, v)))
      case UserError(msg) => SubmitError.UserError(msg)
      case _: TemplatePreconditionViolated => SubmitError.TemplatePreconditionViolated()
      case CreateEmptyContractKeyMaintainers(tid, arg, _) =>
        SubmitError.CreateEmptyContractKeyMaintainers(tid, arg)
      case FetchEmptyContractKeyMaintainers(tid, keyValue) =>
        SubmitError.FetchEmptyContractKeyMaintainers(GlobalKey.assertBuild(tid, keyValue))
      case WronglyTypedContract(cid, exp, act) => SubmitError.WronglyTypedContract(cid, exp, act)
      case ContractDoesNotImplementInterface(iid, cid, tid) =>
        SubmitError.ContractDoesNotImplementInterface(cid, tid, iid)
      case ContractDoesNotImplementRequiringInterface(requiringIid, requiredIid, cid, tid) =>
        SubmitError.ContractDoesNotImplementRequiringInterface(cid, tid, requiredIid, requiringIid)
      case NonComparableValues => SubmitError.NonComparableValues()
      case ContractIdInContractKey(_) => SubmitError.ContractIdInContractKey()
      case ContractIdComparability(cid) => SubmitError.ContractIdComparability(cid.toString)
      case e @ Dev(_, innerError) =>
        SubmitError.DevError(
          innerError.getClass.getSimpleName,
          Pretty.prettyDamlException(e).renderWideStream.mkString,
        )
    }
  }

  // Projects the scenario submission error down to the script submission error
  private def fromScenarioError(err: scenario.Error): SubmitError = err match {
    case scenario.Error.RunnerException(e: SError.SErrorCrash) =>
      SubmitError.UnknownError(e.toString)
    case scenario.Error.RunnerException(SError.SErrorDamlException(err)) =>
      fromInterpretationError(err)

    case scenario.Error.Internal(reason) => SubmitError.UnknownError(reason)
    case scenario.Error.Timeout(timeout) => SubmitError.UnknownError("Timeout: " + timeout)

    // We treat ineffective contracts (ie, ones that don't exist yet) as being not found
    case scenario.Error.ContractNotEffective(cid, tid, effectiveAt) =>
      SubmitError.ContractNotFound(
        NonEmpty(Seq, cid),
        Some(SubmitError.ContractNotFound.AdditionalInfo.NotEffective(tid, effectiveAt)),
      )

    case scenario.Error.ContractNotActive(cid, templateId, _) =>
      SubmitError.ContractNotActive(templateId, cid)

    // Similarly, we treat contracts that we can't see as not being found
    case scenario.Error.ContractNotVisible(cid, tid, actAs, readAs, observers) =>
      SubmitError.ContractNotFound(
        NonEmpty(Seq, cid),
        Some(SubmitError.ContractNotFound.AdditionalInfo.NotVisible(tid, actAs, readAs, observers)),
      )

    case scenario.Error.ContractKeyNotVisible(_, key, _, _, _) =>
      SubmitError.ContractKeyNotFound(key)

    case scenario.Error.CommitError(
          ScenarioLedger.CommitError.UniqueKeyViolation(ScenarioLedger.UniqueKeyViolation(gk))
        ) =>
      SubmitError.DuplicateContractKey(Some(gk))

    case scenario.Error.LookupError(err, _, _) =>
      // TODO[SW]: Implement proper Lookup error throughout
      SubmitError.UnknownError("Lookup error: " + err.toString)

    // This covers MustFailSucceeded, InvalidPartyName, PartyAlreadyExists which should not be throwable by a command submission
    // It also covers PartiesNotAllocated.
    case err => SubmitError.UnknownError("Unexpected error type: " + err.toString)
  }
  // Build a SubmissionError with empty transaction
  private def makeEmptySubmissionError(err: scenario.Error): ScenarioRunner.SubmissionError =
    ScenarioRunner.SubmissionError(
      err,
      IncompleteTransaction(
        transaction = Transaction(Map.empty, ImmArray.empty),
        locationInfo = Map.empty,
      ),
    )

  private def getReferencePackageId(ref: Reference): PackageId =
    ref match {
      case Reference.Package(packageId) => packageId
      case Reference.Module(packageId, _) => packageId
      case Reference.Definition(name) => name.packageId
      case Reference.TypeSyn(name) => name.packageId
      case Reference.DataType(name) => name.packageId
      case Reference.DataRecord(name) => name.packageId
      case Reference.DataRecordField(name, _) => name.packageId
      case Reference.DataVariant(name) => name.packageId
      case Reference.DataVariantConstructor(name, _) => name.packageId
      case Reference.DataEnum(name) => name.packageId
      case Reference.DataEnumConstructor(name, _) => name.packageId
      case Reference.Value(name) => name.packageId
      case Reference.Template(name) => name.packageId
      case Reference.Interface(name) => name.packageId
      case Reference.TemplateKey(name) => name.packageId
      case Reference.InterfaceInstance(_, name) => name.packageId
      case Reference.ConcreteInterfaceInstance(_, ref) => getReferencePackageId(ref)
      case Reference.TemplateChoice(name, _) => name.packageId
      case Reference.InterfaceChoice(name, _) => name.packageId
      case Reference.InheritedChoice(name, _, _) => name.packageId
      case Reference.TemplateOrInterface(name) => name.packageId
      case Reference.Choice(name, _) => name.packageId
      case Reference.Method(name, _) => name.packageId
      case Reference.Exception(name) => name.packageId
    }

  private def getLookupErrorPackageId(err: LookupError): PackageId =
    err match {
      case LookupError.NotFound(notFound, _) => getReferencePackageId(notFound)
      case LookupError.AmbiguousInterfaceInstance(instance, _) => getReferencePackageId(instance)
    }

  private def makeLookupError(
      err: LookupError
  ): ScenarioRunner.SubmissionError = {
    val packageId = getLookupErrorPackageId(err)
    val packageMetadata = getPackageIdReverseMap().lift(packageId).map {
      case ScriptLedgerClient.ReadablePackageId(packageName, packageVersion) =>
        PackageMetadata(packageName, packageVersion, None)
    }
    makeEmptySubmissionError(scenario.Error.LookupError(err, packageMetadata, packageId))
  }

  private def makePartiesNotAllocatedError(
      unallocatedSubmitters: Set[Party]
  ): ScenarioRunner.SubmissionError =
    makeEmptySubmissionError(scenario.Error.PartiesNotAllocated(unallocatedSubmitters))

  // unsafe version of submit that does not clear the commit.
  private def unsafeSubmit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext): Future[
    Either[
      ScenarioRunner.SubmissionError,
      ScenarioRunner.Commit[ScenarioLedger.CommitResult],
    ]
  ] = Future {
    val unallocatedSubmitters: Set[Party] =
      (actAs.toSet union readAs) -- allocatedParties.values.map(_.party)
    if (unallocatedSubmitters.nonEmpty) {
      Left(makePartiesNotAllocatedError(unallocatedSubmitters))
    } else {
      @tailrec
      def loop(
          result: ScenarioRunner.SubmissionResult[ScenarioLedger.CommitResult]
      ): Either[
        ScenarioRunner.SubmissionError,
        ScenarioRunner.Commit[ScenarioLedger.CommitResult],
      ] =
        result match {
          case _ if canceled() =>
            throw script.Runner.TimedOut
          case ScenarioRunner.Interruption(continue) =>
            loop(continue())
          case err: ScenarioRunner.SubmissionError => Left(err)
          case commit @ ScenarioRunner.Commit(result, _, _) =>
            val referencedParties: Set[Party] =
              result.richTransaction.blindingInfo.disclosure.values
                .fold(Set.empty[Party])(_ union _)
            val unallocatedParties = referencedParties -- allocatedParties.values.map(_.party)
            Either.cond(
              unallocatedParties.isEmpty,
              commit,
              ScenarioRunner.SubmissionError(
                scenario.Error.PartiesNotAllocated(unallocatedParties),
                commit.tx,
              ),
            )
        }

      // We use try + unsafePreprocess here to avoid the addition template lookup logic in `preprocessApiCommands`
      val eitherSpeedyCommands =
        try {
          Right(preprocessor.unsafePreprocessApiCommands(commands.to(ImmArray)))
        } catch {
          case Error.Preprocessing.Lookup(err) => Left(makeLookupError(err))
        }

      val ledgerApi = ScenarioRunner.ScenarioLedgerApi(ledger)

      for {
        speedyCommands <- eitherSpeedyCommands
        translated = compiledPackages.compiler.unsafeCompile(speedyCommands, ImmArray.empty)
        result =
          ScenarioRunner.submit(
            compiledPackages,
            ledgerApi,
            actAs.toSet,
            readAs,
            translated,
            optLocation,
            nextSeed(),
            traceLog,
            warningLog,
          )(Script.DummyLoggingContext)
        res <- loop(result)
      } yield res
    }
  }

  override def submit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]] =
    unsafeSubmit(actAs, readAs, commands, optLocation).map {
      case Right(ScenarioRunner.Commit(result, _, _)) =>
        _currentSubmission = None
        _ledger = result.newLedger
        val transaction = result.richTransaction.transaction
        def convRootEvent(id: NodeId): ScriptLedgerClient.CommandResult = {
          val node = transaction.nodes.getOrElse(
            id,
            throw new IllegalArgumentException(s"Unknown root node id $id"),
          )
          node match {
            case create: Node.Create => ScriptLedgerClient.CreateResult(create.coid)
            case exercise: Node.Exercise =>
              ScriptLedgerClient.ExerciseResult(
                exercise.templateId,
                exercise.interfaceId,
                exercise.choiceId,
                exercise.exerciseResult.get,
              )
            case _: Node.Fetch | _: Node.LookupByKey | _: Node.Rollback =>
              throw new IllegalArgumentException(s"Invalid root node: $node")
          }
        }
        Right(transaction.roots.toSeq.map(convRootEvent))
      case Left(ScenarioRunner.SubmissionError(err, tx)) =>
        _currentSubmission = Some(ScenarioRunner.CurrentSubmission(optLocation, tx))
        throw err
    }

  override def submitMustFail(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext, mat: Materializer): Future[Either[Unit, Unit]] =
    unsafeSubmit(actAs, readAs, commands, optLocation).map {
      case Right(ScenarioRunner.Commit(_, _, tx)) =>
        _currentSubmission = Some(ScenarioRunner.CurrentSubmission(optLocation, tx))
        Left(())
      case Left(_) =>
        _currentSubmission = None
        _ledger = ledger.insertAssertMustFail(actAs.toSet, readAs, optLocation)
        Right(())
    }

  override def submitTree(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[ScriptLedgerClient.TransactionTree] =
    unsafeSubmit(actAs, readAs, commands, optLocation).map {
      case Right(ScenarioRunner.Commit(result, _, _)) =>
        _currentSubmission = None
        _ledger = result.newLedger
        val transaction = result.richTransaction.transaction
        def convEvent(id: NodeId): Option[ScriptLedgerClient.TreeEvent] =
          transaction.nodes(id) match {
            case create: Node.Create =>
              Some(ScriptLedgerClient.Created(create.templateId, create.coid, create.arg))
            case exercise: Node.Exercise =>
              Some(
                ScriptLedgerClient.Exercised(
                  exercise.templateId,
                  exercise.interfaceId,
                  exercise.targetCoid,
                  exercise.choiceId,
                  exercise.chosenValue,
                  exercise.children.collect(Function.unlift(convEvent(_))).toList,
                )
              )
            case _: Node.Fetch | _: Node.LookupByKey | _: Node.Rollback => None
          }
        ScriptLedgerClient.TransactionTree(
          transaction.roots.collect(Function.unlift(convEvent(_))).toList
        )
      case Left(ScenarioRunner.SubmissionError(err, tx)) =>
        _currentSubmission = Some(ScenarioRunner.CurrentSubmission(optLocation, tx))
        throw new IllegalStateException(err)
    }

  override def allocateParty(partyIdHint: String, displayName: String)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ) = {
    val usedNames = allocatedParties.keySet
    Future.fromTry(for {
      name <-
        if (partyIdHint != "") {
          // Try to allocate the given hint as party name. Will fail if the name is already taken.
          if (usedNames contains partyIdHint) {
            Failure(scenario.Error.PartyAlreadyExists(partyIdHint))
          } else {
            Success(partyIdHint)
          }
        } else {
          // Allocate a fresh name based on the display name.
          // Empty party ids are not allowed, fall back to "party" on empty display name.
          val namePrefix = if (displayName.isEmpty) { "party" }
          else { displayName }
          val candidates = namePrefix #:: LazyList.from(1).map(namePrefix + _.toString())
          Success(candidates.find(s => !(usedNames contains s)).get)
        }
      party <- Ref.Party
        .fromString(name)
        .fold(msg => Failure(scenario.Error.InvalidPartyName(name, msg)), Success(_))

      // Create and store the new party.
      partyDetails = PartyDetails(
        party = party,
        displayName = Some(displayName),
        isLocal = true,
        metadata = ObjectMeta.empty,
        identityProviderId = IdentityProviderId.Default,
      )
      _ = allocatedParties += (name -> partyDetails)
    } yield partyDetails.party)
  }

  override def listKnownParties()(implicit ec: ExecutionContext, mat: Materializer) = {
    Future.successful(allocatedParties.values.toList)
  }

  override def getStaticTime()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Time.Timestamp] = {
    Future.successful(ledger.currentTime)
  }

  override def setStaticTime(time: Time.Timestamp)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = {
    val diff = time.micros - ledger.currentTime.micros
    // ScenarioLedger only provides pass, so we have to calculate the diff.
    // Note that ScenarioLedger supports going backwards in time.
    _ledger = ledger.passTime(diff)
    Future.unit
  }

  override def createUser(
      user: User,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]] =
    userManagementStore
      .createUser(user, rights.toSet)(LoggingContext.empty)
      .map(_.toOption.map(_ => ()))

  override def getUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[User]] =
    userManagementStore
      .getUser(id, IdentityProviderId.Default)(LoggingContext.empty)
      .map(_.toOption)

  override def deleteUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]] =
    userManagementStore
      .deleteUser(id, IdentityProviderId.Default)(LoggingContext.empty)
      .map(_.toOption)

  override def listAllUsers()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[User]] =
    userManagementStore.listAllUsers()

  override def grantUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    userManagementStore
      .grantRights(id, rights.toSet, IdentityProviderId.Default)(LoggingContext.empty)
      .map(_.toOption.map(_.toList))

  override def revokeUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    userManagementStore
      .revokeRights(id, rights.toSet, IdentityProviderId.Default)(LoggingContext.empty)
      .map(_.toOption.map(_.toList))

  override def listUserRights(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    userManagementStore
      .listUserRights(id, IdentityProviderId.Default)(LoggingContext.empty)
      .map(_.toOption.map(_.toList))

  override def trySubmit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Either[SubmitError, Seq[ScriptLedgerClient.CommandResult]]] =
    unsafeSubmit(actAs, readAs, commands, optLocation).map {
      case Right(ScenarioRunner.Commit(result, _, _)) =>
        _currentSubmission = None
        _ledger = result.newLedger
        val transaction = result.richTransaction.transaction
        def convRootEvent(id: NodeId): ScriptLedgerClient.CommandResult = {
          val node = transaction.nodes.getOrElse(
            id,
            throw new IllegalArgumentException(s"Unknown root node id $id"),
          )
          node match {
            case create: Node.Create => ScriptLedgerClient.CreateResult(create.coid)
            case exercise: Node.Exercise =>
              ScriptLedgerClient.ExerciseResult(
                exercise.templateId,
                exercise.interfaceId,
                exercise.choiceId,
                exercise.exerciseResult.get,
              )
            case _: Node.Fetch | _: Node.LookupByKey | _: Node.Rollback =>
              throw new IllegalArgumentException(s"Invalid root node: $node")
          }
        }
        Right(transaction.roots.toSeq.map(convRootEvent))
      case Left(ScenarioRunner.SubmissionError(err, tx)) =>
        _currentSubmission = Some(ScenarioRunner.CurrentSubmission(optLocation, tx))
        Left(fromScenarioError(err))
    }

  def getPackageIdMap(): Map[ScriptLedgerClient.ReadablePackageId, PackageId] =
    getPackageIdPairs().toMap
  def getPackageIdReverseMap(): Map[PackageId, ScriptLedgerClient.ReadablePackageId] =
    getPackageIdPairs().map(_.swap).toMap

  def getPackageIdPairs(): Set[(ScriptLedgerClient.ReadablePackageId, PackageId)] = {
    originalCompiledPackages.packageIds
      .collect(
        Function.unlift(pkgId =>
          for {
            pkgSig <- originalCompiledPackages.pkgInterface.lookupPackage(pkgId).toOption
            meta <- pkgSig.metadata
            readablePackageId = meta match {
              case PackageMetadata(name, version, _) =>
                ScriptLedgerClient.ReadablePackageId(name, version)
            }
          } yield (readablePackageId, pkgId)
        )
      )
  }

  override def vetPackages(packages: List[ScriptLedgerClient.ReadablePackageId])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = Future {
    val packageMap = getPackageIdMap()
    val pkgIdsToVet = packages.map(pkg =>
      packageMap.getOrElse(pkg, throw new IllegalArgumentException(s"Unknown package $pkg"))
    )

    unvettedPackages = unvettedPackages -- pkgIdsToVet.toSet
    updateCompiledPackages()
  }

  override def unvetPackages(packages: List[ScriptLedgerClient.ReadablePackageId])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = Future {
    val packageMap = getPackageIdMap()
    val pkgIdsToUnvet = packages.map(pkg =>
      packageMap.getOrElse(pkg, throw new IllegalArgumentException(s"Unknown package $pkg"))
    )

    unvettedPackages = unvettedPackages ++ pkgIdsToUnvet.toSet
    updateCompiledPackages()
  }

  override def listVettedPackages()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[ScriptLedgerClient.ReadablePackageId]] =
    Future.successful(getPackageIdMap().filter(kv => !unvettedPackages(kv._2)).keys.toList)

  override def listAllPackages()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[ScriptLedgerClient.ReadablePackageId]] =
    Future.successful(getPackageIdMap().keys.toList)
}