// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.*
import com.digitalasset.canton.admin.api.client.commands.{GrpcAdminCommand, SequencerPublicCommands}
import com.digitalasset.canton.admin.api.client.data.StaticDomainParameters as ConsoleStaticDomainParameters
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.console.CommandErrors.NodeNotStarted
import com.digitalasset.canton.console.commands.*
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.domain.mediator.{
  MediatorNodeBootstrapX,
  MediatorNodeConfigCommon,
  MediatorNodeX,
  RemoteMediatorConfig,
}
import com.digitalasset.canton.domain.sequencing.config.{
  RemoteSequencerConfig,
  SequencerNodeConfigCommon,
}
import com.digitalasset.canton.domain.sequencing.{SequencerNodeBootstrapX, SequencerNodeX}
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.admin.data.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.MetricValue
import com.digitalasset.canton.participant.config.{
  BaseParticipantConfig,
  LocalParticipantConfig,
  RemoteParticipantConfig,
}
import com.digitalasset.canton.participant.{
  ParticipantNodeBootstrapX,
  ParticipantNodeCommon,
  ParticipantNodeX,
}
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

// TODO(#15161): Fold all *Common traits into X-derivers
trait InstanceReferenceCommon
    extends AdminCommandRunner
    with Helpful
    with NamedLogging
    with FeatureFlagFilter
    with PrettyPrinting {

  val name: String
  protected val instanceType: String

  protected[canton] def executionContext: ExecutionContext

  override def pretty: Pretty[InstanceReferenceCommon] =
    prettyOfString(inst => show"${inst.instanceType.unquoted} ${inst.name.singleQuoted}")

  val consoleEnvironment: ConsoleEnvironment

  override protected[console] def tracedLogger: TracedLogger = logger

  override def hashCode(): Int = {
    val init = this.getClass.hashCode()
    val t1 = MurmurHash3.mix(init, consoleEnvironment.hashCode())
    val t2 = MurmurHash3.mix(t1, name.hashCode)
    t2
  }

  // this is just testing, because the cached values should remain unchanged in operation
  @Help.Summary("Clear locally cached variables", FeatureFlag.Testing)
  @Help.Description(
    "Some commands cache values on the client side. Use this command to explicitly clear the caches of these values."
  )
  def clear_cache(): Unit = {
    topology.clearCache()
  }

  type Status <: NodeStatus.Status

  def id: NodeIdentity

  def health: HealthAdministrationCommon[Status]

  def keys: KeyAdministrationGroup

  def topology: TopologyAdministrationGroupCommon
}

/** InstanceReferenceX with different topology administration x
  */
trait InstanceReferenceX extends InstanceReferenceCommon {

  @Help.Summary("Inspect parties")
  @Help.Group("Parties")
  def parties: PartiesAdministrationGroupX

  override def topology: TopologyAdministrationGroupX

  private lazy val trafficControl_ =
    new TrafficControlAdministrationGroup(
      this,
      topology,
      this,
      consoleEnvironment,
      loggerFactory,
    )
  @Help.Summary("Traffic control related commands")
  @Help.Group("Traffic")
  def traffic_control: TrafficControlAdministrationGroup = trafficControl_

}

/** Pointer for a potentially running instance by instance type (domain/participant) and its id.
  * These methods define the REPL interface for these instances (e.g. participant1 start)
  */
trait LocalInstanceReferenceCommon extends InstanceReferenceCommon with NoTracing {

  val name: String
  val consoleEnvironment: ConsoleEnvironment
  private[console] val nodes: Nodes[CantonNode, CantonNodeBootstrap[CantonNode]]

  @Help.Summary("Database related operations")
  @Help.Group("Database")
  object db extends Helpful {

    @Help.Summary("Migrates the instance's database if using a database storage")
    @Help.Description(
      """When instances reside on different nodes, their database migration can be run in parallel
        |to save time. Please not that the migration commands must however must be run on each node
        |individually, because remote migration through `participants.remote...` is not supported.
        |"""
    )
    def migrate(): Unit = consoleEnvironment.run(migrateDbCommand())

    @Help.Summary(
      "Only use when advised - repairs the database migration of the instance's database"
    )
    @Help.Description(
      """In some rare cases, we change already applied database migration files in a new release and the repair
        |command resets the checksums we use to ensure that in general already applied migration files have not been changed.
        |You should only use `db.repair_migration` when advised and otherwise use it at your own risk - in the worst case running
        |it may lead to data corruption when an incompatible database migration (one that should be rejected because
        |the already applied database migration files have changed) is subsequently falsely applied.
        |"""
    )
    def repair_migration(force: Boolean = false): Unit =
      consoleEnvironment.run(repairMigrationCommand(force))

  }

  @Help.Summary("Start the instance")
  def start(): Unit = consoleEnvironment.run(startCommand())

  @Help.Summary("Stop the instance")
  def stop(): Unit = consoleEnvironment.run(stopCommand())

  @Help.Summary("Check if the local instance is running")
  def is_running: Boolean = nodes.isRunning(name)

  @Help.Summary("Check if the local instance is running and is fully initialized")
  def is_initialized: Boolean = nodes.getRunning(name).exists(_.isInitialized)

  @Help.Summary("Config of node instance")
  def config: LocalNodeConfig

  @Help.Summary("Manage public and secret keys")
  @Help.Group("Keys")
  override def keys: LocalKeyAdministrationGroup = _keys

  private val _keys =
    new LocalKeyAdministrationGroup(this, this, consoleEnvironment, crypto, loggerFactory)(
      executionContext
    )

  private[console] def migrateDbCommand(): ConsoleCommandResult[Unit] =
    migrateInstanceDb().toResult(_.message, _ => ())

  private[console] def repairMigrationCommand(force: Boolean): ConsoleCommandResult[Unit] =
    repairMigrationOfInstance(force).toResult(_.message, _ => ())

  private[console] def startCommand(): ConsoleCommandResult[Unit] =
    startInstance()
      .toResult({
        case m: PendingDatabaseMigration =>
          s"${m.message} Please run `${m.name}.db.migrate` to apply pending migrations"
        case m => m.message
      })

  private[console] def stopCommand(): ConsoleCommandResult[Unit] =
    try {
      stopInstance().toResult(_.message)
    } finally {
      ErrorUtil.withThrowableLogging(clear_cache())
    }

  protected def migrateInstanceDb(): Either[StartupError, ?] = nodes.migrateDatabase(name)
  protected def repairMigrationOfInstance(force: Boolean): Either[StartupError, Unit] = {
    Either
      .cond(force, (), DidntUseForceOnRepairMigration(name))
      .flatMap(_ => nodes.repairDatabaseMigration(name))
  }

  protected def startInstance(): Either[StartupError, Unit] =
    nodes.startAndWait(name)
  protected def stopInstance(): Either[ShutdownError, Unit] = nodes.stopAndWait(name)
  protected[canton] def crypto: Crypto

  protected def runCommandIfRunning[Result](
      runner: => ConsoleCommandResult[Result]
  ): ConsoleCommandResult[Result] =
    if (is_running)
      runner
    else
      NodeNotStarted.ErrorCanton(this)

  override protected[console] def adminCommand[Result](
      grpcCommand: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] = {
    runCommandIfRunning(
      consoleEnvironment.grpcAdminCommandRunner
        .runCommand(name, grpcCommand, config.clientAdminApi, None)
    )
  }

}

trait LocalInstanceReferenceX extends LocalInstanceReferenceCommon with InstanceReferenceX {

  @Help.Summary("Access the local nodes metrics")
  @Help.Group("Metrics")
  object metrics {

    private def filterByNodeAndAttribute(
        attributes: Map[String, String]
    )(value: MetricValue): Boolean = {
      value.attributes.get("node").contains(name) && attributes.forall { case (k, v) =>
        value.attributes.get(k).contains(v)
      }
    }

    private def getOne(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): Either[String, MetricValue] = check(FeatureFlag.Testing) {
      val res = consoleEnvironment.environment.configuredOpenTelemetry.onDemandMetricsReader
        .read()
        .find { data =>
          data.getName.equals(metricName)
        }
        .toList
        .flatMap(MetricValue.fromMetricData)
        .filter(filterByNodeAndAttribute(attributes))
      res match {
        case one :: Nil => Right(one)
        case Nil =>
          Left(s"No metric of name ${metricName} with instance name ${name} found")
        case other => Left(s"Found ${other.length} matching metrics")
      }
    }

    private def getOneOfType[TargetType <: MetricValue](
        metricName: String,
        attributes: Map[String, String],
    )(implicit
        M: ClassTag[TargetType]
    ): TargetType =
      consoleEnvironment.run(ConsoleCommandResult.fromEither(for {
        item <- getOne(metricName, attributes)
        casted <- item
          .select[TargetType]
          .toRight(s"Metric is not a ${M.showType} but ${item.getClass}")
      } yield casted))

    @Help.Summary("Get a particular metric")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes, or error if multiple matching are found."""
    )
    def get(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): MetricValue =
      consoleEnvironment.run(ConsoleCommandResult.fromEither(getOne(metricName, attributes)))

    @Help.Summary("Get a particular histogram")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes, or error if multiple matching are found."""
    )
    def get_histogram(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): MetricValue.Histogram = getOneOfType[MetricValue.Histogram](metricName, attributes)

    @Help.Summary("Get a particular summary")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes, or error if multiple matching are found."""
    )
    def get_summary(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): MetricValue.Summary = getOneOfType[MetricValue.Summary](metricName, attributes)

    @Help.Summary("Get a particular long point")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes, or error if multiple matching are found."""
    )
    def get_long_point(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): MetricValue.LongPoint = getOneOfType[MetricValue.LongPoint](metricName, attributes)

    @Help.Summary("Get a particular double point")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes, or error if multiple matching are found."""
    )
    def get_double_point(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): MetricValue.DoublePoint = getOneOfType[MetricValue.DoublePoint](metricName, attributes)

    @Help.Summary("List all metrics")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes."""
    )
    def list(
        filterName: String = "",
        attributes: Map[String, String] = Map(),
    ): Map[String, Seq[MetricValue]] =
      check(FeatureFlag.Testing) {
        consoleEnvironment.environment.configuredOpenTelemetry.onDemandMetricsReader
          .read()
          .filter(_.getName.startsWith(filterName))
          .flatMap(dt => MetricValue.fromMetricData(dt).map((dt.getName, _)))
          .filter { case (_, value) =>
            filterByNodeAndAttribute(attributes)(value)
          }
          .groupMap { case (name, _) => name } { case (_, value) => value }
      }

  }

}

trait RemoteInstanceReference extends InstanceReferenceCommon {
  @Help.Summary("Manage public and secret keys")
  @Help.Group("Keys")
  override val keys: KeyAdministrationGroup =
    new KeyAdministrationGroup(this, this, consoleEnvironment, loggerFactory)
}

trait GrpcRemoteInstanceReference extends RemoteInstanceReference {

  def config: NodeConfig

  override protected[console] def adminCommand[Result](
      grpcCommand: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] =
    consoleEnvironment.grpcAdminCommandRunner.runCommand(
      name,
      grpcCommand,
      config.clientAdminApi,
      None,
    )
}

// TODO(#15161): Fold into single deriver
trait InstanceReferenceWithSequencerConnection extends InstanceReferenceCommon {
  def sequencerConnection: GrpcSequencerConnection
}

/** Bare, Canton agnostic parts of the ledger-api client
  *
  * This implementation allows to access any kind of ledger-api client, which does not need to be Canton based.
  * However, this comes at some cost, as some of the synchronization between nodes during transaction submission
  * is not supported
  *
  * @param hostname the hostname of the ledger api server
  * @param port the port of the ledger api server
  * @param tls the tls config to use on the client
  * @param token the jwt token to use on the client
  */
class ExternalLedgerApiClient(
    hostname: String,
    port: Port,
    tls: Option[TlsClientConfig],
    val token: Option[String] = None,
)(implicit val consoleEnvironment: ConsoleEnvironment)
    extends BaseLedgerApiAdministration
    with LedgerApiCommandRunner
    with FeatureFlagFilter
    with NamedLogging {

  override protected val name: String = s"$hostname:${port.unwrap}"

  override val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory.append("client", name)

  override protected def domainOfTransaction(transactionId: String): DomainId =
    throw new NotImplementedError("domain_of is not implemented for external ledger api clients")

  override protected[console] def ledgerApiCommand[Result](
      command: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] =
    consoleEnvironment.grpcAdminCommandRunner
      .runCommand("sourceLedger", command, ClientConfig(hostname, port, tls), token)

  override protected def optionallyAwait[Tx](
      tx: Tx,
      txId: String,
      optTimeout: Option[NonNegativeDuration],
  ): Tx = tx

}

/** Allows to query the public api of a sequencer (e.g., sequencer connect service).
  *
  * @param trustCollectionFile a file containing certificates of all nodes the client trusts. If none is specified, defaults to the JVM trust store
  */
class SequencerPublicApiClient(
    sequencerConnection: GrpcSequencerConnection,
    trustCollectionFile: Option[ExistingFile],
)(implicit val consoleEnvironment: ConsoleEnvironment)
    extends PublicApiCommandRunner
    with NamedLogging {

  private val endpoint = sequencerConnection.endpoints.head1

  private val name: String = endpoint.toString

  override val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory.append("sequencer-public-api", name)

  protected[console] def publicApiCommand[Result](
      command: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] =
    consoleEnvironment.grpcAdminCommandRunner
      .runCommand(
        sequencerConnection.sequencerAlias.unwrap,
        command,
        ClientConfig(
          endpoint.host,
          endpoint.port,
          tls = trustCollectionFile.map(f =>
            TlsClientConfig(trustCollectionFile = Some(f), clientCert = None)
          ),
        ),
        token = None,
      )
}

object ExternalLedgerApiClient {

  def forReference[ParticipantNodeT <: ParticipantNodeCommon](
      participant: LocalParticipantReferenceCommon[ParticipantNodeT],
      token: String,
  )(implicit
      env: ConsoleEnvironment
  ): ExternalLedgerApiClient = {
    val cc = participant.config.ledgerApi.clientConfig
    new ExternalLedgerApiClient(
      cc.address,
      cc.port,
      cc.tls,
      Some(token),
    )
  }
}

sealed trait ParticipantReferenceCommon
    extends ConsoleCommandGroup
    with ParticipantAdministration
    with LedgerApiAdministration
    with LedgerApiCommandRunner
    with AdminCommandRunner
    with InstanceReferenceCommon {

  override type Status = ParticipantStatus

  override protected val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory.append("participant", name)

  @Help.Summary(
    "Yields the globally unique id of this participant. " +
      "Throws an exception, if the id has not yet been allocated (e.g., the participant has not yet been started)."
  )
  override def id: ParticipantId = topology.idHelper(ParticipantId(_))

  def config: BaseParticipantConfig

  @Help.Summary("Commands used for development and testing", FeatureFlag.Testing)
  @Help.Group("Testing")
  def testing: ParticipantTestingGroup

  @Help.Summary("Commands to pruning the archive of the ledger", FeatureFlag.Preview)
  @Help.Group("Ledger Pruning")
  def pruning: ParticipantPruningAdministrationGroup = pruning_
  private lazy val pruning_ =
    new ParticipantPruningAdministrationGroup(this, consoleEnvironment, loggerFactory)

  @Help.Summary("Manage participant replication")
  @Help.Group("Replication")
  def replication: ParticipantReplicationAdministrationGroup = replicationGroup
  lazy private val replicationGroup =
    new ParticipantReplicationAdministrationGroup(this, consoleEnvironment)

  @Help.Summary("Commands to repair the participant contract state", FeatureFlag.Repair)
  @Help.Group("Repair")
  def repair: ParticipantRepairAdministration

  override def health
      : HealthAdministrationCommon[ParticipantStatus] & ParticipantHealthAdministrationCommon

}

sealed trait RemoteParticipantReferenceCommon
    extends LedgerApiCommandRunner
    with ParticipantReferenceCommon {

  def config: RemoteParticipantConfig

  override protected[console] def ledgerApiCommand[Result](
      command: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] =
    consoleEnvironment.grpcAdminCommandRunner.runCommand(
      name,
      command,
      config.clientLedgerApi,
      config.token,
    )

  override protected[console] def token: Option[String] = config.token

  private lazy val testing_ = new ParticipantTestingGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Commands used for development and testing", FeatureFlag.Testing)
  @Help.Group("Testing")
  override def testing: ParticipantTestingGroup = testing_

  private lazy val repair_ =
    new ParticipantRepairAdministration(consoleEnvironment, this, loggerFactory)

  @Help.Summary("Commands to repair the participant contract state", FeatureFlag.Repair)
  @Help.Group("Repair")
  def repair: ParticipantRepairAdministration = repair_
}

sealed trait LocalParticipantReferenceCommon[ParticipantNodeT <: ParticipantNodeCommon]
    extends LedgerApiCommandRunner
    with ParticipantReferenceCommon
    with LocalInstanceReferenceCommon
    with BaseInspection[ParticipantNodeT] {

  override val name: String

  def config: LocalParticipantConfig

  def adminToken: Option[String]

  override protected[console] def ledgerApiCommand[Result](
      command: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] =
    runCommandIfRunning(
      consoleEnvironment.grpcAdminCommandRunner
        .runCommand(name, command, config.clientLedgerApi, adminToken)
    )

  override protected[console] def token: Option[String] = adminToken

  @Help.Summary("Commands used for development and testing", FeatureFlag.Testing)
  @Help.Group("Testing")
  def testing: LocalParticipantTestingGroup

  @Help.Summary("Commands to repair the local participant contract state", FeatureFlag.Repair)
  @Help.Group("Repair")
  def repair: LocalParticipantRepairAdministration
}

abstract class ParticipantReferenceX(
    override val consoleEnvironment: ConsoleEnvironment,
    val name: String,
) extends ParticipantReferenceCommon
    with InstanceReferenceX {

  override protected val instanceType: String = ParticipantReferenceX.InstanceType
  override protected def runner: AdminCommandRunner = this

  @Help.Summary("Health and diagnostic related commands")
  @Help.Group("Health")
  override def health: ParticipantHealthAdministrationX =
    new ParticipantHealthAdministrationX(this, consoleEnvironment, loggerFactory)

  override def parties: ParticipantPartiesAdministrationGroupX

  private lazy val topology_ =
    new TopologyAdministrationGroupX(
      this,
      health.status.successOption.map(_.topologyQueue),
      consoleEnvironment,
      loggerFactory,
    )
  @Help.Summary("Topology management related commands")
  @Help.Group("Topology")
  @Help.Description("This group contains access to the full set of topology management commands.")
  override def topology: TopologyAdministrationGroupX = topology_

  /** Waits until for every participant p (drawn from consoleEnvironment.participantsX.all) that is running and initialized
    * and for each domain to which both this participant and p are connected
    * the vetted_package transactions in the authorized store are the same as in the domain store.
    */
  override protected def waitPackagesVetted(timeout: NonNegativeDuration): Unit = {
    val connected = domains.list_connected().map(_.domainId).toSet

    // for every participant
    consoleEnvironment.participantsX.all
      .filter(p => p.health.running() && p.health.initialized())
      .foreach { participant =>
        // for every domain this participant is connected to as well
        participant.domains.list_connected().foreach {
          case item if connected.contains(item.domainId) =>
            ConsoleMacros.utils.retry_until_true(timeout)(
              {
                // ensure that vetted packages on the domain match the ones in the authorized store
                val onDomain = participant.topology.vetted_packages
                  .list(
                    filterStore = item.domainId.filterString,
                    filterParticipant = id.filterString,
                  )
                  .flatMap(_.item.packageIds)
                  .toSet

                // Vetted packages from the participant's authorized store
                val onParticipantAuthorizedStore = topology.vetted_packages
                  .list(filterStore = "Authorized", filterParticipant = id.filterString)
                  .filter(_.item.domainId.forall(_ == item.domainId))
                  .flatMap(_.item.packageIds)
                  .toSet

                val ret = onParticipantAuthorizedStore == onDomain
                if (!ret) {
                  logger.debug(
                    show"Still waiting for package vetting updates to be observed by Participant ${participant.name} on ${item.domainId}: vetted -- onDomain is ${onParticipantAuthorizedStore -- onDomain} while onDomain -- vetted is ${onDomain -- onParticipantAuthorizedStore}"
                  )
                }
                ret
              },
              show"Participant ${participant.name} has not observed all vetting txs of $id on domain ${item.domainId} within the given timeout.",
            )
          case _ =>
        }
      }
  }
  override protected def participantIsActiveOnDomain(
      domainId: DomainId,
      participantId: ParticipantId,
  ): Boolean = topology.domain_trust_certificates.active(domainId, participantId)

}
object ParticipantReferenceX {
  val InstanceType = "ParticipantX"
}

class RemoteParticipantReferenceX(environment: ConsoleEnvironment, override val name: String)
    extends ParticipantReferenceX(environment, name)
    with GrpcRemoteInstanceReference
    with RemoteParticipantReferenceCommon {

  @Help.Summary("Inspect and manage parties")
  @Help.Group("Parties")
  override def parties: ParticipantPartiesAdministrationGroupX = partiesGroup

  // above command needs to be def such that `Help` works.
  lazy private val partiesGroup =
    new ParticipantPartiesAdministrationGroupX(id, this, consoleEnvironment)

  @Help.Summary("Return remote participant config")
  def config: RemoteParticipantConfig =
    consoleEnvironment.environment.config.remoteParticipantsByString(name)

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: RemoteParticipantReferenceX =>
        x.consoleEnvironment == consoleEnvironment && x.name == name
      case _ => false
    }
  }

}

class LocalParticipantReferenceX(
    override val consoleEnvironment: ConsoleEnvironment,
    name: String,
) extends ParticipantReferenceX(consoleEnvironment, name)
    with LocalParticipantReferenceCommon[ParticipantNodeX]
    with LocalInstanceReferenceX {

  override private[console] val nodes = consoleEnvironment.environment.participantsX

  @Help.Summary("Return participant config")
  def config: LocalParticipantConfig =
    consoleEnvironment.environment.config.participantsByString(name)

  override def runningNode: Option[ParticipantNodeBootstrapX] =
    consoleEnvironment.environment.participantsX.getRunning(name)

  override def startingNode: Option[ParticipantNodeBootstrapX] =
    consoleEnvironment.environment.participantsX.getStarting(name)

  /** secret, not publicly documented way to get the admin token */
  def adminToken: Option[String] = underlying.map(_.adminToken.secret)

  // TODO(#14048) these are "remote" groups. the normal participant node has "local" versions.
  //   but rather than keeping this, we should make local == remote and add local methods separately
  @Help.Summary("Inspect and manage parties")
  @Help.Group("Parties")
  def parties: LocalParticipantPartiesAdministrationGroupX = partiesGroup
  // above command needs to be def such that `Help` works.
  lazy private val partiesGroup =
    new LocalParticipantPartiesAdministrationGroupX(this, this, consoleEnvironment, loggerFactory)

  private lazy val testing_ =
    new LocalParticipantTestingGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Commands used for development and testing", FeatureFlag.Testing)
  @Help.Group("Testing")
  override def testing: LocalParticipantTestingGroup = testing_

  private lazy val commitments_ =
    new LocalCommitmentsAdministrationGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Commands to inspect and extract bilateral commitments", FeatureFlag.Preview)
  @Help.Group("Commitments")
  def commitments: LocalCommitmentsAdministrationGroup = commitments_

  private lazy val repair_ =
    new LocalParticipantRepairAdministration(consoleEnvironment, this, loggerFactory) {
      override protected def access[T](handler: ParticipantNodeCommon => T): T =
        LocalParticipantReferenceX.this.access(handler)
    }

  @Help.Summary("Commands to repair the local participant contract state", FeatureFlag.Repair)
  @Help.Group("Repair")
  def repair: LocalParticipantRepairAdministration = repair_
}

trait SequencerNodeReferenceCommon
    extends InstanceReferenceCommon
    with InstanceReferenceWithSequencerConnection {

  override type Status = SequencerNodeStatus

  @Help.Summary(
    "Yields the globally unique id of this sequencer. " +
      "Throws an exception, if the id has not yet been allocated (e.g., the sequencer has not yet been started)."
  )
  def id: SequencerId = topology.idHelper(SequencerId(_))

}

object SequencerNodeReferenceX {
  val InstanceType = "SequencerX"
}

abstract class SequencerNodeReferenceX(
    val consoleEnvironment: ConsoleEnvironment,
    name: String,
) extends SequencerNodeReferenceCommon
    with InstanceReferenceX
    with SequencerNodeAdministrationGroupXWithInit {
  self =>

  override protected def runner: AdminCommandRunner = this

  override protected def disable_member(member: Member): Unit =
    repair.disable_member(member)

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: SequencerNodeReferenceX =>
        x.consoleEnvironment == consoleEnvironment && x.name == name
      case _ => false
    }
  }

  override protected val instanceType: String = SequencerNodeReferenceX.InstanceType
  override protected val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory.append("sequencerx", name)

  private lazy val topology_ =
    new TopologyAdministrationGroupX(
      this,
      health.status.successOption.map(_.topologyQueue),
      consoleEnvironment,
      loggerFactory,
    )

  protected def publicApiClient: SequencerPublicApiClient

  override def topology: TopologyAdministrationGroupX = topology_

  private lazy val parties_ = new PartiesAdministrationGroupX(this, consoleEnvironment)

  override def parties: PartiesAdministrationGroupX = parties_

  private val staticDomainParameters: AtomicReference[Option[ConsoleStaticDomainParameters]] =
    new AtomicReference[Option[ConsoleStaticDomainParameters]](None)

  private val domainId: AtomicReference[Option[DomainId]] =
    new AtomicReference[Option[DomainId]](None)

  @Help.Summary("Health and diagnostic related commands")
  @Help.Group("Health")
  override def health =
    new HealthAdministrationX[SequencerNodeStatus](
      this,
      consoleEnvironment,
      SequencerNodeStatus.fromProtoV30,
    )

  private lazy val sequencerXTrafficControl = new TrafficControlSequencerAdministrationGroup(
    this,
    topology,
    this,
    consoleEnvironment,
    loggerFactory,
  )

  @Help.Summary("Admin traffic control related commands")
  @Help.Group("Traffic")
  override def traffic_control: TrafficControlSequencerAdministrationGroup =
    sequencerXTrafficControl

  @Help.Summary("Return domain id of the domain")
  def domain_id: DomainId = {
    domainId.get() match {
      case Some(id) => id
      case None =>
        val id = consoleEnvironment.run(
          publicApiClient.publicApiCommand(SequencerPublicCommands.GetDomainId)
        )
        domainId.set(Some(id))

        id
    }
  }

  object mediators {
    object groups {
      @Help.Summary("Propose a new mediator group")
      @Help.Description("""
         group: the mediator group identifier
         threshold: the minimum number of mediators that need to come to a consensus for a message to be sent to other members.
         active: the list of mediators that will take part in the mediator consensus in this mediator group
         observers: the mediators that will receive all messages but will not participate in mediator consensus
         """)
      def propose_new_group(
          group: NonNegativeInt,
          threshold: PositiveInt,
          active: Seq[MediatorReferenceX],
          observers: Seq[MediatorReferenceX] = Nil,
      ): Unit = {

        val domainId = domain_id
        val staticDomainParameters = domain_parameters.static.get()

        val mediators = active ++ observers

        mediators.foreach { mediator =>
          val identityState = mediator.topology.transactions.identity_transactions()

          topology.transactions.load(identityState, domainId.filterString)
        }

        topology.mediators
          .propose(
            domainId = domainId,
            threshold = threshold,
            active = active.map(_.id),
            observers = observers.map(_.id),
            group = group,
          )
          .discard

        mediators.foreach(
          _.setup.assign(
            domainId,
            staticDomainParameters,
            SequencerConnections.single(sequencerConnection),
          )
        )
      }

      @Help.Summary("Propose an update to a mediator group")
      @Help.Description("""
         group: the mediator group identifier
         threshold: the minimum number of mediators that need to come to a consensus for a message to be sent to other members.
         additionalActive: the new mediators that will take part in the mediator consensus in this mediator group
         additionalObservers: the new mediators that will receive all messages but will not participate in mediator consensus
         """)
      def propose_delta(
          group: NonNegativeInt,
          threshold: PositiveInt,
          additionalActive: Seq[MediatorReferenceX],
          additionalObservers: Seq[MediatorReferenceX] = Nil,
      ): Unit = {

        val staticDomainParameters = domain_parameters.static.get()
        val domainId = domain_id

        val currentMediators = topology.mediators
          .list(filterStore = domainId.filterString, group = Some(group))
          .maxByOption(_.context.serial)
          .getOrElse(throw new IllegalArgumentException(s"Unknown mediator group $group"))

        val currentActive = currentMediators.item.active
        val currentObservers = currentMediators.item.observers
        val current = currentActive ++ currentObservers

        val newMediators =
          (additionalActive ++ additionalObservers).filterNot(m => current.contains(m.id))

        newMediators.foreach { med =>
          val identityState = med.topology.transactions.identity_transactions()

          topology.transactions.load(identityState, domainId.filterString)
        }

        topology.mediators
          .propose(
            domainId = domainId,
            threshold = threshold,
            active = (currentActive ++ additionalActive.map(_.id)).distinct,
            observers = (currentObservers ++ additionalObservers.map(_.id)).distinct,
            group = group,
          )
          .discard

        newMediators.foreach(
          _.setup.assign(
            domainId,
            staticDomainParameters,
            SequencerConnections.single(sequencerConnection),
          )
        )
      }
    }
  }

  @Help.Summary("Domain parameters related commands")
  @Help.Group("Domain parameters")
  object domain_parameters {
    object static {
      @Help.Summary("Return static domain parameters of the domain")
      def get(): ConsoleStaticDomainParameters = {
        staticDomainParameters.get() match {
          case Some(parameters) => parameters
          case None =>
            val parameters = consoleEnvironment.run(
              publicApiClient.publicApiCommand(SequencerPublicCommands.GetStaticDomainParameters)
            )

            staticDomainParameters.set(Some(parameters))
            parameters
        }
      }
    }
  }
}

trait LocalSequencerNodeReferenceCommon extends LocalInstanceReferenceCommon {
  this: SequencerNodeReferenceCommon =>

  def config: SequencerNodeConfigCommon

  override lazy val sequencerConnection: GrpcSequencerConnection =
    config.publicApi.toSequencerConnectionConfig.toConnection
      .fold(err => sys.error(s"Sequencer $name has invalid connection config: $err"), identity)
}

class LocalSequencerNodeReferenceX(
    override val consoleEnvironment: ConsoleEnvironment,
    val name: String,
) extends SequencerNodeReferenceX(consoleEnvironment, name)
    with LocalSequencerNodeReferenceCommon
    with LocalInstanceReferenceX
    with BaseInspection[SequencerNodeX] {

  override protected[canton] def executionContext: ExecutionContext =
    consoleEnvironment.environment.executionContext

  @Help.Summary("Returns the sequencerx configuration")
  override def config: SequencerNodeConfigCommon =
    consoleEnvironment.environment.config.sequencersByString(name)

  private[console] val nodes: SequencerNodesX[?] =
    consoleEnvironment.environment.sequencersX

  override protected[console] def runningNode: Option[SequencerNodeBootstrapX] =
    nodes.getRunning(name)

  override protected[console] def startingNode: Option[SequencerNodeBootstrapX] =
    nodes.getStarting(name)

  protected lazy val publicApiClient: SequencerPublicApiClient = new SequencerPublicApiClient(
    sequencerConnection = sequencerConnection,
    trustCollectionFile = config.publicApi.tls.map(_.certChainFile),
  )(consoleEnvironment)
}

trait RemoteSequencerNodeReferenceCommon
    extends SequencerNodeReferenceCommon
    with RemoteInstanceReference {
  def environment: ConsoleEnvironment

  @Help.Summary("Returns the remote sequencer configuration")
  def config: RemoteSequencerConfig

  override def sequencerConnection: GrpcSequencerConnection =
    config.publicApi.toConnection
      .fold(err => sys.error(s"Sequencer $name has invalid connection config: $err"), identity)

  override protected[console] def adminCommand[Result](
      grpcCommand: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] =
    config match {
      case config: RemoteSequencerConfig.Grpc =>
        consoleEnvironment.grpcAdminCommandRunner.runCommand(
          name,
          grpcCommand,
          config.clientAdminApi,
          None,
        )
    }
}

class RemoteSequencerNodeReferenceX(val environment: ConsoleEnvironment, val name: String)
    extends SequencerNodeReferenceX(environment, name)
    with RemoteSequencerNodeReferenceCommon {

  override protected[canton] def executionContext: ExecutionContext =
    consoleEnvironment.environment.executionContext

  @Help.Summary("Returns the sequencerx configuration")
  override def config: RemoteSequencerConfig =
    environment.environment.config.remoteSequencersByString(name)

  protected lazy val publicApiClient: SequencerPublicApiClient = new SequencerPublicApiClient(
    sequencerConnection = sequencerConnection,
    trustCollectionFile = config.publicApi.customTrustCertificates.map(_.pemFile),
  )(consoleEnvironment)
}

trait MediatorReferenceCommon extends InstanceReferenceCommon {

  @Help.Summary(
    "Yields the mediator id of this mediator. " +
      "Throws an exception, if the id has not yet been allocated (e.g., the mediator has not yet been initialised)."
  )
  def id: MediatorId = topology.idHelper(MediatorId(_))

  override type Status = MediatorNodeStatus

}

object MediatorReferenceX {
  val InstanceType = "MediatorX"
}

abstract class MediatorReferenceX(val consoleEnvironment: ConsoleEnvironment, name: String)
    extends MediatorReferenceCommon
    with MediatorXAdministrationGroupWithInit
    with InstanceReferenceX {

  override protected def runner: AdminCommandRunner = this

  override protected val instanceType: String = MediatorReferenceX.InstanceType
  override protected val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory
      .append(MediatorNodeBootstrapX.LoggerFactoryKeyName, name)

  @Help.Summary("Health and diagnostic related commands")
  @Help.Group("Health")
  override def health =
    new HealthAdministrationX[MediatorNodeStatus](
      this,
      consoleEnvironment,
      MediatorNodeStatus.fromProtoV30,
    )

  private lazy val topology_ =
    new TopologyAdministrationGroupX(
      this,
      health.status.successOption.map(_.topologyQueue),
      consoleEnvironment,
      loggerFactory,
    )

  override def topology: TopologyAdministrationGroupX = topology_

  private lazy val parties_ = new PartiesAdministrationGroupX(this, consoleEnvironment)

  override def parties: PartiesAdministrationGroupX = parties_

  override def equals(obj: Any): Boolean =
    obj match {
      case x: MediatorReferenceX => x.consoleEnvironment == consoleEnvironment && x.name == name
      case _ => false
    }
}

class LocalMediatorReferenceX(consoleEnvironment: ConsoleEnvironment, val name: String)
    extends MediatorReferenceX(consoleEnvironment, name)
    with LocalInstanceReferenceX
    with SequencerConnectionAdministration
    with BaseInspection[MediatorNodeX] {

  override protected[canton] def executionContext: ExecutionContext =
    consoleEnvironment.environment.executionContext

  @Help.Summary("Returns the mediator-x configuration")
  override def config: MediatorNodeConfigCommon =
    consoleEnvironment.environment.config.mediatorsByString(name)

  private[console] val nodes: MediatorNodesX[?] = consoleEnvironment.environment.mediatorsX

  override protected[console] def runningNode: Option[MediatorNodeBootstrapX] =
    nodes.getRunning(name)

  override protected[console] def startingNode: Option[MediatorNodeBootstrapX] =
    nodes.getStarting(name)
}

class RemoteMediatorReferenceX(val environment: ConsoleEnvironment, val name: String)
    extends MediatorReferenceX(environment, name)
    with GrpcRemoteInstanceReference {

  @Help.Summary("Returns the remote mediator configuration")
  def config: RemoteMediatorConfig =
    environment.environment.config.remoteMediatorsByString(name)

  override protected[canton] def executionContext: ExecutionContext =
    consoleEnvironment.environment.executionContext
}
