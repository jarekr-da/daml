// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.daml.error.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt, PositiveLong}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.TopologyManagementErrorGroup.TopologyManagerErrorGroup
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol.OnboardingRestriction
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  GenericTopologyTransaction,
  TxHash,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.value.Value.ContractId

sealed trait TopologyManagerError extends CantonError

object TopologyManagerError extends TopologyManagerErrorGroup {

  @Explanation(
    """This error indicates that there was an internal error within the topology manager."""
  )
  @Resolution("Inspect error message for details.")
  object InternalError
      extends ErrorCode(
        id = "TOPOLOGY_MANAGER_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    final case class ImplementMe(msg: String = "")(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "TODO(#14048) implement me" + (if (msg.nonEmpty) s": $msg" else "")
        )
        with TopologyManagerError

    final case class Other(s: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"TODO(#14048) other failure: $s"
        )
        with TopologyManagerError

    final case class TopologySigningError(error: SigningError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Creating a signed transaction failed due to a crypto error"
        )
        with TopologyManagerError
  }

  @Explanation("""The topology manager has received a malformed message from another node.""")
  @Resolution("Inspect the error message for details.")
  object TopologyManagerAlarm extends AlarmErrorCode(id = "TOPOLOGY_MANAGER_ALARM") {
    final case class Warn(override val cause: String)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(cause)
        with TopologyManagerError {
      override def logOnCreation: Boolean = false
    }
  }

  @Explanation("This error indicates that a topology transaction could not be found.")
  @Resolution(
    "The topology transaction either has been rejected, is not valid anymore, is not yet valid, or does not yet exist."
  )
  object TopologyTransactionNotFound
      extends ErrorCode(
        id = "TOPOLOGY_TRANSACTION_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(txHash: TxHash, effective: EffectiveTime)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Topology transaction with hash $txHash does not exist or is not active or is not an active proposal at $effective"
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that more than one topology transaction with the same transaction hash was found."
  )
  @Resolution(
    """Inspect the topology state for consistency and take corrective measures before retrying.
      |The metadata details of this error contains the transactions in the field ``transactions``."""
  )
  object TooManyTransactionsWithHash
      extends ErrorCode(
        "TOPOLOGY_TOO_MANY_TRANSACTION_WITH_HASH",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(
        txHash: TxHash,
        effectiveTime: EffectiveTime,
        transactions: Seq[GenericSignedTopologyTransaction],
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Found more than one topology transaction with hash $txHash at $effectiveTime: $transactions"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the secret key with the respective fingerprint can not be found."""
  )
  @Resolution(
    "Ensure you only use fingerprints of secret keys stored in your secret key store."
  )
  object SecretKeyNotInStore
      extends ErrorCode(
        id = "SECRET_KEY_NOT_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(keyId: Fingerprint)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Secret key with given fingerprint could not be found"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a command contained a fingerprint referring to a public key not being present in the public key store."""
  )
  @Resolution(
    "Upload the public key to the public key store using $node.keys.public.load(.) before retrying."
  )
  object PublicKeyNotInStore
      extends ErrorCode(
        id = "PUBLIC_KEY_NOT_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(keyId: Fingerprint)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Public key with given fingerprint is missing in the public key store"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the uploaded signed transaction contained an invalid signature."""
  )
  @Resolution(
    "Ensure that the transaction is valid and uses a crypto version understood by this participant."
  )
  object InvalidSignatureError extends AlarmErrorCode(id = "INVALID_TOPOLOGY_TX_SIGNATURE_ERROR") {

    final case class Failure(error: SignatureCheckError)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(cause = "Transaction signature verification failed")
        with TopologyManagerError

    final case class KeyStoreFailure(error: CryptoPrivateStoreError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Creating a signed transaction failed due to a key store error: $error"
        )
        with TopologyManagerError
  }

  object SerialMismatch
      extends ErrorCode(id = "SERIAL_MISMATCH", ErrorCategory.InvalidGivenCurrentSystemStateOther) {
    final case class Failure(expected: PositiveInt, actual: PositiveInt)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The given serial $actual did not match the expected serial $expected."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error is returned if a transaction was submitted that is restricted to another domain."""
  )
  @Resolution(
    """Recreate the content of the transaction with a correct domain identifier."""
  )
  object InvalidDomain
      extends ErrorCode(id = "INVALID_DOMAIN", ErrorCategory.InvalidIndependentOfSystemState) {
    final case class Failure(invalid: DomainId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Invalid domain $invalid"
        )
        with TopologyManagerError

    final case class InvalidFilterStore(filterStore: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"No domain store found for the filter store provided: $filterStore"
        )
        with TopologyManagerError

    final case class MultipleDomainStores(filterStore: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Multiple domain stores found for the filter store provided: $filterStore. Specify the entire domainID to avoid ambiguity."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a transaction has already been added previously."""
  )
  @Resolution(
    """Nothing to do as the transaction is already registered. Note however that a revocation is " +
    final. If you want to re-enable a statement, you need to re-issue an new transaction."""
  )
  object DuplicateTransaction
      extends ErrorCode(
        id = "DUPLICATE_TOPOLOGY_TRANSACTION",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Failure(
        transaction: GenericTopologyTransaction,
        authKey: Fingerprint,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The given topology transaction already exists."
        )
        with TopologyManagerError

    final case class ExistsAt(ts: CantonTimestamp)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The given topology transaction already exists at $ts."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a topology transaction would create a state that already exists and has been authorized with the same key."""
  )
  @Resolution("""Your intended change is already in effect.""")
  object MappingAlreadyExists
      extends ErrorCode(
        id = "TOPOLOGY_MAPPING_ALREADY_EXISTS",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Failure(existing: TopologyMapping, keys: NonEmpty[Set[Fingerprint]])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "A matching topology mapping x authorized with the same keys already exists in this state"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error results if the topology manager did not find a secret key in its store to authorize a certain topology transaction."""
  )
  @Resolution("""Inspect your topology transaction and your secret key store and check that you have the
      appropriate certificates and keys to issue the desired topology transaction. If the list of candidates is empty,
      then you are missing the certificates.""")
  object NoAppropriateSigningKeyInStore
      extends ErrorCode(
        id = "NO_APPROPRIATE_SIGNING_KEY_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(candidates: Seq[Fingerprint])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Could not find an appropriate signing key to issue the topology transaction"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempt to add a transaction was rejected, as the signing key is not authorized within the current state."""
  )
  @Resolution(
    """Inspect the topology state and ensure that valid namespace or identifier delegations of the signing key exist or upload them before adding this transaction."""
  )
  object UnauthorizedTransaction extends AlarmErrorCode(id = "UNAUTHORIZED_TOPOLOGY_TRANSACTION") {

    final case class Failure(reason: String)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(cause = s"Topology transaction is not properly authorized: $reason")
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempt to add a removal transaction was rejected, as the mapping affecting the removal did not exist."""
  )
  @Resolution(
    """Inspect the topology state and ensure the mapping of the active transaction you are trying to revoke matches your revocation arguments."""
  )
  object NoCorrespondingActiveTxToRevoke
      extends ErrorCode(
        id = "NO_CORRESPONDING_ACTIVE_TX_TO_REVOKE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Mapping(mapping: TopologyMapping)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "There is no active topology transaction matching the mapping of the revocation request"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempted key removal would remove the last valid key of the given entity, making the node unusable."""
  )
  @Resolution(
    """Add the `force = true` flag to your command if you are really sure what you are doing."""
  )
  object RemovingLastKeyMustBeForced
      extends ErrorCode(
        id = "REMOVING_LAST_KEY_MUST_BE_FORCED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(key: Fingerprint, purpose: KeyPurpose)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Topology transaction would remove the last key of the given entity"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a dangerous command that could break a node was rejected.
      |This is the case if a dangerous command is run on a node to issue transactions for another node.
      |Such commands must be run with force, as they are very dangerous and could easily break
      |the node.
      |As an example, if we assign an encryption key to a participant that the participant does not
      |have, then the participant will be unable to process an incoming transaction. Therefore we must
      |be very careful to not create such situations.
      | """
  )
  @Resolution("Set the ForceFlag.AlienMember if you really know what you are doing.")
  object DangerousCommandRequiresForce
      extends ErrorCode(
        id = "DANGEROUS_COMMAND_REQUIRES_FORCE_ALIEN_MEMBER",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class AlienMember(member: Member, topologyMapping: TopologyMapping.Code)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Issuing $topologyMapping for alien members requires ForceFlag.AlienMember"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempted key removal would create dangling topology transactions, making the node unusable."""
  )
  @Resolution(
    """Add the `force = true` flag to your command if you are really sure what you are doing."""
  )
  object RemovingKeyWithDanglingTransactionsMustBeForced
      extends ErrorCode(
        id = "REMOVING_KEY_DANGLING_TRANSACTIONS_MUST_BE_FORCED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(key: Fingerprint, purpose: KeyPurpose)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "Topology transaction would remove a key that creates conflicts and dangling transactions"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that it has been attempted to increase the ``ledgerTimeRecordTimeTolerance`` domain parameter in an insecure manner.
      |Increasing this parameter may disable security checks and can therefore be a security risk.
      |"""
  )
  @Resolution(
    """Make sure that the new value of ``ledgerTimeRecordTimeTolerance`` is at most half of the ``mediatorDeduplicationTimeout`` domain parameter.
      |
      |Use ``myDomain.service.set_ledger_time_record_time_tolerance`` for securely increasing ledgerTimeRecordTimeTolerance.
      |
      |Alternatively, add the flag ``ForceFlag.LedgerTimeRecordTimeToleranceIncrease`` to your command, if security is not a concern for you.
      |The security checks will be effective again after twice the new value of ``ledgerTimeRecordTimeTolerance``.
      |Using ``ForceFlag.LedgerTimeRecordTimeToleranceIncrease`` is safe upon domain bootstrapping.
      |"""
  )
  object IncreaseOfLedgerTimeRecordTimeTolerance
      extends ErrorCode(
        id = "INCREASE_OF_LEDGER_TIME_RECORD_TIME_TOLERANCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class TemporarilyInsecure(
        oldValue: NonNegativeFiniteDuration,
        newValue: NonNegativeFiniteDuration,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The parameter ledgerTimeRecordTimeTolerance can currently not be increased to $newValue."
        )
        with TopologyManagerError

    final case class PermanentlyInsecure(
        newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
        mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Unable to increase ledgerTimeRecordTimeTolerance to $newLedgerTimeRecordTimeTolerance, because it must not be more than half of mediatorDeduplicationTimeout ($mediatorDeduplicationTimeout)."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that the attempted update of the extra traffic limits for a particular member failed because the new limit is lower than the current limit."
  )
  @Resolution(
    """Extra traffic limits can only be increased. Submit the topology transaction with a higher limit.
      |The metadata details of this error contain the expected minimum value in the field ``expectedMinimum``."""
  )
  object InvalidTrafficLimit
      extends ErrorCode(
        id = "INVALID_TRAFFIC_LIMIT",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class TrafficLimitTooLow(
        member: Member,
        actual: PositiveLong,
        expectedMinimum: PositiveLong,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The extra traffic limit for $member should be at least $expectedMinimum, but was $actual."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that members referenced in a topology transaction have not declared at least one signing key or at least 1 encryption key or both."
  )
  @Resolution(
    """Ensure that all members referenced in the topology transaction have declared at least one signing key and at least one encryption key, then resubmit the failed transaction.
      |The metadata details of this error contain the members with the missing keys in the field ``members``."""
  )
  object InsufficientKeys
      extends ErrorCode(
        id = "INSUFFICIENT_KEYS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(members: Seq[Member])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Members ${members.sorted.mkString(", ")} are missing a signing key or an encryption key or both."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that the topology transaction references members that are currently unknown."
  )
  @Resolution(
    """Wait for the onboarding of the members to be become active or remove the unknown members from the topology transaction.
      |The metadata details of this error contain the unknown member in the field ``members``."""
  )
  object UnknownMembers
      extends ErrorCode(
        id = "UNKNOWN_MEMBERS",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(members: Seq[Member])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Members ${members.sorted.mkString(", ")} are unknown."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that the topology transaction references parties that are currently unknown."
  )
  @Resolution(
    """Wait for the onboarding of the parties to be become active or remove the unknown parties from the topology transaction.
      |The metadata details of this error contain the unknown parties in the field ``parties``."""
  )
  object UnknownParties
      extends ErrorCode(
        id = "UNKNOWN_PARTIES",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(parties: Seq[PartyId])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Parties ${parties.sorted.mkString(", ")} are unknown."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a participant is trying to rescind their domain trust certificate
      |while still being hosting parties."""
  )
  @Resolution(
    """The participant should work with the owners of the parties mentioned in the ``parties`` field in the
      |error details metadata to get itself removed from the list of hosting participants of those parties."""
  )
  object IllegalRemovalOfDomainTrustCertificate
      extends ErrorCode(
        id = "ILLEGAL_REMOVAL_OF_DOMAIN_TRUST_CERTIFICATE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class ParticipantStillHostsParties(
        participantId: ParticipantId,
        parties: Seq[PartyId],
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Cannot remove domain trust certificate for $participantId because it still hosts parties ${parties.sorted
                .mkString(",")}"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a participant was not able to onboard to a domain because onboarding restrictions are in place."""
  )
  @Resolution(
    """Verify the onboarding restrictions of the domain. If the domain is not locked, then the participant needs first to be put on the allow list by issuing a ParticipantDomainPermission transaction."""
  )
  object ParticipantOnboardingRefused
      extends ErrorCode(
        id = "PARTICIPANT_ONBOARDING_REFUSED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(
        participantId: ParticipantId,
        restriction: OnboardingRestriction,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The $participantId can not join the domain because onboarding restrictions are in place"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the submitted topology mapping was invalid."""
  )
  @Resolution(
    """The participant should work with the owners of the parties mentioned in the ``parties`` field in the
      |error details metadata to get itself removed from the list of hosting participants of those parties."""
  )
  object InvalidTopologyMapping
      extends ErrorCode(
        id = "INVALID_TOPOLOGY_MAPPING",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Reject(
        description: String
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The topology transaction was rejected due to an invalid mapping: $description"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the removal of a topology mapping also changed the content compared
      |to the mapping with the previous serial."""
  )
  @Resolution(
    "Submit the transaction with the same topology mapping as the previous serial, but with the operation REMOVE."
  )
  object RemoveMustNotChangeMapping
      extends ErrorCode(
        id = "TOPOLOGY_REMOVE_MUST_NOT_CHANGE_MAPPING",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(actual: TopologyMapping, expected: TopologyMapping)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"""REMOVE must not change the topology mapping:
         |actual: $actual
         |expected: $expected""".stripMargin)
        with TopologyManagerError {}
  }

  @Explanation(
    "This error indicates that the submitted topology snapshot was internally inconsistent."
  )
  @Resolution(
    """Inspect the transactions mentioned in the ``transactions`` field in the error details metadata for inconsistencies."""
  )
  object InconsistentTopologySnapshot
      extends ErrorCode(
        id = "INCONSISTENT_TOPOLOGY_SNAPSHOT",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class MultipleEffectiveMappingsPerUniqueKey(
        transactions: Seq[(String, Seq[GenericStoredTopologyTransaction])]
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The topology snapshot was rejected because it was inconsistent."
        )
        with TopologyManagerError
  }

  object MissingTopologyMapping
      extends ErrorCode(
        id = "MISSING_TOPOLOGY_MAPPING",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Reject(
        missingMappings: Map[Member, Seq[TopologyMapping.Code]]
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = {
            val mappingString = missingMappings.toSeq
              .sortBy(_._1)
              .map { case (member, mappings) =>
                s"$member: ${mappings.mkString(", ")}"
              }
              .mkString("; ")
            s"The following members are missing certain topology mappings: $mappingString"
          }
        )
        with TopologyManagerError

    final case class MissingDomainParameters(effectiveTime: EffectiveTime)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Missing domain parameters at $effectiveTime"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a topology transaction attempts to add mediators to multiple mediator groups."""
  )
  @Resolution(
    "Either first remove the mediators from their current groups or choose other mediators to add."
  )
  object MediatorsAlreadyInOtherGroups
      extends ErrorCode(
        id = "MEDIATORS_ALREADY_IN_OTHER_GROUPS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(
        group: NonNegativeInt,
        mediators: Map[MediatorId, NonNegativeInt],
    )(implicit override val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"Tried to add mediators to group $group, but they are already assigned to other groups: ${mediators.toSeq
                .sortBy(_._1.toProtoPrimitive)
                .mkString(", ")}"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the namespace is already used by another entity."""
  )
  @Resolution(
    """Change the namespace used in the submitted topology transaction."""
  )
  object NamespaceAlreadyInUse
      extends ErrorCode(
        id = "NAMESPACE_ALREADY_IN_USE",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Reject(
        namespace: Namespace
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The namespace $namespace is already in use by another entity."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that the partyId to allocate is the same as an already existing admin party."
  )
  @Resolution("Submit the topology transaction with a changed partyId.")
  object PartyIdConflictWithAdminParty
      extends ErrorCode(
        id = "TOPOLOGY_PARTY_ID_CONFLICT_WITH_ADMIN_PARTY",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Reject(partyId: PartyId)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Tried to allocate party $partyId with the same UID as an already existing admin party."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that a participant failed to be onboarded, because it has the same UID as an already existing party."
  )
  @Resolution(
    "Change the identity of the participant by either changing the namespace or the participant's UID and try to onboard to the domain again."
  )
  object ParticipantIdConflictWithPartyId
      extends ErrorCode(
        id = "TOPOLOGY_PARTICIPANT_ID_CONFLICT_WITH_PARTY",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Reject(participantId: ParticipantId, partyId: PartyId)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Tried to onboard participant $participantId while party $partyId with the same UID already exists."
        )
        with TopologyManagerError
  }

  abstract class DomainErrorGroup extends ErrorGroup()

  abstract class ParticipantErrorGroup extends ErrorGroup()

  object ParticipantTopologyManagerError extends ParticipantErrorGroup {
    @Explanation(
      """This error indicates that a dangerous package vetting command was rejected.
        |This is the case when a command is revoking the vetting of a package.
        |Use the force flag to revoke the vetting of a package."""
    )
    @Resolution("Set the ForceFlag.PackageVettingRevocation if you really know what you are doing.")
    object DangerousVettingCommandsRequireForce
        extends ErrorCode(
          id = "DANGEROUS_VETTING_COMMAND_REQUIRES_FORCE_FLAG",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Reject()(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(
            cause = "Revoking a vetted package requires ForceFlag.PackageVettingRevocation"
          )
          with TopologyManagerError
    }

    @Explanation(
      """This error indicates a vetting request failed due to dependencies not being vetted.
        |On every vetting request, the set supplied packages is analysed for dependencies. The
        |system requires that not only the main packages are vetted explicitly but also all dependencies.
        |This is necessary as not all participants are required to have the same packages installed and therefore
        |not every participant can resolve the dependencies implicitly."""
    )
    @Resolution("Vet the dependencies first and then repeat your attempt.")
    object DependenciesNotVetted
        extends ErrorCode(
          id = "DEPENDENCIES_NOT_VETTED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Reject(unvetted: Set[PackageId])(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "Package vetting failed due to dependencies not being vetted"
          )
          with TopologyManagerError
    }

    @Explanation(
      """This error indicates that a package vetting command failed due to packages not existing locally.
        |This can be due to either the packages not being present or their dependencies being missing.
        |When vetting a package, the package must exist on the participant, as otherwise the participant
        |will not be able to process a transaction relying on a particular package."""
    )
    @Resolution(
      "Upload the package locally first before issuing such a transaction."
    )
    object CannotVetDueToMissingPackages
        extends ErrorCode(
          id = "CANNOT_VET_DUE_TO_MISSING_PACKAGES",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      final case class Missing(packages: PackageId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "Package vetting failed due to packages not existing on the local node"
          )
          with TopologyManagerError
    }

    @Resolution(
      s"""To unvet the package id, you must archive all contracts using this package id."""
    )
    object PackageIdInUse
        extends ErrorCode(
          id = "TOPOLOGY_PACKAGE_ID_IN_USE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Reject(used: PackageId, contract: ContractId, domain: DomainId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"Cannot unvet package $used as it is still in use by $contract on domain $domain. " +
                s"It may also be used by contracts on other domains."
          )
          with TopologyManagerError
    }

    @Explanation(
      """This error indicates that a dangerous PartyToParticipant mapping deletion was rejected.
        |If the command is run and there are active contracts where the party is a stakeholder, these contracts
        |will become will never get pruned, resulting in storage that cannot be reclaimed.
        | """
    )
    @Resolution("Set the ForceFlag.PackageVettingRevocation if you really know what you are doing.")
    object DisablePartyWithActiveContractsRequiresForce
        extends ErrorCode(
          id = "TOPOLOGY_DISABLE_PARTY_WITH_ACTIVE_CONTRACTS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Reject(partyId: PartyId, domainId: DomainId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"Disable party $partyId failed because there are active contracts on domain $domainId, on which the party is a stakeholder. " +
                s"It may also have other contracts on other domains. " +
                s"Set the ForceFlag.DisablePartyWithActiveContracts if you really know what you are doing."
          )
          with TopologyManagerError
    }
  }
}
