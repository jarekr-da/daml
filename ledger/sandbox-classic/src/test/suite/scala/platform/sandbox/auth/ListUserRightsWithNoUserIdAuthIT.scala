// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.user_management_service._
import org.scalatest.Assertion

import scala.concurrent.Future

class ListUserRightsWithNoUserIdAuthIT extends ServiceCallAuthTests {
  override def serviceCallName: String = "UserManagementService#ListUserRights(<no-user-id>)"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(UserManagementServiceGrpc.stub(channel), token).listUserRights(ListUserRightsRequest())

  protected def expectRights(
      token: Option[String],
      expectedRights: Vector[Right],
  ): Future[Assertion] =
    serviceCallWithToken(token).map(assertResult(ListUserRightsResponse(expectedRights))(_))

  behavior of serviceCallName

  it should "deny unauthenticated access" in {
    expectUnauthenticated(serviceCallWithToken(None))
  }

  it should "deny access for a standard token referring to an unknown user" in {
    expectUnauthenticated(serviceCallWithToken(canReadAsUnknownUserStandardJWT))
  }

  it should "return rights of the 'participant_admin' when using its standard token" in {
    expectRights(
      canReadAsAdminStandardJWT,
      Vector(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))),
    )
  }

  it should "return invalid argument for custom token" in {
    expectInvalidArgument(serviceCallWithToken(canReadAsAdmin))
  }

}
