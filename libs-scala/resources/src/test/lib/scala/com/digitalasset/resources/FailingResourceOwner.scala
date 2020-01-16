// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import scala.concurrent.Future

object FailingResourceOwner {
  def apply[T](): ResourceOwner[T] =
    new TestResourceOwner[T](
      Future.failed(new FailingResourceFailedToOpen),
      _ => Future.failed(new TriedToReleaseAFailedResource),
    )

  final class FailingResourceFailedToOpen extends Exception("Something broke!")

  final class TriedToReleaseAFailedResource extends Exception("Tried to release a failed resource.")
}
