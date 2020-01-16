// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import java.util.concurrent.atomic.AtomicBoolean

import com.digitalasset.resources.TestCloseable._

final class TestCloseable[T](val value: T, acquired: AtomicBoolean) extends AutoCloseable {
  if (!acquired.compareAndSet(false, true)) {
    throw new TriedToAcquireTwice
  }

  override def close(): Unit = {
    if (!acquired.compareAndSet(true, false)) {
      throw new TriedToReleaseTwice
    }
  }
}

object TestCloseable {
  final class TriedToAcquireTwice extends Exception("Tried to acquire twice.")

  final class TriedToReleaseTwice extends Exception("Tried to release twice.")
}
