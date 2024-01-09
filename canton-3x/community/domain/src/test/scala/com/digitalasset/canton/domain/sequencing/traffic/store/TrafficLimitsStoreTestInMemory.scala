// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficLimitsStore
import org.scalatest.wordspec.AsyncWordSpec

class TrafficLimitsStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with TrafficLimitsStoreTest {
  "InMemoryTrafficLimitsStore" should {
    behave like trafficLimitsStore(() => new InMemoryTrafficLimitsStore(loggerFactory))
  }
}