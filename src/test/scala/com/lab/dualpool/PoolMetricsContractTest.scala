package com.lab.dualpool

import org.scalatest.funsuite.AnyFunSuite

/** Contract tests from proposal §5.5 / §7.2. Bodies are placeholders until the library is implemented. */
class PoolMetricsContractTest extends AnyFunSuite {

  test("every pool exposes the same metric set under its own name") { pending }

  test("a pool that has not served a query is still registered") { pending }

  test("saturating the write pool does not change read pool metrics") { pending }
}

class PoolConfigValidatorTest extends AnyFunSuite {

  test("missing readDb section falls back to the write pool") { pending }

  test("numThreads different from maxConnections is reported") { pending }

  test("duplicate or empty pool names are rejected") { pending }

  test("total connections above the database limit fails fast") { pending }
}
