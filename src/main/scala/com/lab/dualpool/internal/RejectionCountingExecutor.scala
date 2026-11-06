package com.lab.dualpool.internal

import slick.util.AsyncExecutor
import slick.util.AsyncExecutor.{PrioritizedRunnable, Priority}

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.LongAdder
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
 * Wraps an executor so the work its bounded queue refuses is counted, which is what
 * `slick_executor_rejected_total` reports. Everything else passes straight through.
 */
private[dualpool] object RejectionCountingExecutor {

  def apply(delegate: AsyncExecutor, rejections: LongAdder): AsyncExecutor = new AsyncExecutor {

    lazy val executionContext: ExecutionContext = {
      val inner = delegate.executionContext
      new ExecutionContextExecutor {
        def execute(runnable: Runnable): Unit =
          try inner.execute(runnable)
          catch {
            case e: RejectedExecutionException =>
              rejections.increment()
              throw e
          }

        def reportFailure(cause: Throwable): Unit = inner.reportFailure(cause)
      }
    }

    override def prioritizedRunnable(priority: => Priority,
                                     run: PrioritizedRunnable.SetConnectionReleased => Unit): PrioritizedRunnable =
      delegate.prioritizedRunnable(priority, run)

    def close(): Unit = delegate.close()
  }
}
