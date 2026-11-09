package ledger.processor.fraud

import cats.kernel.Eq
import cats.kernel.laws.discipline.CommutativeGroupTests
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

/**
 * Checks with thousands of random values that WindowStats follows the group rules
 * (associative, has a zero, has an inverse, order doesn't matter). The sliding window
 * only gives correct totals if these hold.
 */
class WindowStatsLawsSpec extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  implicit val arbStats: Arbitrary[WindowStats] = Arbitrary(for {
    count <- Gen.choose(-10000L, 10000L)
    total <- Gen.choose(-1000000000L, 1000000000L)
  } yield WindowStats(count, total))

  implicit val eqStats: Eq[WindowStats] = Eq.fromUniversalEquals

  checkAll("WindowStats", CommutativeGroupTests[WindowStats].commutativeGroup)
}
