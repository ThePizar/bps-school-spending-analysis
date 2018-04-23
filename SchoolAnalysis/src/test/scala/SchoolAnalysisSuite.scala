package schoolanalysis

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

@RunWith(classOf[JUnitRunner])
class SchoolAnalysisSuite extends FunSuite with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    import SchoolAnalysis._
    sc.stop()
  }

  test("Simple split") {
    import SchoolAnalysis._

    val sample = "a,b,c,d"
    assert(splitRow(sample) === Array("a", "b", "c", "d"))
  }

  test("Split with one quote") {
    import SchoolAnalysis._

    val sample = "a,b,\"c, d\",e"
    assert(splitRow(sample) === Array("a", "b", "\"c, d\"", "e"))
  }
}
