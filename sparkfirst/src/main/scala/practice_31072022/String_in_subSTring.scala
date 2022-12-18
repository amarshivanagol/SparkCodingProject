

package practice_31072022
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import scala.io.Source
import spray.json._
import org.apache.spark.sql.catalyst.expressions.DateAdd
object String_in_subSTring {

  /*  Using spark Read the format csv with some options and load the data*/

  def isSubSequence(str1: String, str2: String, m: Int, n: Int): Boolean = {

    if (m == 0) {
      return true
    } else if (n == 0) {
      return false
    } else if (str1(m - 1) == str2(n - 1)) {
      return isSubSequence(str1, str2, m - 1, n - 1)
    } else
      return isSubSequence(str1, str2, m, n - 1)
  }

  def main(args: Array[String]) {
    var str1 = "gks"
    var str2 = "geeksforgeeks"
    if (isSubSequence(str1, str2, str1.length, str2.length)) {
      print("yes")
    } else {
      print("No")
    }
  }
}