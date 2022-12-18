package practice_31072022

import org.apache.spark._
import sys.process._

object primeNumber {
  def isPrime(i: Int): Boolean = {

    val conf = new SparkConf().setAppName("first").setMaster("Local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    if (i <= 1)
      false
    else if (i == 2)
      true
    else
      !(2 until i).exists(n => i % n == 0)

    (1 to 20).filter(isPrime)
    isPrime(1102)
  }
}