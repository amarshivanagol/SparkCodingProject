package pack__Test_Practice_10_09_2022
import org.apache.spark.SparkContext

object pack_Test_Practice_10_09_2022 {

  def main(args: Array[String]): Unit = {
    /*    val sc = new SparkContext()
    val df = sc.textFile("C:\\Users\\002NAM744\\Documents\\Cloudera_13-03-2022\\Data\\datatxns")
    println(df)*/

    /*    Add the spark-core.jar to call SparkContext class
    for RDD Processing*/
    //    import SparkSession
    //    Assign Power (sc)
    //    read the sc and do the processing

    //    To process data using Sprk -- we have to use its methods
    //    Those methods sitting inside Class (SparkContext,SparkSession)
    //    Those Classes are sitting Inside Jars(Spark-core, Spark-sql)
    //    Thats why we added those spark jars

    //    Requirement:
    //    Iterate each element of the list and filter number > 2 and save it in to new list
    //
    println("==================raw list===================")
    val lsin = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    lsin.foreach(println)
    //To process the list of elements Once upon a time its use to very Tough
    //Later, all the langauage (Java, scala, python) Got a very good support to process list elements that would help spark also
    println("==================new list===================")
    val newlis = lsin.filter(x => x > 2)
    newlis.foreach(println)
  }

}