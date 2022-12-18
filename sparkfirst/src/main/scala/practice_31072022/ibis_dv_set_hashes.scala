package practice_31072022
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object ibis_dv_set_hashes {

  def main(args: Array[String]): Unit = {

    //Requirement
    /*  We have data known as datatxns.txt
  Read this data
  Iterate each element filter row contains Gymnastics word*/

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("C:///Users/002NAM744/Documents/Cloudera_13-03-2022/Data/ibis_dv_sat_hashes.txt")
    println("============raw data===========")
    data.foreach(println)

    println("=========display process data===========")
    val fildata = data.filter(x => x.contains(" AS VARCHAR ),'~#$#~')|| '|'|| IFNULL(CAST("))
    fildata.foreach(println)

    val repdata = data.map(x => x.replace(" AS VARCHAR ),'~#$#~')|| '|'|| IFNULL(CAST(", ", "))
    repdata.foreach(println)

    val repdata1 = repdata.map(x => x.replace(" AS VARCHAR ),'~#$#~'))", " "))
    repdata1.foreach(println)

    println("=========Final process data===========")
    val repdata2 = repdata1.map(x => x.replace(".sql:HASH(IFNULL(CAST(", " : "))
    repdata2.foreach(println)

    println("=========Load data===========")
    repdata2.coalesce(1).saveAsTextFile("C:///Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Proc_ibis_dv_sat_hashes_new_Load")
    println
    println("=========Proc Load data completed===========")
    println
    //Additional Data process table name and column name
    val repdata3 = repdata2.map(x => x.replace("ibis__dv__", ""))
    repdata2.foreach(println)
    val repdata4 = repdata3.map(x => x.replace("_s_h_", ": s_h_"))
    repdata4.foreach(println)
    repdata4.coalesce(1).saveAsTextFile("C:///Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Final_Proc1_ibis_dv_sat_hashes_new_Load")
    println
    println("=========Proc Final Load data completed===========")
    println
  
  }
}
