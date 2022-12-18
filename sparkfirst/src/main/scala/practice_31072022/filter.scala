package practice_31072022

object filter {
  def main(args: Array[String]): Unit = {

    println("===========Raw data============")
    val list = List("Zeyobron", "Zeyo", "azeyobron", "bron")
    list.foreach(println)
    val fillist = list.filter(x => x.contains("Zeyo"))
    println("===========Filter data============")
    fillist.foreach(println)
  }
}