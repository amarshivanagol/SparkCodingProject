package practice_31072022

object dofunction_map {

  def main(args: Array[String]): Unit = {

    println("===========Raw data============")
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    list.foreach(println)
    val fillist = list.map(x => x * 2)
    println("===========Filter data============")
    fillist.foreach(println)
  }
}
