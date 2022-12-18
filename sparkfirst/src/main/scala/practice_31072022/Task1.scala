package practice_31072022

object Task1 {
  def main(args: Array[String]): Unit = {
    println("=============Started================")

    val list = List("Gymnastics", "cash")
    list.foreach(println)
    println
    println("=============Filter================")
    val fillist = list.filter(x => x.contains("Gymnastics"))
    fillist.foreach(println)

  }
}