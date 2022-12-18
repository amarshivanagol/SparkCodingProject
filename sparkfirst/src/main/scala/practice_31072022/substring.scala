package practice_31072022

object substring {
  def main(args: Array[String]): Unit = {

    val list = List("Zeyobron", "Zeyo", "azeyobron", "bron")
    list.foreach(println)
    val fillist = list.filter(x => x.substring(0, 3).equals("Zeyo"))
    println("===========Filter data============")
    fillist.foreach(println)

  }
}