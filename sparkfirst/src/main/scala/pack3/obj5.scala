package pack3

object obj5 {
  def main(args: Array[String]): Unit = {
    println(" ===== raw list ==== ")

    val lisstr = List(

      "zeyobron",
      "azeyobron",
      "analytics",
      "zeyo")

    lisstr.foreach(println)

    println(" ===== map data =====")

    val mapdata = lisstr.map(x => "Amar,"  +x + ",Shivangol")

    mapdata.foreach(println)
  }
}