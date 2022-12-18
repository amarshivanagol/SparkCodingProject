package pack3

object obj4 {

  def main(args: Array[String]): Unit = {

    println(" ===== raw list ==== ")

    val lisstr = List(

      "zeyobron",
      "azeyobron",
      "analytics",
      "zeyo")

    lisstr.foreach(println)

    println(" ===== map data =====")

    val replacedata = lisstr.map(x => x.replace("zeyo", "tera"))

    replacedata.foreach(println)

  }
}

