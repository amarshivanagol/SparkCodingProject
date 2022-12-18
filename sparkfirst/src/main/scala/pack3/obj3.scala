package pack3

object obj3 {

  def main(args: Array[String]): Unit = {

    val lisstr = List(
      "State- >TamilNadu",
      "City- >Chennai",
      "State- > Kerala",
      "City- > Trivandrum")

    val states = lisstr.filter(x => x.contains("State"))
    println(" === states === ")
    states.foreach(println)

    val city = lisstr.filter(x => x.contains("City"))
    println(" === cities === ")
    city.foreach(println)

  }
}

