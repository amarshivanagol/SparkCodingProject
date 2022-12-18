package pack3

object obj6 {
  def main(args: Array[String]): Unit = {

    //flatMap
    //Split the element and separate with comma ,"

    println(" === Started =====")

    println(" ===== raw list ==== ")

    val lisstr = List(
      "State->TN~City->Chennai",
      "State->Gujarat~City->GandhiNagar")

    lisstr.foreach(println)

    println(" === flat data=== ")

    val flatdata = lisstr.flatMap(x => x.split("~"))
    flatdata.foreach(println)

    println(" ==== filter data =====")

    val states = flatdata.filter(x => x.contains("State"))

    val cities = flatdata.filter(x => x.contains("City"))

    println("======states========")

    states.foreach(println)

    println("======cities=======")

    cities.foreach(println)

    println(" =======finalstates =====  ")

    val finalstate = states.map(x => x.replace("State->", ""))
    finalstate.foreach(println)

    println(" =======finalcities =====  ")

    val finalcities = cities.map(x => x.replace("City->", ""))

    finalcities.foreach(println)


  }
}