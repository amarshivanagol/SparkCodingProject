package practice_31072022

object usecase {
  def main(args: Array[String]): Unit = {
    println("==========started===============")
    println("==========Raw data==============")
    val liststr = List(
      "State->TN~City->Chennai",
      "State->Gujarat~City->GandhiNagar")
    liststr.foreach(println)
    println
    val splitlist = liststr.flatMap(x => x.split("~"))
    val filstate = splitlist.filter(x => x.contains("State"))
    val filcity = splitlist.filter(x => x.contains("City"))
    println("==========Flatten data State==============")
    filstate.foreach(println)
    println
    println("==========Flatten data City==============")
    filcity.foreach(println)
    println
    println("==========Replace Final data State==============")
    val repstate = filstate.map(x => x.replace("State->", ""))
    repstate.foreach(println)
    println
    println("==========Replace Final data City==============")
    val repcity = filcity.map(x => x.replace("City->", ""))
    repcity.foreach(println)
    println
    //val state = List("TN","Gujarat")
    //val city = List("Chennai","GandhiNagar")
  }
}