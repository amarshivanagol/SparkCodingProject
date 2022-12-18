package practice_31072022

object flatmap {
  def main(args: Array[String]): Unit = {
    println("===========Started===========")
    println("===========raw list===========")
    val liststr = List(
      "A~B",
      "C~D~E~D~E~D~E~D~E~D~E~D~E~D~E~D~E~D~E")
    liststr.foreach(println)
    println("===========flatten data=============")
    val flatdata = liststr.flatMap(x => x.split("~"))
    flatdata.foreach(println)
    println

    println("===========Started===========")
    println("===========raw list===========")
    val liststr1 = List(
      "Amar,Shivangol",
      "Zeyobron,analytics,")
    liststr.foreach(println)
    println("===========flatten data=============")
    val flatdata1 = liststr1.flatMap(x => x.split(","))
    flatdata1.foreach(println)

  }
}