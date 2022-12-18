package practice_31072022

object map_replace {

  def main(args: Array[String]): Unit = {

    println("===========Raw data============")
    val list = List("Zeyobron", "Zeyo", "aZeyobron", "bron", "analytics")
    list.foreach(println)
    val fillist = list.map(x => x.replace("Zeyo", "tera"))
    println("===========Filter data============")
    fillist.foreach(println)
    println
    //Iterate each element add ,Amar at the end

    println("===========Raw data============")
    val list1 = List("Zeyobron", "Zeyo", "aZeyobron", "bron", "analytics")
    list.foreach(println)
    val fillist1 = list1.map(x => x.concat(",Amar"))
    println("===========Filter data============")
    fillist1.foreach(println)
    println

    //Iterate each element add ,Prefix and suffix at the end

    println("===========Raw data============")
    val list2 = List("Zeyobron", "Zeyo", "aZeyobron", "bron", "analytics")
    list.foreach(println)
    val fillist2 = list2.map(x => " Amar, " + x + " ,Shivangol ")
    println("===========Filter data============")
    fillist2.foreach(println)
    println

  }
}
