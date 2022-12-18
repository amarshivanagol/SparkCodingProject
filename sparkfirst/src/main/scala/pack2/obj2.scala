package pack2

object obj2 {
  
    def main(args: Array[String]): Unit = {
      
      println("======started=======")
      val liststr = List("zeyobron","zeyo","analytics","azeyobron")
      
      println("=== raw list ===")
      liststr.foreach(println)
      
      println("=== filter list str ===")
      val filstr = liststr.filter( x => x.contains("zeyo") )
      filstr.foreach(println)
      
      
    }

}

