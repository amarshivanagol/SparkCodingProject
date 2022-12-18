package inter_questions_skill_test

object inter_questions_skill_test {
  def main(args: Array[String]): Unit = {
    
    println("Welcome to the Scala Worksheet") //> Welcome to the Scala worksheet
    
    val list1 = List.empty //>list1 : List[Nothing] = List()
    list1 eq Nil //> res0: Boolean = true
    
    val list2 = 1:: 2:: 3:: Nil //> list2 : List[Int] = List(1,2,3) //Nill is empty list and Nill is terminator
    val list3 = Nil //> list3 : scala.collection.immutable.Nil.type = List()
    
    //Null
    //Null Type does not have any methods
    //null is instance and Null is type of that
    //

    //Nothing
    //It is also subtype of each and every class of scala
    //we have functions where exception related 
    //nothing as return type
    //it is doesn't have any values
    
  }
}