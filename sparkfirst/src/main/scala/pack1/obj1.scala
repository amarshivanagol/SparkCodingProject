package pack1

object obj1 {

  def main(args: Array[String]): Unit = {

    println("started")

    val a = 2
    println(a);

    val b = "zeyobron"
    println(b);

    println("===Iterate each element of the List filter number > 2 and save it in new List ==============");

    println("===raw list =========");
    val lsin = List(1, 2, 3, 4, 5)
    lsin.foreach(println);

    //Iterate each element of the List filter number > 2 and save it in new List

    println("===new list =========");
    val newlis = lsin.filter(x => x > 2);
    newlis.foreach(println);

    println("===raw list =========");
    val lsin1 = List(100, 105, -200, 30, -4, 50, 70)
    lsin.foreach(println);

    //Iterate each element of the List filter number > 2 and save it in new List

    println("===new list =========");
    val newlis1 = lsin1.filter(x => x > 100);
    newlis1.foreach(println);
    //We have to Use Lambda Operation

    //Assign list
    println("===raw list =========");
    val lsin2 = List(1, 2, 3, 4)
    lsin2.foreach(println);
    println("=========Iterate each element and Multiply 2 foreach element========");
    println("===new list =========");
    val newlis2 = lsin2.map(x => x * 2)
    newlis2.foreach(println)

  }

}