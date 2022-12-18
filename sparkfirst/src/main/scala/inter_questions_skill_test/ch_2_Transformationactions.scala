package inter_questions_skill_test

object ch_2_Transformationactions {
  
  //Transformation & actions
  
  //1) RDD
  //Transformation & Actions
  
  //2) DataFrame
  //Transformation & Actions
  
  //Map,flat is transformation
  //colllect,show,SaveAsfile -->Actions
  
  //Spark is having Lazy Evaulations
/*  1) Reading file
  2) map operations on top of it
  3) filter
  4) SaveAsFile*/
  //Till 1,2,3 steps compiler will do noting 
  //only at 4th step there is action then only compiler will start the evaluation
  //That means bottom to top
  
  //-------------------------------------------------------
/*  Transformation
  ---------------
  Narrow
  
  * map
  * filter
  * flatMap
  * mapPartitions
  * mapPartitionsWithIndex
  * groupBy
  * sortBy
  Etc......*/
/*  
  Wide
  
  * groupByKey
  * reduceByKey
  * reduceByKeyLocally
  * foldByKey
  * aggregateByKey
  * sortByKey
  * combineByKey
  
  ---------------------------
  
  Actions
  * reduce
  * collect
  * aggregate
  * fold
  * first
  * take
  * forEach
  * top
  * treeAggregate
  * forEachPartition
  * collectAsMap
  Etc.....*/
  
  
  
  
  
  
  
}