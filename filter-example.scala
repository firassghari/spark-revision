
// dataframe 
val salaries = sc.textFile("/spark/salaries/salaries.txt") 
val salariesMap = salaries.map(row =>(row.split(",")(0).toInt, row.split(",")(1).toInt, row.split(",")(2).toInt));
val salariesDF = salariesMap.toDF 
val filtredDF = salariesDF.select("*").where("_2>2000")
val rddMap = df.map(row=>(row(0) +"\t"+row(1)+"\t"+row(2)+"\t"))

//RDD
val salaries = sc.textFile("/spark/salaries/salaries.txt") 
val salariesMap = salaries.map(row =>(row.split(",")(0).toInt, row.split(",")(1).toInt, row.split(",")(2).toInt));
val filtredRDD = salariesMap.filter(_._2>2000)
filtredRDD.foreach(println)

