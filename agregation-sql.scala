val salaireData = sc.textFile("/salaires/salaires.txt")
val mappedData = salaireData.map(row =>(row.split(',')(0).toInt , row.split(',')(1), row.split(',')(2).toInt, row.split(',')(3).toInt))
val salaireDf = mappedData.toDF("id","nom","salaire","naissance")
salaireDf.registerTemTable("salaire") 
val resultDf = sqlContext.("select naissance , avg(salaire) from salaire groupBy naissance") ; 
salData.map(row =>(row(0) + "\n"+row(1)))
saData.saveAsTextFile("/hello/result")
