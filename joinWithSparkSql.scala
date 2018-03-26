val empl =  sc.textFile("/data/rh/emp.txt")
val salaire = sc.textFile("/data/rh/salaire.txt")
val emplRDD = empl.map(row=>(row.split(',')(0).toInt,row.split(',')(1),row.split(',')(2)))
val emplDF = emplRDD.toDF("id","sexe","ville")
emplDF.registerTempTable("employers")
val salaireRDD =  salaire.map(row => (row.split(',')(0).toInt,row.split(',')(1)))
val salaireDG = salaireRDD.toDF("id","salaire")
salaireDG.registerTempTable("salaires") 
val joinedData = salaireDG.join(emplDF,"id")

// sql 
val result = sqlContext.sql("select sexe , ville, avg(salaire) as moyenne from employers inner join salaires on employers.id = salaires.id group by employers.ville,employers.sexe") 
result.rdd.map(row=>row(0)+"\t"+row(1)+"\t"+row(2)).repartition(1).saveAsTextFile("/data/rh/result1")
