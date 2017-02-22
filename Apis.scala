val employeesRDD = sc.parallelize(Seq(employee1, employee2, employee3, employee4, employee5, employee6, employee7, employee8, employee9))
val employeesDF = employeesRDD.toDF()
val employeesDS = employeesRDD.toDS()
employeesDS.createOrReplaceTempView("employees")

val citizensRDD = sc.parallelize(Seq(citizen1,citizen2,citizen3,citizen4,citizen5,citizen6,citizen7,citizen8,citizen9,citizen10,citizen11,citizen12,citizen13))
val citizensDF = citizensRDD.toDF()
val citizensDS = citizensRDD.toDS()
citizensDS.createOrReplaceTempView("citizens")

val orgsRDD = sc.parallelize(Seq(org1))
val orgsDF = orgsRDD.toDF()
val orgsDS = orgsRDD.toDS()
orgsDS.createOrReplaceTempView("orgs")

employeesRDD.collect.foreach(println)
println("\nDS and DF")
//Same for DF and DS
employeesDS.show()
citizensDS.show()
orgsDS.show()
