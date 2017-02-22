// Find out how many people are of same ages
println("\nRDDs")
val agesFromRDD = employeesRDD.map(each => (each.name, each.age))
                       .groupBy(a=>a._2)
                       .collect
                       .foreach(a => println(a._1 + " : " + a._2))

println("\nDataFrames")
val agesFromDF = employeesDF.groupByKey(row => row.getInt(2))
                            .mapGroups((k,v) => (k, v.mkString(" AND ")))
                            .toDF("age", "employee_list") // rename columns for clarity
                            .show(false) //don't truncate the length

println("\nDataSets")
//Mostly same as DF
val agesFromDS = employeesDS.groupByKey(row => row.age)
                            .mapGroups((k,v) => (k, v.mkString(" AND "))) 
                            .withColumnRenamed("_1", "age") // gosh! need an API to replace 'em all in one shot (in DS only)
                            .withColumnRenamed("_2", "all_employees")
                            .show(false)

println("\nSparkSQL")
val agesQ = "select age, collect_list(name), count(age) from employees group by age"
val agesFromSS = sqlContext.sql(agesQ).show(false)
