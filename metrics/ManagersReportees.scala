//All Managers and their reportees
println("\nRDDs")
val managersFromRDD = employeesRDD.groupBy(_.manager)
                                  .collect
                                  .foreach(a => println(a._1 + " has " + (a._2.toList.length) + " reporters"))
                                  
println("\nDataFrames")
val managersFromDF = employeesDF.groupBy($"manager").count().select($"manager", $"count").show

println("\nDataSets")
//Almost same as DF
val managersFromDS = employeesDS.groupBy("manager").count().select($"manager", $"count").show

println("\nSparkSQL")
//Look how elegant this is, although untyped!
val managersQ = "select manager, count(citizenId) from employees group by manager"
val managersFromSS = sqlContext.sql(managersQ).show

//If you want the reportees and exclude people who report to themselves:
val managersDetailedQ = "select manager, count(name), collect_list(name) from employees where name NOT LIKE manager group by manager"
val managersDetailedFromSS = sqlContext.sql(managersDetailedQ).show(false)
