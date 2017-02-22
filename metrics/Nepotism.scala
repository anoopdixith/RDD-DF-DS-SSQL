// Nepotism
println("\nAll couples in the org from RDD\n=============")
val allRelativesFromRDD = onboardMatchRDD.map(a => (a._2.uniqueId, a._2.name))
                                         .join(onboardMatchRDD.map(a => (a._2.spouseId, a._2.name)))
                                         .collect
                                         .foreach(row => println(row._2._1 + " and " + row._2._2))

//DS same as DF
println("\nAll couples in the org from DF and DS\n=============")
val allRelativesFromDS = citizensDS.join(employeesDS, employeesDS("citizenId") === citizensDS("spouseId"))
                                   .select(employeesDS("name"), citizensDS("name"))
                                   .show

println("\nAll couples in the org from SparkSQL\n=============")
val allRelativesQ = "select a.name, b.name from (select c.name, c.uniqueId, c.spouseId from citizens c join employees e on c.uniqueId = e.citizenId) a, (select c.name, c.uniqueId, c.spouseId from citizens c join employees e on c.uniqueId = e.citizenId) b where a.uniqueId = b.spouseId"
val allRelativesFromSS = sqlContext.sql(allRelativesQ).show
