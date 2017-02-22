// Employed female percentage
val emloyedWomenPercentageFromRDD: Double = (employeesRDD.filter(_.gender == false).count().toDouble / citizensRDD.filter(_.gender == false).count().toDouble) * 100  
println("\nRDDs \n " + emloyedWomenPercentageFromRDD)

val employedWomenPercentageFromDF = (employeesDF.filter("gender == false").count.toDouble)/(citizensDF.filter("gender == false").count.toDouble) * 100 
println("\nDataFrames and DataSets \n " + employedWomenPercentageFromDF)

println("\nSparkSQL")
val employedWomenPercentageQ = "SELECT (SELECT COUNT(*) FROM employees WHERE gender = false)/(SELECT COUNT(*) FROM citizens WHERE gender = false)*100"
val employedWomenPercentageFromSS = sqlContext.sql(employedWomenPercentageQ).show
