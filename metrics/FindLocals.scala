// Find locals
println("\nRDDs")
val localsFromRDD = citizensRDD.filter(a => a.city == a.placeOfBirth).collect.foreach(println)

println("\nDataFrames")
val localsFromDF = citizensDF.filter("city == placeOfBirth").show

println("\nDataSets")
val localsFromDS = citizensDS.filter(a => a.placeOfBirth == a.city).show
//Another method without filter
val localsFromDSAlt1 = citizensDS.map(a => if(a.placeOfBirth == a.city) Some(a) else None).na.drop().show

println("\nSparkSQL")
val localsQ = "SELECT * from citizens where city == placeOfBirth"
val localsFromSS = sqlContext.sql(localsQ).show
