// Aggregate experience
println("\nRDDs")
val zeroExperience = 0.0
//To show the usage of RDD's aggregate API 
val totalExperienceFromRDD = employeesRDD.aggregate(zeroExperience)((accumulator, person) => accumulator + person.experience, _+_)
val totalExperienceFromRDDAlt1 = employeesRDD.map(_.experience)
                                   .reduce(_+_)
println("With agg: "  + totalExperienceFromRDD)
println("Without agg: " + totalExperienceFromRDDAlt1)

println("\nDataFrames")
val totalExperienceFromDF = employeesDF.agg(sum($"experience")).show

println("\nDataSets")
// Almost same as DataFrames
val totalExperienceFromDS = employeesDS.agg(sum("experience")).show

println("\nSParkSQL")
val totalExperienceQ = "SELECT sum(experience) from employees"
val totalExperienceFromSS = sqlContext.sql(totalExperienceQ).show

