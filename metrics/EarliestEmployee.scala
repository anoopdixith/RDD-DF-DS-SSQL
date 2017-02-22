//Earliest Employee
println("\nRDDs")
val employeesOrdering = new Ordering[Employee]() {
  override def compare(x: Employee, y: Employee): Int = 
      Ordering[Int].compare(x.employeeId, y.employeeId)
}

val earliestEmployeeFromRDD = employeesRDD.min()(employeesOrdering)
val earliestEmployeeFromRDDAlt1 = employeesRDD.map(a=>(a, a.employeeId))
                                  .reduce((personAge1, personAge2)=> 
                                           if(personAge1._1.employeeId < personAge2._1.employeeId) (personAge1._1, personAge1._2) 
                                           else (personAge2._1, personAge2._2))
println("Using ordering: " + earliestEmployeeFromRDD)
println("Using just map-reduce: " + earliestEmployeeFromRDD)

println("\nDataFrames")
val earliestEmployeeFromDF = employeesDF.filter($"experience" === employeesDF.agg(max($"experience"))
                                        .collect()(0)
                                        .getDouble(0))
                                        .show

println("\nDataSets")
// Almost same as DataFrames
val earliestEmployeeFromDS = employeesDS.filter($"experience" === employeesDS.agg(max("experience"))
                                        .collect()(0)
                                        .getDouble(0))
                                        .show

println("\nSparkSQL")
val earliestEmployeeQ = "select * from employees where experience = (select max(experience) from employees)"
val earliestEmployeeFromSS = sqlContext.sql(earliestEmployeeQ).show
