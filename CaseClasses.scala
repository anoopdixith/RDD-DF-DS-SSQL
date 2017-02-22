import spark.implicits._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._

abstract class Base extends Serializable with Product
object BaseEncoder {
  implicit def baseEncoder: org.apache.spark.sql.Encoder[Base] = org.apache.spark.sql.Encoders.kryo[Base]
}

case class Employee(employeeId: Integer,
                  name: String, 
                  age: Integer, 
                  manager: String, 
                  gender: Boolean,
                  experience: Double,
                  citizenId: Integer) extends Base

case class Organization(orgId: Integer,
                        name: String,
                        city: String,
                        employees: List[Employee]) extends Base

case class Citizen(uniqueId: Integer,
                        name: String,
                        gender: Boolean,
                        age: Integer,
                        placeOfBirth: String,
                        city: String,
                        fatherId: Integer,
                        motherId: Integer,
                        spouseId: Integer) extends Base
