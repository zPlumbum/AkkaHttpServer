package web.server.akka

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

trait Staffer
case class StafferData(name: String, salary: Int) extends Staffer
case class StafferResponse(id: Int, name: String, salary: Int, born: String, education: String) extends Staffer
case class StafferBiography(name: String, born: String, education: String) extends Staffer

trait StafferJsonProtocol extends DefaultJsonProtocol {
  implicit val stafferDataFormat: RootJsonFormat[StafferData] = jsonFormat2(StafferData)
  implicit val stafferBiographyFormat: RootJsonFormat[StafferBiography] = jsonFormat3(StafferBiography)
  implicit val StafferResponseFormat: RootJsonFormat[StafferResponse] = jsonFormat5(StafferResponse)
}
