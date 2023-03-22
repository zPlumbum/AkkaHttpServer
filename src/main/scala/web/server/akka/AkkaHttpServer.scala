package web.server.akka

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import Config._

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn


object AkkaHttpServer extends StafferJsonProtocol with SprayJsonSupport {

  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "AkkaHttpServer")
  implicit val executionContext: ExecutionContextExecutor = system.executionContext
  val dbWorker = new DataBaseConnector

  val route: Route = concat(
    (path("api" / "add_staffer_data") & post) {
      entity(as[StafferData]) { staffer: StafferData =>
        dbWorker.writeStafferToDb(schemaName, tableNameData, staffer)
      }
    },
    (path("api" / "add_staffer_biography") & post) {
      entity(as[StafferBiography]) { staffer: StafferBiography =>
        dbWorker.writeStafferToDb(schemaName, tableNameBio, staffer)
      }
    },
    (path("api" / "get_staffer_full_data") & get & parameter("name")) { name =>
      dbWorker.readStafferFromDb(schemaName, tableNameData, tableNameBio, name)
    },
    (path("api" / "fix_anomalies") & get) {
      dbWorker.fixAnomalies(schemaName, tableNameData, tableNameBio)
      complete(StatusCodes.OK, "OK")
    }
  )

  def main(args: Array[String]): Unit = {
    val httpServer = Http().newServerAt("localhost", 8080).bind(route)
    println(s"Server now online. Please navigate to http://localhost:8080/api/hello\nPress RETURN to stop...")
    StdIn.readLine()
    httpServer
      .flatMap(_.unbind())
      .onComplete(_ => system.terminate())
  }
}
