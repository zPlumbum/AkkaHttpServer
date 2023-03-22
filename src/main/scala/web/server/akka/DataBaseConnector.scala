package web.server.akka

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.StandardRoute
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import Config._
import java.sql.DriverManager


class DataBaseConnector extends StafferJsonProtocol with SprayJsonSupport {
  val spark: SparkSession = SparkSession.builder()
    .appName("Postgresql connector")
    .master("local")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  createDbIfNotExists(schemaName, tableNameData, tableNameBio)

  def readStafferFromDb(schemaName: String, dataTableName: String,
                        bioTableName: String, stafferName: String): StandardRoute = {
    val stafferDataDf = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$schemaName")
      .option("dbtable", s"$dataTableName")
      .option("user", userName)
      .option("password", password)
      .load()
      .filter(col("name") === stafferName)

    val stafferBiographyDf = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$schemaName")
      .option("dbtable", s"$bioTableName")
      .option("user", userName)
      .option("password", password)
      .load()
      .filter(col("name") === stafferName)

    if (stafferDataDf.isEmpty && stafferBiographyDf.isEmpty) {
      complete(StatusCodes.NotFound, "Employee not found in any of the tables.")
    } else if (stafferDataDf.isEmpty || stafferBiographyDf.isEmpty) {
      complete(StatusCodes.Conflict, "Employee found in only one of the tables.")
    } else {
      val staffer = stafferDataDf
        .join(stafferBiographyDf, Seq("name"))
        .as[StafferResponse]
        .collect()(0)
      complete(staffer)
    }
  }

  def writeStafferToDb(schemaName: String, tableName: String, stafferData: Staffer): StandardRoute = {
    val stafferDf: DataFrame = stafferData match {
      case StafferData(name, salary) =>
        Seq((name, salary.toString)).toDF("name", "salary")
          .select(col("name"), col("salary").cast("integer"))
      case StafferBiography(name, born, education) =>
        Seq((name, born, education)).toDF("name", "born", "education")
          .select(col("name"), col("born").cast("timestamp"), col("education"))
    }

    val is_existing = {
      val staffer = spark.read
        .format("jdbc")
        .option("url", s"jdbc:postgresql://localhost:5432/$schemaName")
        .option("dbtable", s"$tableName")
        .option("user", userName)
        .option("password", password)
        .load()
        .filter(col("name") === stafferDf.select("name").collect().take(1)(0).getString(0))
      if (staffer.head(1).isEmpty) false else true
    }

    if (!is_existing) {
      stafferDf
        .write
        .mode("append")
        .format("jdbc")
        .option("url", s"jdbc:postgresql://localhost:5432/$schemaName")
        .option("dbtable", s"$tableName")
        .option("user", userName)
        .option("password", password)
        .save()
      complete(StatusCodes.Created, "Staffer has been created.")
    } else {
      complete(StatusCodes.BadRequest, "Staffer with this name already exists.")
    }
  }

  def fixAnomalies(schemaName: String, dataTableName: String, bioTableName: String): Unit = {
    val stafferDataDf = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$schemaName")
      .option("dbtable", s"$dataTableName")
      .option("user", userName)
      .option("password", password)
      .load()

    val stafferBiographyDf = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$schemaName")
      .option("dbtable", s"$bioTableName")
      .option("user", userName)
      .option("password", password)
      .load()

    val rowsToDelete: List[String] = stafferBiographyDf
      .join(stafferDataDf, Seq("name"), "left_anti")
      .select("name")
      .collect().map(_.getString(0)).toList

    rowsToDelete.foreach(name => deleteStafferFromDb(schemaName, bioTableName, name))

    val rowsToAdd: DataFrame = stafferDataDf
      .join(stafferBiographyDf, Seq("name"), "left_anti")
      .select("name")
      .withColumn("born", to_timestamp(lit("1900-01-01 00:00:00")))
      .withColumn("education", lit("УТОЧНИТЬ!"))

    rowsToAdd
      .write
      .mode("append")
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$schemaName")
      .option("dbtable", s"$bioTableName")
      .option("user", userName)
      .option("password", password)
      .save()
  }

  private def deleteStafferFromDb(schemaName: String, tableName: String, stafferName: String): Unit = {
    val query = s"DELETE FROM $tableName WHERE name = '$stafferName'"
    val connection = DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/$schemaName?user=$userName&password=$password")
    val statement = connection.createStatement()
    statement.execute(query)
  }

  private def createDbIfNotExists(schemaName: String, dataTableName: String, bioTableName: String): Unit = {
    val queryDataTableName = s"CREATE TABLE IF NOT EXISTS $dataTableName (id serial primary key, name varchar(80) not null, salary integer not null)"
    val queryBioTableName = s"CREATE TABLE IF NOT EXISTS $bioTableName (name varchar(80) not null, born timestamp not null, education varchar(100) not null)"
    val connection = DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/$schemaName?user=$userName&password=$password")
    val statement = connection.createStatement()

    statement.execute(queryDataTableName)
    statement.execute(queryBioTableName)
  }
}