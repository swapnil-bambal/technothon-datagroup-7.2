package org.example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.sql.DriverManager
import java.io.File
import java.util.Properties

object dataProjectMain {
  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataProject")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");
    return spark;
  }

  def readConfigFile(Json_file_path: String, file_type: String): DataFrame = {
    val spark = createSparkSession();
    val Json_fileData = spark.read.format(file_type).option("multiline", true).load(Json_file_path);
    return Json_fileData;
  }


  def getSrcConnections(configFileData: DataFrame): DataFrame = {
    val src_ct_df = configFileData.filter(configFileData("type") === "source").select("fileType");
    return src_ct_df;
  }

  def getTarConnections(configFileData: DataFrame): DataFrame = {
    val tar_ct_df = configFileData.filter(configFileData("type") === "target").select("conectionType");
    return tar_ct_df;
  }

  def getDataFromFile(configFileData: DataFrame): DataFrame = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "source").select("filePath", "fileType");
    import spark.implicits._
    val src_file_path = df.select("filePath").distinct().map(f => f.getString(0)).collect().toList(0);
    val src_file_type = df.select("fileType").distinct().map(f => f.getString(0)).collect().toList(0);
    val src_file_data = spark.read.format(src_file_type).option("header", "true").load(src_file_path);
    return src_file_data;
  }

  def putDataInAWSPostgreSQL(configFileData: DataFrame, src_file_data: DataFrame): String = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "target")
      .select("userName", "password", "dataBaseName", "schemaName", "tableName");
    import spark.implicits._
    val post_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_Schema_Name = df.select("schemaName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_table_name = df.select("tableName").distinct().map(f => f.getString(0)).collect().toList(0);

    val pgConnectionType = new Properties();
    pgConnectionType.setProperty("user", s"$post_userName");
    pgConnectionType.setProperty("password", s"$post_password");

    val url = s"jdbc:postgresql://database-1.ckaqa1w4hirj.ap-south-1.rds.amazonaws.com:5432/"
    val tableUrl = s"\"$post_Schema_Name\".$post_table_name"
    val conn = DriverManager.getConnection(url, post_userName, post_password)
    val statement = conn.createStatement()
    statement.executeUpdate(s"CREATE DATABASE $post_databaseName")
    val url1 = s"jdbc:postgresql://database-1.ckaqa1w4hirj.ap-south-1.rds.amazonaws.com:5432/$post_databaseName"
    val conn1 = DriverManager.getConnection(url1, post_userName, post_password)
    val statement1 = conn1.createStatement()
    statement1.executeUpdate(s"CREATE SCHEMA $post_Schema_Name")

    // write file to destination
    src_file_data.write
      .mode(SaveMode.Overwrite)
      .jdbc(url1, s"$tableUrl", pgConnectionType)

    return "Success";
  }

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession();
    val config_Json_file_path = "src/main/resources/JsonData.json";
    val file_type = "json";
    val file = new File(config_Json_file_path)

    // checking file is there or not
    if (file.exists() && file.isFile()) {
      // val objConfi = new configFile;
      val configFileData = readConfigFile(config_Json_file_path, file_type);
      // configFileData
      //configFileData.show();
      // val objConnection = new connection;
      val srcConectuonData = getSrcConnections(configFileData);
      val tarConectuonData = getTarConnections(configFileData);

      //srcConectuonData.show();
      //tarConectuonData.show();

      import spark.implicits._
      val srcConectionTypeData = srcConectuonData.select("fileType").distinct()
        .map(f => f.getString(0)).collect().toList;
      val tarConectionTypeData = tarConectuonData.select("conectionType").distinct()
        .map(f => f.getString(0)).collect().toList;
      // println(tarConectionTypeData(0));

      // creating empty dataframe
      var srcFileData = spark.emptyDataFrame
      if (srcConectionTypeData(0) == "csv" || srcConectionTypeData(0) == "parquet") {
        srcFileData = getDataFromFile(configFileData);
      }
      if (tarConectionTypeData(0) == "postgreSQL") {
        val message = putDataInAWSPostgreSQL(configFileData, srcFileData);
        println(message);
      }
    }
  }
}