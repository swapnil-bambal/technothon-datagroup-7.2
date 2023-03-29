package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.example.dataProjectMain.createSparkSession
import java.io.File


object dataProjectMain {

  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataProject")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");
    return spark;
  }

  def checkConfigFile(Json_file_path: String, file_type: String): Any = {

    try {
      val file = new File(Json_file_path)
      if (file.exists() && file.isFile()) {
        return "Success";
      }
      else {
        // File does not exist or is not a regular file
        throw new Exception("File does not exist")
      }
    } catch {
      case e: Exception => {
        println("An error occurred: " + e.getMessage);
      }

    }


  }

  def readConfigFile(Json_file_path: String, file_type: String): DataFrame = {

    val spark = createSparkSession();
    val Json_fileData = spark.read.format(file_type).option("multiline", true).load(Json_file_path);
    return Json_fileData;
  }

  def getSrcConnections(configFileData: DataFrame): DataFrame = {
    val src_ct_df = configFileData.filter(configFileData("type") === "source").select("conectionType");
    return src_ct_df;

  }

  def getTarConnections(configFileData: DataFrame): DataFrame = {
    val tar_ct_df = configFileData.filter(configFileData("type") === "target").select("conectionType");
    return tar_ct_df;

  }




  def main(args: Array[String]): Unit = {

    val spark = createSparkSession();
    val config_Json_file_path = "src/main/resources/JsonData.json";
    val file_type = "json";
    var configFileData = spark.emptyDataFrame;
    val check = checkConfigFile(config_Json_file_path, file_type);
    if (check == "Success") {
      configFileData = readConfigFile(config_Json_file_path, file_type);
      configFileData.show();

      val srcConectuonData = getSrcConnections(configFileData);
      val tarConectuonData = getTarConnections(configFileData);
      srcConectuonData.show();
      tarConectuonData.show();

    }
    //
    //    val file = new File(config_Json_file_path)
    //    //      checking file is there or not
    //    if (file.exists() && file.isFile()) {
    //
    //    }
  }

}
