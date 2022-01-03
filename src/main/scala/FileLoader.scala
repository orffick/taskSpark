import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object FileLoader {

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val in_path = "resources/input/"  //args(0)
    val out_path = "resources/output/"  //args(1)

    val spark = SparkSession.builder()
      .appName("CSV_TO_Parquet")
      .master("local[*]")
      .getOrCreate()

    val partitionKeys = Map(
      "Client"    -> "RegisterDate",
      "Account"   -> "DateOpen",
      "Operation" -> "DateOp",
      "Rate"      -> "RateDate")

    val filesHere = (new java.io.File(in_path)).listFiles

    for (
      file <- filesHere
    ) {
      val fileName = file.getName.substring(0, file.getName.length - 5) // отбрасываем 's.csv' от названия файла
      spark.read.option("delimiter", ",")
        .option("header", "true")
        .csv(file.getPath)
        .write.mode(SaveMode.Overwrite)
        .partitionBy(partitionKeys(fileName))
        .parquet(out_path + "/" + fileName)
    }

  }
}
