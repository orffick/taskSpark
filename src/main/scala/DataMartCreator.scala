import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}

import scala.io.Source

object DataMartCreator {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark: SparkSession = SparkSession.builder()
    .appName("DataMartCreator")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val in_path = "resources/output/" // данные для витрин берём из созданных паркетов
    val out_path = in_path + "DataMarts/" // сюда будут записываться сформированные витрины
    val config_file = "config/calculation_params_tech.csv" // 4 пункт тз
    val sql_path = "resources/SQL/" // здесь лежат sql-запросы для формирования витрин

    val client_df = getDataFrameFromParquet(in_path + "Client")
    val account_df = getDataFrameFromParquet(in_path + "Account")
    val operation_df = getDataFrameFromParquet(in_path + "Operation")
    val rate_df = getDataFrameFromParquet(in_path + "Rate")
    val tech_df = getDataFrameFromCSV(config_file) // дата фрейм технологической таблицы

    client_df.createOrReplaceTempView("clients")
    account_df.createOrReplaceTempView("accounts")
    operation_df.createOrReplaceTempView("operations")
    rate_df.createOrReplaceTempView("rates")
    tech_df.createOrReplaceTempView("tech_params") // темп вью для технологической таблицы

    val corporate_payments_df = getDataFrameFromSQL(sql_path + "CreateCorporatePayments.sql")
    corporate_payments_df.createOrReplaceTempView("corporate_payments")

    val corporate_account_df = getDataFrameFromSQL(sql_path + "CreateCorporateAccount.sql")
    corporate_account_df.createOrReplaceTempView("corporate_account")

    val corporate_info_df = getDataFrameFromSQL(sql_path + "CreateCorporateInfo.sql")

    val numPartitions = 1
    val partitionKey = "CutoffDt"

    saveDataFrameAsParquet(corporate_payments_df,
      numPartitions,
      partitionKey,
      out_path + "corporate_payments"
    )

    saveDataFrameAsParquet(corporate_account_df,
      numPartitions,
      partitionKey,
      out_path + "corporate_account"
    )

    saveDataFrameAsParquet(corporate_info_df,
      numPartitions,
      partitionKey,
      out_path + "corporate_info"
    )

  }

  def getDataFrameFromParquet(path: String): DataFrame =
    spark.read.parquet(path)

  def getDataFrameFromCSV(path: String): DataFrame =
    spark.read.option("delimiter", ",")
              .option("header", "true")
              .csv(path)

  def getDataFrameFromSQL(path: String): DataFrame = {
    spark.sql(Source.fromFile(path).mkString)
  }

  def saveDataFrameAsCSV(df: DataFrame, numPartitions: Int, path: String): Unit ={ // для отладки
    df.repartition(numPartitions)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(path)
  }

  def saveDataFrameAsParquet(df: DataFrame, numPartitions: Int, partitionKey: String, path: String): Unit ={
    df.repartition(numPartitions)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionKey)
      .parquet(path)
  }
}
