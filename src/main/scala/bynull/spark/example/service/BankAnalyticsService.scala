package bynull.spark.example.service

import bynull.spark.example.DataManager
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by null on 19.02.17.
  */
object BankAnalyticsService {
  def apply(dataLoader: DataManager, spark: SparkSession): BankAnalyticsService = {
    new AbstractAnalyticsService(dataLoader, spark) with BankAnalyticsService
  }
}

trait BankAnalyticsService extends AbstractAnalyticsService {
  protected val log: Logger = LoggerFactory.getLogger(classOf[BankAnalyticsService])

  dataManager.loadCsv("data.csv").createOrReplaceTempView("bank")

  def accountsYoungerThanThirty(): Unit = {
    val result = spark.sql(AnalyticsQuery.youngerThanThirtyGroupByAge)
    result.write.parquet(s"${dataManager.outputDirectory}/accountsYoungerThanThirty.parquet")
    log.info("Accounts younger than 30 years. Request:\n" + AnalyticsQuery.youngerThanThirtyGroupByAge)
    result.show()
  }

  def topAccounts(): Unit = {
    val result = spark.sql(AnalyticsQuery.topTenAccounts)
    result.write.parquet(s"${dataManager.outputDirectory}/accountsByAge.parquet")
    log.info("Top ten accounts. Request:\n" + AnalyticsQuery.topTenAccounts)
    result.show()
  }

  def accountsHigherThanAverage(): Unit = {
    val result = spark.sql(AnalyticsQuery.accountsHigherThanAverage)
    result.write.parquet(s"${dataManager.outputDirectory}/accountsHigherThanAverage.parquet")
    log.info("Accounts higher than average. Request:\n" + AnalyticsQuery.accountsHigherThanAverage)
    result.show()
  }

  def richestAccountsByJob(): Unit = {
    val result = spark.sql(AnalyticsQuery.richestAccountsByJob)
    result.write.parquet(s"${dataManager.outputDirectory}/richestAccountsByJob.parquet")
    log.info("Richest accounts by job. Request:\n" + AnalyticsQuery.richestAccountsByJob)
    result.show()
  }
}

private object AnalyticsQuery {

  val youngerThanThirtyGroupByAge: String = "select age, count(age) as number_of_accounts from bank where age < 30 group by age order by age"

  val topTenAccounts: String = "select * from bank order by balance desc limit 10"

  val accountsHigherThanAverage: String =
    """select *
      |from bank
      |where balance > (select avg(balance) from bank where balance > 0)
      |order by balance desc""".stripMargin

  val richestAccountsByJob: String =
    """select
      |  distinct(groups.job),
      |  (select max(b.balance) from bank b where b.job = groups.job) as max_balance
      |from (select job from bank group by job) as groups
      |join bank on groups.job = bank.job""".stripMargin
}