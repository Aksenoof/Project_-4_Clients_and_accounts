
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object main extends App{
  // создаем сессию Spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("ClientsAndAccounts")
    .getOrCreate()

  // подключаемся к postgres
  val maskTable = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/postgres")
    .option("dbTable", "public.comment_filter")
    .option("user", "postgres")
    .option("password", "postgres")
    .load()

  // прописываем пути к файлам
  val pathAccount = "/Users/i_aksenov/Project №4 Clients and accounts/Account.csv"
  val pathClients = "/Users/i_aksenov/Project №4 Clients and accounts/Clients.csv"
  val pathOperation = "/Users/i_aksenov/Project №4 Clients and accounts/Operation.csv"
  val pathRate = "/Users/i_aksenov/Project №4 Clients and accounts/Rate.csv"

  // таблица клиентов
  val dfClientsInit = spark.read.option("delimiter", ";").option("header", "true").csv(pathClients)
  // таблица счетов
  val dfAccountInit = spark.read.option("delimiter", ";").option("header", "true").csv(pathAccount)
  // операции по счетам
  val dfOperationInit = spark.read.option("delimiter", ";").option("header", "true").csv(pathOperation)
  // курсы валют по отношению к рублю
  val dfRateInit = spark.read.option("delimiter", ";").option("header", "true").csv(pathRate)

  // создание таблицы операций с конвертируемой суммой операции в национальную валюту по актуальному курсу
  // и ключу партиции CutoffDt
  val dfOperation = dfOperationInit.as("dfOi")
    .join(dfRateInit.as("dfRi"), col("dfOi.Currency") === col("dfRi.Currency"), "cross")
    .withColumn("Amount", regexp_replace(col("Amount"), ",", ".").cast("float"))
    .withColumn("Rate", regexp_replace(col("Rate"), ",", ".").cast("float"))
    .withColumn("convertAmount", round(col("Amount") * col("Rate"), 2))
    .withColumnRenamed("DateOp", "CutoffDt")
    .filter(col("CutoffDt").contains("2020-11-04")) // Ключ партиции CutoffDt
    .drop("Currency", "RateDate", "Amount", "Rate")

  //  таблица операций по ИД счету (AccountId) по таблице счетов и счету дебета проводки (AccountDB) по таблице операций
  val dfAccountDB = dfAccountInit.as("dfAi")
    .join(dfOperation.as("dfO"), col("dfAi.AccountID") === col("dfO.AccountDB"), "inner")
    .withColumnRenamed("convertAmount", "AmountDB")

  // сумма операций по счету, где счет клиента указан в дебете проводки
  val dfPayment = dfAccountDB.groupBy( "AccountDB", "AccountNum")
    .agg(round(sum("AmountDB"), 2).as("PaymentAmt"))
    .withColumnRenamed("AccountNum", "AccountNumDB")

  // таблица операций по ИД счету (AccountId) по таблице счетов и счету кредита проводки (AccountCR) по таблице операций
  val dfAccountCR = dfAccountInit.as("dfAi")
    .join(dfOperation.as("dfO"), col("dfAi.AccountID") === col("dfO.AccountCR"), "inner")
    .withColumnRenamed("convertAmount", "AmountCR")

  // сумма операций по счету, где счет клиента указан в кредите проводки
  val dfEnrollement = dfAccountCR.groupBy( "AccountCR", "AccountNum")
    .agg(round(sum("AmountCR"), 2).as("EnrollementAmt"))
    .withColumnRenamed("AccountNum", "AccountNumCR")

  // таблица даты операции
  val dfCutoffDt = dfAccountDB.union(dfAccountCR)
    .select("AccountID", "CutoffDt")

  // конвертируем списки масок из таблицы в виде списка
  val mask1 = maskTable.select("list1").collect.map(_(0)).filterNot(_ == null).toList
  val mask2 = maskTable.select("list2").collect.map(_(0)).filterNot(_ == null).toList

  // функция проверки назначения платежа по маскам списка 1
  val checkerMask1 = udf {(args: String) =>
    val inLine: List[String] = args.split(",").toList
    var count: Int = 0
    for (i <- inLine)
      if (mask1.contains(i.trim))
        count += 1
    if (count == 0)
      "no"
    else
      "yes"
  }

  // функция проверки назначения платежа по маскам списка 2
  val checkerMask2 = udf { (args: String) =>
    val inLine: List[String] = args.split(",").toList
    var count: Int = 0
    for (i <- inLine)
      if (mask2.contains(i.trim))
        count += 1
    if (count == 0)
      "no"
    else
      "yes"
  }

  // собираем таблицу согласно маскам Списка 1
  val dfCheckMask1 = dfAccountDB.withColumn("checkMask1", checkerMask1(col("Comment")))

  // сумма операций, где счет клиента указан в дебете проводки и назначение платежа не содержит слов по маскам Списка 1
  val dfCars = dfCheckMask1.withColumn("Amount", when(col("checkMask1").contains("no"), col("AmountDB")).otherwise(0))
    .groupBy("AccountDB")
    .agg(round(sum("Amount"), 2).as("CarsAmt"))

  // собираем таблицу согласно маскам Списка 2
  val dfCheckMask2 = dfAccountCR.withColumn("checkMask2", checkerMask2(col("Comment")))

  // сумма операций, где счет клиента указан в кредите проводки и назначение платежа содержит слова по маскам Списка 2
  val dfFood = dfCheckMask2.withColumn("Amount", when(col("checkMask2").contains("yes"), col("AmountCR")).otherwise(0))
    .groupBy("AccountCR")
    .agg(round(sum("Amount"), 2).as("FoodAmt"))

  // сборная таблица
  val prefTable = dfClientsInit.as("dfCi")
    .join(dfAccountInit.as("dfAi"), col("dfCi.ClientId") === col("dfAi.ClientId"), "left")
    .join(dfCutoffDt.as("dfCD"), col("dfAi.AccountID") === col("dfCD.AccountID"), "inner")
    .join(dfPayment.as("dfP"), col("dfAi.AccountID") === col("dfP.AccountDB"), "left")
    .join(dfEnrollement.as("dfE"), col("dfAi.AccountID") === col("dfE.AccountCR"), "left")
    .join(dfCars.as("dfC"), col("dfAi.AccountID") === col("dfC.AccountDB"), "left")
    .join(dfFood.as("dfF"), col("dfAi.AccountID") === col("dfF.AccountCR"), "left")
    .na.fill(0, Seq("PaymentAmt", "EnrollementAmt", "CarsAmt", "FoodAmt"))
    // сумма операций где счет клиента указан в дебете, и счет кредита 40702
    .withColumn("EnrollementAmt_40702", when(col("AccountNumCR").contains("40702"), col("EnrollementAmt")).otherwise(0))
    .withColumn("TaxAmt", col("EnrollementAmt_40702") + col("PaymentAmt"))
    // сумма операций где счет клиента указан в кредите, и счет дебета 40802
    .withColumn("PaymentAmt_40802", when(col("AccountNumDB").contains("40802"), col("PaymentAmt")).otherwise(0))
    .withColumn("ClearAmt", col("PaymentAmt_40802") + col("EnrollementAmt"))
    // сумма операций с физ. лицами. Счет клиента указан в дебете проводки, а клиент в кредите проводки – ФЛ
    .withColumn("EnrollementAmt_FL", when(col("dfCi.Type").contains("Ф"), col("EnrollementAmt")).otherwise(0))
    .withColumn("FLAmt", col("PaymentAmt") + col("EnrollementAmt_FL"))
    .withColumn("TotalAmt", col("PaymentAmt") + col("EnrollementAmt"))
    .drop(col("dfAi.ClientId"))
    .drop(col("dfCD.AccountID"))
    .distinct()

  //сборка витрины _corporate_account_
  val _corporate_payments_ = prefTable.select("dfAi.AccountID", "dfCi.ClientId", "PaymentAmt", "EnrollementAmt", "TaxAmt", "ClearAmt", "CarsAmt", "FoodAmt", "FLAmt", "CutoffDt")
    .sort("dfCi.ClientId")
  // сохраняем в паркет
  _corporate_payments_.write.partitionBy("CutoffDt")
                      .parquet("/Users/i_aksenov/Project №4 Clients and accounts/corporate_payments/_corporate_payments_2020-11-04.parquet")

  //сборка витрины corporate_account
  val _corporate_account_ = prefTable
    .select("dfAi.AccountID", "dfAi.AccountNum", "dfAi.DateOpen", "dfCi.ClientId", "dfCi.ClientName", "TotalAmt", "CutoffDt")
    .sort("dfCi.ClientId")

  // сохраняем в паркет
  _corporate_account_.write.partitionBy("CutoffDt")
                      .parquet("/Users/i_aksenov/Project №4 Clients and accounts/corporate_account/_corporate_account_2020-11-04.parquet")

  //сборка витрины _corporate_info_
  val _corporate_info_ = prefTable.as("pT")
    .withColumn("TotalAmt", round(sum(col("pT.TotalAmt")).over(Window.partitionBy("pT.ClientId")), 2))
    .select("pT.ClientId", "pT.ClientName", "pT.Type", "pT.Form", "pT.RegisterDate", "TotalAmt", "pT.CutoffDt")
    .distinct()
    .sort("pT.ClientId")

  // сохраняем в паркет
  _corporate_info_.write.partitionBy("CutoffDt")
                      .parquet("/Users/i_aksenov/Project №4 Clients and accounts/corporate_info/_corporate_info_2020-11-04.parquet")

  println(_corporate_payments_.show())
  println(_corporate_account_.show())
  println(_corporate_info_.show())
  spark.sparkContext.setLogLevel("WARN")
}
