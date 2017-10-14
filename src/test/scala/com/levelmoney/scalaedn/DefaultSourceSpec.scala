package com.levelmoney.scalaedn

import org.apache.spark.sql.types.{ArrayType, _}

class DefaultSourceSpec extends UnitSpec {

  trait UsersRelation extends SparkTestContext {
    val usersRelation = Option(getClass.getClassLoader.getResource("users.edn")).map {
      resource =>
        sparkSession.read.edn(resource.getPath).cache.createOrReplaceTempView("users")
    } getOrElse(throw new IllegalStateException("Could not find [users.edn] in resources folder."))

  }

  trait MerchantsRelation  extends SparkTestContext {
    val merchantsRelation = Option(getClass.getClassLoader.getResource("merchants.edn")).map {
      resource =>
        sparkSession.read.edn(resource.getPath).cache.createOrReplaceTempView("merchants")
    } getOrElse(throw new IllegalStateException("Could not find [merchants.edn] in resources folder."))
  }

  "An EDN DataSource" when {

    "pointed to users.edn" should {

      "be able to filter users by name" in new UsersRelation {

        assert(sparkSession.sql("select * from users where `:user_name` like 'tc_%'").count() == 3)

      }

    }

    "pointed to merchants.edn" should {

      "be able to load all regular expressions" in new MerchantsRelation {

        assert(sparkSession.sql("select * from merchants").schema == StructType(
          Seq(
            StructField(":name", StringType),
            StructField(":value", StringType)
          )
        ))

        assert(sparkSession.sql("select * from merchants").count() == 11)

      }

    }
  }

}