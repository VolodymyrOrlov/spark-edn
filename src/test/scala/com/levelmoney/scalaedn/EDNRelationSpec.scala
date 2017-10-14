package com.levelmoney.scalaedn

class EDNRelationSpec extends UnitSpec {

  trait UsersRelation extends SparkTestContext {
    val usersRelation = Option(getClass.getClassLoader.getResource("users.edn")).map {
      resource =>
        EDNRelation(resource.getPath, None)(sparkSession.sqlContext)
    } getOrElse(throw new IllegalStateException("Could not find [users.edn] in resources folder."))

  }

  "A EDNRelation" when {

    "pointed to users test database" should {

      "load 5 users in total" in new UsersRelation {

        assert(usersRelation.buildScan(Array(":user_id", ":user_name")).count() == 5)

      }

    }
  }

}