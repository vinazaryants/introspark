package com.gd.intro

import org.apache.spark.sql.test.SharedSparkSession

class MainTest extends SharedSparkSession {

  test("topUsers should work") {
    val session = spark
    import session.implicits._

    val data = session.createDataset(Seq(
      UserCount(1, 10),
      UserCount(2, 1),
      UserCount(3, 2)
    ))
    val alice = UserDir(1, "Alice", "A")
    val bob = UserDir(2, "Bob", "B")
    val sylvia = UserDir(3, "Sylvia", "S")
    val userDirs = session.createDataset(Seq(alice, bob, sylvia))
    val expected = Array(alice, sylvia, bob)
    assertResult(expected)(Main.topUsers(data.toDF(), userDirs)(session).collect())
  }

  test("first wave check") {
    val session = spark
    import session.implicits._

    val alice = UserDir(1, "Alice", "A")
    val bob = UserDir(2, "Bob", "B")
    val sylvia = UserDir(3, "Sylvia", "S")
    val userDirs = session.createDataset(Seq(alice, bob, sylvia))

    val messages = session.createDataset(Seq(Message(1, 1), Message(2, 2), Message(3, 3)))
    val retweets = session.createDataset(Seq(Retweet(2, 1, 2), Retweet(2, 3, 2), Retweet(1, 3, 1)))
    val expected = Array(bob, alice)
    assertResult(expected)(Main.topFirstWaveUsers(retweets, messages, userDirs)(session).collect())
  }

  test("second wave check") {
    val session = spark
    import session.implicits._

    val alice = UserDir(1, "Alice", "A")
    val bob = UserDir(2, "Bob", "B")
    val sylvia = UserDir(3, "Sylvia", "S")
    val norman = UserDir(4, "Norman", "N")
    val userDirs = session.createDataset(Seq(alice, bob, sylvia, norman))
    val retweets = session.createDataset(Seq(
      Retweet(1, 2, 1), Retweet(1, 3, 1), Retweet(2, 3, 2),
      Retweet(2, 3, 1), Retweet(3, 4, 2), Retweet(3, 1, 2)
    ))
    val expected = Array(bob, alice)
    assertResult(expected)(Main.topSecondWaveUsers(retweets, userDirs)(session).collect())
  }
}

case class UserCount(userId: Int, cnt: Long)
