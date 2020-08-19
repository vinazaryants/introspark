package com.gd.intro

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Main extends App {

  def topUsers(data: DataFrame, userDirs: Dataset[UserDir])(session: SparkSession): Dataset[UserDir] = {
    import session.implicits._

    data.join(userDirs, "userId")
      .sort($"cnt".desc)
      .select("userId", "firstName", "lastName")
      .as[UserDir]
  }

  def topFirstWaveUsers(retweets: Dataset[Retweet], messages: Dataset[Message], userDirs: Dataset[UserDir])(session: SparkSession): Dataset[UserDir] = {

    val firstWave = retweets.join(messages, Seq("userId", "messageId"))
      .groupBy("userId")
      .agg(count("messageId").as("cnt"))
    topUsers(firstWave, userDirs)(session)
  }

  def topSecondWaveUsers(retweets: Dataset[Retweet], userDirs: Dataset[UserDir])(session: SparkSession): Dataset[UserDir] = {
    import session.implicits._

    val secondWave = retweets.as("left").join(retweets.as("right"), $"left.userId" === $"right.subscriberId" and $"left.messageId" === $"right.messageId")
      .groupBy("right.userId")
      .agg(count("right.messageId").as("cnt"))
    topUsers(secondWave, userDirs)(session)
  }

  private val session: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  import session.implicits._

  private val userDirs: Dataset[UserDir] = session.read.format("avro").load(s"${System.getProperty("user.home")}/1/intro/user_dir").as[UserDir]
  private val messages: Dataset[Message] = session.read.format("avro").load(s"${System.getProperty("user.home")}/1/intro/message").as[Message]
  private val retweets: Dataset[Retweet] = session.read.format("avro").load(s"${System.getProperty("user.home")}/1/intro/retweet").as[Retweet]

  println("Top first wave")
  topFirstWaveUsers(retweets, messages, userDirs)(session).show(10)
  println("Top second wave")
  topSecondWaveUsers(retweets, userDirs)(session).show(10)

  session.stop()
}
