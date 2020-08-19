package com.gd.intro

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataWriter extends App {
  val session = SparkSession.builder().master("local[*]").getOrCreate()
  import session.implicits._

  val userDirs = (1 to 20).map(i => UserDir(i, s"Name $i", s"Last Name $i"))
  val subscribers = {
    (for {
      tail <- userDirs.tails if tail.nonEmpty
    } yield {
      if (tail.head.userId <= 10)
        tail.head.userId -> tail.tail
      else {
        tail.head.userId -> List(userDirs.head)
      }
    }).toMap
  }
  val messageDirs = (1 to 20).map(i => MessageDir(i, s"Message $i"))
  val messages: Seq[Message] = userDirs.zip(messageDirs).map {
    case (ud, md) => Message(ud.userId, md.messageId)
  }
  val retweets: Seq[Retweet] = {
    val firstWave = for {
      md <- messages
      sub <- subscribers(md.userId)
    } yield {
      Retweet(md.userId, sub.userId, md.messageId)
    }
    val secondWave = for {
      m <- firstWave
      sub <- subscribers(m.subscriberId)
    } yield {
      Retweet(m.subscriberId, sub.userId, m.messageId)
    }
    firstWave ++ secondWave
  }
  session.createDataset(userDirs).repartition(1).write.format("avro").mode(SaveMode.Overwrite).save(s"${System.getProperty("user.home")}/1/intro/user_dir")
  session.createDataset(messageDirs).repartition(1).write.format("avro").mode(SaveMode.Overwrite).save(s"${System.getProperty("user.home")}/1/intro/message_dir")
  session.createDataset(messages).repartition(1).write.format("avro").mode(SaveMode.Overwrite).save(s"${System.getProperty("user.home")}/1/intro/message")
  session.createDataset(retweets).repartition(1).write.format("avro").mode(SaveMode.Overwrite).save(s"${System.getProperty("user.home")}/1/intro/retweet")

  session.stop()
}
