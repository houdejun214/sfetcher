package com.sfetcher.core.crawldb

import slick.driver.H2Driver.api._
import slick.jdbc.meta.MTable

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by dejun on 13/2/16.
  */

object CrawlDBOfH2 extends CrawlDB {

  import scala.concurrent.ExecutionContext.Implicits.global

  private lazy val db = Database.forURL("jdbc:h2:file:./.crawl.links;DB_CLOSE_DELAY=-1",
    driver = "org.h2.Driver",
    executor = AsyncExecutor("asyncExecutor", numThreads = 10, queueSize = 1000),
    keepAliveConnection = true)

  private val links = TableQuery[CrawlLinks]

  Await.result(db.run(links.schema.drop), Duration.Inf)
  createTableIfNotExists(links)

  private def createTableIfNotExists(table: TableQuery[_ <: Table[_]]): Unit = {
    db.run(MTable.getTables(table.baseTableRow.tableName)).flatMap {
      result => {
        if (result.isEmpty) {
          db.run(table.schema.create)
        }
        Future.successful()
      }
    }
  }

  override def exists(url: String): Boolean = {
    // Construct a query where the price of Coffees is > 9.0
    val filterQuery = links.filter(_.url === url)
      .result.headOption.flatMap {
      case Some(i) => DBIO.successful(true)
      case None => DBIO.successful(false)
    }.transactionally
    Await.result(db.run(filterQuery), Duration.Inf)
  }

  override def append(urls: String*): Unit = {
    val buffer = mutable.ListBuffer[CrawlLink]()
    for (url <- urls) {
      buffer += CrawlLink(url, CrawlLinkState.Init.id)
    }
    val insertAction: DBIO[Unit] = (links ++= buffer).map { result => Unit }
    Await.result(db.run(insertAction), Duration.Inf)
  }

  override def appendIfNotExists(url: String): Option[Boolean] = {
    try {
      val insertAction = links.filter(_.url === url)
        .result
        .headOption
        .flatMap {
          case Some(link) =>
            DBIO.successful(Option(false))
          case None =>
            append(url)
            DBIO.successful(Option(true))
        }.transactionally
      Await.result(db.run(insertAction), Duration.Inf)
    } catch {
      case ex: Exception => Option(false)
    }
  }

  def size(): Int = {
    val run: Future[Int] = db.run(links.length.result)
    Await.result(run, Duration.Inf)
  }
}

object CrawlLinkState extends Enumeration {
  val Init, Process = Value
}

case class CrawlLink(url: String, state: Int, id: Option[Int] = None)

class CrawlLinks(tag: Tag)
  extends Table[CrawlLink](tag, "CRAWL_LINKS") {
  // This is the primary key column:
  def id: Rep[Int] = column[Int]("ID", O.PrimaryKey, O.AutoInc)

  def url: Rep[String] = column[String]("URL")

  def state: Rep[Int] = column[Int]("STATE")

  // Every table needs a * projection with the same type as the table's type parameter
  def * = (url, state, id.?) <>(CrawlLink.tupled, CrawlLink.unapply)

  def idx = index("idx_url", (url), unique = true)
}
