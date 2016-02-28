package com.sfetcher.core.route

import com.lakeside.core.utils.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Router that match the url path with specify handler
  * matching orders.
  */
object Router {
  private val log: Logger = LoggerFactory.getLogger(classOf[Router[_]])
}

final class Router[T] {

  private val _routes: mutable.Map[PathMatcher, T] = new mutable.HashMap[PathMatcher, T]()

  /** Returns all routes in this router, an unmodifiable map of {@code Path -> Target}. */
  def routes: Map[PathMatcher, T] = {
    _routes.toMap
  }

  /**
    * This method does nothing if the path has already been added.
    * A path can only point to one target.
    */
  def addRoute(path: String, target: T): Router[T] = {
    val p: PathMatcher = new PathMatcher(path)
    if (_routes.contains(p)) {
      return this
    }
    _routes += (p->target)
    return this
  }

  /** Removes the route specified by the path. */
  def removePath(path: String) {
    val p: PathMatcher = new PathMatcher(path)
    _routes.remove(p)
  }

  /** @return { @code null} if no match; note: { @code queryParams} is not set in { @link RouteResult}*/
  def route(requestPath: String): RouteResult[T] = {
    if (StringUtils.isEmpty(requestPath)) {
      return null
    }
    val pathParams: mutable.Map[String, String] = new mutable.HashMap[String, String]
    import scala.collection.JavaConversions._
    for (entry <- routes.entrySet) {
      val path: PathMatcher = entry.getKey
      if (path.`match`(requestPath, pathParams)) {
        val target: T = entry.getValue
        return new RouteResult[T](target, pathParams.toMap)
      }
      pathParams.clear
    }
    return null
  }
}
