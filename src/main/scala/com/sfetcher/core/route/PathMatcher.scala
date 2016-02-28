package com.sfetcher.core.route

import java.util.regex.Pattern

import com.lakeside.core.resource.AntPathMatcher
import com.lakeside.core.utils.StringUtils

import scala.collection.mutable

/**
  * The path can contain constants or placeholders, example:
  * {@code constant1/:placeholder1/constant2/:*}.
  * {@code :*} is a special placeholder to catch the rest of the path
  * (may include slashes). If exists, it must appear at the end of the path.
  *
  * The path must not contain URL query, example:
  * {@code constant1/constant2?foo=bar}.
  *
  * The path will be broken to paths, example:
  * {@code ["constant1", ":variable", "constant2", ":*"]}
  */
object PathMatcher {

  private val PARAM_PATTERN: Pattern = Pattern.compile("\\{([^\\{\\}/]*)\\}")
  private val ANT_MATCHER = new AntPathMatcher

  def apply(path:String) = {
    new PathMatcher(path)
  }
}

/**
  * The path must not contain URL query, example:
  * {@code constant1/constant2?foo=bar}.
  *
  * The path will be stored without slashes at both ends.
  */
class PathMatcher(_path:String) extends Serializable{

  var path:String = null
  var antPathPattern:String = null
  initPath(_path)

  protected def initPath(_path:String) = {
    path = StringUtils.trim(_path, "/")
    antPathPattern = PathMatcher.PARAM_PATTERN.matcher(path).replaceAll("*")
  }

  override def hashCode: Int = {
    path.hashCode
  }

  override def equals(o: Any): Boolean = {
    if (o == null) {
      return false
    }
    o.asInstanceOf[PathMatcher].path == path
  }

  override def toString: String = path

  def `match`(queryPath: String): Boolean = `match`(queryPath, null)

  /**
    * {@code params} will be updated with params embedded in the path.
    *
    * Ant-style path patterns.
    *
    * This method signature is designed so that {@code pathTokens} and {@code params}
    * can be created only once then reused, to optimize for performance when a
    * large number of paths need to be matched.
    *
    * @return { @code false} if not matched; in this case params should be reset
    */
  def `match`(queryPath: String, params: mutable.Map[String, String]): Boolean = {
    if (PathMatcher.ANT_MATCHER.`match`(this.antPathPattern, queryPath)) {
      if (params != null) {
      }
      return true
    }
    false
  }
}
