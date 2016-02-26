package com.sfetcher.core.route

import scala.collection.mutable;

class RouteResult[T](val _target: T,
                     val _pathParams:Map[String, String],
                     val _queryParams:Map[String, Seq[String]]
                    ) {

  /** The maps will be wrapped in Collections.unmodifiableMap. */
  def this(target: T, pathParams: Map[String, String]) {
    this(target, pathParams, null)
  }

  def target = _target

  def found: Boolean = {
    target != null
  }

  /** Returns all params embedded in the request path. */
  def pathParams = _pathParams

  /** Returns all params in the query part of the request URI. */
  def queryParams = _queryParams

  /**
    * Extracts the first matching param in {@code queryParams}.
    *
    * @return { @code null} if there's no match
    */
  def queryParam(name: String): String = {
    val values: Seq[String] = _queryParams(name)
    if (values == null) null else values(0)
  }

  /**
    * Extracts the param in {@code pathParams} first, then falls back to the first matching
    * param in {@code queryParams}.
    *
    * @return { @code null} if there's no match
    */
  def param(name: String): String = {
    val pathValue: String = pathParams(name)
    if (pathValue == null) queryParam(name) else pathValue
  }

  /**
    * Extracts all params in {@code pathParams} and {@code queryParams} matching the name.
    *
    * @return Unmodifiable list; the list is empty if there's no match
    */
  def params(name: String): Seq[String] = {
    val values: Seq[String] = queryParams(name)
    val value: String = pathParams(name)
    if (values == null) {
      return if (value == null) Seq() else Seq(value)
    }
    if (value == null) {
      values
    }
    else {
      val aggregated: mutable.Buffer[String] = new mutable.ListBuffer[String]()
      aggregated ++= (values)
      aggregated += (value)
      aggregated.toSeq
    }
  }
}
