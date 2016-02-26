package com.sfetcher.core.route

import com.lakeside.core.utils.StringUtils
import org.specs2.mutable.Specification

/**
  * Created by dejun on 26/2/16.
  */
class PathTest extends Specification {

  "PathTest" should {
    "match" in {
      val url: String = "http://www.amazon.com/s/ref=lp_7147441011_ex_n_1?rh=n%3A7141123011%2Cn%3A10445813011"
      val path: Path = new Path("http://www.amazon.com/s/*")
      path.`match`(url) must beTrue
    }

    "match2" in {
      val url: String = "https://hanxierka.tmall.com/category-1046631575.htm?search=y&catName=%B4%BA%CF%C4%D7%B0%D0%C2%C6%B7%C7%F8"
      val path: Path = new Path("https://hanxierka.tmall.com/category-{id}.htm?*")
      path.`match`(url) must beTrue
      path.`match`("https://hanxierka.tmall.com/category.htm?search=y&catName=%B4%BA%CF%C4%D7%B0%D0%C2%C6%B7%C7%F8") must beFalse
    }

    "path" in {
      val url: String = "https://hanxierka.tmall.com/category-1046631575.htm?search=y&catName=%B4%BA%CF%C4%D7%B0%D0%C2%C6%B7%C7%F8"
      val path = new Path("https://hanxierka.tmall.com/category-{name}-{id}.htm?*")
      val paths: Array[String] = StringUtils.split(url, "/")
      ok
    }

    "toString" in {
      ok
    }

    "equals" in {
      ok
    }

    "hashCode" in {
      ok
    }

  }
}
