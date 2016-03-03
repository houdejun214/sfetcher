package com.sfetcher.core

import org.scalatest.FreeSpec

/**
  * Created by dejun on 28/2/16.
  */
class HttpPathTest extends FreeSpec {

  "HttpPath Regex " in {
    val regex: String = "^((\\w+) )?https?:\\/\\/.*$"
    assert("POST http://google.com".matches(regex))
    assert("GET http://google.com".matches(regex))
    assert("GET https://google.com".matches(regex))
    assert(!"GEThttp://google.com".matches(regex))
    assert(!"POSThttp://google.com".matches(regex))
  }

  "HttpPath complain object" in {
    val path: Path = Path("POST http://google.com")
    assert(path.isInstanceOf[HttpPath])
  }

  "HttpPath Constructor" in {
    val path = new HttpPath("POST http://google.com")
    assert(path.method == "POST")
    assert(path.path == "http://google.com")
  }

  "HttpPath Parameters" in {
    val path: HttpPath = new HttpPath("POST http://google.com")
      .withParam("a" -> "A")
      .withParam("b"->"B")
    assert(path.parameters.size == 2)
  }

  "HttpPath with parameters string" in {
    val path: HttpPath = new HttpPath("POST http://google.com")
      .withParams("a=A&b=B&c=C")
    assert(path.parameters.size == 3)

    val path1: HttpPath = new HttpPath("POST http://google.com")
      .withParams("gmBean.anNum=1493&gmBean.anType=TMZCSQ&gmBean.regNum=&gmBean.pageNum=&pagenum=2&pagesize=15&sum=10098&countpage=674&goNum=1")
    assert(path1.parameters.size == 9)
  }

}
