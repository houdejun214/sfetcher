package com.sfetcher.core.route

import org.specs2.mutable.Specification

/**
  * Created by dejun on 26/2/16.
  */
class RouterTest extends Specification {

  "RouterTest" should {
    "removePath" in {
      val router: Router[AnyRef] = new Router[AnyRef]
      router.addRoute("http://www.amazon.com/s/*", "1")
      router.addRoute("http://www.amazon.com/sp/*", "2")
      router.removePath("http://www.amazon.com/sp/*")
      router.removePath("http://www.amazon.com/sp/*")
      router.routes.size === 1
    }

    "routes" in {
      val router: Router[AnyRef] = new Router[AnyRef]
      router.addRoute("http://www.amazon.com/s/*", "1")
      router.addRoute("http://www.amazon.com/sp/*", "2")
      var result: RouteResult[AnyRef] = router.route("http://www.amazon.com/s/ref=lp_7147441011_ex_n_1?rh=n%3A7141123011%2Cn%3A10445813011")
      result.target === "1"
      result = router.route("http://www.amazon.com/sp/ref=lp_7147441011_ex_n_1?rh=n%3A7141123011%2Cn%3A10445813011")
      result.target === "2"
    }

    "addRoute" in {
      val router: Router[AnyRef] = new Router[AnyRef]
      router.addRoute("http://www.amazon.com/s/*", "1")
      router.addRoute("http://www.amazon.com/sp/*", "2")
      router.routes.size must ===(2)
    }

    "route" in {
      ok
    }


  }
}
