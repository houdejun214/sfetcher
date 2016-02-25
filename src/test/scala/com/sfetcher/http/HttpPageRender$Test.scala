package com.sfetcher.http

import com.lakeside.core.utils.{PathUtils, ApplicationResourceUtils}
import org.specs2.mutable.Specification

/**
 * Created by dejun on 13/2/16.
 */
class HttpPageRender$Test extends Specification {

  "HttpPageRender render" >> {
    val testRoot: String = ApplicationResourceUtils.getClassRoot
    val home: String = PathUtils.join(testRoot, "../../src/main")
    sys.props += ("SDATA_HOME"->home)

    val content = PhantomJSRender.render("https://www.google.com.sg/?gfe_rd=cr&ei=6Sa_VtS9DMGAoAPU0Z-wCw&gws_rd=ssl")
    content must startWith("<!DOCTYPE html>")
  }

}
