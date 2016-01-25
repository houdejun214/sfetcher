package fetcher

import com.sdata.core.{FetchDatum, FetchDispatch}
import com.sdata.core.fetcher.SdataFetcher

/**
 * Created by dejun on 21/11/14.
 */
class ScalaFetcher extends SdataFetcher{

  /**
   * fetch datum list
   * @param dispatch
   */
  override def fetchDatumList(dispatch: FetchDispatch): Unit = {
    while(true) {

    }
  }

  /**
   * fetch a single datum
   * @param datum
   * @return
   */
  override def fetchDatum(datum: FetchDatum): FetchDatum = {

    return datum;
  }


  def select(selector:String) {

  }

  def iterator: Unit = {

  }

  def links: Unit = {

  }

  def datum: Unit = {

  }
}
