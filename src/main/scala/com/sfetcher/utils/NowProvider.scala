package com.sfetcher.utils;

import org.joda.time.DateTime;

class NowProvider {
  def nowMillis = System.currentTimeMillis()
  def now = new DateTime()
}