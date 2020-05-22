package org.apache.hudi

import java.util
import java.util.Collections

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.common.config.SerializableConfiguration
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.log4j.LogManager
import org.apache.spark.api.java.function.MapPartitionsFunction
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}

object MapPartitionHelper {
  private val log = LogManager.getLogger(classOf[DefaultSource])

  def mapPartitions(rows: DataFrame, basePath: String, encoder: ExpressionEncoder[Row], serConfig: SerializableConfiguration, parallelism: Int)
  : Dataset[java.lang.Boolean] = {
    try {
      val basePathDir = new Path(basePath)
      val fs = FSUtils.getFs(basePath, serConfig.get)
      if (!fs.exists(basePathDir)) fs.mkdirs(basePathDir)
      /*rows.sort("partition", "key").coalesce(parallelism).mapPartitions(new MapPartitionsFunction[Row, Boolean] {
        override def call(input: util.Iterator[Row]): util.Iterator[Boolean] = {
          Collections.singleton(true).iterator()
        }
      })*/
      null
    } catch {
      case e: Exception =>
        System.err.println("Exception thrown in WriteHelper " + e.getCause + " ... " + e.getMessage)
        e.printStackTrace()
        throw e
    }
  }
}
