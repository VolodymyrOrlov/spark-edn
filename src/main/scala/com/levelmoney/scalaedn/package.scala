package com.levelmoney

import org.apache.spark.sql.DataFrameReader

package object scalaedn {

  implicit class EDNDataFrameReader(reader: DataFrameReader) {

    def edn(path: String) = reader.format("com.levelmoney.scalaedn").
      option("path", path).
      load

  }


}
