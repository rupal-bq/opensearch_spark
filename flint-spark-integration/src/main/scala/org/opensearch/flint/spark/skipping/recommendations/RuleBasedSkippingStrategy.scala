/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.Set

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.flint.{findField, loadTable, parseTableName}

/**
 * Data type based skipping index column and algorithm selection.
 */
class RuleBasedSkippingStrategy extends AnalyzeSkippingStrategy {

  /**
   * Recommend skipping index columns and algorithm.
   *
   * @param inputs
   *   inputs for recommendation strategy. This can table name, columns or functions.
   * @return
   *   skipping index recommendation dataframe
   */
  override def analyzeSkippingIndexColumns(
      inputs: Map[String, Map[String, Set[String]]],
      spark: SparkSession): Seq[Row] = {

    val tableName = inputs.keySet.toList(0)
    val table = getTable(tableName, spark)
    val partitionFields = getPartitionFields(table)
    val recommendations = new RecommendationRules

    val result = ArrayBuffer[Row]()
    val columns = getColumnsMap(table, inputs.get(tableName).get)
    columns.keySet.foreach(column => {
      val field = findField(table.schema(), column).get
      if (columns.get(column).get.size > 0) {
        columns
          .get(column)
          .foreach(functions => {
            functions.foreach(function => {
              result += Row(
                field.name,
                field.dataType.typeName,
                recommendations.getSkippingType(function),
                recommendations.getReason(function))
            })
          })
      } else if (partitionFields.contains(column)) {
        result += Row(
          field.name,
          field.dataType.typeName,
          recommendations.getSkippingType("PARTITION"),
          recommendations.getReason("PARTITION"))
      } else if (recommendations.containsRule(field.dataType.toString)) {
        result += Row(
          field.name,
          field.dataType.typeName,
          recommendations.getSkippingType(field.dataType.toString),
          recommendations.getReason(field.dataType.toString))
      }
    })
    result
  }

  private def getPartitionFields(table: Table): Array[String] = {
    table.partitioning().flatMap { transform =>
      transform
        .references()
        .collect({ case reference =>
          reference.fieldNames()
        })
        .flatten
        .toSet
    }
  }

  private def getColumnsMap(
      table: Table,
      columns: Map[String, Set[String]]): Map[String, Set[String]] = {
    if (columns.isEmpty) {
      val columnsMap = Map[String, Set[String]]()
      table
        .schema()
        .fields
        .map(field => {
          columnsMap += (field.name -> Set.empty)
        })
      columnsMap
    } else {
      columns
    }
  }

  private def getTable(tableName: String, spark: SparkSession): Table = {
    val (catalog, ident) = parseTableName(spark, tableName)
    loadTable(catalog, ident).getOrElse(
      throw new IllegalStateException(s"Table $tableName is not found"))
  }
}
