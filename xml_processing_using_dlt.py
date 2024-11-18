# Databricks notebook source
# MAGIC %md
# MAGIC # Example of reading XML using DLT
# MAGIC
# MAGIC This code utilizes 
# MAGIC * the standard XML library
# MAGIC * select Expressions (so you can embed SQL logic in the dataframe)
# MAGIC * SQL higher order functions
# MAGIC * SQL table functions
# MAGIC * Autoloader
# MAGIC * Streaming tables
# MAGIC * SQL Functions
# MAGIC
# MAGIC Of course an alternative to higher order functions is to use dataframe constructs

# COMMAND ----------

from pyspark.sql.functions import expr, col, coalesce, concat_ws,flatten, concat,lit
import dlt


# COMMAND ----------

@dlt.view
def raw_events():
  return spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "xml") \
  .option("rowTag", "xs:element") \
  .option("cloudFiles.inferColumnTypes", True) \
  .option("cloudFiles.schemaLocation", "/Volumes/magnusp_catalog/database1/sources") \
  .option("cloudFiles.schemaEvolutionMode", "rescue") \
  .load("/Volumes/magnusp_catalog/database1/sources/model.xml")


# COMMAND ----------

@dlt.view
def select_fields():
  df= dlt.read_stream("raw_events").selectExpr ( 
        "_name",
        "`xs:complexType`.`xs:annotation`.`xs:documentation` as documentation", 
        "`xs:complexType`.`xs:attribute` as attributes", 
        "`xs:key`", 
        "`xs:keyref`", 
        "`xs:unique`"
      ).withColumnRenamed (
        "_name" , "tablename"
      )
  return df


# COMMAND ----------

# MAGIC %md
# MAGIC <p>
# MAGIC Instead of iterating the tree, I reformat the tree using higher order functions (in place). Higher order functions are very efficient and can save quite much resources vs unpacking a fairly complex structure.
# MAGIC </p>

# COMMAND ----------

@dlt.view
def simplify_xml_attributes():
  return dlt.read_stream("select_fields").select (
  "tablename",
  "documentation", 
  expr("""
    transform(
      attributes, x -> 
        named_struct(
          'fieldname', x._name, 
          'fieldtype',coalesce(
            concat('${',x._type, '}'), 
            substring(x.`xs:simpleType`.`xs:restriction`._base from 4)
          ), 
          'use', x._use, 
          'maxLength', x.`xs:simpleType`.`xs:restriction`.`xs:maxLength`._value,
          'enumeration', x.`xs:simpleType`.`xs:restriction`.`xs:enumeration`,
          'fractionDigits', x.`xs:simpleType`.`xs:restriction`.`xs:fractionDigits`,
          'totalDigits', x.`xs:simpleType`.`xs:restriction`.`xs:totalDigits`
        )
    ) as attributes 
  """), 
  expr("""
    transform(
      `xs:key`, x -> 
        named_struct(
          'name', x._name, 
          'fields', transform(
            x["xs:field"]["_xpath"], y -> 
              substring(y from 2)
          )
        )
    ) as primary_key
  """), 
  expr("""
    transform(
      `xs:keyref`, x -> 
        named_struct(
          'name', x._name,
          'refer',  x._refer,  
          'fields', transform(
            x["xs:field"]["_xpath"], y -> 
              substring(y from 2)
          ),
          'target', substring(x["xs:selector"]["_xpath"] from 4)
        )
    ) as foreign_key
  """),
   "xs:unique")

# COMMAND ----------

@dlt.table(
  
)
def erwin_tables_view():
  return dlt.read_stream("simplify_xml_attributes").select(
  "tablename", 
  concat_ws(
    ",", 
    expr("""
      transform(attributes,x -> 
        concat(
          x.fieldname , 
          ' ' , 
          replace(x.fieldtype, 'xs:', '')
        )
      ) 
    """)
  ).alias("fields"),
  col("primary_key.name")[0].alias("pk_name"), 
  concat_ws(
    ",",
    flatten(
      col("primary_key.fields")
    )
  ).alias("pk_fields"), 
  expr("""
       transform(foreign_key, x -> 
          named_struct(
            'name', x.name, 
            'fields' , concat_ws(',',x.fields), 
            'target', x.target, 
            'refer', x.refer
          )
       )
  """).alias("fk_fields")
)

# COMMAND ----------

# MAGIC %md
# MAGIC <p>
# MAGIC Generating surrogate keys are fairly hard in DLT. My approach uses a table function written in SQL and fetching the max of the previous run, such that I can generate a sequence of keys.
# MAGIC </p>

# COMMAND ----------

from pyspark.sql.functions import col, max, current_timestamp, row_number
from pyspark.sql.window import Window

@dlt.table(
    
)
def source_to_target_keys():
    # get the max id from the key table
    x = 0
    status = spark.sql("select magnusp_catalog.database1.table_exists('magnusp_catalog','dlt_example','source_to_target_keys')").collect()[0][0]
    if status == False:
        x = 0
    else:
        x = spark.read.table (
            "magnusp_catalog.dlt_example.source_to_target_keys"
        ).agg(
            max(
                col("generated_id")
            ).alias("max_generated_id")
        ).head()["max_generated_id"]+1 

    # count the number of rows in the input dataframe
    y = dlt.read("erwin_tables_view").count() 
    kdf = spark.sql("select * from magnusp_catalog.database1.generateIds({0}, {1})".format(x,y))
    df = dlt.read(
        "erwin_tables_view"
    ).withColumn(
        "source_id", 
        row_number().over(
            Window.orderBy(col("tablename"))
        )
    )
    return df.join(
        kdf, 
        "source_id",
        how="inner"
    ).select(
        col("tablename").alias("source_id"), 
        "generated_id"
    ).withColumn(
        "changed_ts", 
        current_timestamp()
    )

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

@dlt.table(
  comment="Tables that exist in the Erwin data model",
  # cluster_by=["table_id","table_name"]
)
def erwin_tables():
  df0 = dlt.read("source_to_target_keys")

  # use a ordered analytical function to generate a column that we can join on
  df = dlt.read (
    "erwin_tables_view"
  ).withColumn (
    "source_id", 
    col("tablename")
  )
  return df.join (
    df0, 
    "source_id", 
    how="inner"
  ).drop (
    "source_id"
  ).select (
    expr("generated_id as table_id"), 
    expr("tablename as table_name"), 
    "fields", 
    "pk_name", 
    "pk_fields", 
    "fk_fields"
  )
                                                                               
