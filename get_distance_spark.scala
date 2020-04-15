import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, sqrt, pow}

val estacoes_df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("estacoes.csv")

val imoveis_df = spark.read.format("parquet").option("inferSchema", "true").option("header", "true").load("imoveis_20200402.gz.parquet")

def getDistance(longit_a:Column, latit_a:Column, longit_b:Column, latit_b:Column): (Column) = {
val delta_longit = abs(longit_b-longit_a)*60*1852
val delta_latit = abs(latit_b-latit_a)*60*1852
val distance = sqrt(pow(delta_longit, 2))+sqrt(pow(delta_latit, 2))
return distance
}

val estacoes_imoveis_join = imoveis_df.crossJoin(estacoes_df.withColumnRenamed("lat", "estacaoLat").withColumnRenamed("lon", "estacaoLon"))

val df_with_distance = estacoes_imoveis_join.withColumn("distance", getDistance($"lon", $"lat", $"estacaoLon", $"estacaoLat"))

val df_rua_not_null = df_with_distance.filter("rua is not null")

val df_lat_lon_not_null = df_rua_not_null.filter("lat is not null or lon is not null")

val df_min_distance = df_lat_lon_not_null.groupBy("dataid").agg(min("distance")).withColumnRenamed("min(distance)", "distance")

val df_result = df_min_distance.join(df_lat_lon_not_null, Seq("dataid", "distance"))

val result = df_result.select("dataid", "rua", "estacao", "estacaoLat", "estacaoLon", "distance")

result.coalesce(1).write.format("parquet").option("compression", "gzip").save("estacoes_imoveis.gzip")

result.coalesce(1).write.format("parquet").option("compression", "snappy").save("estacoes_imoveis.snappy")

result.coalesce(1).write.mode("overwrite").format("parquet").option("compression", "snappy").save("estacoes_imoveis.snappy")

