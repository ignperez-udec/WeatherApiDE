from pyspark.sql import SparkSession, DataFrame # type: ignore
from pyspark.sql.functions import avg, min, max, sum, when, count, floor  # type: ignore
from silver.read_weather_hist_from_db import read_weather_hist
from silver.load_koppen_model_to_db import load_koppen_model

def koppen_class(data: DataFrame) -> DataFrame:
    data = data.withColumn('koppen_class',
        when(
            data['p'] < data['p_threshold'],
            'B'
        )
        .when(
            data['t_min'] >= 18,
            'A'
        )
        .when(
            (data['t_min'] > -3) & 
            (data['t_min'] <= 18),
            'C'
        )
        .when(
            (data['t_max'] >= 10) & 
            (data['t_min'] <= -3),
            'D'
        )
        .when(
            data['t_max'] < 10, 
            'E'
        )
    )

    return data

def koppen_subclass(data: DataFrame) -> DataFrame:
    data = data.withColumn('koppen_subclass',
        when(
            data['koppen_class'] == 'A', 
            when(
                data['p_driest'] >= 60, 
            'Af'
            )
            .when(
                (data['p_driest'] < 60) &
                (data['p_driest'] >= 100 - data['p'] / 25),
            'Am'
            )
            .when(
                (data['p_driest'] < 60) &
                (data['p_driest'] < 100 - data['p'] / 25), 
            'Aw'
            )
        )
        .when(
            data['koppen_class'] == 'B', 
            when(
                (data['p'] < data['p_threshold'] / 2) &
                (data['t'] >= 18),
            'BWh'
            )
            .when(
                (data['p'] < data['p_threshold'] / 2) &
                (data['t'] < 18),
            'BWk'
            )
            .when(
                (data['p_threshold'] / 2 <= data['p']) & 
                (data['p'] < data['p_threshold']) &
                (data['t'] >= 18),
            'BSh'
            )
            .when(
                (data['p_threshold'] / 2 <= data['p']) & 
                (data['p'] < data['p_threshold']) &
                (data['t'] < 18),
            'BSk'
            )
        )
        .when(
            data['koppen_class'] == 'C',
            when(
                (data['p_summer_driest'] < 40) &
                (data['p_summer_driest'] < data['p_winter_wettest'] / 3) &
                (data['t_max'] >= 22),
            'Csa'
            )
            .when(
                (data['p_summer_driest'] < 40) &
                (data['p_summer_driest'] < data['p_winter_wettest'] / 3) &
                (data['t_max'] < 22) &
                (data['t_mon10'] >= 4),
            'Csb'
            )
            .when(
                (data['p_summer_driest'] < 40) &
                (data['p_summer_driest'] < data['p_winter_wettest'] / 3) &
                (data['t_mon10'] >= 1) &
                (data['t_mon10'] < 4),
            'Csc'
            )
            .when(
                (data['p_winter_driest'] < data['p_summer_wettest'] / 10) &
                (data['t_max'] >= 22),
            'Cwa'
            )
            .when(
                (data['p_winter_driest'] < data['p_summer_wettest'] / 10) &
                (data['t_max'] < 22) &
                (data['t_mon10'] >= 4),
            'Cwb'
            )
            .when(
                (data['p_winter_driest'] < data['p_summer_wettest'] / 10) &
                (data['t_mon10'] >= 1) &
                (data['t_mon10'] < 4),
            'Cwc'
            )
            .when(
                (data['t_max'] >= 22),
            'Cfa'
            )
            .when(
                (data['t_max'] < 22) &
                (data['t_mon10'] >= 4),
            'Cfb'
            )
            .when(
                (data['t_mon10'] >= 1) &
                (data['t_mon10'] < 4),
            'Cfc'
            )
        )
        .when(
            data['koppen_class'] == 'D',
            when(
                (data['p_summer_driest'] < 40) &
                (data['p_summer_driest'] < data['p_winter_wettest'] / 3) &
                (data['t_max'] >= 22),
            'Dsa'
            )
            .when(
                (data['p_summer_driest'] < 40) &
                (data['p_summer_driest'] < data['p_winter_wettest'] / 3) &
                (data['t_max'] < 22) &
                (data['t_mon10'] >= 4),
            'Dsb'
            )
            .when(
                (data['p_summer_driest'] < 40) &
                (data['p_summer_driest'] < data['p_winter_wettest'] / 3) &
                (data['t_min'] < -38),
            'Dsd'
            )
            .when(
                (data['p_summer_driest'] < 40) &
                (data['p_summer_driest'] < data['p_winter_wettest'] / 3),
            'Dsc'
            )
            .when(
                (data['p_winter_driest'] < data['p_summer_wettest'] / 10) &
                (data['t_max'] >= 22),
            'Dwa'
            )
            .when(
                (data['p_winter_driest'] < data['p_summer_wettest'] / 10) &
                (data['t_max'] < 22) &
                (data['t_mon10'] >= 4),
            'Dwb'
            )
            .when(
                (data['p_winter_driest'] < data['p_summer_wettest'] / 10) &
                (data['t_min'] < -38),
            'Dwd'
            )
            .when(
                (data['p_winter_driest'] < data['p_summer_wettest'] / 10),
            'Dwc'
            )
            .when(
                (data['t_max'] >= 22),
            'Dfa'
            )
            .when(
                (data['t_max'] < 22) &
                (data['t_mon10'] >= 4),
            'Dfb'
            )
            .when(
                (data['t_min'] < -38),
            'Dfd'
            )
            .otherwise('Dfc')
        )
        .when(
            data['koppen_class'] == 'E',
            when(
                (data['t_max'] > 0) &
                (data['t_max'] <= 10),
                'ET'
            )
            .when(
                data['t_max'] <= 0,
                'EF'
            )
        )
    )

    return data

def get_koppen_classification():
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("Koppen Climate Classification") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

    query = """SELECT
                    cod_location,
                    EXTRACT(YEAR FROM time) AS year,
                    EXTRACT(MONTH FROM time) AS month,
                    temperature_mean AS temperature,
                    precipitation_sum AS precipitation
                FROM silver.weather_hist"""
    
    daily_weather = read_weather_hist(spark, query)
    daily_weather = daily_weather.withColumn('decade', floor(daily_weather['year'] / 10) * 10)

    monthly_weather = daily_weather.groupBy('cod_location', 'decade', 'month') \
        .agg(
            avg('temperature').alias('temperature_mean'),
            sum('precipitation').alias('precipitation_total')
        )

    monthly_weather = monthly_weather.withColumn('semester',
        when(monthly_weather['month'].isin(10, 11, 12, 1, 2, 3), 'Hot')
        .when(monthly_weather['month'].isin(4, 5, 6, 7, 8, 9), 'Cold')
    )

    monthly_weather = monthly_weather.withColumn('season',
        when(monthly_weather['month'].isin(12, 1, 2), 'Summer')
        .when(monthly_weather['month'].isin(3, 4, 5), 'Autumn')
        .when(monthly_weather['month'].isin(6, 7, 8), 'Winter')
        .when(monthly_weather['month'].isin(9, 10, 11), 'Spring')
    )

    semesterly_weather = monthly_weather.groupBy('cod_location', 'decade', 'semester') \
        .agg(
            sum('precipitation_total').alias('precipitation_total')
        )

    hottest_semesterly_weather = semesterly_weather.filter(semesterly_weather['semester']=='Hot') \
        .withColumnRenamed('precipitation_total', 'p_hot_semester') \
        .drop('semester')
    coldest_semesterly_weather = semesterly_weather.filter(semesterly_weather['semester']=='Cold') \
        .withColumnRenamed('precipitation_total', 'p_cold_semester') \
        .drop('semester')

    koppen_metrics = monthly_weather.groupBy('cod_location', 'decade') \
        .agg(
            min('temperature_mean').alias('t_min'),
            max('temperature_mean').alias('t_max'),
            avg('temperature_mean').alias('t'),
            sum('precipitation_total').alias('p'),
            count(when(monthly_weather['temperature_mean'] > 10, 1)).alias('t_mon10'),
            min('precipitation_total').alias('p_driest'),
            min(when(monthly_weather['season'] == 'Summer', monthly_weather['precipitation_total'])).alias('p_summer_driest'),
            min(when(monthly_weather['season'] == 'Winter', monthly_weather['precipitation_total'])).alias('p_winter_driest'),
            max(when(monthly_weather['season'] == 'Summer', monthly_weather['precipitation_total'])).alias('p_summer_wettest'),
            max(when(monthly_weather['season'] == 'Winter', monthly_weather['precipitation_total'])).alias('p_winter_wettest'),
        )

    koppen_metrics = koppen_metrics.join(hottest_semesterly_weather, on=['cod_location', 'decade'], how='left')
    koppen_metrics = koppen_metrics.join(coldest_semesterly_weather, on=['cod_location', 'decade'], how='left')

    koppen_metrics = koppen_metrics.withColumn('p_threshold',
        when(koppen_metrics['p_hot_semester']/koppen_metrics['p'] >= 0.7, 20 * koppen_metrics['t'] + 280)
        .when(koppen_metrics['p_cold_semester']/koppen_metrics['p'] >= 0.7, 20 * koppen_metrics['t'])
        .otherwise(20 * koppen_metrics['t'] + 140)
    )

    koppen_metrics = koppen_metrics.drop('p_hot_semester', 'p_cold_semester')

    koppen_metrics = koppen_class(koppen_metrics)
    koppen_metrics = koppen_subclass(koppen_metrics)

    load_koppen_model(spark, koppen_metrics)

if __name__ == "__main__":
    get_koppen_classification()