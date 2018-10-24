# -*- coding: utf-8 -*-
#Apartado 1 Ejercicio 3

'''Gabriel Sellés Salvà y Daniel Bastarrica Lacalle declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera deshonesta
ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.'''

#Dentro de utilities están los imports correspondientes (tanto de métodos propios como de librerías externas)
from utilities import createDataframe, episodes_path, scripts_path,calculatePearsonCorrelation

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.context import SparkContext
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.mllib.stat import Statistics
import csv 


def getLocationsAppearences(sparkObj):
        
    #Devuelve un dataframe que asocia los episodios (identicados por episode_id) con su nota en IMDB.
    episodes_df=createDataframe(sparkObj, ["_c0","_c9"],["episode_id","imdb_rating"], \
                                episodes_path, "id")
    #Devuelve un dataframe que que asocia episodios de los simpson (identificados por episode_id) con localizaciones (identificadas por location_id).
    locations_df=createDataframe(sparkObj,["_c7","_c1"],["location_id","episode_id"], \
                                scripts_path, "location_id") \
                                .dropDuplicates()
    
    locations_df=locations_df.groupBy("episode_id").count() 
    #El resultado es un dataframe que indica cuantas localizaciones aparecen en cada episodio
    #Moficamos el nombre de la columna count.
    locations_df= locations_df.selectExpr("episode_id as episode_id","count as num_locations")
    
    return episodes_df.join(locations_df, on="episode_id")

if __name__ == "__main__":
    #Creamos el objeto spark.
    spark = SparkSession \
        .builder \
        .appName("Practica1SGDI") \
        .getOrCreate()
    
    locations=getLocationsAppearences(spark)
    locations.show()
    
    #Apartado 5
    #Descomentar para ejecutar. El cálculo tarda considerablemente más
    print("Pearson correlation: " + calculatePearsonCorrelation(locations, "imdb_rating","num_locations" ))
    
    
