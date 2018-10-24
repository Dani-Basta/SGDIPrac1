# -*- coding: utf-8 -*-

#Apartado 3 Ejercicio 3

'''Gabriel Sellés Salvà y Daniel Bastarrica Lacalle declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera deshonesta
ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.'''

#Dentro de utilities están los imports correspondientes (tanto de métodos propios como de librerías externas)

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.context import SparkContext
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.mllib.stat import Statistics
import csv 

from utilities import createDataframe, scripts_path, episodes_path, calculatePearsonCorrelation



def getLinesAppearences(sparkObj):
        episodes_df=createDataframe(sparkObj, ["_c0","_c9"],["episode_id","imdb_rating"], \
                                episodes_path, "id")
        script_df=createDataframe(sparkObj,["_c1","_c12"],["episode_id","spoken_words"], \
                                scripts_path, "episode_id")
    
        #Obtenemos el número de diálogos de cada episodio.
        num_lines=script_df.groupBy("episode_id").count() 
        
        '''
        Para obtener el número de palabras que aparecen en cada episodio, es necesario parsear
        el contenido de la columna "spoken words", que es tipo String. Lo parseamos a tipo Double
        para poder sumar su contenido. 
        '''
        num_words= script_df.withColumn("spoken_words", script_df["spoken_words"].cast("double")) \
                            .groupBy("episode_id").sum("spoken_words")
        
        #Obtenemos el resultado final y renombramos las columnas.
        return episodes_df.join(num_words, on="episode_id") \
                .join(num_lines, on ="episode_id") \
                .withColumnRenamed("sum(spoken_words)", "Total_spoken_words") \
                .withColumnRenamed("count", "Total_lines")
        
        return episodes_df

if __name__ == "__main__":
    #Creamos el objeto spark.
    spark = SparkSession \
        .builder \
        .appName("Practica1SGDI") \
        .getOrCreate()
    
    script=getLinesAppearences(spark)
    script.show()

    #Apartado 5
    #Descomentar para ejecutar. El cálculo tarda considerablemente 
    print("Pearson correlation (imdb_rating/total_spoken_words): " + calculatePearsonCorrelation(script, "imdb_rating","Total_spoken_words"))
    print("Pearson correlation (imdb_rating/Total_lines): " + calculatePearsonCorrelation(script, "imdb_rating","Total_lines"))

