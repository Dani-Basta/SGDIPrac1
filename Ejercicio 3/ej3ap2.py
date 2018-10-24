# -*- coding: utf-8 -*-
#Apartado 2 Ejercicio 3

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

from utilities import createDataframe, scripts_path, episodes_path, characters_path, calculatePearsonCorrelation

def getCharactersAppearences(sparkObj):
    #Devuelve un dataframe que asocia los episodios (identicados por episode_id) con su nota en IMDB.
    episodes_df=createDataframe(sparkObj, ["_c0","_c9"],["episode_id","imdb_rating"], \
                                episodes_path, "id")
    characters_df=createDataframe(sparkObj, ["_c0","_c3"],["character_id","gender"], \
                                 characters_path,"id")


    script_df=createDataframe(sparkObj, ["_c1","_c6"],["episode_id", "character_id"], \
                              scripts_path, "episode_id").dropDuplicates()

    #No eliminamos los valores erróneos en character_id. Estos se 
    #eliminarán cuando se hagan los joins correspondientes.
    characters=episodes_df.join(script_df,on="episode_id") \
                          .join(characters_df, on= "character_id")
    
    #Contendrá cuántos personajes masculinos y femeninos hay en cada episodio.
    genders=characters.groupBy("episode_id", "gender").count() 
    #Contendrá cuantos personajes (independientemente del género) hay en cada episodio.
    num_characters= characters.groupBy("episode_id").count()

    #Eliminamos las columnas genders.
    characters=characters.select(["episode_id","imdb_rating"])

    '''
    Cogemos los personajes masculinos. Cogemos las columnas que nos interesan (el id del episodios y el número 
    de personajes masculinos que aparecen el él) y renombramos las columnas.
    '''    
    m= genders.filter(genders.gender == "m") \
              .select(["episode_id","count"]) \
              .selectExpr("episode_id as episode_id", "count as male_characters")

    '''
    Cogemos los personajes femeninos. Cogemos las columnas que nos interesan (el id del episodios y el número 
    de personajes femeninos que aparecen el él) y renombramos las columnas.
    '''
    f= genders.filter(genders.gender == "f") \
       .select(["episode_id","count"]) \
       .selectExpr("episode_id as episode_id", "count as female_characters")


    #Unimos los dataframes resultantes y eliminamos duplicados.
    return characters.join(num_characters, on= "episode_id") \
                         .join(m, on="episode_id") \
                         .join(f, on= "episode_id") 


if __name__ == "__main__":
    #Creamos el objeto spark.
    spark = SparkSession \
        .builder \
        .appName("Practica1SGDI") \
        .getOrCreate()
    
    characters=getCharactersAppearences(spark)
    characters.show()
    
    #Apartado 5
    #Descomentar para ejecutar. El cálculo tarda considerablemente 
    print("Pearson correlation (imdb_rating/count): " + calculatePearsonCorrelation(characters, "imdb_rating","count" ))
    print("Pearson correlation (imdb_rating/female_characters): " + calculatePearsonCorrelation(characters, "imdb_rating","female_characters" ))
    print("Pearson correlation (imdb_rating/male_characters): " + calculatePearsonCorrelation(characters, "imdb_rating","male_characters" ))

