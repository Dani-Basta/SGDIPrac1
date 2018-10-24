# -*- coding: utf-8 -*-
#Apartado 4 Ejercicio 3


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

from utilities import createDataframe, scripts_path, episodes_path, happiness_path, calculatePearsonCorrelation

#Función que calcula la felicidad asociada a un texto
def countHappiness(text):
    try:
        count=0
        for x in str(text).split():
            if str(x) in words.value:
                count= count + float(words.value[x])        
        return count     
    except:
        return 0
    

#Lee el contenido de happinnes.txt y lo vuelca en un diccionario.
def readHappiness():
    words={}
    
    with open(happiness_path) as csvfile:
        csvreader= csv.reader(csvfile, delimiter= '\t') #El delimitador es \t --> Tabulador.
        for row in csvreader:
            words[row[0]]=row[2]
    
    return words
        

def calculateSentiment(sparkObj):
        
    script_df=createDataframe(sparkObj,["_c1","_c11"],["episode_id","text"], \
                                scripts_path, "episode_id")

    #Necesario su uso para aplicar una función propia a una columna entera.
    countHappiness_udf =udf(lambda x: countHappiness(x) )
    script_df=script_df.withColumn('text', countHappiness_udf("text"))

    #Ahora, en "script_df", tenemos varias líneas para cada episodio con su respectiva
    #sentimiento. Para obtener el sentimiento de un episodio, agrupamos las líneas por episodio
    #y sumamos los valores de las columnas.
    #Primero, parseamos la columna text a double.
    script_df= script_df.withColumn("text", script_df["text"].cast("double"))
    script_df=script_df.groupBy("episode_id").sum("text")
    #Renombramos la columna.
    script_df=script_df.withColumnRenamed("sum(text)", "Happiness")

    episodes_df=createDataframe(sparkObj, ["_c0","_c9"],["episode_id","imdb_rating"], \
                        episodes_path, "id")
    

    return episodes_df.join(script_df, on="episode_id")


if __name__ == "__main__":
    #Creamos el objeto spark.
    spark = SparkSession \
        .builder \
        .appName("Practica1SGDI") \
        .getOrCreate()
    
    #Creamos la variable broadcast
    sc = SparkContext.getOrCreate()
    words=sc.broadcast(readHappiness())
    
    sentiment=calculateSentiment(spark)
    sentiment.show()
    
     #Apartado 5
    #Descomentar para ejecutar. El cálculo tarda considerablemente 
    print("Pearson correlation (imdb_rating/happiness): " + calculatePearsonCorrelation(sentiment, "imdb_rating","Happiness"))
