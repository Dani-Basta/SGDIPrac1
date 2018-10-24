# -*- coding: utf-8 -*-

'''Gabriel Sellés Salvà y Daniel Bastarrica Lacalle declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera deshonesta
ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.'''

#Conjunto de funciones, variables y imports que se utilizan en los distintos apartados del ejercicio 3
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.context import SparkContext
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.mllib.stat import Statistics
import csv 

'''
Variables que contienen las ubicaciones de distintos ficheros de interés para la práctica.
Modificar aquí para cambiar el path de los .csv de los distintos apartados.
'''
episodes_path="simpsons_episodes.csv"
characters_path="simpsons_characters.csv"
scripts_path= "simpsons_script_lines.csv"
happiness_path="happiness.txt"

'''
Crea un dataframe a partir de un csv situado en path.
Parámetros:

-sparkObj: objeto spark creado a partir de una sesión SparkSession. Se utilizará para leer el fichero.csv 
y a partir de él crear el dataframe.

-columns: array que indica las columnas relevantes que no se eliminarán del dataframe original. Dada la implementación
de la función, debe tener tamaño 2.

-names: nombres que recibirán las columnas del dataframe original. En la primera posición de la lista, aparecerá el nombre 
que recibirá la columna columns[0]. En la segunda posición, la que recibirá columns[1].

-path: ubicación del fichero .csv a partir del cual se creará el dataframe.

-id_name: cadena que caracteres para el filtrado de la fila que contiene los nombres de los parámetros.
En función del dataframe, el contenido de la fila a eliminar es distinto. Por ello, esta cadena de caracteres
debe contener el criterio de búsqueda para el filtrado de dicha fila.
'''

def createDataframe(sparkObj, columns, names, path, id_name):
    '''
    1.Cargamos el dataframe correspondiente. Se cargará a partir de un csv que se encuentre en path.
    2.Cogemos las columnas que nos interesan: las que se marcan en columns.
    3.Eliminamos la instancia que contiene los nombres de las columnas. Como el criterio de búsqueda dependerá del problema, 
    introducimos dicho criterio de búsqueda a través del parámetro id_name.
    4.Cambiamos el nombre de las columnas. Se intercambiarán por los nombres del parámetro names.
    '''
    
    df= sparkObj.read.csv(path) 
    return df.select(columns) \
             .filter(df[columns[0]]!=id_name) \
             .selectExpr(columns[0] + " as " + names[0], columns[1] + " as " + names[1])
        
'''
Calcula el coeficiente de correlación de pearson.

-dataframe: dataframe que contiene las columnas con las variables que usaremos para calcular el coeficiente.

-var1: nombre de la primera columna en la que están los datos para calcular el coeficiente. 

-var2: nombre de la segunda columna en la que están los datos para calcular el coeficiente. 

'''
def calculatePearsonCorrelation(dataframe,var1, var2):
    #Convertimos las columnas con las variables que nos interesan en rdd.
    #Luego, parseamos su valor de Row(x) a x.
    rdd1=dataframe.select(var1).rdd.map(lambda x: float(x.__getitem__(var1))) 
    rdd2=dataframe.select(var2).rdd.map(lambda x: float(x.__getitem__(var2)))
    return str(Statistics.corr(rdd1,rdd2,method="pearson"))
