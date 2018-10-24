# -*- coding: utf-8 -*-
#Apartado 2 Ejercicio 1

'''Gabriel Sellés Salvà y Daniel Bastarrica Lacalle declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera deshonesta
ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.'''


from pyspark.sql import SparkSession
import sys

def main():
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext 

    lines = sc.textFile(sys.argv[1]) 

    #Diferenciamos cada uno de los campos que forman una línea del csv en una lista.
    #Eliminamos las ĺíneas que no almacenen un entero en el campo "batería" (la cabecera).
    lines=lines.filter(lambda x: x[0]!='d').map(lambda x:x.split(","))

    #Cogemos solo los aspectos que nos interesan: la fecha y la batería. De la fecha cogemos el mes y el año.
    lines=lines.map(lambda x: (x[0][0:7],float(x[8])))

    #Cambiamos el orden de las fechas para que encaje con la salida del enunciado
    lines=lines.map(lambda (x,y): (x[5:7]+ "/" + x[0:4],y))

    avg=lines.reduceByKey(lambda x,y: x+y)
    minimum=lines.reduceByKey(lambda x,y: min(x,y))
    maximum=lines.reduceByKey(lambda x,y: max(x,y))

    keys=lines.keys()			
    numK=keys.countByValue()	#Número de veces que aparece cada clave.

    avg=avg.map(lambda (x,y): (x,y/numK[x])) #Calculamos la media.
    result=maximum.join(avg).join(minimum)

    #Utilizamos el formato propuesto en el enunciado.
    result=result.map(lambda (x,((y,z),w)): (x,{'max':y,'avg':z, 'min':w}))
    
    #Resultado final.
    result= result.collect()
	
    for x in result:
	print (x)
   
    sc.stop()


if __name__ == "__main__":
    main()
