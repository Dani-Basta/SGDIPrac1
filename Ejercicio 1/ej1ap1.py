# -*- coding: utf-8 -*-

#Ejercicio 1 apartado 1

"""
Daniel Bastarrica y Gabriel Sellés declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera desho-
nesta ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.
"""

from mrjob.job import MRJob

class MRMaxMinAvg (MRJob):
    
    def mapper(self, key, line):
        #Si la línea contiene el nombre de los campos, la obviamos.        
        if(line[0].isdigit()):
            #Parseamos la línea.
            fields=line.split(',')
            yield(fields[0][0:7], float(fields[8]))
        
    def reducer(self, key,values):
        
        #Buscamos el mínimo, el máximo y la media
        avg=0
        max=float('-inf')  #Menos infinito.
        min=float('inf')   #Infinito
        size= 0
        for value in values:
            avg=avg+ value
            size=size+1
            if max< value:
                max =value
            elif  min > value:
                min = value
                
        #Almacenamos el máximo, el mínimo y la media del mes.
        yield(key[5:7]+ "/" + key[0:4] ,{'max': max, 'avg': avg/size, 'min': min})
        
if __name__ == '__main__':
    MRMaxMinAvg.run()
