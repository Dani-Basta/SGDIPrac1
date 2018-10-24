# -*- coding: utf-8 -*-

#Ejercicio 1 apartado 3

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

class MRMaxMinAvgComb (MRJob):
    
    def mapper(self, key, line):
        #Si la línea contiene el nombre de los campos, la obviamos.        
        if(line[0].isdigit()):
            #Parseamos la línea.
            fields=line.split(',')
            yield(fields[0][0:7], {'max':float(fields[8]), 'min':float(fields[8]), 'acum':float(fields[8]), 'nelemns':1})
        
    def combiner(self, key,values):
        max=float('-inf')  #Menos infinito.
        min=float('inf')   #Infinito
        size=0
        acumul=0
        for value in values:
           acumul=value['acum']+acumul
           size=size+ value['nelemns']
           if max< value['max']:
               max =value['max']
           if  min > value['min']:
               min = value['min']
               
        yield(key ,{'max': max, 'min': min, 'acum': acumul, 'nelemns':size})

        
        
    def reducer(self, key,values):
        
        max=float('-inf')  #Menos infinito.
        min=float('inf')   #Infinito
        size=0
        acumul=0
        for value in values:
           acumul=value['acum']+acumul
           size=size+ value['nelemns']
           if max< value['max']:
               max =value['max']
           if  min > value['min']:
               min = value['min']
               

        #Almacenamos el máximo, el mínimo y la media del mes.
        yield(key[5:7]+ "/" + key[0:4] ,{'max': max, 'avg': acumul/size, 'min': min})
        
if __name__ == '__main__':
    MRMaxMinAvgComb.run()
