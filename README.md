# Big-Data-Lab
Laboratorio Big Data TET

## Se tiene un conjunto de acciones de la bolsa, en la cual se reporta a diario el valor promedio por acción.

    company,price,date

    exito,77.5,2015-01-01
    EPM,23,2015-01-01
    exito,80,2015-01-02
    EPM,22,2015-01-02
 
* Los datos se encuentran en dataempresas.csv

* Realizar un programa en Map/Reduce, con hadoop en Python, que permita calcular:

1. Por acción, dia-menor-valor, día-mayor-valor: **diaMin-diaMax.py**
2. Listado de acciones que siempre han subido o se mantienen estables: **acciones-estables.py**
3. DIA NEGRO: Saque el día en el que la mayor cantidad de acciones tienen el menor valor de acción (DESPLOME), suponga una inflación independiente del tiempo: **dia-negro.py**

* Para correr los programas se puede hacer siguiendo el comando desde la linea de comandos: 
* $ **python cualquiera.py dataempresas.csv**


