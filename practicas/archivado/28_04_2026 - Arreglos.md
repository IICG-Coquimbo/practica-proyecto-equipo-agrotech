## Arreglos a los scrapers 
- Incorporar comuna, región y especificar dentro de las etiquetas la fecha completa
- Separar las etiquetas de forma individual, considerando las etiquetas comunas para que conincida
- Agregar más etiquetas como la región y la comuna para comparar luego en la regresión

> Los datos estan restringidos a 2025, y a las comunas dentro de la región de Coquimbo

## Cambios en la etiquetas ("Nuevo Formato")
1.  integrante
2.  mes 
3.  año
4.  precio
5.  fecha_captura
6.  precio_producto
7.  temperatura
8.  comuna
9.  radiacion_uv
10. precio_petroleo
11. varp_fertilizante
12. humedad
13. producto
14. precipitaciones
15. usd

## Avances pendientes para la entrega. (miercoles y jueves)
### Individual
- [ ] Ajustar el scraper para agregar las etiquetas al nuevo formato
- [ ] Ordenar las etiquetas generales como esta en el formato (ej: mes, año, comuna, integrante, etc)

### Grupal
- [ ] Rellenar el README incluyendo
    - Situación y descripción de la problematica
    - Incluir la propuesta de valor
    - Análisis breve de las 4V Big data
    - Indicar las librerías ocupadas en el Dockerfile
    - Incluir pantallazos de los contenedores corriendo (Docker + servicios)
    - Incluir instrucciones para montar ejecutar el contenedor y los servicios. 
    - Dejar especificado la metodología de extración, los años y comunas escogidas y las decisiones de extracción que se realizaron. 

> Dejar especificado en el README como acceder a mongo Atlas (agregar ahi es password y ocultarlo del docker-compose y del main.py)



