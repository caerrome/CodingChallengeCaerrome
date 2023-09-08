# CodingChallengeCaerrome
- 05/09/2023: Primera prueba, acercxamiento a control de envio de datos por medio de POST y POSTMAN, integrado a base de datos en Spark (Sujeto a software que mas conozco), se completa el primer punto del ejercicio a falta de migrar el desarrollo a salida SQL
- Siguiente paso, implementar 2 parte del ejercicio y modificar la base de datos a SQL
- 06/09/2023: Se implementa la base de datos de SQL, junto con la subida de la data historica y de batch en dicha base, ademas de obtener los requerimientos de datos de SQL, por el momento en formato json
- Siguiente paso, implementar salida en formato tabla, ajustar, organizar codigo, doucmentar, implementar test y pasar al punto adicional 
-07/09/2023: Se ajusta y organiza el codigo, se implementan los primeros test del desarrollo (existe fallo en los metodos batch), se ajusta el proceso para ser dinamico a fin de que sea escalable con lo que respecta a tablas y configurable en lo que respecta a parametros de la base de datos y querys de salida, se implementa el docker file y se genra el container en local.
- Siguiente paso terminar test del modulo de src, por temas de tiempo se avanzar hasta lo mayor posible.  
-08/09/2023: Se ajustan los test_unitarios, se obtiene la estructura mas los test relacionados a escritura en batch generan problemas, se mantiene con status error, se debe de ajustar, se genera el docker y las pruebas de ejecución, se debe revisar ya que la conexión a las solicitudes no funciona de forma adecuada.
- Siguiente paso ajustar test de batch y conexión con docker 