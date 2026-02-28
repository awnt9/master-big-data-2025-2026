# Clase práctica: Plataforma IoT de monitorización ambiental

## Contexto

Trabajas para **SensorHub S.L.**, una empresa que instala dispositivos IoT en edificios para monitorizar la calidad del aire interior. Los sensores están repartidos por salas de reuniones, oficinas y almacenes.

Cada sensor envía lecturas individuales a una API REST en tiempo real: temperatura, humedad y CO2. El equipo de datos necesita reportes CSV agregados por hora guardados en un almacenamiento de objetos (MinIO), para que los científicos de datos puedan descargarlos y analizarlos sin tocar la base de datos de producción.

**Tu misión: implementar esta plataforma.**

---

## Endpoints

### `GET /health`
Devuelve un JSON simple confirmando que la API está en marcha.

### `POST /readings`
Recibe una lectura de sensor en JSON y la guarda en MongoDB. Los campos mínimos son `device_id`, `location`, `temperature`, `humidity` y `co2`. El campo `timestamp` puede ser opcional (si no llega, usa la hora actual).

### `GET /readings`
Lista las lecturas guardadas. Debe permitir filtrar por `device_id` y limitar el número de resultados.

### `GET /readings/stats`
Devuelve estadísticas agregadas por dispositivo: número de lecturas, temperatura media y desviación típica, humedad media, CO2 medio y máximo. Usa **pandas** y **numpy**.

### `GET /export`
Descarga todas las lecturas crudas en formato CSV como fichero adjunto.

### `POST /reports/generate`
El núcleo del sistema. Acepta opcionalmente un parámetro `hour` (datetime ISO) para indicar qué hora procesar; si no se pasa, usa la hora actual. Consulta MongoDB para ese rango horario, agrega los datos por dispositivo y ubicación con pandas y numpy, genera un CSV en memoria y lo sube al bucket de MinIO. Devuelve la clave del objeto y el enlace.

### `GET /reports`
Lista los reportes CSV disponibles en el bucket de MinIO con su nombre, tamaño y fecha de modificación.

### `GET /reports/{report_name}`
Descarga un reporte concreto desde MinIO como fichero CSV. El nombre puede incluir subdirectorios (por ejemplo `2026-03-02/0900.csv`), así que ten en cuenta cómo FastAPI maneja los path params con barras.

---

## Ejercicio de análisis

Una vez que la API esté funcionando y hayas generado al menos un reporte, escribe un script Python (`ejercicio_analisis.py`) que descargue un reporte desde MinIO a través de la API y lo analice localmente con pandas y numpy.

Algunas ideas de qué analizar:
- Ranking de dispositivos por CO2 medio
- Ubicaciones con mayor variabilidad de temperatura
- Alertas: dispositivos que superan algún umbral crítico

---

## Pistas

**El objetivo final es tener todo el sistema corriendo con un solo comando** (`docker compose up` o similar): la API, MongoDB y MinIO levantando juntos, con la API esperando a que la base de datos esté lista antes de arrancar.

- Un **`docker-compose.yml`** define los tres servicios y sus dependencias. Piensa en healthchecks para asegurarte de que MongoDB está listo antes de que la API intente conectarse.
- El **`Dockerfile`** de la API debe instalar las dependencias y arrancar el servidor. Con `uv` el flujo cambia respecto a pip: investiga cómo funciona.
- Usa un fichero **`.env`** para las credenciales y URIs de conexión, y nunca hardcodees valores en el código. El `docker-compose.yml` puede leer ese mismo fichero.
- Un **Justfile** (o Makefile) te ahorra escribir los mismos comandos una y otra vez: levantar servicios, insertar datos de prueba, generar un reporte, lanzar el análisis...
- MongoDB no tiene esquema fijo: puedes insertar documentos directamente como dicts de Python. Puedes inicializar la base de datos con un script que se ejecute al arrancar el contenedor.
- MinIO tiene su propio cliente Python (`minio`), distinto de boto3. Necesita endpoint, access key y secret key. Tiene una consola web donde puedes ver los buckets y objetos subidos.
- Piensa en cómo separar responsabilidades en el código: conexión a la base de datos, cliente de MinIO, lógica de generación de reportes... no lo pongas todo en el mismo fichero.

---

**Consulta la documentación oficial y pregunta al profesor cuando lo necesites.**
