Tomando como base cómo QueAI presenta su ecosistema —orquestación modular, despliegue local o cloud, plugins desacoplados con `manifest.json`, instalación desde dashboard y foco en privacidad/local-first—, este README conviene posicionarlo como un **módulo OCR local, instalable y operable desde QueAI**, no como una API suelta. ([QueAI][1])

Aquí tienes una propuesta nueva y mucho más alineada con QueAI:

````markdown
# QueAI OCR Local CPU

Módulo OCR local para **QueAI** que permite extraer texto de **imágenes, documentos escaneados y PDFs** de forma **offline**, mediante una arquitectura desacoplada basada en contenedores.

Está diseñado para integrarse como un plugin instalable dentro del ecosistema de QueAI, manteniendo el enfoque **local-first**, modular y sin dependencia obligatoria de servicios externos.

## Qué hace este módulo

- Extrae texto desde:
  - PDF
  - PNG
  - JPG / JPEG
  - TIFF
  - WEBP
- Procesa archivos localmente en CPU.
- Expone progreso en tiempo real mediante **SSE**.
- Ejecuta el procesamiento pesado en background usando **Redis + RQ**.
- Incluye interfaz web propia servida desde el módulo.
- Se integra con el enrutamiento del core mediante **Traefik**.
- Está preparado para ser distribuido como módulo descargable dentro de QueAI.

## Casos de uso

- Digitalización de documentos escaneados
- Extracción de texto desde formularios
- OCR local para clínicas, despachos o equipos con requisitos de privacidad
- Preprocesamiento para flujos posteriores de clasificación, RAG o automatización documental
- Conversión de archivos físicos o escaneados en texto utilizable sin salir de la red local

## Arquitectura del módulo

El módulo se compone de tres piezas principales:

- **api**  
  Servicio FastAPI que:
  - recibe archivos
  - valida entradas
  - crea workspaces temporales
  - publica eventos SSE
  - sirve la UI del módulo

- **worker**  
  Proceso background que:
  - consume jobs desde Redis/RQ
  - ejecuta OCR
  - publica avances y resultado final

- **redis**  
  Cola y bus temporal de eventos para coordinación entre API y worker.

## Flujo general

1. El usuario sube un archivo desde la UI del módulo o consume el endpoint HTTP.
2. La API valida el archivo y crea un workspace temporal.
3. El job se encola en Redis/RQ.
4. El worker procesa el documento.
5. La API transmite eventos SSE con progreso.
6. Al finalizar, el texto extraído se devuelve al cliente.