import asyncio
import json
import os
import re
from io import BytesIO
import pandas as pd # Para exportar a Excel

# Importar las bibliotecas de Azure AI Document Intelligence
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import HttpResponseError

# Importar Azure OpenAI
from openai import AzureOpenAI



# --- Inicializar clientes ---
document_intelligence_client = DocumentIntelligenceClient(
    endpoint=AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT,
    credential=AzureKeyCredential(AZURE_DOCUMENT_INTELLIGENCE_KEY)
)

openai_client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_KEY,
    api_version="2024-12-01-preview"
)


# --- Funciones de Utilidad ---
def clean_json_text(json_text):
    """Limpiar texto JSON para quitar caracteres no deseados."""
    cleaned_text = json_text.strip()
    if cleaned_text.startswith("```json"):
        cleaned_text = cleaned_text[len("```json"):].strip()
    if cleaned_text.endswith("```"):
        cleaned_text = cleaned_text[:-len("```")].strip()
    return cleaned_text

# --- Función para extraer texto y estructura con Azure AI Document Intelligence (SÍNCRONA) ---
def extract_data_with_document_intelligence(file_stream, file_name):
    """
    Extrae texto y estructura (incluyendo tablas y texto manuscrito)
    de un archivo usando el modelo preconstruido 'layout' de Azure AI Document Intelligence.
    Esta función ahora es SÍNCRONA, bloqueando hasta que Document Intelligence termine.
    """
    print(f"--- Procesando archivo: {file_name} ---")
    print("Paso 1: Extrayendo texto y estructura con Azure AI Document Intelligence...")
    try:
        poller = document_intelligence_client.begin_analyze_document(
            "prebuilt-layout",
            file_stream,
            content_type="application/octet-stream"
        )
        result: AnalyzeResult = poller.result()

        if result:
            extracted_text = ""
            extracted_tables_data = []
            
            if result.paragraphs:
                for paragraph in result.paragraphs:
                    extracted_text += paragraph.content + "\n"
            print("\nTexto plano extraído:")
            print(extracted_text)

            if result.tables:
                for table_idx, table in enumerate(result.tables):
                    print(f"\nTabla {table_idx + 1} detectada:")
                    table_content = []
                    
                    headers = {}
                    # Primero, mapeamos los encabezados de columna explícitos
                    for cell in table.cells:
                        if cell.kind == "columnHeader":
                            headers[cell.column_index] = cell.content.strip()
                        # Si la celda es un campo de selección y tiene un contenido que podría ser un nombre, lo mapeamos también
                        # Esto es útil si los nombres de tipo (Esales, comercio) no son headers explícitos, sino parte de la celda de selección
                        elif hasattr(cell, 'selection_state') and cell.selection_state is not None and cell.content.strip():
                             headers[cell.column_index] = cell.content.strip()


                    current_row = {}
                    last_row_index = -1
                    
                    sorted_cells = sorted(table.cells, key=lambda c: (c.row_index, c.column_index))
                    
                    for cell in sorted_cells:
                        # Ignorar columna "FIRMA"
                        if "firma" in cell.content.lower() or cell.column_index == 7: # Asumiendo que 7 es la columna de firma
                            continue

                        # Si la celda es de una nueva fila, agregamos la fila anterior y reseteamos
                        if cell.row_index != last_row_index:
                            if current_row:
                                table_content.append(current_row)
                            current_row = {}
                            last_row_index = cell.row_index
                        
                        # --- MODIFICACIÓN CLAVE AQUÍ: Verificar si selection_state existe antes de accederlo ---
                        if hasattr(cell, 'selection_state') and cell.selection_state is not None:
                            # Si es una celda de selección (checkbox/radio button)
                            # Buscamos el nombre de la columna desde los headers mapeados o el contenido de la celda
                            col_name = headers.get(cell.column_index) or cell.content.strip() or f"Tipo_{cell.column_index}"
                            current_row[col_name] = cell.selection_state.value # Guarda 'selected' o 'unselected'
                        else:
                            # Manejo de celdas normales
                            col_name = headers.get(cell.column_index, f"Col_{cell.column_index}")
                            current_row[col_name] = cell.content.strip() # Limpiar contenido para el LLM

                    # Asegurarse de agregar la última fila procesada si hay alguna
                    if current_row:
                        table_content.append(current_row)
                    
                    print(f"Contenido de la tabla {table_idx + 1}:")
                    print(json.dumps(table_content, indent=2, ensure_ascii=False))
                    extracted_tables_data.append(table_content)
            else:
                print("\nNo se detectaron tablas estructuradas.")

            return {
                'text_content': extracted_text.strip(),
                'tables': extracted_tables_data
            }
        return None
    except HttpResponseError as e:
        print(f"ERROR de Azure Document Intelligence: {e.reason} - {e.message}")
        return None
    except Exception as e:
        # Aquí capturamos cualquier otro error inesperado y lo imprimimos
        print(f"ERROR inesperado durante la extracción de documentos: {e}")
        return None

# --- Función para convertir el texto en JSON usando Azure OpenAI ---
def parse_as_json(extracted_content, json_template):
    """
    Convierte el texto y la estructura de tablas extraídos por Document Intelligence
    en un JSON usando el modelo de Azure OpenAI.
    Ahora espera un array de objetos de registro de asistencia.
    """
    print("\nPaso 2: Enviando datos a Azure OpenAI para estructuración...")
    text_to_parse = extracted_content.get('text_content', '')
    tables_to_parse = extracted_content.get('tables', [])

    prompt_tables_info = ""
    if tables_to_parse:
        prompt_tables_info = "\n\nSe ha detectado la siguiente información estructurada en tablas:\n"
        for table_idx, table_data in enumerate(tables_to_parse):
            prompt_tables_info += f"--- Tabla {table_idx + 1} ---\n"
            for row in table_data:
                # Asegura que las filas de la tabla se representen claramente para el LLM
                # Aquí se incluyen TODAS las columnas, incluyendo las de tipo (Esales, comercio)
                # con sus valores 'selected'/'unselected', para que el LLM las vea.
                row_items = []
                for k, v in row.items():
                    if "firma" not in k.lower(): # Excluir la columna de firma
                        # MODIFICACIÓN: Eliminada la condición `v.strip() != ''`
                        row_items.append(f"'{k}': '{v}'") 
                prompt_tables_info += "{" + ", ".join(row_items) + "}\n"
            prompt_tables_info += "---------------------\n"

    messages = [
        {"role": "system", "content": "Eres un experto en formato y validación de datos. Tu tarea es identificar distintas secciones de eventos y sus tablas de asistencia correspondientes dentro del contenido del documento proporcionado. Cada sección de evento tendrá un 'NOMBRE DEL PROGRAMA', 'TIPO DE ACTIVIDAD', etc., y una tabla de 'asistentes'. Debes generar una **lista de objetos JSON**, donde cada objeto representa un evento distinto y contiene sus detalles y sus asistentes asociados. La columna 'firma' debe ser ignorada o dejarse vacía."},
        {"role": "user", "content": (
            f"Convierte el siguiente contenido del documento en una lista de objetos JSON que **debe coincidir exactamente** con la estructura proporcionada en la plantilla. "
            f"Cada elemento de la lista debe ser un objeto que represente una sección de evento única, incluyendo su información general y su lista de 'asistentes' correspondiente. "
            f"Presta especial atención a asociar los asistentes con los detalles correctos de su evento. "
            f"**Incluye todas las columnas tal como se te presentan en la tabla de datos, incluyendo las que indican el tipo de asistente (ej., 'Esales', 'comercio', 'Tenderos') con sus valores 'selected' o 'unselected'.**" # <--- INSTRUCCIÓN CLAVE AQUÍ
            f"**Asegúrate de ignorar cualquier dato en la columna 'firma' o campos de firma similares.**\n\n"
            f"Aquí está el contenido del texto del documento:\n{text_to_parse}\n"
            f"{prompt_tables_info}\n\n"
            f"El objeto JSON debe adherirse estrictamente a esta estructura de lista, incluyendo todas las claves y elementos anidados, incluso si los datos en el texto están incompletos. "
            f"Proporciona cadenas vacías para los valores faltantes si un campo no se encuentra. "
            f"**Aquí está la plantilla JSON a seguir:**\n{json_template}\n\n"
            "Responde exclusivamente con el objeto JSON formateado correctamente, nada más."
            "**En el campo 'NOMBRE EMPRESA/ENTIDAD': Si el OCR extrae 'COMA' pero el contexto del documento (como 'Cámara de Comercio', 'CCMMNA', 'Magdalena Medio', etc.) sugiere que debería ser 'CCMA', corrige el valor a 'CCMA'.** "
        )}
    ]

    try:
        response = openai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=messages,
            max_tokens=4096,
            temperature=0
        )

        if response.choices:
            parsed_json_text = response.choices[0].message.content.strip()
            cleaned_json_text = clean_json_text(parsed_json_text)
            
            print("\nJSON generado por Azure OpenAI:")
            print(cleaned_json_text)

            try:
                # Se espera una lista de objetos, no un solo objeto
                parsed_data = json.loads(cleaned_json_text)
                if isinstance(parsed_data, list):
                    return parsed_data
                else:
                    print("ERROR: El JSON generado por OpenAI no es un array como se esperaba.")
                    return None
            except json.JSONDecodeError as e:
                print(f"ERROR al decodificar el JSON generado por OpenAI: {e}")
                print(f"JSON problemático: {cleaned_json_text}")
                return None
        else:
            print("ERROR: No se obtuvo una respuesta válida del modelo OpenAI.")
            return None
    except Exception as e:
        print(f"ERROR al comunicarse con Azure OpenAI: {e}")
        return None

# --- Función para obtener la plantilla JSON (para tu formulario) ---
def get_json_template(document_type):
    """Carga la plantilla JSON para el tipo de documento de registro de asistencia.
    Ahora devuelve un array de objetos de registro de asistencia.
    NOTA: Las columnas de tipo (Esales, comercio, etc.) no están predefinidas aquí.
    Se espera que OpenAI las incluya dinámicamente si las detecta."""
    if document_type == "Registro de Asistencia":
        # La plantilla ahora es un array que contiene un ejemplo de objeto de registro
        template = [
            {
                "NOMBRE DEL PROGRAMA": "string",
                "TIPO DE ACTIVIDAD": "string",
                "LUGAR": "string",
                "MUNICIPIO": "string",
                "ORIENTADO POR": "string",
                "FECHA": "string",
                "asistentes": [
                    {
                        "NOMBRE COMPLETO": "string",
                        "NÚMERO DOCUMENTO": "string",
                        "NOMBRE EMPRESA/ENTIDAD": "string",
                        "MUNICIPIO/ CORREGIMIENTO/ VEREDA": "string",
                        "NÚMERO CONTACTO": "string",
                        "CORREO ELECTRÓNICO": "string"
                        # NO se añade "Tipo": "string" aquí para que el LLM las incluya dinámicamente
                        # o para que salgan como columnas separadas si el LLM las detecta así.
                    }
                ]
            }
        ]
        return template
    else:
        print(f"ADVERTENCIA: No se encontró una plantilla para el tipo de documento: {document_type}")
        return None

# --- Función principal para procesar archivos locales (SÍNCRONA) ---
def main():
    print("Iniciando el proceso de extracción de registros de asistencia...")
    
    # Lista para recolectar todos los datos consolidados de todos los archivos
    all_consolidated_data = [] 

    # --- Definir campos generales y de asistente (fuera del bucle para reutilización) ---
    general_info_fields = [
        "NOMBRE DEL PROGRAMA", "TIPO DE ACTIVIDAD", "LUGAR",
        "MUNICIPIO", "ORIENTADO POR", "FECHA"
    ]
    
    # attendee_specific_fields NO incluye "Tipo" en esta etapa, ya que se espera
    # que OpenAI devuelva las columnas individuales de tipo.
    attendee_specific_fields_base = [
        "NOMBRE COMPLETO",
        "NÚMERO DOCUMENTO",
        "NOMBRE EMPRESA/ENTIDAD",
        "MUNICIPIO/ CORREGIMIENTO/ VEREDA",
        "NÚMERO CONTACTO",
        "CORREO ELECTRÓNICO"
    ]

    if not os.path.exists(INPUT_FILES_PATH):
        print(f"ERROR: La ruta de entrada '{INPUT_FILES_PATH}' no existe. Por favor, crea la carpeta y coloca tus archivos.")
        return

    files_to_process = [f for f in os.listdir(INPUT_FILES_PATH) if f.lower().endswith(('.pdf', '.png', '.jpg', '.jpeg', '.tiff'))]
    
    if not files_to_process:
        print(f"ADVERTENCIA: No se encontraron archivos PDF o imagen en la carpeta '{INPUT_FILES_PATH}'.")
        return

    print(f"\nSe encontraron {len(files_to_process)} archivos para procesar en '{INPUT_FILES_PATH}'.")

    for file_name in files_to_process:
        file_path = os.path.join(INPUT_FILES_PATH, file_name)
        
        try:
            with open(file_path, "rb") as file_handle:
                file_stream = BytesIO(file_handle.read())
            
            # Llamada síncrona a la función de Document Intelligence
            extracted_content = extract_data_with_document_intelligence(file_stream, file_name)

            if extracted_content:
                json_template = get_json_template("Registro de Asistencia")
                
                if json_template:
                    # parse_as_json ahora devuelve una LISTA de registros de asistencia
                    parsed_events_data = parse_as_json(extracted_content, json_template)
                    
                    # Asegurarse de que parsed_events_data sea una lista antes de iterar
                    if parsed_events_data and isinstance(parsed_events_data, list):
                        # Iterar sobre cada evento detectado en el JSON
                        for registro_evento in parsed_events_data:
                            # Extraer la información general del evento de ESTE registro de evento
                            event_data = {field: registro_evento.get(field, '') for field in general_info_fields}
                            event_data["Fuente_Archivo"] = file_name # Añadir la fuente del archivo como una columna más
                            
                            # Procesar cada asistente asociado a ESTE evento
                            if registro_evento.get("asistentes"):
                                for attendee in registro_evento["asistentes"]:
                                    # Crear un diccionario para cada asistente, combinando info del evento
                                    # y TODA la info del asistente (incluyendo las columnas de tipo si el LLM las devuelve)
                                    combined_row = {
                                        **event_data, 
                                        **{k: v for k, v in attendee.items()} # Se añaden todos los campos del asistente tal cual vienen
                                    }
                                    all_consolidated_data.append(combined_row)
                                
                                print(f"\n--- Datos de asistentes de un evento en '{file_name}' agregados a la lista consolidada ---")
                            else:
                                print(f"No se extrajeron asistentes para un evento en '{file_name}'.")
                    else:
                        print(f"ADVERTENCIA: El JSON generado para '{file_name}' no contiene un array de registros o está vacío.")
                else:
                    print(f"ADVERTENCIA: No se pudo cargar la plantilla JSON para '{file_name}'.")
            else:
                print(f"ADVERTENCIA: No se pudo extraer contenido de '{file_name}'.")
        except FileNotFoundError:
            print(f"ERROR: Archivo no encontrado: {file_path}")
        except Exception as e:
            print(f"ERROR general al procesar '{file_name}': {e}")
        print("-" * 50)

    # Paso 3: Consolidar y exportar todos los datos a un único archivo Excel
    if all_consolidated_data:
        # Se crea el DataFrame sin un orden de columnas predefinido para capturar TODAS las columnas
        # que el LLM haya detectado, incluidas las de tipo (Esales, comercio, etc.).
        df_final = pd.DataFrame(all_consolidated_data)
        
        # Intentar definir un orden general. Las columnas de tipo se añadirán al final.
        # Primero, las columnas generales del evento + Fuente_Archivo
        ordered_columns = general_info_fields + ["Fuente_Archivo"] 
        
        # Luego, las columnas base de asistente (nombre, documento, etc.)
        ordered_columns.extend(attendee_specific_fields_base) 
        
        # Finalmente, identificar las columnas de tipo dinámicamente
        # Recorremos todas las columnas que el DataFrame ha detectado y añadimos las que no están en ordered_columns
        for col in df_final.columns:
            if col not in ordered_columns and col not in ["Tipo"]: # Excluir "Tipo" si apareció por error
                ordered_columns.append(col) # Esto capturará "Esales", "comercio", etc.
        
        # Asegurarse de que las columnas existentes en df_final coincidan con ordered_columns
        # Esto previene errores si alguna columna de ordered_columns no existe en df_final
        final_ordered_columns = [col for col in ordered_columns if col in df_final.columns]
        df_final = df_final[final_ordered_columns]

        try:
            # Pandas con xlsxwriter engine pondrá los encabezados en negrita por defecto
            df_final.to_excel(OUTPUT_EXCEL_FILENAME, index=False, engine='xlsxwriter')
            print(f"\nPROCESO FINALIZADO: Todos los datos consolidados han sido exportados exitosamente a '{OUTPUT_EXCEL_FILENAME}'.")
        except Exception as e:
            print(f"ERROR al exportar a Excel: {e}")
    else:
        print("\nPROCESO FINALIZADO: No se extrajeron datos de asistentes de ningún archivo.")

# Ejecutar la función principal síncrona
if __name__ == "__main__":
    main()