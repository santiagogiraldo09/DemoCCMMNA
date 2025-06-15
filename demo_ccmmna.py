import streamlit as st
import json
import re
from io import BytesIO
import pandas as pd
import xlsxwriter

# Importar las bibliotecas de Azure AI Document Intelligence
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import HttpResponseError

# Importar Azure OpenAI
from openai import AzureOpenAI

st.set_page_config(page_title="Extractor de Registros de Asistencia", layout="wide")

# --- Configuración de Credenciales ---
AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT = st.secrets["AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT"]
AZURE_DOCUMENT_INTELLIGENCE_KEY = st.secrets["AZURE_DOCUMENT_INTELLIGENCE_KEY"]
AZURE_OPENAI_ENDPOINT = st.secrets["AZURE_OPENAI_ENDPOINT"]
AZURE_OPENAI_KEY = st.secrets["AZURE_OPENAI_KEY"]
AZURE_OPENAI_DEPLOYMENT_NAME = st.secrets["AZURE_OPENAI_DEPLOYMENT_NAME"]

# --- Inicializar clientes ---
@st.cache_resource
def get_document_intelligence_client():
    try:
        return DocumentIntelligenceClient(
            endpoint=AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT,
            credential=AzureKeyCredential(AZURE_DOCUMENT_INTELLIGENCE_KEY)
        )
    except KeyError as e:
        st.error(f"Error de configuración: La clave de secreto '{e}' no se encontró para Azure Document Intelligence. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente.")
        st.stop()
    except Exception as e:
        st.error(f"Error al inicializar el cliente de Document Intelligence: {e}")
        st.stop()

@st.cache_resource
def get_openai_client():
    try:
        return AzureOpenAI(
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=AZURE_OPENAI_KEY,
            api_version="2024-12-01-preview"
        )
    except KeyError as e:
        st.error(f"Error de configuración: La clave de secreto '{e}' no se encontró para Azure OpenAI. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente.")
        st.stop()
    except Exception as e:
        st.error(f"Error al inicializar el cliente de OpenAI: {e}")
        st.stop()

document_intelligence_client = get_document_intelligence_client()
openai_client = get_openai_client()

# --- Funciones de Utilidad ---
def clean_json_text(json_text):
    """Limpiar texto JSON para quitar caracteres no deseados."""
    cleaned_text = json_text.strip()
    if cleaned_text.startswith("```json"):
        cleaned_text = cleaned_text[len("```json"):].strip()
    if cleaned_text.endswith("```"):
        cleaned_text = cleaned_text[:-len("```")].strip()
    return cleaned_text

def clean_and_infer_email(email_str, company_name=""):
    """
    Limpia y normaliza una cadena de correo electrónico, e intenta inferir el dominio
    basándose en el nombre de la empresa.
    """
    if not isinstance(email_str, str):
        return ""

    original_email = email_str.lower().strip()
    cleaned_email = original_email

    # 1. Limpieza inicial
    cleaned_email = re.sub(r'[^\w.@\-\_]+', '', cleaned_email)
    cleaned_email = cleaned_email.replace('www.', '')
    cleaned_email = cleaned_email.replace(' ', '').replace('\n', '')

    # Separar usuario y dominio
    username = ""
    domain = ""
    if '@' in cleaned_email:
        parts = cleaned_email.split('@')
        if len(parts) == 2:
            username, domain = parts
            domain = domain.strip()
        else:
            at_index = cleaned_email.find('@')
            if at_index != -1:
                username = cleaned_email[:at_index]
                domain = cleaned_email[at_index+1:].strip()
    else:
        username = cleaned_email
        domain = ""
    
    # Intentar inferir dominio si es necesario
    if domain == "" or '.' not in domain or domain.endswith('.'):
        if domain.endswith('.'):
            domain = domain.rstrip('.')

        if company_name:
            company_lower_clean = re.sub(r'[^a-z0-9]', '', company_name.lower())
            if not domain and company_lower_clean:
                domain = f"{company_lower_clean}.com.co" 
        
        if not domain or len(domain.split('.')[-1]) < 2:
            if domain and '.' not in domain:
                domain += ".com.co"
            elif not domain and username:
                domain = "example.com"
    
    if username and domain:
        final_email = f"{username}@{domain}"
    else:
        final_email = ""

    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_regex, final_email):
        return ""
    
    return final_email

# --- Función para extraer texto y estructura ---
@st.spinner("Extrayendo texto...")
def extract_data_with_document_intelligence(file_stream, file_name):
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

            if result.tables:
                for table_idx, table in enumerate(result.tables):
                    table_content = []
                    headers = {}
                    for cell in table.cells:
                        if cell.kind == "columnHeader":
                            headers[cell.column_index] = cell.content.strip()
                        elif hasattr(cell, 'selection_state') and cell.selection_state is not None and cell.content.strip():
                             headers[cell.column_index] = cell.content.strip()

                    current_row = {}
                    last_row_index = -1
                    sorted_cells = sorted(table.cells, key=lambda c: (c.row_index, c.column_index))
                    
                    for cell in sorted_cells:
                        if "firma" in cell.content.lower() or cell.column_index == 7:
                            continue

                        if cell.row_index != last_row_index:
                            if current_row:
                                table_content.append(current_row)
                            current_row = {}
                            last_row_index = cell.row_index
                        
                        if hasattr(cell, 'selection_state') and cell.selection_state is not None:
                            col_name = headers.get(cell.column_index) or cell.content.strip() or f"Tipo_{cell.column_index}"
                            current_row[col_name] = cell.selection_state.value
                        else:
                            col_name = headers.get(cell.column_index, f"Col_{cell.column_index}")
                            current_row[col_name] = cell.content.strip()

                    if current_row:
                        table_content.append(current_row)
                    extracted_tables_data.append(table_content)
            return {
                'text_content': extracted_text.strip(),
                'tables': extracted_tables_data
            }
        return None
    except HttpResponseError as e:
        st.error(f"ERROR de Azure Document Intelligence: {e.reason} - {e.message}")
        return None
    except Exception as e:
        st.error(f"ERROR inesperado durante la extracción de documentos: {e}")
        return None

# --- Función para convertir el texto en JSON ---
@st.spinner("Estructurando información...")
def parse_as_json(extracted_content, json_template):
    text_to_parse = extracted_content.get('text_content', '')
    tables_to_parse = extracted_content.get('tables', [])

    prompt_tables_info = ""
    if tables_to_parse:
        prompt_tables_info = "\n\nSe ha detectado la siguiente información estructurada en tablas:\n"
        for table_idx, table_data in enumerate(tables_to_parse):
            prompt_tables_info += f"--- Tabla {table_idx + 1} ---\n"
            for row in table_data:
                row_items = []
                for k, v in row.items():
                    if "firma" not in k.lower() and v != ":unselected:":
                        row_items.append(f"'{k}': '{v}'") 
                prompt_tables_info += "{" + ", ".join(row_items) + "}\n"
            prompt_tables_info += "---------------------\n"

    messages = [
        {"role": "system", "content": "Eres un experto en formato y validación de datos. Tu tarea es identificar distintas secciones de eventos y sus tablas de asistencia correspondientes dentro del contenido del documento proporcionado. Cada sección de evento tendrá un 'NOMBRE DEL PROGRAMA', 'TIPO DE ACTIVIDAD', etc., y una tabla de 'asistentes'. Debes generar una **lista de objetos JSON**, donde cada objeto representa un evento distinto y contiene sus detalles y sus asistentes asociados. La columna 'firma' debe ser ignorada o dejarse vacía."},
        {"role": "user", "content": (
            f"Convierte el siguiente contenido del documento en una lista de objetos JSON que **debe coincidir exactamente** con la estructura proporcionada en la plantilla. "
            f"Cada elemento de la lista debe ser un objeto que represente una sección de evento única, incluyendo su información general y su lista de 'asistentes' correspondiente. "
            f"Presta especial atención a asociar los asistentes con los detalles correctos de su evento. "
            f"**Incluye todas las columnas tal como se te presentan en la tabla de datos, incluyendo las que indican el tipo de asistente (ej., 'Esales', 'comercio', 'Tenderos') con sus valores 'selected' o 'unselected'.**"
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
            
            try:
                parsed_data = json.loads(cleaned_json_text)
                if isinstance(parsed_data, list):
                    return parsed_data
                else:
                    st.warning("El JSON generado por OpenAI no es un array como se esperaba. Revisando el formato...")
                    if isinstance(parsed_data, dict):
                        return [parsed_data]
                    return None
            except json.JSONDecodeError as e:
                st.error(f"ERROR al decodificar el JSON generado por OpenAI: {e}. JSON problemático: {cleaned_json_text[:500]}...")
                return None
        else:
            st.error("No se obtuvo una respuesta válida del modelo OpenAI.")
            return None
    except Exception as e:
        st.error(f"ERROR al comunicarse con Azure OpenAI: {e}")
        return None

# --- Función para obtener la plantilla JSON ---
def get_json_template(document_type):
    if document_type == "Registro de Asistencia":
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
                        "EMPRESARIO NO MATRICULADO": "string",
                        "TENDEROS": "string",
                        "PERSONA NATURAL REGISTRADA": "string",
                        "ESALES": "string",
                        "PERSONA JURÍDICA - MANUFACTURA": "string",
                        "PERSONA JURÍDICA - SERVICIOS": "string",
                        "PERSONA JURÍDICA / COMERCIO": "string",
                        "OTRAS PERSONAS JURÍDICAS": "string",
                        "MUNICIPIO/ CORREGIMIENTO/ VEREDA": "string",
                        "NÚMERO CONTACTO": "string",
                        "CORREO ELECTRÓNICO": "string"
                    }
                ]
            }
        ]
        return template
    else:
        st.warning(f"No se encontró una plantilla para el tipo de documento: {document_type}")
        return None

# --- Streamlit UI ---
def main_streamlit_app():
    st.header("Sube tus Archivos")
    uploaded_files = st.file_uploader(
        "Sube tu archivo de registro de asistencia (PDF)",
        type=["pdf", "jpg", "jpeg", "png", "tiff"],
        accept_multiple_files=True
    )

    all_consolidated_data = []
    
    # Campos de información general
    general_info_fields = [
        "NOMBRE DEL PROGRAMA", "TIPO DE ACTIVIDAD", "LUGAR",
        "MUNICIPIO", "ORIENTADO POR", "FECHA"
    ]
    
    # Campos específicos de asistentes
    attendee_specific_fields_base = [
        "NOMBRE COMPLETO",
        "NÚMERO DOCUMENTO",
        "NOMBRE EMPRESA/ENTIDAD",
        "EMPRESARIO NO MATRICULADO",
        "TENDEROS",
        "PERSONA NATURAL REGISTRADA",
        "ESALES",
        "PERSONA JURÍDICA - MANUFACTURA",
        "PERSONA JURÍDICA - SERVICIOS",
        "PERSONA JURÍDICA / COMERCIO",
        "OTRAS PERSONAS JURÍDICAS",
        "MUNICIPIO/ CORREGIMIENTO/ VEREDA",
        "NÚMERO CONTACTO",
        "CORREO ELECTRÓNICO"
    ]
    
    # Campos de tipo de asistente para consolidar
    tipo_asistente_fields = [
        "EMPRESARIO NO MATRICULADO",
        "TENDEROS",
        "PERSONA NATURAL REGISTRADA",
        "ESALES",
        "PERSONA JURÍDICA - MANUFACTURA",
        "PERSONA JURÍDICA - SERVICIOS",
        "PERSONA JURÍDICA / COMERCIO",
        "OTRAS PERSONAS JURÍDICAS"
    ]

    if uploaded_files:
        if st.button("Procesar Archivos"):
            progress_bar = st.progress(0)
            total_files = len(uploaded_files)
            
            for i, uploaded_file in enumerate(uploaded_files):
                file_name = uploaded_file.name
                st.subheader(f"Procesando: {file_name}")
                
                file_stream = BytesIO(uploaded_file.read())
                
                try:
                    # Extraer datos con Document Intelligence
                    extracted_content = extract_data_with_document_intelligence(file_stream, file_name)

                    if extracted_content:
                        json_template = get_json_template("Registro de Asistencia")
                        
                        if json_template:
                            # Parsear a JSON con OpenAI
                            parsed_events_data = parse_as_json(extracted_content, json_template)
                            
                            if parsed_events_data and isinstance(parsed_events_data, list):
                                for registro_evento in parsed_events_data:
                                    event_data = {field: registro_evento.get(field, '') for field in general_info_fields}
                                    event_data["Fuente_Archivo"] = file_name
                                    
                                    if registro_evento.get("asistentes"):
                                        if not registro_evento["asistentes"]:
                                            st.warning(f"La lista de asistentes para un evento en '{file_name}' está vacía. Saltando.")
                                            continue
                                            
                                        for attendee in registro_evento["asistentes"]:
                                            original_email = attendee.get("CORREO ELECTRÓNICO", "")
                                            company_name_for_email_infer = attendee.get("NOMBRE EMPRESA/ENTIDAD", "")
                                            
                                            # Limpiar email
                                            cleaned_email = clean_and_infer_email(original_email, company_name_for_email_infer)
                                            attendee["CORREO ELECTRÓNICO"] = cleaned_email
                                            
                                            # ====== NUEVO: DETERMINAR TIPO ASISTENTE ======
                                            tipo_asistente = ""
                                            for campo in tipo_asistente_fields:
                                                if attendee.get(campo, "").strip().lower() == "selected":
                                                    tipo_asistente = campo
                                                    break
                                            
                                            # Eliminar campos individuales de tipo
                                            for campo in tipo_asistente_fields:
                                                if campo in attendee:
                                                    del attendee[campo]
                                            
                                            # Agregar nuevo campo consolidado
                                            attendee["Tipo asistente"] = tipo_asistente
                                            # ====== FIN DE MODIFICACIÓN ======
                                            
                                            combined_row = {
                                                **event_data, 
                                                **{k: v for k, v in attendee.items()}
                                            }
                                            all_consolidated_data.append(combined_row)
                                    else:
                                        st.info(f"No se extrajeron asistentes para un evento en '{file_name}'.")
                            else:
                                st.warning(f"El JSON generado para '{file_name}' no contiene un array de registros o está vacío.")
                        else:
                            st.warning(f"No se pudo cargar la plantilla JSON para '{file_name}'.")
                    else:
                        st.warning(f"No se pudo extraer contenido de '{file_name}'.")
                except Exception as e:
                    st.error(f"Ocurrió un error al procesar {file_name}: {e}")
                
                progress_bar.progress((i + 1) / total_files)
            
            if all_consolidated_data:
                df_final = pd.DataFrame(all_consolidated_data)
                
                # Ordenar columnas con la nueva estructura
                ordered_columns = general_info_fields + ["Fuente_Archivo"] 
                # Mantener solo campos base sin los de tipo
                base_fields_without_type = [
                    "NOMBRE COMPLETO",
                    "NÚMERO DOCUMENTO",
                    "NOMBRE EMPRESA/ENTIDAD",
                    "Tipo asistente",  # Nueva columna consolidada
                    "MUNICIPIO/ CORREGIMIENTO/ VEREDA",
                    "NÚMERO CONTACTO",
                    "CORREO ELECTRÓNICO"
                ]
                ordered_columns.extend(base_fields_without_type)
                
                # Añadir cualquier columna adicional
                for col in df_final.columns:
                    if col not in ordered_columns and col not in tipo_asistente_fields:
                        ordered_columns.append(col)
                
                final_ordered_columns = [col for col in ordered_columns if col in df_final.columns]
                df_final = df_final[final_ordered_columns]

                st.success("¡Procesamiento completado!")

                # Opción de descarga de Excel
                excel_buffer = BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                    df_final.to_excel(writer, index=False, sheet_name='Registros')
                excel_buffer.seek(0)
                
                st.download_button(
                    label="Descargar datos en Excel",
                    data=excel_buffer,
                    file_name="registros_asistencia_consolidados.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.info("No se extrajeron datos de asistentes de ningún archivo subido.")
    else:
        st.info("Sube uno o más archivos para comenzar el procesamiento.")

if __name__ == "__main__":
    main_streamlit_app()