# Streamlit app para gestionar la tabla hive_metastore.default.dbrsettings
import streamlit as st
import pandas as pd
import json
from datetime import datetime

# Configuración inicial de la página
st.set_page_config(page_title="Gestor de DBR Settings", layout="wide")

# Definir el nombre completo de la tabla
table_name = "hive_metastore.default.dbrsettings"

# Función para cargar los datos
@st.cache_data(ttl=60)
def load_data():
    return spark.sql(f"SELECT * FROM {table_name}").toPandas()

# Función para actualizar un registro
def update_record(id, data):
    # Convertir el diccionario a formato JSON para las columnas de tipo string que contienen JSON
    for col in ['SourceSettings', 'CopyQueries', 'CopySettings', 'SinkSettings', 'ParseColumns', 'UserDefineFunctions']:
        if col in data and data[col]:
            try:
                if isinstance(data[col], str):
                    # Verificar si ya es un JSON válido
                    json.loads(data[col])
                else:
                    data[col] = json.dumps(data[col])
            except:
                data[col] = json.dumps(data[col]) if not isinstance(data[col], str) else data[col]
    
    # Actualizar el registro
    update_query = f"""
    UPDATE {table_name}
    SET 
        System = '{data['System']}',
        Domain = '{data['Domain']}',
        SourceType = '{data['SourceType']}',
        SourceSettings = '{data['SourceSettings']}',
        CopyQueries = '{data['CopyQueries']}',
        CopySettings = '{data['CopySettings']}',
        SinkSettings = '{data['SinkSettings']}',
        DataLoadingBehavior = '{data['DataLoadingBehavior']}',
        EntityType = '{data['EntityType']}',
        CopyEnabled = {str(data['CopyEnabled']).lower()},
        LastIncrementalUpdate = '{data['LastIncrementalUpdate']}',
        LoadMark = {str(data['LoadMark']).lower()},
        ParseColumns = '{data['ParseColumns']}',
        UserDefineFunctions = '{data['UserDefineFunctions']}',
        LastIngestionDate = '{data['LastIngestionDate']}',
        StatusIngestion = {str(data['StatusIngestion']).lower()},
        LoadPurge = {str(data['LoadPurge']).lower()},
        LastIngestionDatePurge = '{data['LastIngestionDatePurge']}'
    WHERE Id = {id}
    """
    spark.sql(update_query)
    st.success(f"Registro con ID {id} actualizado correctamente.")

# Función para insertar un nuevo registro
def insert_record(data):
    # Preparar datos para la inserción
    for col in ['SourceSettings', 'CopyQueries', 'CopySettings', 'SinkSettings', 'ParseColumns', 'UserDefineFunctions']:
        if col in data and data[col]:
            try:
                if isinstance(data[col], str):
                    # Verificar si ya es un JSON válido
                    json.loads(data[col])
                else:
                    data[col] = json.dumps(data[col])
            except:
                data[col] = json.dumps(data[col]) if not isinstance(data[col], str) else data[col]
    
    # Generar un nuevo ID (podría ser necesario personalizar esta lógica)
    max_id_df = spark.sql(f"SELECT MAX(Id) as max_id FROM {table_name}")
    max_id = max_id_df.collect()[0]['max_id']
    new_id = max_id + 1 if max_id else 1
    
    # Insertar el registro
    insert_query = f"""
    INSERT INTO {table_name} VALUES (
        {new_id},
        '{data['System']}',
        '{data['Domain']}',
        '{data['SourceType']}',
        '{data['SourceSettings']}',
        '{data['CopyQueries']}',
        '{data['CopySettings']}',
        '{data['SinkSettings']}',
        '{data['DataLoadingBehavior']}',
        '{data['EntityType']}',
        {str(data['CopyEnabled']).lower()},
        '{data['LastIncrementalUpdate']}',
        {str(data['LoadMark']).lower()},
        '{data['ParseColumns']}',
        '{data['UserDefineFunctions']}',
        '{data['LastIngestionDate']}',
        {str(data['StatusIngestion']).lower()},
        {str(data['LoadPurge']).lower()},
        '{data['LastIngestionDatePurge']}'
    )
    """
    spark.sql(insert_query)
    st.success(f"Nuevo registro creado con ID {new_id}.")

# Función para eliminar un registro
def delete_record(id):
    delete_query = f"DELETE FROM {table_name} WHERE Id = {id}"
    spark.sql(delete_query)
    st.success(f"Registro con ID {id} eliminado correctamente.")

# Función para formato legible de JSON
def pretty_json(json_str):
    try:
        if json_str and isinstance(json_str, str):
            return json.dumps(json.loads(json_str), indent=2)
        return json_str
    except:
        return json_str

# Interfaz principal
st.title("Gestor de DBR Settings")

# Crear pestañas para las diferentes funcionalidades
tab1, tab2, tab3, tab4 = st.tabs(["Visualizar", "Editar", "Insertar", "Eliminar"])

# Cargar los datos
try:
    df = load_data()
    
    # Pestaña de visualización
    with tab1:
        st.subheader("Tabla de Configuraciones DBR")
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        with col1:
            system_filter = st.selectbox("Filtrar por Sistema", options=["Todos"] + list(df["System"].unique()))
        with col2:
            domain_filter = st.selectbox("Filtrar por Dominio", options=["Todos"] + list(df["Domain"].unique()))
        with col3:
            source_type_filter = st.selectbox("Filtrar por Tipo de Fuente", options=["Todos"] + list(df["SourceType"].unique()))
            
        # Aplicar filtros
        filtered_df = df.copy()
        if system_filter != "Todos":
            filtered_df = filtered_df[filtered_df["System"] == system_filter]
        if domain_filter != "Todos":
            filtered_df = filtered_df[filtered_df["Domain"] == domain_filter]
        if source_type_filter != "Todos":
            filtered_df = filtered_df[filtered_df["SourceType"] == source_type_filter]
            
        # Mostrar tabla con datos filtrados
        st.dataframe(filtered_df)
        
        # Detalle de un registro seleccionado
        st.subheader("Detalle del Registro")
        selected_id = st.selectbox("Seleccionar ID", options=filtered_df["Id"].tolist())
        
        if selected_id:
            selected_record = filtered_df[filtered_df["Id"] == selected_id].iloc[0]
            
            # Mostrar detalles en formato expandible
            with st.expander("Información Básica", expanded=True):
                cols = st.columns(4)
                cols[0].metric("ID", selected_record["Id"])
                cols[1].metric("Sistema", selected_record["System"])
                cols[2].metric("Dominio", selected_record["Domain"])
                cols[3].metric("Tipo de Fuente", selected_record["SourceType"])
            
            # Mostrar JSON en un formato más legible
            json_cols = ["SourceSettings", "CopyQueries", "CopySettings", "SinkSettings", "ParseColumns", "UserDefineFunctions"]
            for col in json_cols:
                with st.expander(f"{col}", expanded=False):
                    st.code(pretty_json(selected_record[col]), language="json")
            
            # Mostrar estados y fechas
            with st.expander("Estados y Fechas", expanded=True):
                cols = st.columns(4)
                cols[0].metric("Copy Enabled", str(selected_record["CopyEnabled"]))
                cols[1].metric("Load Mark", str(selected_record["LoadMark"]))
                cols[2].metric("Status Ingestion", str(selected_record["StatusIngestion"]))
                cols[3].metric("Load Purge", str(selected_record["LoadPurge"]))
                
                st.write("**Fechas importantes:**")
                date_cols = st.columns(3)
                date_cols[0].metric("Last Incremental Update", str(selected_record["LastIncrementalUpdate"]))
                date_cols[1].metric("Last Ingestion Date", str(selected_record["LastIngestionDate"]))
                date_cols[2].metric("Last Ingestion Date Purge", str(selected_record["LastIngestionDatePurge"]))
    
    # Pestaña de edición
    with tab2:
        st.subheader("Editar Configuración")
        edit_id = st.selectbox("Seleccionar ID para editar", options=df["Id"].tolist(), key="edit_id")
        
        if edit_id:
            edit_record = df[df["Id"] == edit_id].iloc[0].to_dict()
            
            # Formulario para editar
            with st.form(key="edit_form"):
                st.write("**Información Básica**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    edit_record["System"] = st.text_input("Sistema", value=edit_record["System"])
                with col2:
                    edit_record["Domain"] = st.text_input("Dominio", value=edit_record["Domain"])
                with col3:
                    edit_record["SourceType"] = st.text_input("Tipo de Fuente", value=edit_record["SourceType"])
                
                st.write("**Configuraciones JSON**")
                json_cols = ["SourceSettings", "CopyQueries", "CopySettings", "SinkSettings", "ParseColumns", "UserDefineFunctions"]
                for col in json_cols:
                    edit_record[col] = st.text_area(f"{col}", value=pretty_json(edit_record[col]), height=100)
                
                st.write("**Comportamiento y Tipo**")
                col1, col2 = st.columns(2)
                with col1:
                    edit_record["DataLoadingBehavior"] = st.text_input("Comportamiento de Carga", value=edit_record["DataLoadingBehavior"])
                with col2:
                    edit_record["EntityType"] = st.text_input("Tipo de Entidad", value=edit_record["EntityType"])
                
                st.write("**Estados**")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    edit_record["CopyEnabled"] = st.checkbox("Copy Enabled", value=bool(edit_record["CopyEnabled"]))
                with col2:
                    edit_record["LoadMark"] = st.checkbox("Load Mark", value=bool(edit_record["LoadMark"]))
                with col3:
                    edit_record["StatusIngestion"] = st.checkbox("Status Ingestion", value=bool(edit_record["StatusIngestion"]))
                with col4:
                    edit_record["LoadPurge"] = st.checkbox("Load Purge", value=bool(edit_record["LoadPurge"]))
                
                st.write("**Fechas**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    edit_record["LastIncrementalUpdate"] = st.text_input("Last Incremental Update", value=edit_record["LastIncrementalUpdate"])
                with col2:
                    edit_record["LastIngestionDate"] = st.text_input("Last Ingestion Date", value=edit_record["LastIngestionDate"])
                with col3:
                    edit_record["LastIngestionDatePurge"] = st.text_input("Last Ingestion Date Purge", value=edit_record["LastIngestionDatePurge"])
                
                submit_button = st.form_submit_button(label="Actualizar Registro")
                if submit_button:
                    update_record(edit_id, edit_record)
                    st.experimental_rerun()
    
    # Pestaña de inserción
    with tab3:
        st.subheader("Insertar Nueva Configuración")
        
        # Formulario para insertar
        with st.form(key="insert_form"):
            st.write("**Información Básica**")
            col1, col2, col3 = st.columns(3)
            with col1:
                system = st.text_input("Sistema")
            with col2:
                domain = st.text_input("Dominio")
            with col3:
                source_type = st.text_input("Tipo de Fuente")
            
            st.write("**Configuraciones JSON**")
            source_settings = st.text_area("Source Settings", value="{}", height=100)
            copy_queries = st.text_area("Copy Queries", value="{}", height=100)
            copy_settings = st.text_area("Copy Settings", value="{}", height=100)
            sink_settings = st.text_area("Sink Settings", value="{}", height=100)
            parse_columns = st.text_area("Parse Columns", value="{}", height=100)
            user_define_functions = st.text_area("User Define Functions", value="{}", height=100)
            
            st.write("**Comportamiento y Tipo**")
            col1, col2 = st.columns(2)
            with col1:
                data_loading_behavior = st.text_input("Comportamiento de Carga")
            with col2:
                entity_type = st.text_input("Tipo de Entidad")
            
            st.write("**Estados**")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                copy_enabled = st.checkbox("Copy Enabled")
            with col2:
                load_mark = st.checkbox("Load Mark")
            with col3:
                status_ingestion = st.checkbox("Status Ingestion")
            with col4:
                load_purge = st.checkbox("Load Purge")
            
            st.write("**Fechas**")
            col1, col2, col3 = st.columns(3)
            with col1:
                last_incremental_update = st.text_input("Last Incremental Update", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            with col2:
                last_ingestion_date = st.text_input("Last Ingestion Date", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            with col3:
                last_ingestion_date_purge = st.text_input("Last Ingestion Date Purge", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            # Preparar datos para la inserción
            new_record = {
                "System": system,
                "Domain": domain,
                "SourceType": source_type,
                "SourceSettings": source_settings,
                "CopyQueries": copy_queries,
                "CopySettings": copy_settings,
                "SinkSettings": sink_settings,
                "DataLoadingBehavior": data_loading_behavior,
                "EntityType": entity_type,
                "CopyEnabled": copy_enabled,
                "LastIncrementalUpdate": last_incremental_update,
                "LoadMark": load_mark,
                "ParseColumns": parse_columns,
                "UserDefineFunctions": user_define_functions,
                "LastIngestionDate": last_ingestion_date,
                "StatusIngestion": status_ingestion,
                "LoadPurge": load_purge,
                "LastIngestionDatePurge": last_ingestion_date_purge
            }
            
            submit_button = st.form_submit_button(label="Insertar Registro")
            if submit_button:
                insert_record(new_record)
                st.experimental_rerun()
    
    # Pestaña de eliminación
    with tab4:
        st.subheader("Eliminar Configuración")
        delete_id = st.selectbox("Seleccionar ID para eliminar", options=df["Id"].tolist())
        
        if delete_id:
            delete_record_info = df[df["Id"] == delete_id]
            st.write(f"Estás a punto de eliminar el registro con ID: **{delete_id}**")
            st.write(f"Sistema: **{delete_record_info.iloc[0]['System']}**")
            st.write(f"Dominio: **{delete_record_info.iloc[0]['Domain']}**")
            st.write(f"Tipo de Fuente: **{delete_record_info.iloc[0]['SourceType']}**")
            
            confirm_delete = st.checkbox("Confirmar eliminación")
            if confirm_delete:
                if st.button("Eliminar Registro", type="primary"):
                    delete_record(delete_id)
                    st.experimental_rerun()
            else:
                st.warning("Por favor, confirma la eliminación marcando la casilla.")

except Exception as e:
    st.error(f"Error al acceder a la tabla: {str(e)}")
    st.info("Asegúrate de que la tabla 'hive_metastore.default.dbrsettings' existe y es accesible.")