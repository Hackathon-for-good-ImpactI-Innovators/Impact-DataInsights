## Libraries

import pandas as pd
import boto3
import numpy as np
import json
import time
import uuid
import boto3
import re
import json
import sagemaker
import datetime
from sagemaker.processing import ProcessingInput, ProcessingOutput

# Sagemaker session
sess = sagemaker.Session()
region = boto3.Session().region_name
s3 = boto3.resource('s3')
my_bucket = s3.Bucket('hackatonforgoodpariss3bucket')
bucket_list=[my_bucket_object.key for my_bucket_object in my_bucket.objects.all() ]


# Functions

# Extract conversation text
def extract_transcript_text(data):
    if 'transcripts' in data:
        transcripts = data['transcripts']
        if transcripts:
            transcript = transcripts[0]
            if 'transcript' in transcript:
                return transcript['transcript']
    return None

# unique flow export ID

def get_s3_files(bucket_name,datakeys):
    dfs=[]
    for file in range(0,len(datakeys)):
        data_location = 's3://{}/{}'.format(bucket_name, datakeys[file])
        data = pd.read_json(data_location, lines = True )
        df = pd.DataFrame(data)
        flow_export_id = f"{time.strftime('%d-%H-%M-%S', time.gmtime())}-{str(uuid.uuid4())[:8]}"
        flow_export_name = f"flow-{flow_export_id}"
        df['flow_export_id'] = flow_export_name
        #df['dialog']= records['transcript_text'][0]
        dfs.append(df)
        records = pd.concat(dfs, ignore_index=True)
    return records

# Extract logs into another table
def extract_items_text(data):
    data_items = data['results'][0]
    items_data = data_items.get('items', [])
    df_items = pd.DataFrame(items_data)
    df_items['flow_export_id']= records['flow_export_id'][0]
    df_items = df_items[df_items['type']=='pronunciation']
    return df_items

# Load the insights Dataset from audio

def write_s3_insights(current_year,month,day,bucket):
    print(f"Data Wrangler export storage bucket: {bucket}")
    file_name = f"records_{current_year}-{month}-{day}.csv"
    data_location = 'insigths_data/{}/{}/{}/{}'.format(current_year,month,day,file_name)
    records.to_csv(f"./Data/{file_name}")
    s3.meta.client.upload_file(f"./Data/{file_name}", bucket,data_location)
    pass


# Generate json files to automate filling forms

def extraer_respuestas(texto):
    respuestas = {}
    
    # Extraer consentimiento
    consentimiento_patron = r"legislación vigente\?\s*(\bSí\b)"
    si_encontrado = re.search(consentimiento_patron, texto)
    if si_encontrado:
        respuestas['consentimiento'] = si_encontrado.group(1)
    else:
        respuestas['consentimiento'] = None
        
    # Extraer el nombre completo
    nombre_patron = r"Nombre completo\?\s*([\w\s]+)\s"
    nombre_encontrado = re.search(nombre_patron, texto)
    if nombre_encontrado:
        respuestas['nombre_completo'] = nombre_encontrado.group(1)
    else:
        respuestas['nombre_completo'] = None
     
    # Extraer el país de origen
    pais_patron = r"Cuál es tu pais de origen\?\s*([\w\s]+)\."
    pais_encontrado = re.search(pais_patron, texto)
    if pais_encontrado:
        respuestas['pais'] = pais_encontrado.group(1)
    else:
        respuestas['pais'] = None

    # Extraer la edad
    edad_patron = r"Edad\?\s*(\d+)\s*"
    edad_encontrada = re.search(edad_patron, texto)
    if edad_encontrada:
        respuestas['edad'] = int(edad_encontrada.group(1))
    else:
        respuestas['edad'] = None

    # Extraer el tiempo en España
    tiempo_patron = r"Cuánto tiempo llevas en españa\?\s*(\d+)\s*meses"
    tiempo_encontrado = re.search(tiempo_patron, texto)
    if tiempo_encontrado:
        respuestas['tiempo_en_españa'] = int(tiempo_encontrado.group(1))
    else:
        respuestas['tiempo_en_espana'] = None


    # Extraer el Nivel de estudios
    estudios_patron = r"Nivel de estudios\?\s*([\w\s]+)\."
    estudios_encontrado = re.search(estudios_patron, texto)
    if estudios_encontrado:
        respuestas['estudios'] = estudios_encontrado.group(1)
    else:
        respuestas['estudios'] = None    


    # Extraer el servicio prestado 
    patron = r"En qué podemos ayudarte\?\s*([\w\s]+)\."
    string_encontrado = re.search(patron, texto)
    if string_encontrado:
        respuestas['ssv_encontrado'] = string_encontrado.group(1)
    else:
         respuestas['ssv_encontrado'] = None
        
    # Extraer tiene_hijos
    patron_tiene_hijos = r"Tienes hijos\?\s*(\bSí\b)"
    match_tiene_hijos = re.search(patron_tiene_hijos, texto)
    if match_tiene_hijos:
        respuestas['tiene_hijos'] = match_tiene_hijos.group(1)
    else:
        respuestas['tiene_hijos'] = None
    
    # Extraer num_hijos
    patron_num_hijos = r"Cuántos hijos tienes\?\s*(\d+)\s*"
    match_num_hijos = re.search(patron_num_hijos, texto)
    if match_num_hijos:
        respuestas['num_hijos'] = int(match_num_hijos.group(1))
    else:
        respuestas['num_hijos'] = None
    
    # Extraer estado_civil
    patron_estado_civil = r"Estado Civil\?\s*(\w+)"
    match_estado_civil = re.search(patron_estado_civil, texto)
    if match_estado_civil:
        respuestas['estado_civil'] = match_estado_civil.group(1)
    else:
        respuestas['estado_civil'] = None
    
    # Extraer residencia_compartida
    patron_residencia_compartida = r"Resides con tu pareja en la misma casa\?\s*(\w+)"
    match_residencia_compartida = re.search(patron_residencia_compartida, texto)
    if match_residencia_compartida:
        respuestas['residencia_compartida'] = match_residencia_compartida.group(1)
    else:
        respuestas['residencia_compartida'] = None
    
    # Extraer direccion
    patron_direccion = r"Podrías darme tu dirección completa con código postal\? Si, (.*), Codigo Postal (\d+)"
    match_direccion = re.search(patron_direccion, texto)
    if match_direccion:
        respuestas['direccion'] = match_direccion.group(1)
        respuestas['codigo_postal'] = match_direccion.group(2)
    else:
        respuestas['direccion'] = None
        respuestas['codigo_postal'] = None
    
    # Extraer correo electrónico
    patron_correo = r"¿Correo Electrónico\? (\S+@\S+)"
    match_correo = re.search(patron_correo, texto)
    if match_correo:
        respuestas['correo_electronico'] = match_correo.group(1)
    else:
        respuestas['correo_electronico'] = None

    # Extraer profesión
    patron_profesion = r"¿Cuál es tu profesión\? (\w+)"
    match_profesion = re.search(patron_profesion, texto)
    if match_profesion:
        respuestas['profesion'] = match_profesion.group(1)
    else:
        respuestas['profesion'] = None

    # Extraer ingresos
    patron_ingresos = r"¿Tienes ingresos\? (\w+)"
    match_ingresos = re.search(patron_ingresos, texto)
    if match_ingresos:
        respuestas['ingresos'] = match_ingresos.group(1)
    else:
        respuestas['ingresos'] = None

    # Extraer ayuda administración
    patron_ayuda = r"¿Recibes alguna ayuda de la administración\? (\S.*)\."
    match_ayuda = re.search(patron_ayuda, texto)
    if match_ayuda:
        respuestas['ayuda_administracion'] = match_ayuda.group(1)
    else:
        respuestas['ayuda_administracion'] = None

    # Extraer asistencia sanitaria
    patron_asistencia = r"¿Requieres alguna asistencia sanitaria\? (\w+)"
    match_asistencia = re.search(patron_asistencia, texto)
    if match_asistencia:
        respuestas['asistencia_sanitaria'] = match_asistencia.group(1)
    else:
        respuestas['asistencia_sanitaria'] = None

    # Extraer fumador
    patron_fumador = r"¿Eres fumador\? (\w+)"
    match_fumador = re.search(patron_fumador, texto)
    if match_fumador:
        respuestas['fumador'] = match_fumador.group(1)
    else:
        respuestas['fumador'] = None

    # Extraer discapacidad
    patron_discapacidad = r"¿Tienes algún grado de discapacidad\? (\w+)"
    match_discapacidad = re.search(patron_discapacidad, texto)
    if match_discapacidad:
        respuestas['discapacidad'] = match_discapacidad.group(1)
    else:
        respuestas['discapacidad'] = None

    # Extraer estado de salud
    patron_salud = r"¿Cómo diría que es su estado de salud entre (\w+), (\w+), (\w+)\?"
    match_salud = re.search(patron_salud, texto)
    if match_salud:
        respuestas['estado_salud'] = match_salud.group(1)
    else:
        respuestas['estado_salud'] = None

    # Extraer alquiler o casa en propiedad
    patron_vivienda = r"¿Tienes alquiler o casa en propiedad\? (\w+)"
    match_vivienda = re.search(patron_vivienda, texto)
    if match_vivienda:
        respuestas['vivienda'] = match_vivienda.group(1)
    else:
        respuestas['vivienda'] = None

    # Extraer pago de alquiler
    patron_pago_alquiler = r"¿Cuánto pagas de alquiler\? (\d+ euros)"
    match_pago_alquiler = re.search(patron_pago_alquiler, texto)
    if match_pago_alquiler:
        respuestas['pago_alquiler'] = match_pago_alquiler.group(1)
    else:
        respuestas['pago_alquiler'] = None
    
    return respuestas

# Extract answers from transcript conversation-questions and load into Dict
def save_answers(records):
    resultados = {}
    for _, row in records.iterrows():
        texto = row['transcript_text']
        respuestas = extraer_respuestas(texto)
        resultados[row['flow_export_id']] = respuestas
    # Save results in json
    with open(f'./Data/resultados-{current_year}-{month}-{day}.json', 'w') as file:
        json.dump(resultados, file, indent=4)
    return None

# Load to json in S3 to send to FrontEnd
def load_json_s3(current_year,month,day,bucket):
    print(f"Data Wrangler export storage bucket: {bucket}")
    file_name = f"resultados-{current_year}-{month}-{day}.json"
    data_location = 'output_data/{}/{}/{}/{}'.format(current_year,month,day,file_name)
    s3.meta.client.upload_file(f"./Data/{file_name}", bucket,data_location)
    
    
#Main function

if __name__ == "__main__":
    
    # main
    def main():
        # bucket info
        bucket = "hackatonforgoodpariss3bucket"
        # elements in bucket
        data_keys = [element for element in bucket_list if f"log_data/{current_year}/{month}/{day}/transcription" in element]

        # Date - month - day folder filters
        current_year = datetime.datetime.now().year
        month = datetime.datetime.now().month
        day = datetime.datetime.now().day
        
        # get records transcription
        records = get_s3_files(bucket,data_keys)
        records['transcript_text'] = records['results'].apply(extract_transcript_text)
        
        # extract logs of AWS transcribe 
        extract_items_text(records)
        
        #show DF with recordings
        records 
        # write df in Bucket
        write_s3_insights(current_year,month,day,bucket)
        
        #Save json file with data to fill forms
        save_answers(records)
        
        #Save in S3 bucket to send to frontend
        load_json_s3(current_year,month,day,bucket)
    print(f'Check the bucket {bucket} to obtain the processed information')