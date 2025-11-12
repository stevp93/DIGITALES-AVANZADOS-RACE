from flask import Flask, render_template, request, jsonify, redirect, url_for
import datetime
from datetime import timezone
import time
from waitress import serve
import threading
import os
from queue import Queue, Empty
import pytz
import gspread
# --- Importaciones de Firebase ---
import firebase_admin
from firebase_admin import credentials, firestore
LOCAL_TZ = pytz.timezone("America/Bogota")

# --- Configuración e Inicialización de Firebase ---
CREDS_FILE = 'firebase-creds.json'
COLLECTION_COMPETITORS = 'competitors'
COLLECTION_RACE_STATUS = 'race_status'

script_dir = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(script_dir, CREDS_FILE)

# --- Configuración de Google Sheets ---
SHEET_NAME = "Tiempos Carrera RFID" 
sheets_queue = Queue()

# --- Función de Ayuda para Borrar Colección ---
def delete_collection(coll_ref, batch_size):
    """Borra una colección de Firestore en lotes."""
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0
    for doc in docs:
        doc.reference.delete()
        deleted += 1
    return deleted

# --- Inicialización de Firebase ---
try:
    if not firebase_admin._apps:
        cred = credentials.Certificate(creds_path)
        firebase_admin.initialize_app(cred)
        
    db = firestore.client()
    print("Conexión con Firebase Firestore exitosa.")
    
    print("Forzando reseteo del estado de la carrera...")
    db.collection(COLLECTION_RACE_STATUS).document('status').set({
        'raceStarted': False,
        'globalStartTime': None
    })
    
    print("Borrando datos de la carrera anterior...")
    coll_ref = db.collection(COLLECTION_COMPETITORS)
    while True:
        deleted_count = delete_collection(coll_ref, 10) 
        if deleted_count == 0:
            break
    
    print("¡Sistema Firebase reseteado y listo!") # <-- Corregí el log
    
except Exception as e:
    print(f"Error al conectar/resetear Firebase: {e}")
    db = None

# --- Inicialización de Google Sheets ---
try:
    gc = gspread.service_account(filename=creds_path)
    sh = gc.open(SHEET_NAME).sheet1
    sh.clear()
    # --- CORRECCIÓN: Título de columna ---
    sh.append_row(["Evento", "Tag ID", "Nombre", "Hora de Registro"])
    print("Conexión con Google Sheets exitosa y hoja limpiada.")
except Exception as e:
    print(f"Error al conectar con Google Sheets: {e}")    
    sh = None

# --- Hilo: Google Sheets ---
def sheets_worker():
    """Toma datos de 'sheets_queue' y los escribe en Google Sheets."""
    if not sh:
        print("Hilo de Google Sheets no iniciado (error de conexión).")
        return
    
    print("Hilo de Google Sheets iniciado.")
    while True:
        try:
            data_row = sheets_queue.get(timeout=5) 
            utc_time = data_row[3].replace(tzinfo=timezone.utc)
            local_time = utc_time.astimezone(LOCAL_TZ)
            data_row[3] = local_time.strftime('%Y-%m-%d %H:%M:%S')
            sh.append_row(data_row)
            sheets_queue.task_done()
        except Empty:
            pass
        except Exception as e:
            print(f"Error en el hilo de Sheets: {e}")
            time.sleep(30)

# --- Inicialización de Flask ---
app = Flask(__name__)

# --- API Endpoints ---

@app.route('/api/register_competitor', methods=['POST'])
def register_competitor():
    if not db: return jsonify({"error": "Base de datos no conectada"}), 500
    try:
        data = request.json
        tag_id = data['tag_id']
        tag_name = data['tag_name']
        if not tag_id or not tag_name:
            return jsonify({"error": "Faltan tag_id o tag_name"}), 400
        doc_ref = db.collection(COLLECTION_COMPETITORS).document(tag_id)
        doc_ref.set({
            'name': tag_name, 'startTime': None, 'cp2Time': None, 
            'cp3Time': None, 'totalTime': None, 'totalTimeSeconds': float('inf')
        })
        if sh:
            sheets_queue.put([
                "Competidor Registrado", tag_id, tag_name, 
                datetime.datetime.now() # Usamos hora local para el log
            ])
        print(f"Competidor registrado/actualizado: {tag_id} -> {tag_name}")
        return jsonify({"status": "success", "message": f"{tag_name} registrado."}), 201
    except Exception as e:
        print(f"Error en /api/register_competitor: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/start_race', methods=['POST'])
def start_race():
    if not db: return jsonify({"error": "Base de datos no conectada"}), 500
    db.collection(COLLECTION_RACE_STATUS).document('status').set({
        'raceStarted': True,
        'globalStartTime': firestore.SERVER_TIMESTAMP
    })
    
    def update_start_times():
        print("Iniciando actualización de tiempos de inicio para todos...")
        competitors_ref = db.collection(COLLECTION_COMPETITORS)
        docs = competitors_ref.stream()
        batch = db.batch()
        for doc in docs:
            batch.update(doc.reference, {'startTime': firestore.SERVER_TIMESTAMP})
        batch.commit()
        print("¡Tiempos de inicio actualizados para todos!")
        if sh:
            # --- CORRECCIÓN: Datos para la cola ---
            sheets_queue.put([
                "CARRERA INICIADA", "Hora de Inicio", "", 
                datetime.datetime.now() # Usamos hora local
            ])
            
    threading.Thread(target=update_start_times).start()
    return redirect(url_for('dashboard'))

#registros de tiempos 
@app.route('/api/register_time', methods=['POST'])
def register_time():
    if not db: return jsonify({"error": "Base de datos no conectada"}), 500
    try:
        data = request.json
        tag_id = data['tag_id']
        checkpoint_id = data['checkpoint_id']
        timestamp = datetime.datetime.now(timezone.utc)
        doc_ref = db.collection(COLLECTION_COMPETITORS).document(tag_id)
        
        doc = doc_ref.get()
        if not doc.exists:
            return jsonify({"error": "Tag no registrado"}), 404
            
        competitor_data = doc.to_dict()
        start_time = competitor_data.get('startTime')
        
        if not start_time:
            return jsonify({"error": "La carrera no ha iniciado"}), 400

        update_data = {}
        log_evento = ""
        
        if checkpoint_id == "Checkpoint_2":            
            if competitor_data.get('cp2Time') is not None:
                print(f"ADVERTENCIA: {tag_id} ya tiene un tiempo en CP2. Ignorando.")
                return jsonify({"status": "ignored", "message": "Time already registered"}), 200
                
            update_data['cp2Time'] = timestamp
            log_evento = "Tiempo CP2 Registrado"
            print(f"Registrado CP2 para {tag_id}")
            
        elif checkpoint_id == "Checkpoint_3":            
            if competitor_data.get('cp3Time') is not None:
                print(f"ADVERTENCIA: {tag_id} ya tiene un tiempo final. Ignorando.")
                return jsonify({"status": "ignored", "message": "Time already registered"}), 200
                
            update_data['cp3Time'] = timestamp
            total_time_delta = timestamp - start_time
            total_seconds = total_time_delta.total_seconds()
            update_data['totalTime'] = str(total_time_delta).split('.')[0]
            update_data['totalTimeSeconds'] = total_seconds
            log_evento = "TIEMPO FINAL CP3"
            print(f"Registrado CP3 para {tag_id}. Tiempo Total: {total_seconds}s")
        
        if update_data:
            doc_ref.update(update_data)
            if sh:
                sheets_queue.put([
                    log_evento, tag_id, competitor_data.get('name', 'N/A'), 
                    datetime.datetime.now() # Usamos hora local
                ])
            
        return jsonify({"status": "success"}), 201
    except Exception as e:
        print(f"Error en /api/register_time: {e}")
        return jsonify({"error": str(e)}), 500

# --- Página Web Dashboard ) ---
@app.route('/')
def dashboard():
    """
    Muestra la tabla de resultados.
    """
    if not db:
        return "Error: No se pudo conectar a la base de datos de Firebase.", 500

    results = []
    winner_name = None
    min_time = float('inf')    
    winner_time_str = "---"
    
    status_doc = db.collection(COLLECTION_RACE_STATUS).document('status').get()
    race_status = status_doc.to_dict()
    
    competitors_docs = db.collection(COLLECTION_COMPETITORS).stream()
    
    for doc in competitors_docs:
        res = doc.to_dict()
        res['tag_id'] = doc.id
        
        if res.get('startTime'):
            res['startTime_str'] = res['startTime'].astimezone().strftime('%Y-%m-%d %H:%M:%S')
        if res.get('cp2Time'):
            res['cp2Time_str'] = res['cp2Time'].astimezone().strftime('%Y-%m-%d %H:%M:%S')
        if res.get('cp3Time'):
            res['cp3Time_str'] = res['cp3Time'].astimezone().strftime('%Y-%m-%d %H:%M:%S')
            
        results.append(res)
        
        if res.get('totalTimeSeconds') and res['totalTimeSeconds'] < min_time:
            min_time = res['totalTimeSeconds']
            winner_name = res['name']
            winner_time_str = res.get('totalTime', '---')
            
    results_sorted = sorted(results, key=lambda x: x.get('totalTimeSeconds', float('inf')))

    return render_template('index.html', 
                           results_sorted=results_sorted,
                           race_status=race_status,
                           winner_name=winner_name,
                           winner_time_str=winner_time_str) 

if __name__ == '__main__':
    threading.Thread(target=sheets_worker, daemon=True).start()
    print(f"Iniciando servidor web en http://0.0.0.0:5000")

    serve(app, host='0.0.0.0', port=5000)
