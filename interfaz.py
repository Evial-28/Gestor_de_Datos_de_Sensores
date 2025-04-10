# Archivo: interfaz.py

import os
import tkinter as tk
from tkinter import messagebox
from tkinter import ttk # Importar ttk
import mysql.connector
# Importar funciones de otros scripts (asegúrate que estén accesibles)
# from download_attachment import main as run_download_attachment
# from get_data_to_database_test import main as run_get_data_to_db
import threading
import download_attachment # Para ejecutar en hilo
import subprocess # Para ejecutar get_data_to_database_test.py
import pandas as pd
import traceback
import queue
from PIL import Image, ImageTk

# --- FUNCIONES DE LA BASE DE DATOS (sin cambios) ---
def connect_to_database():
    try:
        conn = mysql.connector.connect(
            host="localhost", user="root", password="1234", database="labiot_data_sensed"
        )
        return conn
    except mysql.connector.Error as err:
        messagebox.showerror("Error de Conexión", f"Error de Conexión: {err}")
        return None

def consultar_sensor(sensor_id, conn):
    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        query = f"""
        SELECT id, sensor_id, time, water_flow_value, total_pulse,
               flow_per_hour, last_pulse, battery
        FROM sensor_data WHERE sensor_id = {sensor_id} ORDER BY time
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        raise Exception(f"Error en Consulta: {str(e)}")
    finally:
        if cursor: cursor.close()

def obtener_datos_sensor(conn):
    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT id, sensor_id, time, water_flow_value, total_pulse,
               flow_per_hour, last_pulse, battery
        FROM sensor_data
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        raise Exception(f"Error en Consulta General: {str(e)}")
    finally:
        if cursor: cursor.close()

# --- FUNCIONES DE LA INTERFAZ (consultar y descargar sin cambios funcionales) ---
def consultar_sensor_gui():
    sensor_map = {"sw01": 1, "swm-02": 2, "swm-03": 3, "swm-04": 4, "swm-05": 5}
    sensor_seleccionado = combo.get()
    sensor_id = sensor_map.get(sensor_seleccionado)

    if sensor_id is None:
        messagebox.showerror("Error de Selección", "Por favor, seleccione un sensor válido.")
        return

    conn = connect_to_database()
    if conn is None: return

    try:
        for item in tree.get_children(): tree.delete(item)

        rows = consultar_sensor(sensor_id, conn)

        if rows:
            for i, row in enumerate(rows):
                flow_value = row.get('flow_per_hour')
                flow_display = f"{flow_value:.2f}" if flow_value is not None else ""
                # Aplicar tag para colores alternos (puede que no funcione dependiendo del tema ttk)
                tag_to_use = 'evenrow' if i % 2 == 0 else 'oddrow'
                values_to_insert = (
                    row.get('id', ''), row.get('sensor_id', ''), row.get('time', ''),
                    row.get('water_flow_value', ''), row.get('total_pulse', ''),
                    flow_display, row.get('last_pulse', ''), row.get('battery', '')
                )
                tree.insert("", "end", values=values_to_insert, tags=(tag_to_use,)) # Añadir tag aquí
            messagebox.showinfo("Consulta Exitosa", f"Datos del sensor {sensor_seleccionado} consultados.")
        else:
            messagebox.showwarning("Sin Resultados", f"No se encontraron datos para {sensor_seleccionado}.")
    except Exception as e:
        messagebox.showerror("Error en Consulta GUI", str(e))
    finally:
        if conn and conn.is_connected(): conn.close()

def descargar_datos_gui():
    sensor_map = {"sw01": 1, "swm-02": 2, "swm-03": 3, "swm-04": 4, "swm-05": 5}
    sensor_seleccionado = combo.get()
    sensor_id = sensor_map.get(sensor_seleccionado)
    if sensor_id is None: messagebox.showerror("Error", "Seleccione sensor."); return
    conn = connect_to_database()
    if conn is None: return
    try:
        rows = consultar_sensor(sensor_id, conn)
        if rows:
            save_dir = "C:/Users/erika/Desktop/SERVICIO SOCIAL - BD/PYTHON/Datos en XLSX/"
            os.makedirs(save_dir, exist_ok=True)
            file_name = os.path.join(save_dir, f"sensor_data_{sensor_seleccionado}.xlsx")
            df = pd.DataFrame(rows)
            column_order = ['id', 'sensor_id', 'time', 'water_flow_value', 'total_pulse', 'flow_per_hour', 'last_pulse', 'battery']
            df_columns_existing = [col for col in column_order if col in df.columns]
            df = df[df_columns_existing]
            df.to_excel(file_name, index=False)
            messagebox.showinfo("Descarga Exitosa", f"Datos guardados en\n{file_name}.")
        else:
            messagebox.showwarning("Sin Datos", "No hay datos para descargar.")
    except Exception as e:
        messagebox.showerror("Error en Descarga", f"Error: {str(e)}\n{traceback.format_exc()}")
    finally:
        if conn and conn.is_connected(): conn.close()

# --- FUNCIONES PARA EJECUTAR SCRIPTS (sin cambios) ---
def ejecutar_download_attachment():
    result_queue = queue.Queue()
    def download_attachment_thread():
        try:
            download_attachment.main()
            result_queue.put("Descarga de archivos adjuntos completada.")
        except Exception as e:
            result_queue.put(f"Error descarga: {e}\n{traceback.format_exc()}")
    thread = threading.Thread(target=download_attachment_thread)
    thread.start()
    messagebox.showinfo("Descarga Iniciada", "Descargando adjuntos en segundo plano...")
    def check_result():
        try:
            result = result_queue.get_nowait()
            if "Error" in result: messagebox.showerror("Error Descarga", result)
            else: messagebox.showinfo("Descarga Completada", result)
        except queue.Empty: ventana.after(200, check_result)
    ventana.after(100, check_result)

def ejecutar_get_data_to_database_test():
    try:
        process = subprocess.Popen(["python", "get_data_to_database_test.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
        stdout, stderr = process.communicate()
        if process.returncode == 0:
            messagebox.showinfo("Carga a BD", "Script ejecutado.\nConsulta de nuevo para ver datos actualizados.")
        else:
            messagebox.showerror("Error Carga a BD", f"Error al ejecutar script:\n{stderr}")
    except FileNotFoundError: messagebox.showerror("Error", "No se encontró 'get_data_to_database_test.py' o 'python'.")
    except Exception as e: messagebox.showerror("Error", f"Error al ejecutar script:\n{e}")

# --- CREACIÓN DE LA VENTANA PRINCIPAL ---
ventana = tk.Tk()
ventana.title("Gestor de Sensores LabIoT")
ventana.geometry("950x700") # Un poco más ancho
ventana.configure(bg='#F5F5F5') # Un gris muy claro para el fondo

# --- ESTILOS ---
def configurar_estilos():
    global estilo, color_guinda, color_blanco, color_gris_claro, color_gris_muy_claro, color_gris_medio, color_texto_principal, color_texto_cabecera, color_seleccion_bg, color_seleccion_fg
    estilo = ttk.Style()

    # Paleta de colores
    color_guinda = '#6A0035'
    color_blanco = '#FFFFFF'
    color_gris_claro = '#F0F0F0' # Para fondos de frames, cabeceras
    color_gris_muy_claro = '#FAFAFA' # Para filas alternas
    color_gris_medio = '#E0E0E0' # Para borde activo/seleccion
    color_texto_principal = '#333333'
    color_texto_cabecera = '#444444'
    color_seleccion_bg = '#D5D5E0' # Fondo azulado/gris para selección
    color_seleccion_fg = '#000000' # Texto negro en selección

    try:
        # Probar tema 'clam' o 'alt' que suelen ser más personalizables
        estilo.theme_use('clam')
    except tk.TclError:
        print("Advertencia: Tema 'clam' no disponible, usando default.")
        try:
            estilo.theme_use('default') # Plan B
        except tk.TclError:
            print("Advertencia: No se pudo cambiar el tema ttk.")

    # --- Estilo Botones: SIN borde ttk, borde simulado con Frame ---
    estilo.configure('BotonPrincipal.TButton',
                   background=color_blanco,
                   foreground=color_guinda,
                   font=('Segoe UI', 10, 'bold'), # Fuente un poco diferente
                   padding=(15, 6), # Más padding horizontal
                   borderwidth=0,     # SIN borde ttk
                   relief="flat")     # Relieve plano
    estilo.map('BotonPrincipal.TButton',
              background=[('active', color_gris_muy_claro), ('pressed', color_gris_medio)], # Ligeros cambios al activar
              foreground=[('active', color_guinda), ('pressed', color_guinda)])

    # --- Estilo Cabeceras Treeview ---
    estilo.configure('Treeview.Heading',
                   font=('Segoe UI', 10, 'bold'),
                   background=color_gris_claro,
                   foreground=color_texto_cabecera,
                   relief='flat', # Borde plano
                   padding=(6, 8))

    # --- Estilo Treeview (Tabla) ---
    # Intentar añadir líneas de separación (puede no funcionar en todos los temas/OS)
    estilo.layout("Treeview", [('Treeview.treearea', {'sticky': 'nswe'})]) # Layout base
    estilo.configure("Treeview",
                   font=('Segoe UI', 10), # Fuente más grande para datos
                   rowheight=30,        # Filas más altas
                   background=color_blanco,
                   fieldbackground=color_blanco,
                   foreground=color_texto_principal,
                   borderwidth=0,
                   relief='flat')
    # Color de Selección
    estilo.map('Treeview',
               background=[('selected', color_seleccion_bg)],
               foreground=[('selected', color_seleccion_fg)])

    # Estilo Combobox
    estilo.configure('TCombobox', font=('Segoe UI', 10), padding=5)
    try: # Estilos específicos de estado/tema
        estilo.map('TCombobox', fieldbackground=[('readonly', color_blanco)])
        estilo.map('TCombobox', selectbackground=[('readonly', color_gris_medio)])
        estilo.map('TCombobox', selectforeground=[('readonly', color_texto_principal)])
    except tk.TclError: pass

    # Estilos Labels
    estilo.configure('Titulo.TLabel', font=('Segoe UI', 18, 'bold'), foreground=color_guinda, background=ventana['bg'], padding=(0, 10, 0, 5))
    estilo.configure('Label.TLabel', font=('Segoe UI', 10), foreground=color_texto_principal, background=ventana['bg'])
    estilo.configure('Content.TFrame', background=ventana['bg'])

# --- Función para crear botones con borde de color ---
def crear_boton_con_borde(parent, text, command, color_borde, padding_borde=2):
    # Frame exterior que simula el borde
    borde_frame = tk.Frame(parent, bg=color_borde)
    # Botón ttk interior
    boton = ttk.Button(borde_frame, text=text, command=command, style='BotonPrincipal.TButton')
    # Empaquetar el botón dentro del frame con un padding que crea el efecto borde
    boton.pack(padx=padding_borde, pady=padding_borde, fill='both', expand=True)
    return borde_frame # Devolver el frame que contiene todo

# --- WIDGETS ---
configurar_estilos()

main_frame = ttk.Frame(ventana, padding=20, style='Content.TFrame')
main_frame.pack(fill='both', expand=True)

# --- Frame superior (Logos y Título) ---
top_frame = ttk.Frame(main_frame, style='Content.TFrame')
top_frame.pack(fill='x', pady=(0, 15))
top_frame.grid_columnconfigure(0, weight=1); top_frame.grid_columnconfigure(1, weight=3); top_frame.grid_columnconfigure(2, weight=1)

# Cargar Logos
logo_label_ipn = None; logo_label_citedi = None
try:
    logo_img_ipn_orig = Image.open("logo1.png"); logo_img_citedi_orig = Image.open("logo2.png")
    target_height_ipn = 100; target_height_citedi = 45
    # Redimensionar IPN
    w, h = logo_img_ipn_orig.size
    ratio = min(target_height_ipn / h, 1) # No agrandar si es más pequeña
    logo_img_ipn = logo_img_ipn_orig.resize((int(w * ratio), int(h * ratio)), Image.Resampling.LANCZOS)
    # Redimensionar CITEDI
    w, h = logo_img_citedi_orig.size
    ratio = min(target_height_citedi / h, 1)
    logo_img_citedi = logo_img_citedi_orig.resize((int(w * ratio), int(h * ratio)), Image.Resampling.LANCZOS)

    logo_tk_ipn = ImageTk.PhotoImage(logo_img_ipn); logo_tk_citedi = ImageTk.PhotoImage(logo_img_citedi)
    # Colocar logos (IPN izquierda, CITEDI derecha)
    logo_label_ipn = tk.Label(top_frame, image=logo_tk_ipn, bg=ventana['bg']); logo_label_ipn.grid(row=0, column=0, padx=(0, 10), pady=5, sticky='w'); logo_label_ipn.image = logo_tk_ipn
    logo_label_citedi = tk.Label(top_frame, image=logo_tk_citedi, bg=ventana['bg']); logo_label_citedi.grid(row=0, column=2, padx=(10, 0), pady=5, sticky='e'); logo_label_citedi.image = logo_tk_citedi
except Exception as e: print(f"Error al cargar logos: {e}")

# Título
title_frame = ttk.Frame(top_frame, style='Content.TFrame'); title_frame.grid(row=0, column=1, sticky='ew'); title_frame.grid_columnconfigure(0, weight=1)
titulo_label = ttk.Label(title_frame, text="Gestor de Datos de Sensores", style='Titulo.TLabel', anchor='center'); titulo_label.grid(row=0, column=0, pady=(5,0), sticky='ew')

# --- Frame de Controles ---
controls_frame = ttk.Frame(main_frame, style='Content.TFrame')
controls_frame.pack(fill='x', pady=10)

# Selección de Sensor
sensor_select_frame = ttk.Frame(controls_frame, style='Content.TFrame'); sensor_select_frame.pack(side='left', padx=(0, 10), fill='x', expand=True)
sensor_label = ttk.Label(sensor_select_frame, text="Sensor:", style='Label.TLabel'); sensor_label.pack(side='left', padx=(0, 5))
combo = ttk.Combobox(sensor_select_frame, values=["sw01", "swm-02", "swm-03", "swm-04", "swm-05"], style='TCombobox', state='readonly'); combo.set("Selecciona uno"); combo.pack(side='left', fill='x', expand=True)

# --- Botones de Acción con borde ---
# Crear botones usando la función auxiliar
borde_boton_consultar = crear_boton_con_borde(controls_frame, "Consultar", consultar_sensor_gui, color_guinda, padding_borde=1)
borde_boton_consultar.pack(side='left', padx=6)

borde_boton_descargar = crear_boton_con_borde(controls_frame, "Guardar XLSX", descargar_datos_gui, color_guinda, padding_borde=1)
borde_boton_descargar.pack(side='left', padx=6)

# --- Frame de Scripts ---
scripts_frame = ttk.Frame(main_frame, style='Content.TFrame')
scripts_frame.pack(fill='x', pady=15) # Más espacio vertical

# --- Botones de Scripts con borde ---
borde_boton_download = crear_boton_con_borde(scripts_frame, "Descargar Reportes Gmail", ejecutar_download_attachment, color_guinda, padding_borde=1)
borde_boton_download.pack(side='left', padx=6, fill='x', expand=True)

borde_boton_get_data = crear_boton_con_borde(scripts_frame, "Cargar Datos a BD", ejecutar_get_data_to_database_test, color_guinda, padding_borde=1)
borde_boton_get_data.pack(side='left', padx=6, fill='x', expand=True)


# --- Frame del Treeview ---
tree_container_frame = ttk.Frame(main_frame, style='Content.TFrame')
tree_container_frame.pack(fill='both', expand=True, pady=(10, 0))

scrollbar_vertical = ttk.Scrollbar(tree_container_frame, orient="vertical")
scrollbar_horizontal = ttk.Scrollbar(tree_container_frame, orient="horizontal")

# --- Treeview ---
# Configurar para mostrar líneas (si el tema lo soporta)
tree = ttk.Treeview(tree_container_frame,
                    columns=("ID", "Sensor ID", "Time", "Water Flow", "Total Pulse", "Flow Per Hour", "Last Pulse", "Battery"),
                    show='headings', # Cambiar a 'tree headings' si quieres líneas de árbol
                    yscrollcommand=scrollbar_vertical.set,
                    xscrollcommand=scrollbar_horizontal.set,
                    style='Treeview',
                    padding=(5, 0, 5, 0)) # Padding horizontal en celdas

# --- Añadir líneas de separación (solo funciona en algunos temas) ---
style = ttk.Style()
# Podría necesitar 'vista' en Windows, 'aqua' en Mac, o 'clam'/'alt'
if 'clam' in estilo.theme_names() or 'alt' in estilo.theme_names():
     try:
         estilo.configure("Treeview", rowheight=30) # Re-asegurar altura si se usa clam/alt
         # Intenta configurar el layout para mostrar separadores
         # Esto es muy dependiente del tema y puede no funcionar
         estilo.layout("Treeview.Item",
                      [('Treeitem.padding', {'sticky': 'nswe', 'children':
                          [('Treeitem.indicator', {'side': 'left', 'sticky': ''}),
                           ('Treeitem.image', {'side': 'left', 'sticky': ''}),
                           #('Treeitem.focus', {'side': 'left', 'sticky': '', 'children': [
                           ('Treeitem.text', {'side': 'left', 'sticky': ''}),
                           #]})
                           ]})])
         # Intenta añadir un separador visual (puede no funcionar)
         # estilo.configure('Treeview.Separator', background='#CCCCCC')
     except Exception as e:
         print(f"No se pudo configurar layout/separador del Treeview: {e}")


scrollbar_vertical.config(command=tree.yview)
scrollbar_horizontal.config(command=tree.xview)
scrollbar_vertical.pack(side="right", fill="y")
scrollbar_horizontal.pack(side="bottom", fill="x")
tree.pack(side="left", fill="both", expand=True)

# Configurar Tags para filas alternas (redefinir por si acaso)
tree.tag_configure('oddrow', background=color_gris_muy_claro, foreground=color_texto_principal)
tree.tag_configure('evenrow', background=color_blanco, foreground=color_texto_principal)

# Cabeceras y Columnas (sin cambios en definición, solo estilo arriba)
tree.heading("ID", text="ID", anchor=tk.CENTER); tree.column("ID", width=40, anchor=tk.CENTER, stretch=tk.NO)
tree.heading("Sensor ID", text="Sensor", anchor=tk.CENTER); tree.column("Sensor ID", width=60, anchor=tk.CENTER, stretch=tk.NO)
tree.heading("Time", text="Timestamp", anchor=tk.W); tree.column("Time", width=150, anchor=tk.W, stretch=tk.YES)
tree.heading("Water Flow", text="W Flow", anchor=tk.E); tree.column("Water Flow", width=100, anchor=tk.E)
tree.heading("Total Pulse", text="T Pulse", anchor=tk.E); tree.column("Total Pulse", width=100, anchor=tk.E)
tree.heading("Flow Per Hour", text="Flow/Hr", anchor=tk.E); tree.column("Flow Per Hour", width=90, anchor=tk.E)
tree.heading("Last Pulse", text="L Pulse", anchor=tk.E); tree.column("Last Pulse", width=90, anchor=tk.E)
tree.heading("Battery", text="Bat.", anchor=tk.E); tree.column("Battery", width=50, anchor=tk.E, stretch=tk.NO)

# --- BUCLE PRINCIPAL ---
ventana.mainloop()