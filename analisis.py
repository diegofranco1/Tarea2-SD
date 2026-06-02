import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ==========================================
# RUTAS DE LOS ARCHIVOS CSV
# ==========================================
ARCHIVO_ZIPF = "kafka_consumer/metricas_zipf.csv"
ARCHIVO_UNIFORME = "kafka_consumer/metricas_uniforme.csv"

def analizar_metricas_completas():
    print("📊 Iniciando Análisis de Métricas Completas (Kafka)...\n")
    
    if not os.path.exists(ARCHIVO_ZIPF) or not os.path.exists(ARCHIVO_UNIFORME):
        print("❌ Error: No se encuentran los archivos CSV. Verifica la ruta.")
        return

    df_zipf = pd.read_csv(ARCHIVO_ZIPF)
    df_uni = pd.read_csv(ARCHIVO_UNIFORME)

    def calcular_metricas(df, nombre):
        total_mensajes = len(df)
        if total_mensajes == 0: return None
        
        # 1. Métricas Base (Rendimiento)
        hits = len(df[df['origen'] == 'Cache Hit (Redis)'])
        hit_rate = (hits / total_mensajes) * 100

        lat_prom = df['latencia_segundos'].mean()
        lat_p50 = df['latencia_segundos'].quantile(0.50)
        lat_p95 = df['latencia_segundos'].quantile(0.95)

        tiempo_total = df['tiempo_procesado'].max() - df['tiempo_creacion'].min()
        throughput = total_mensajes / tiempo_total if tiempo_total > 0 else 0

        # 2. Métricas de Tolerancia a Fallos (Kafka)
        if 'retry_count' in df.columns and 'status' in df.columns:
            reintentos_totales = len(df[df['retry_count'] > 0])
            dlq_mensajes = len(df[df['status'] == 'DLQ'])
            recuperados = len(df[(df['retry_count'] > 0) & (df['status'] != 'DLQ')])

            retry_rate = (reintentos_totales / total_mensajes) * 100
            dlq_rate = (dlq_mensajes / total_mensajes) * 100
            recovery_rate = (recuperados / total_mensajes) * 100
        else:
            retry_rate = dlq_rate = recovery_rate = 0.0

        # 3. Recovery Time
        tiempo_fin_inyeccion = df['tiempo_creacion'].max()
        tiempo_fin_procesamiento = df['tiempo_procesado'].max()
        recovery_time = max(0, tiempo_fin_procesamiento - tiempo_fin_inyeccion)

        print(f"--- RESULTADOS: {nombre.upper()} ---")
        print(f"🔹 Consultas procesadas: {total_mensajes}")
        print(f"🔹 Throughput:           {throughput:.2f} req/sec")
        print(f"🔹 Cache Hit Rate:       {hit_rate:.2f}%")
        print(f"🔹 Latencia p50 / p95:   {lat_p50:.4f} s / {lat_p95:.4f} s")
        print(f"🔹 Retry Rate:           {retry_rate:.2f}%")
        print(f"🔹 Recovery Rate:        {recovery_rate:.2f}%")
        print(f"🔹 DLQ Rate:             {dlq_rate:.2f}%")
        print(f"🔹 Recovery Time:        {recovery_time:.2f} s")
        print("-" * 40)

        return {
            'nombre': nombre, 'throughput': throughput, 'hit_rate': hit_rate,
            'p50': lat_p50, 'p95': lat_p95, 'retry_rate': retry_rate, 
            'dlq_rate': dlq_rate, 'recovery_time': recovery_time
        }

    m_zipf = calcular_metricas(df_zipf, "Zipf")
    m_uni = calcular_metricas(df_uni, "Uniforme")

    # ==========================================
    # GENERACIÓN DE GRÁFICOS AVANZADOS Y LIMPIOS
    # ==========================================
    sns.set_theme(style="whitegrid")

    # --- GRÁFICO 1: Throughput vs Latencia (Dos ejes Y para evitar que se aplasten) ---
    labels = ['Zipf', 'Uniforme']
    x = np.arange(len(labels))
    width = 0.35

    fig, ax1 = plt.subplots(figsize=(9, 5))
    
    # Eje Y Izquierdo: Throughput
    bar1 = ax1.bar(x - width/2, [m_zipf['throughput'], m_uni['throughput']], width, label='Throughput (req/s)', color='#2ecc71')
    ax1.set_ylabel('Throughput (req/s)', color='#27ae60', fontsize=12, fontweight='bold')
    ax1.tick_params(axis='y', labelcolor='#27ae60')
    
    # Eje Y Derecho: Latencia
    ax2 = ax1.twinx()
    bar2 = ax2.bar(x + width/2, [m_zipf['p95'], m_uni['p95']], width, label='Latencia p95 (s)', color='#e74c3c')
    ax2.set_ylabel('Latencia p95 (Segundos)', color='#c0392b', fontsize=12, fontweight='bold')
    ax2.tick_params(axis='y', labelcolor='#c0392b')
    
    # Títulos y Leyendas unificadas
    plt.title('Rendimiento del Sistema: Throughput vs Latencia Extrema', fontsize=14, pad=15)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    
    # Juntar las leyendas de ambos ejes
    lines, labels_leg = ax1.get_legend_handles_labels()
    lines2, labels_leg2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels_leg + labels_leg2, loc='upper left')
    
    plt.savefig('grafico_rendimiento_kafka.png', dpi=300, bbox_inches='tight')
    plt.close()

    # --- GRÁFICO 2: Evolución real del Backlog en el tiempo ---
    plt.figure(figsize=(10, 5))
    
    # Obtenemos los tiempos absolutos de inyección y procesamiento
    tiempos_inyeccion = df_zipf['tiempo_creacion'].sort_values().values
    tiempos_procesamiento = df_zipf['tiempo_procesado'].sort_values().values
    
    t_inicio = tiempos_inyeccion[0]
    t_fin = tiempos_procesamiento[-1]
    
    # Creamos una línea de tiempo uniforme desde el inicio hasta el fin de la prueba
    timeline = np.linspace(t_inicio, t_fin, 1000)
    timeline_secs = timeline - t_inicio # Para que el gráfico parta desde 0 segundos
    
    # Calculamos cuántos mensajes se habían inyectado y procesado en cada instante t
    inyecciones = np.searchsorted(tiempos_inyeccion, timeline)
    procesados = np.searchsorted(tiempos_procesamiento, timeline)
    
    # Graficamos las líneas
    plt.plot(timeline_secs, inyecciones, label='Mensajes Inyectados', color='#3498db', linewidth=2.5)
    plt.plot(timeline_secs, procesados, label='Mensajes Procesados', color='#2ecc71', linewidth=2.5)
    
    # El relleno entre las curvas es exactamente el tamaño del backlog en ese segundo
    plt.fill_between(timeline_secs, procesados, inyecciones, color='#e74c3c', alpha=0.2, label='Backlog (Mensajes en Cola)')
    
    plt.title('Evolución del Backlog y Recovery Time (Tráfico Zipf)', fontsize=14, pad=15)
    plt.xlabel('Tiempo Transcurrido (segundos)', fontsize=12)
    plt.ylabel('Cantidad de Mensajes', fontsize=12)
    plt.legend(loc='lower right')
    
    plt.savefig('grafico_backlog_kafka.png', dpi=300, bbox_inches='tight')
    plt.close()

    print("\n✅ Análisis completado. Gráficos corregidos y generados:")
    print("- grafico_rendimiento_kafka.png (Doble eje Y aplicado)")
    print("- grafico_backlog_kafka.png (Área de backlog limpia y precisa)")

if __name__ == "__main__":
    analizar_metricas_completas()