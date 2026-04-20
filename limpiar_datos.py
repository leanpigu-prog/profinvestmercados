import pandas as pd
import unicodedata
import os
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# =============================================================
# CONFIGURACION — ajusta la ruta si es necesario
# =============================================================
RUTA_ENTRADA = r'D:\Datos_PC\Descargas\Activos\Análisis_Datos\Datasets\Matriculados participación 2025v2.xlsx'

# Ruta original OneDrive (puede estar bloqueada si OneDrive está sincronizando):
# RUTA_ENTRADA = r'D:\Datos_PC\OneDrive - Universidad de Santander\Proyecto Udes en la Provincea\UDES en las regiones\Base de datos SNIES\Base de datos participación 2019-2023\Matriculados participación 2025v2.xlsx'

RUTA_SALIDA = r'C:\Users\prof.investmercados\Desktop\UDES_Dashboard\matriculados_limpio.csv'

# =============================================================
# Columnas que se conservan (de 48 -> 19)
# =============================================================
COLUMNAS = [
    'INSTITUCIÓN DE EDUCACIÓN SUPERIOR (IES)',
    'SECTOR IES',
    'CARACTER IES',
    'DEPARTAMENTO DE OFERTA DEL PROGRAMA',
    'MUNICIPIO DE OFERTA DEL PROGRAMA',
    'PROGRAMA ACADÉMICO',
    'PROGRAMA ACREDITADO',
    'NIVEL DE FORMACIÓN',
    'METODOLOGÍA',
    'ÁREA DE CONOCIMIENTO',
    'NÚCLEO BÁSICO DEL CONOCIMIENTO (NBC)',
    'AÑO',
    'SEMESTRE',
    'MATRICULADOS',
    'MATRICULADOS PRIMER CURSO',
    'COSTO MATRICULA',
    'NÚMERO CRÉDITOS',
    'NÚMERO PERIODOS DE DURACIÓN',
    'PERIODICIDAD',
]

COLUMNAS_TEXTO = [
    'INSTITUCIÓN DE EDUCACIÓN SUPERIOR (IES)',
    'DEPARTAMENTO DE OFERTA DEL PROGRAMA',
    'MUNICIPIO DE OFERTA DEL PROGRAMA',
    'PROGRAMA ACADÉMICO',
    'ÁREA DE CONOCIMIENTO',
    'NÚCLEO BÁSICO DEL CONOCIMIENTO (NBC)',
    'SECTOR IES',
    'CARACTER IES',
    'METODOLOGÍA',
    'PROGRAMA ACREDITADO',
    'NIVEL DE FORMACIÓN',
    'PERIODICIDAD',
]

COLUMNAS_NUMERICAS = [
    'MATRICULADOS',
    'MATRICULADOS PRIMER CURSO',
    'COSTO MATRICULA',
    'NÚMERO CRÉDITOS',
    'NÚMERO PERIODOS DE DURACIÓN',
]

# =============================================================
# Función de normalización de texto
# Convierte "BOGOTÁ", "Bogota", "bogota" → "BOGOTA"
# =============================================================
def normalizar(texto):
    if pd.isna(texto):
        return texto
    sin_tilde = unicodedata.normalize('NFD', str(texto))
    sin_tilde = ''.join(c for c in sin_tilde if unicodedata.category(c) != 'Mn')
    return sin_tilde.strip().upper()

# =============================================================
# MAIN
# =============================================================
if not os.path.exists(RUTA_ENTRADA):
    print(f"ERROR: No se encontró el archivo en:\n  {RUTA_ENTRADA}")
    print("Edita la variable RUTA_ENTRADA en este script y vuelve a correrlo.")
    sys.exit(1)

print("Leyendo archivo Excel...")
print("(Esto puede tomar entre 2 y 5 minutos — el archivo pesa 90MB)")
print()

try:
    df = pd.read_excel(RUTA_ENTRADA, engine='openpyxl')
except PermissionError:
    print("ERROR: El archivo Excel está abierto en Excel. Ciérralo y vuelve a correr el script.")
    sys.exit(1)
except Exception as e:
    print(f"ERROR inesperado al leer el archivo: {e}")
    sys.exit(1)

print(f"✓ Archivo leído: {len(df):,} filas, {len(df.columns)} columnas")
df.columns = [col.strip() for col in df.columns]

# Verificar que existen las columnas esperadas
columnas_faltantes = [c for c in COLUMNAS if c not in df.columns]
if columnas_faltantes:
    print("\nADVERTENCIA — estas columnas no se encontraron:")
    for c in columnas_faltantes:
        print(f"  - {c}")
    print("\nColumnas disponibles en el archivo:")
    for c in df.columns:
        print(f"  {c}")
    sys.exit(1)

# Seleccionar columnas
df = df[COLUMNAS].copy()

# Normalizar texto
print("Normalizando texto (tildes, mayúsculas)...")
for col in COLUMNAS_TEXTO:
    df[col] = df[col].apply(normalizar)

# Limpiar numéricos
for col in COLUMNAS_NUMERICAS:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

df['MATRICULADOS'] = df['MATRICULADOS'].astype(int)
df['MATRICULADOS PRIMER CURSO'] = df['MATRICULADOS PRIMER CURSO'].astype(int)
df['NÚMERO CRÉDITOS'] = df['NÚMERO CRÉDITOS'].astype(int)
df['NÚMERO PERIODOS DE DURACIÓN'] = df['NÚMERO PERIODOS DE DURACIÓN'].astype(int)

# Eliminar duplicados
filas_antes = len(df)
df = df.drop_duplicates()
filas_despues = len(df)
print(f"✓ Duplicados eliminados: {filas_antes - filas_despues:,} filas")

# Mostrar nombre exacto de UDES
print("\n--- Nombres de UDES encontrados en los datos ---")
udes = df[df['INSTITUCIÓN DE EDUCACIÓN SUPERIOR (IES)'].str.contains('UNIVERSIDAD DE SANTANDER', na=False)]
for nombre in sorted(udes['INSTITUCIÓN DE EDUCACIÓN SUPERIOR (IES)'].unique()):
    print(f"  {nombre}")

# =============================================================
# Aplicar GRUPO_ANALITICO desde el mapeo de normalización
# =============================================================
RUTA_MAPEO = r'C:\Users\prof.investmercados\Desktop\UDES_Dashboard\mapeo_programas.csv'

if os.path.exists(RUTA_MAPEO):
    print("\nAplicando GRUPO_ANALITICO...")
    mapeo_df = pd.read_csv(RUTA_MAPEO, encoding='utf-8-sig')

    # Construir dict con clave compuesta (programa, nivel, nbc)
    mapeo_dict = {
        (row['nombre_original'], row['nivel_formacion'], row['nbc']): row['GRUPO_ANALITICO']
        for _, row in mapeo_df.iterrows()
    }

    def asignar_grupo(row):
        key = (
            row['PROGRAMA ACADÉMICO'],
            row['NIVEL DE FORMACIÓN'],
            row['NÚCLEO BÁSICO DEL CONOCIMIENTO (NBC)'],
        )
        return mapeo_dict.get(key, row['PROGRAMA ACADÉMICO'])

    df['GRUPO_ANALITICO'] = df.apply(asignar_grupo, axis=1)
    asignados = (df['GRUPO_ANALITICO'] != df['PROGRAMA ACADÉMICO']).sum()
    print(f"  ✓ GRUPO_ANALITICO asignado: {asignados:,} filas normalizadas")
else:
    print(f"\nAVISO: No se encontró {RUTA_MAPEO}")
    print("  Ejecuta normalizar_programas.py primero para generar el mapeo.")
    df['GRUPO_ANALITICO'] = df['PROGRAMA ACADÉMICO']

# Guardar
print(f"\nGuardando CSV en:\n  {RUTA_SALIDA}")
df.to_csv(RUTA_SALIDA, index=False, encoding='utf-8-sig')

tam_mb = os.path.getsize(RUTA_SALIDA) / (1024 * 1024)
print(f"\n✓ ¡Listo! Archivo guardado ({tam_mb:.1f} MB)")
print(f"  Filas: {len(df):,}")
print(f"  Columnas: {len(df.columns)}")
print(f"  GRUPOS_ANALITICOS únicos: {df['GRUPO_ANALITICO'].nunique():,}")
print(f"\nRango de años en los datos:")
print(f"  {df['AÑO'].min()} — {df['AÑO'].max()}")
