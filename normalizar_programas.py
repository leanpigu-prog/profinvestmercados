"""
normalizar_programas.py
=======================
Agrupa programas académicos por GRUPO_ANALITICO siguiendo 8 reglas:
  1. No mezclar niveles de formación
  2. No mezclar NBC
  3. Normalización previa (mayúsculas, sin tildes, sin stopwords)
  4. Diccionario semántico como primera prioridad
  5. Fuzzy matching (token_set_ratio) como complemento con umbrales dinámicos
  6. Sin agrupaciones transitivas (líder directo)
  7. Output: campo GRUPO_ANALITICO
  8. La agrupación representa un mercado, no solo similitud textual
"""
import sys
import subprocess
import unicodedata
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

try:
    from rapidfuzz import fuzz
except ImportError:
    print("Instalando rapidfuzz...")
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'rapidfuzz'])
    from rapidfuzz import fuzz

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# RUTAS
# ─────────────────────────────────────────────────────────────────────────────
CSV_ENTRADA = r'C:\Users\prof.investmercados\Desktop\UDES_Dashboard\matriculados_limpio.csv'
CSV_MAPEO   = r'C:\Users\prof.investmercados\Desktop\UDES_Dashboard\mapeo_programas.csv'
TXT_GRUPOS  = r'C:\Users\prof.investmercados\Desktop\UDES_Dashboard\grupos_programas.txt'

COL_PROG  = 'PROGRAMA ACADÉMICO'
COL_NIVEL = 'NIVEL DE FORMACIÓN'
COL_NBC   = 'NÚCLEO BÁSICO DEL CONOCIMIENTO (NBC)'
COL_AREA  = 'ÁREA DE CONOCIMIENTO'

# ─────────────────────────────────────────────────────────────────────────────
# REGLA 3 — Normalización previa para comparación
# (no modifica el nombre que se muestra, solo el texto que se compara)
# ─────────────────────────────────────────────────────────────────────────────
STOPWORDS = {
    'DE', 'EN', 'PARA', 'Y', 'A', 'E', 'O', 'U',
    'LA', 'LAS', 'LOS', 'EL', 'DEL', 'AL', 'CON',
    'SU', 'POR', 'UN', 'UNA', 'CON', 'LO',
}

# Prefijos de nivel usados en nombres de programas → forma natural con preposición
NIVEL_DISPLAY = {
    'DOCTORADO':       'DOCTORADO EN',
    'MAESTRIA':        'MAESTRIA EN',
    'ESPECIALIZACION': 'ESPECIALIZACION EN',
    'TECNOLOGIA':      'TECNOLOGIA EN',
    'TECNOLOGO':       'TECNOLOGO EN',
    'TECNICO':         'TECNICO EN',
    'TECNICA':         'TECNICA EN',
    'LICENCIATURA':    'LICENCIATURA EN',
}

def quitar_tildes(texto: str) -> str:
    nfkd = unicodedata.normalize('NFD', str(texto))
    return ''.join(c for c in nfkd if unicodedata.category(c) != 'Mn')

def clave_cmp(nombre: str) -> str:
    """Versión normalizada solo para comparación: sin tildes, mayúsculas, sin stopwords."""
    tokens = quitar_tildes(nombre).upper().split()
    return ' '.join(t for t in tokens if t not in STOPWORDS)

def buscar_en_semantico(clave: str) -> str | None:
    """
    Busca en SEMANTICO por clave exacta, luego por sufijo progresivo.
    Si el sufijo coincide, reconstruye el GRUPO_ANALITICO con el prefijo de nivel.
    Ejemplo: 'DOCTORADO ESTUDIOS PAZ' → sufijo 'ESTUDIOS PAZ' → 'DOCTORADO EN ESTUDIOS PARA LA PAZ'
    """
    if clave in SEMANTICO:
        return SEMANTICO[clave]
    tokens = clave.split()
    for i in range(1, len(tokens)):
        sufijo = ' '.join(tokens[i:])
        if sufijo in SEMANTICO:
            prefix = tokens[0]
            nivel_str = NIVEL_DISPLAY.get(prefix, prefix)
            return f"{nivel_str} {SEMANTICO[sufijo]}".strip()
    return None

# ─────────────────────────────────────────────────────────────────────────────
# REGLA 4 — Diccionario semántico (PRIORIDAD ALTA)
# Clave: clave_cmp(nombre) → GRUPO_ANALITICO canónico
# Agrega aquí variantes locales de UDES o del mercado colombiano según necesites
# ─────────────────────────────────────────────────────────────────────────────
SEMANTICO: dict[str, str] = {

    # ── ADMINISTRACIÓN ────────────────────────────────────────────────────────
    'ADMINISTRACION':                              'ADMINISTRACION DE EMPRESAS',
    'ADMINISTRACION EMPRESAS':                     'ADMINISTRACION DE EMPRESAS',
    'ADMINISTRACION EMPRESARIAL':                  'ADMINISTRACION DE EMPRESAS',
    'ADMINISTRACION NEGOCIOS':                     'ADMINISTRACION DE EMPRESAS',
    'ADMINISTRACION NEGOCIOS INTERNACIONALES':     'NEGOCIOS INTERNACIONALES',
    'NEGOCIOS INTERNACIONALES':                    'NEGOCIOS INTERNACIONALES',
    'COMERCIO INTERNACIONAL':                      'NEGOCIOS INTERNACIONALES',
    'COMERCIO EXTERIOR':                           'NEGOCIOS INTERNACIONALES',

    # ── SISTEMAS / INFORMÁTICA ────────────────────────────────────────────────
    'INGENIERIA SISTEMAS':                         'INGENIERIA DE SISTEMAS',
    'INGENIERIA INFORMATICA':                      'INGENIERIA DE SISTEMAS',
    'INGENIERIA SISTEMAS INFORMATICA':             'INGENIERIA DE SISTEMAS',
    'INGENIERIA SISTEMAS COMPUTACION':             'INGENIERIA DE SISTEMAS',
    'INGENIERIA SOFTWARE':                         'INGENIERIA DE SISTEMAS',
    'SISTEMAS':                                    'INGENIERIA DE SISTEMAS',

    # ── INDUSTRIAL ────────────────────────────────────────────────────────────
    'INGENIERIA INDUSTRIAL':                       'INGENIERIA INDUSTRIAL',
    'INGENIERIA PRODUCCION':                       'INGENIERIA INDUSTRIAL',

    # ── CIVIL ────────────────────────────────────────────────────────────────
    'INGENIERIA CIVIL':                            'INGENIERIA CIVIL',

    # ── ELECTRÓNICA / TELECOMUNICACIONES ──────────────────────────────────────
    'INGENIERIA ELECTRONICA':                      'INGENIERIA ELECTRONICA Y TELECOMUNICACIONES',
    'INGENIERIA TELECOMUNICACIONES':               'INGENIERIA ELECTRONICA Y TELECOMUNICACIONES',
    'INGENIERIA ELECTRONICA TELECOMUNICACIONES':   'INGENIERIA ELECTRONICA Y TELECOMUNICACIONES',

    # ── DERECHO ───────────────────────────────────────────────────────────────
    'DERECHO':                                     'DERECHO',
    'CIENCIAS JURIDICAS':                          'DERECHO',
    'DERECHO CIENCIAS JURIDICAS':                  'DERECHO',
    'CIENCIAS JURIDICAS POLITICAS':                'DERECHO',

    # ── CONTADURÍA ────────────────────────────────────────────────────────────
    'CONTADURIA':                                  'CONTADURIA PUBLICA',
    'CONTADURIA FINANZAS':                         'CONTADURIA PUBLICA',
    'CONTADURIA PUBLICA FINANZAS':                 'CONTADURIA PUBLICA',

    # ── ECONOMÍA ──────────────────────────────────────────────────────────────
    'ECONOMIA':                                    'ECONOMIA',
    'ECONOMIA FINANZAS':                           'ECONOMIA',

    # ── PSICOLOGÍA ────────────────────────────────────────────────────────────
    'PSICOLOGIA':                                  'PSICOLOGIA',

    # ── MEDICINA ──────────────────────────────────────────────────────────────
    'MEDICINA':                                    'MEDICINA',
    'MEDICINA HUMANA':                             'MEDICINA',
    'MEDICINA CIRUGIA':                            'MEDICINA',

    # ── ENFERMERÍA ────────────────────────────────────────────────────────────
    'ENFERMERIA':                                  'ENFERMERIA',

    # ── ODONTOLOGÍA ───────────────────────────────────────────────────────────
    'ODONTOLOGIA':                                 'ODONTOLOGIA',

    # ── BACTERIOLOGÍA ─────────────────────────────────────────────────────────
    'BACTERIOLOGIA':                               'BACTERIOLOGIA Y LABORATORIO CLINICO',
    'BACTERIOLOGIA LABORATORIO CLINICO':           'BACTERIOLOGIA Y LABORATORIO CLINICO',

    # ── NUTRICIÓN ─────────────────────────────────────────────────────────────
    'NUTRICION DIETETICA':                         'NUTRICION Y DIETETICA',
    'NUTRICION':                                   'NUTRICION Y DIETETICA',

    # ── FISIOTERAPIA / TERAPIAS ───────────────────────────────────────────────
    'FISIOTERAPIA':                                'FISIOTERAPIA',
    'TERAPIA OCUPACIONAL':                         'TERAPIA OCUPACIONAL',
    'FONOAUDIOLOGIA':                              'FONOAUDIOLOGIA',

    # ── TRABAJO SOCIAL ────────────────────────────────────────────────────────
    'TRABAJO SOCIAL':                              'TRABAJO SOCIAL',

    # ── COMUNICACIÓN ──────────────────────────────────────────────────────────
    'COMUNICACION SOCIAL':                         'COMUNICACION SOCIAL Y PERIODISMO',
    'COMUNICACION SOCIAL PERIODISMO':              'COMUNICACION SOCIAL Y PERIODISMO',
    'PERIODISMO':                                  'COMUNICACION SOCIAL Y PERIODISMO',

    # ── MERCADEO ──────────────────────────────────────────────────────────────
    'MERCADEO':                                    'MERCADEO Y PUBLICIDAD',
    'MARKETING':                                   'MERCADEO Y PUBLICIDAD',
    'MERCADEO PUBLICIDAD':                         'MERCADEO Y PUBLICIDAD',
    'PUBLICIDAD MERCADEO':                         'MERCADEO Y PUBLICIDAD',

    # ── EDUCACIÓN (LICENCIATURAS) ─────────────────────────────────────────────
    'LICENCIATURA MATEMATICAS':                    'LICENCIATURA EN MATEMATICAS',
    'LICENCIATURA MATEMATICAS ESTADISTICA':        'LICENCIATURA EN MATEMATICAS',
    'LICENCIATURA BIOLOGIA':                       'LICENCIATURA EN CIENCIAS NATURALES',
    'LICENCIATURA CIENCIAS NATURALES':             'LICENCIATURA EN CIENCIAS NATURALES',
    'LICENCIATURA CIENCIAS NATURALES EDUCACION AMBIENTAL': 'LICENCIATURA EN CIENCIAS NATURALES',
    'LICENCIATURA EDUCACION BASICA CIENCIAS NATURALES': 'LICENCIATURA EN CIENCIAS NATURALES',
    'LICENCIATURA INGLES':                         'LICENCIATURA EN LENGUAS EXTRANJERAS',
    'LICENCIATURA LENGUAS EXTRANJERAS':            'LICENCIATURA EN LENGUAS EXTRANJERAS',
    'LICENCIATURA LENGUAS EXTRANJERAS INGLES':     'LICENCIATURA EN LENGUAS EXTRANJERAS',
    'LICENCIATURA LENGUAS MODERNAS':               'LICENCIATURA EN LENGUAS EXTRANJERAS',

    # ── ARQUITECTURA ──────────────────────────────────────────────────────────
    'ARQUITECTURA':                                'ARQUITECTURA',

    # ── VETERINARIA ───────────────────────────────────────────────────────────
    'MEDICINA VETERINARIA':                        'MEDICINA VETERINARIA',
    'MEDICINA VETERINARIA ZOOTECNIA':              'MEDICINA VETERINARIA Y ZOOTECNIA',
    'ZOOTECNIA':                                   'ZOOTECNIA',

    # ── GOBIERNO / POLÍTICA PÚBLICA ───────────────────────────────────────────
    # Mercado distinto a Administración de Empresas (Regla 8)
    'GOBIERNO POLITICA PUBLICA':                   'GOBIERNO Y POLITICA PUBLICA',
    'GOBIERNO POLITICA PUBLICA ADMINISTRACION PUBLICA': 'GOBIERNO Y POLITICA PUBLICA',
    'ADMINISTRACION PUBLICA':                      'ADMINISTRACION PUBLICA',
    'GESTION PUBLICA':                             'ADMINISTRACION PUBLICA',
    'CIENCIA POLITICA':                            'CIENCIA POLITICA',
    'CIENCIA POLITICA GOBIERNO':                   'CIENCIA POLITICA',
    'CIENCIAS POLITICAS':                          'CIENCIA POLITICA',

    # ── CIENCIAS DEL MAR ──────────────────────────────────────────────────────
    'CIENCIAS MAR':                                'CIENCIAS DEL MAR',
    'OCEANOGRAFIA':                                'CIENCIAS DEL MAR',

    # ── ESTUDIOS (mercados distintos, no mezclar) ─────────────────────────────
    'ESTUDIOS PAZ':                                'ESTUDIOS PARA LA PAZ',
    'ESTUDIOS FAMILIA':                            'ESTUDIOS DE FAMILIA',
    'ESTUDIOS GENERO':                             'ESTUDIOS DE GENERO',
    'ESTUDIOS CULTURALES':                         'ESTUDIOS CULTURALES',
    'ESTUDIOS LITERARIOS':                         'ESTUDIOS LITERARIOS',
    'ESTUDIOS POLITICOS':                          'ESTUDIOS POLITICOS',
    'ESTUDIOS POLITICOS RELACIONES INTERNACIONALES': 'ESTUDIOS POLITICOS Y RELACIONES INTERNACIONALES',
    'ESTUDIOS POLITICOS INTERNACIONALES':          'ESTUDIOS POLITICOS Y RELACIONES INTERNACIONALES',
    'ESTUDIOS POLITICOS JURIDICOS':                'ESTUDIOS POLITICOS Y JURIDICOS',

    # ── SALUD PÚBLICA / EPIDEMIOLOGÍA ────────────────────────────────────────
    'SALUD PUBLICA':                               'SALUD PUBLICA',
    'EPIDEMIOLOGIA':                               'EPIDEMIOLOGIA',

    # ── FINANZAS (distinto a Administración) ──────────────────────────────────
    'FINANZAS':                                    'FINANZAS',
    'FINANZAS RELACIONES INTERNACIONALES':         'FINANZAS Y RELACIONES INTERNACIONALES',
    'GESTION FINANCIERA':                          'FINANZAS',
}

# ─────────────────────────────────────────────────────────────────────────────
# REGLA 5 — Umbrales dinámicos por área de conocimiento
# ─────────────────────────────────────────────────────────────────────────────
UMBRALES_AREA: dict[str, int] = {
    'ECONOMIA, ADMINISTRACION, CONTADURIA Y AFINES':  92,
    'INGENIERIA, ARQUITECTURA, URBANISMO Y AFINES':   87,
    'CIENCIAS DE LA SALUD':                           82,
    'CIENCIAS SOCIALES Y HUMANAS':                    88,
    'EDUCACION':                                      88,
    'MATEMATICAS Y CIENCIAS NATURALES':               93,
    'AGRONOMIA, VETERINARIA Y AFINES':                85,
    'BELLAS ARTES':                                   90,
}
UMBRAL_DEFAULT = 88

def get_umbral(area: str) -> int:
    if pd.isna(area):
        return UMBRAL_DEFAULT
    area_up = str(area).upper()
    for k, v in UMBRALES_AREA.items():
        if k in area_up:
            return v
    return UMBRAL_DEFAULT

# ─────────────────────────────────────────────────────────────────────────────
# REGLAS 6 y 7 — Agrupación con líder directo + output GRUPO_ANALITICO
# ─────────────────────────────────────────────────────────────────────────────
def tokens_similares(a: str, b: str, umbral: int) -> bool:
    """
    Combina token_set_ratio con un filtro de proporción de longitud.
    Evita que nombres cortos absorban nombres largos con vocabulario diferente.
    Ejemplo problemático sin este filtro:
      "ADMINISTRACION" ⊂ "GOBIERNO, POLITICA PUBLICA Y ADMINISTRACION PUBLICA" → 100%
    Con el filtro: tokens_a=1, tokens_b=5 → ratio 0.20 < 0.55 → NO se agrupan.
    """
    if fuzz.token_set_ratio(a, b) < umbral:
        return False
    # Proporción de tokens: el nombre más corto debe tener al menos 55% de tokens del largo
    ta = len(a.split())
    tb = len(b.split())
    if ta == 0 or tb == 0:
        return False
    prop = min(ta, tb) / max(ta, tb)
    return prop >= 0.55


def agrupar_particion(nombres_freq: list[tuple[str, int]], umbral: int) -> dict[str, str]:
    """
    nombres_freq: [(nombre_original, frecuencia), ...] ordenado desc por frecuencia.
    Retorna {nombre_original: GRUPO_ANALITICO}.

    Prioridad:
      1. Diccionario semántico → asigna GRUPO_ANALITICO directamente
      2. Fuzzy matching directo con líder (sin transitividad)
    """
    # Paso 1: asignación por diccionario semántico
    asignacion: dict[str, str] = {}
    sin_semantico: list[tuple[str, int]] = []

    for nombre, freq in nombres_freq:
        clave  = clave_cmp(nombre)
        grupo  = buscar_en_semantico(clave)
        if grupo is not None:
            asignacion[nombre] = grupo
        else:
            sin_semantico.append((nombre, freq))

    # Paso 2: fuzzy matching directo sobre los que no resolvió el diccionario
    # Los líderes son: (a) canónicos semánticos ya asignados + (b) nuevos líderes fuzzy
    lideres_fuzzy: list[str] = []

    for nombre, _ in sin_semantico:
        asignado = False
        clave_n  = clave_cmp(nombre)

        # ¿Coincide con algún GRUPO_ANALITICO semántico ya usado en esta partición?
        for grupo in set(asignacion.values()):
            if tokens_similares(clave_n, clave_cmp(grupo), umbral):
                asignacion[nombre] = grupo
                asignado = True
                break

        if not asignado:
            # ¿Coincide con algún líder fuzzy existente?
            for lider in lideres_fuzzy:
                if tokens_similares(clave_n, clave_cmp(lider), umbral):
                    asignacion[nombre] = lider
                    asignado = True
                    break

        if not asignado:
            lideres_fuzzy.append(nombre)
            asignacion[nombre] = nombre

    return asignacion

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
print("Leyendo CSV...")
df = pd.read_csv(CSV_ENTRADA, encoding='utf-8-sig', low_memory=False)
print(f"  {len(df):,} filas · {df[COL_PROG].nunique():,} nombres únicos de programa")

# Frecuencia de cada (programa, nivel, NBC, área) para elegir canónico y umbral
freq_df = (
    df.groupby([COL_PROG, COL_NIVEL, COL_NBC, COL_AREA], dropna=False)
      .size()
      .reset_index(name='frecuencia')
)
# Frecuencia total del nombre en todo el dataset (para ordenar dentro de la partición)
freq_total = df.groupby(COL_PROG, dropna=False).size().to_dict()

print(f"  Combinaciones (programa + nivel + NBC): {len(freq_df):,}\n")

# Iterar por partición: Nivel > NBC (Reglas 1 y 2)
niveles       = sorted(freq_df[COL_NIVEL].fillna('SIN NIVEL').unique())
total_part    = freq_df.groupby([COL_NIVEL, COL_NBC], dropna=False).ngroups
idx_part      = 0
fusiones_tot  = 0
# Clave compuesta (programa, nivel, nbc) → GRUPO_ANALITICO
# Evita colisiones cuando el mismo nombre aparece en niveles distintos
mapeo_final: dict[tuple, str] = {}
lines_txt: list[str] = []

for nivel in niveles:
    mask_n = freq_df[COL_NIVEL].fillna('SIN NIVEL') == nivel
    nbcs   = sorted(freq_df.loc[mask_n, COL_NBC].fillna('SIN NBC').unique())

    lines_txt += [f"\n{'='*72}", f"NIVEL: {nivel}", f"{'='*72}"]

    for nbc in nbcs:
        mask = mask_n & (freq_df[COL_NBC].fillna('SIN NBC') == nbc)
        sub  = freq_df[mask].copy()

        # Área más frecuente en esta partición → umbral
        area_mode = sub[COL_AREA].mode()
        area      = area_mode.iloc[0] if len(area_mode) else None
        umbral    = get_umbral(area)

        # Ordenar por frecuencia total desc (el más frecuente → líder candidato)
        sub['_freq_total'] = sub[COL_PROG].map(freq_total).fillna(0)
        sub = sub.sort_values('_freq_total', ascending=False)
        nombres_freq = list(zip(sub[COL_PROG], sub['_freq_total']))

        asignacion = agrupar_particion(nombres_freq, umbral)
        for nombre, grupo in asignacion.items():
            mapeo_final[(nombre, nivel, nbc)] = grupo

        # Grupos con variantes para el TXT
        grupos_nbc: dict[str, list[str]] = {}
        for nombre, canonico in asignacion.items():
            grupos_nbc.setdefault(canonico, []).append(nombre)

        variantes = {c: v for c, v in grupos_nbc.items() if len(v) > 1}
        fusiones  = sum(len(v) - 1 for v in variantes.values())
        fusiones_tot += fusiones

        lines_txt.append(
            f"\n  NBC: {nbc}  |  Área: {area or '—'}  |  Umbral: {umbral}%"
            f"  →  {len(nombres_freq)} programas, {len(variantes)} grupos con variantes"
        )
        lines_txt.append(f"  {'-'*66}")

        for grupo, miembros in sorted(variantes.items(), key=lambda x: -len(x[1])):
            otros = [m for m in miembros if m != grupo]
            lines_txt.append(f"  GRUPO_ANALITICO: {grupo}  [+{len(otros)} variante(s)]")
            for v in otros:
                sim = fuzz.token_set_ratio(clave_cmp(v), clave_cmp(grupo))
                fuente = 'SEMANTICO' if clave_cmp(v) in SEMANTICO else f'fuzzy {sim:.0f}%'
                lines_txt.append(f"    ↳ {v}  ({fuente})")

        idx_part += 1
        if idx_part % 40 == 0:
            pct = idx_part / total_part * 100
            print(f"  {pct:.0f}% ({idx_part}/{total_part} particiones)...")

# ─────────────────────────────────────────────────────────────────────────────
# Guardar CSV de mapeo
# ─────────────────────────────────────────────────────────────────────────────
print("\nGuardando mapeo CSV...")
filas = []
for (orig, nivel_k, nbc_k), grupo in sorted(mapeo_final.items()):
    sim    = fuzz.token_set_ratio(clave_cmp(orig), clave_cmp(grupo)) if orig != grupo else 100
    fuente = 'SEMANTICO' if buscar_en_semantico(clave_cmp(orig)) is not None else \
             ('IDENTICO' if orig == grupo else 'FUZZY')
    filas.append({
        'nombre_original':   orig,
        'nivel_formacion':   nivel_k,
        'nbc':               nbc_k,
        'GRUPO_ANALITICO':   grupo,
        'fuente':            fuente,
        'similitud_pct':     sim,
        'fusionado':         'SI' if orig != grupo else 'NO',
    })
pd.DataFrame(filas).to_csv(CSV_MAPEO, index=False, encoding='utf-8-sig')

# ─────────────────────────────────────────────────────────────────────────────
# Guardar TXT de grupos
# ─────────────────────────────────────────────────────────────────────────────
print("Guardando reporte TXT...")
canonicos = set(mapeo_final.values())
sem_count  = sum(1 for r in filas if r['fuente'] == 'SEMANTICO' and r['fusionado'] == 'SI')
fuzz_count = sum(1 for r in filas if r['fuente'] == 'FUZZY')
header = "\n".join([
    "NORMALIZACIÓN DE PROGRAMAS ACADÉMICOS — GRUPO_ANALITICO",
    "========================================================",
    f"Método  : Diccionario semántico (prioridad) + fuzzy token_set_ratio",
    f"Capas   : Nivel de Formación > NBC > nombre",
    f"Entrada : {len(mapeo_final):,} nombres únicos",
    f"Salida  : {len(canonicos):,} GRUPOS_ANALITICOS",
    f"Fusiones: {fusiones_tot:,} nombres absorbidos",
    "",
    "REGLA 8 — Pregunta de validación conceptual:",
    "  '¿Estos programas compiten por el mismo estudiante?'",
    "  Si la respuesta es NO para algún grupo, debe corregirse manualmente.",
    "",
    "Fuentes de agrupación:",
    "  SEMANTICO  → resuelto por diccionario de equivalencias",
    "  FUZZY XX%  → resuelto por similitud textual (revisar con cuidado)",
    "  IDENTICO   → sin variantes, grupo unipersonal",
    "",
    "Solo se muestran grupos con al menos 1 variante fusionada.",
])
with open(TXT_GRUPOS, 'w', encoding='utf-8') as f:
    f.write(header + '\n')
    f.write('\n'.join(lines_txt))

# ─────────────────────────────────────────────────────────────────────────────
# Resumen consola
# ─────────────────────────────────────────────────────────────────────────────
sem_count  = sum(1 for r in filas if r['fuente'] == 'SEMANTICO' and r['fusionado'] == 'SI')
fuzz_count = sum(1 for r in filas if r['fuente'] == 'FUZZY')

print(f"""
{'='*60}
  Nombres únicos entrada  : {len(mapeo_final):,}
  GRUPOS_ANALITICOS       : {len(canonicos):,}
  Fusiones por diccionario: {sem_count:,}
  Fusiones por fuzzy      : {fuzz_count:,}
  Total fusiones          : {fusiones_tot:,}
{'='*60}

Archivos generados:
  {CSV_MAPEO}
  {TXT_GRUPOS}

Revisa grupos_programas.txt aplicando la Regla 8:
  ¿Estos programas compiten por el mismo estudiante?
  Los marcados como FUZZY son los que más atención necesitan.

Cuando valides, avisa para aplicar GRUPO_ANALITICO en limpiar_datos.py.
""")
