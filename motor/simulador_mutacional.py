# B91: Importaciones para escaneo de dependencias estructurales
# ∂Bᵢ/∂Bⱼ
import re
from typing import Dict, List

# B92: Extracción de bloques y derivadas ∂Bᵢ/∂Bⱼ desde texto
# ∂Bᵢ/∂Bⱼ
def extraer_bloques_y_derivadas(contenido: str) -> Dict[str, List[str]]:
    """
    Extrae los bloques definidos y sus dependencias ∂Bᵢ/∂Bⱼ
    """
    bloques = {}
    patron = re.compile(r"#\s*BLOQUE\s+(B\d+(?:\.\d+)?|C\d+)\s+.*?\n#\s*∂(.*?)∂(.*?)\n")
    for match in patron.finditer(contenido):
        bloque = match.group(1).strip()
        derivada = f"∂{match.group(2).strip()}/∂{match.group(3).strip()}"
        if bloque not in bloques:
            bloques[bloque] = []
        bloques[bloque].append(derivada)
    return bloques

# B93: Simulación de remoción de un bloque estructural
# ∂Bᵢ/∂Bⱼ
def simular_remocion_bloque(bloque_objetivo: str, estructura: Dict[str, List[str]]) -> List[str]:
    """
    Dado un bloque, devuelve una lista de BLOQUES que dependen de él
    """
    afectados = []
    for bloque, deps in estructura.items():
        for d in deps:
            if f"∂{bloque_objetivo}/" in d or f"/{bloque_objetivo}" in d:
                afectados.append(bloque)
    return list(set(afectados))

# B94: Diagnóstico mutacional textual para trazabilidad
# ∂Bᵢ/∂Bⱼ
def diagnostico_mutacional(bloque, afectados):
    print("🧬 SIMULACIÓN DE MUTACIÓN:")
    print(f"→ Si se elimina o muta el BLOQUE {bloque}, los siguientes BLOQUES serán afectados:")
    for a in afectados:
        print(f"   - {a} (derivación estructural activa)")
    if not afectados:
        print("✅ Mutación segura: no hay derivaciones cruzadas críticas")
