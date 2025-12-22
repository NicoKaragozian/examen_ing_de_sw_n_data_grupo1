"""Configuración de pytest y fixtures para los tests del pipeline medallion."""

import sys
from pathlib import Path

# Agregar el directorio raíz del proyecto al path de Python
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
