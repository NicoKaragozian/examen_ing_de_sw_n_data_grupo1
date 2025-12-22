"""Tests unitarios para el módulo de transformaciones.

Estos tests validan las funciones de limpieza de datos utilizadas en la capa Bronze
del pipeline medallion.
"""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.transformations import (
    _coerce_amount,
    _normalize_status,
    clean_daily_transactions,
)

class TestCoerceAmount:
    """Tests unitarios para la función _coerce_amount."""

    @pytest.mark.parametrize(
        "valores_entrada,valores_esperados",
        [
            # Valores numéricos válidos
            ([100, 200, 300], [100.0, 200.0, 300.0]),
            # Valores flotantes
            ([10.5, 20.99, 0.01], [10.5, 20.99, 0.01]),
            # Strings numéricos deben ser convertidos
            (["100", "200.50", "300"], [100.0, 200.5, 300.0]),
            # Valores cero
            ([0, 0.0, "0"], [0.0, 0.0, 0.0]),
            # Valores negativos (se convierten, la validación es posterior)
            ([-10, -20.5], [-10.0, -20.5]),
        ],
        ids=[
            "enteros",
            "flotantes",
            "strings_numericos",
            "ceros",
            "valores_negativos",
        ],
    )
    def test_valores_numericos_validos(self, valores_entrada, valores_esperados):
        """Verifica que los valores numéricos válidos se convierten correctamente."""
        serie = pd.Series(valores_entrada)
        resultado = _coerce_amount(serie)

        assert list(resultado) == valores_esperados

    @pytest.mark.parametrize(
        "valores_entrada,indices_nan_esperados",
        [
            # Strings no numéricos se convierten en NaN
            (["abc", "100", "xyz"], [0, 2]),
            # Strings vacíos se convierten en NaN
            (["", "100", ""], [0, 2]),
            # Entradas inválidas mixtas
            (["N/A", "null", "100"], [0, 1]),
            # Caracteres especiales
            (["$100", "100€", "100"], [0, 1]),
        ],
        ids=[
            "strings_no_numericos",
            "strings_vacios",
            "strings_tipo_null",
            "simbolos_moneda",
        ],
    )
    def test_valores_invalidos_se_convierten_en_nan(self, valores_entrada, indices_nan_esperados):
        """Verifica que los valores inválidos se convierten en NaN."""
        serie = pd.Series(valores_entrada)
        resultado = _coerce_amount(serie)

        for idx in indices_nan_esperados:
            assert pd.isna(resultado.iloc[idx]), f"Se esperaba NaN en el índice {idx}"

    def test_valores_none_se_convierten_en_nan(self):
        """Verifica que los valores None se convierten en NaN."""
        serie = pd.Series([100, None, 200])
        resultado = _coerce_amount(serie)

        assert resultado.iloc[0] == 100.0
        assert pd.isna(resultado.iloc[1])
        assert resultado.iloc[2] == 200.0

    def test_serie_vacia(self):
        """Verifica que una serie vacía retorna una serie vacía."""
        serie = pd.Series([], dtype=float)
        resultado = _coerce_amount(serie)

        assert len(resultado) == 0


class TestNormalizeStatus:
    """Tests unitarios para la función _normalize_status."""

    @pytest.mark.parametrize(
        "valores_entrada,valores_esperados",
        [
            # Valores en minúscula pasan sin cambios
            (["completed", "pending", "failed"], ["completed", "pending", "failed"]),
            # Valores en mayúscula se normalizan
            (["COMPLETED", "PENDING", "FAILED"], ["completed", "pending", "failed"]),
            # Valores con mayúsculas mixtas se normalizan
            (["Completed", "Pending", "Failed"], ["completed", "pending", "failed"]),
            # Valores con espacios extra se recortan
            (["  completed  ", " pending", "failed "], ["completed", "pending", "failed"]),
        ],
        ids=[
            "minusculas",
            "mayusculas",
            "mayusculas_mixtas",
            "espacios_recortados",
        ],
    )
    def test_valores_status_validos(self, valores_entrada, valores_esperados):
        """Verifica que los valores de status válidos se normalizan correctamente."""
        serie = pd.Series(valores_entrada)
        resultado = _normalize_status(serie)

        assert list(resultado) == valores_esperados

    @pytest.mark.parametrize(
        "valores_entrada",
        [
            ["invalid", "completed", "unknown"],
            ["success", "error", "completed"],
            ["cancelled", "pending", "active"],
        ],
        ids=[
            "status_invalido",
            "terminologia_incorrecta",
            "status_no_soportado",
        ],
    )
    def test_valores_status_invalidos_se_convierten_en_none(self, valores_entrada):
        """Verifica que los valores de status inválidos se mapean a None."""
        serie = pd.Series(valores_entrada)
        resultado = _normalize_status(serie)

        # El primer valor debería ser None (inválido)
        assert pd.isna(resultado.iloc[0])

    def test_valores_null_se_convierten_en_none(self):
        """Verifica que los valores nulos se manejan correctamente."""
        serie = pd.Series([None, "completed", pd.NA])
        resultado = _normalize_status(serie)

        assert pd.isna(resultado.iloc[0])
        assert resultado.iloc[1] == "completed"
        assert pd.isna(resultado.iloc[2])

    def test_string_vacio_se_convierte_en_none(self):
        """Verifica que los strings vacíos se manejan correctamente."""
        serie = pd.Series(["", "completed", "   "])
        resultado = _normalize_status(serie)

        assert pd.isna(resultado.iloc[0])
        assert resultado.iloc[1] == "completed"
        assert pd.isna(resultado.iloc[2])


class TestCleanDailyTransactions:
    """Tests de integración para la función clean_daily_transactions."""

    @pytest.fixture
    def directorios_temporales(self):
        """Crea directorios temporales para datos crudos y limpios."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dir_raw = Path(tmpdir) / "raw"
            dir_clean = Path(tmpdir) / "clean"
            dir_raw.mkdir()
            yield dir_raw, dir_clean

    @pytest.fixture
    def contenido_csv_ejemplo(self):
        """Retorna contenido CSV de ejemplo para testing."""
        return (
            "transaction_id,customer_id,amount,status,transaction_ts\n"
            "1,1001,250.50,completed,2025-12-01 08:10:00\n"
            "2,1002,99.99,Completed,2025-12-01 09:45:00\n"
            "3,1003,,failed,2025-12-01 11:00:00\n"
            "4,1002,99.99,COMPLETED,2025-12-01 09:45:00\n"
            "5,1004,17.40,completed,2025-12-01 12:30:00\n"
            "6,1005,62.10,pending,2025-12-01 13:15:00\n"
        )

    def test_limpieza_exitosa(self, directorios_temporales, contenido_csv_ejemplo):
        """Verifica que un CSV válido se limpia y guarda como parquet."""
        dir_raw, dir_clean = directorios_temporales
        fecha_ejecucion = date(2025, 12, 1)

        # Escribir el CSV de ejemplo
        ruta_csv = dir_raw / "transactions_20251201.csv"
        ruta_csv.write_text(contenido_csv_ejemplo)

        # Ejecutar la función de limpieza
        ruta_salida = clean_daily_transactions(fecha_ejecucion, dir_raw, dir_clean)

        # Verificar que el archivo de salida existe
        assert ruta_salida.exists()
        assert ruta_salida.suffix == ".parquet"

        # Leer la salida y verificar
        df = pd.read_parquet(ruta_salida)

        # La fila con amount vacío (transaction_id=3) debería eliminarse
        assert len(df) == 5
        assert 3 not in df["transaction_id"].values

    def test_nombres_columnas_normalizados(self, directorios_temporales):
        """Verifica que los nombres de columnas se normalizan a minúsculas."""
        dir_raw, dir_clean = directorios_temporales
        fecha_ejecucion = date(2025, 12, 1)

        # CSV con nombres de columnas en mayúsculas
        contenido_csv = (
            "Transaction_ID,Customer_ID,Amount,STATUS,Transaction_TS\n"
            "1,1001,100.0,completed,2025-12-01 08:00:00\n"
        )
        ruta_csv = dir_raw / "transactions_20251201.csv"
        ruta_csv.write_text(contenido_csv)

        ruta_salida = clean_daily_transactions(fecha_ejecucion, dir_raw, dir_clean)
        df = pd.read_parquet(ruta_salida)

        # Todos los nombres de columnas deberían estar en minúsculas
        assert all(col == col.lower() for col in df.columns)
        assert "transaction_id" in df.columns
        assert "customer_id" in df.columns

    def test_duplicados_eliminados(self, directorios_temporales):
        """Verifica que las filas duplicadas se eliminan."""
        dir_raw, dir_clean = directorios_temporales
        fecha_ejecucion = date(2025, 12, 1)

        # CSV con filas duplicadas
        contenido_csv = (
            "transaction_id,customer_id,amount,status,transaction_ts\n"
            "1,1001,100.0,completed,2025-12-01 08:00:00\n"
            "1,1001,100.0,completed,2025-12-01 08:00:00\n"
            "2,1002,200.0,pending,2025-12-01 09:00:00\n"
        )
        ruta_csv = dir_raw / "transactions_20251201.csv"
        ruta_csv.write_text(contenido_csv)

        ruta_salida = clean_daily_transactions(fecha_ejecucion, dir_raw, dir_clean)
        df = pd.read_parquet(ruta_salida)

        # Debería haber 2 filas únicas
        assert len(df) == 2

    def test_status_normalizado(self, directorios_temporales):
        """Verifica que los valores de status se normalizan correctamente."""
        dir_raw, dir_clean = directorios_temporales
        fecha_ejecucion = date(2025, 12, 1)

        contenido_csv = (
            "transaction_id,customer_id,amount,status,transaction_ts\n"
            "1,1001,100.0,COMPLETED,2025-12-01 08:00:00\n"
            "2,1002,200.0,Pending,2025-12-01 09:00:00\n"
            "3,1003,300.0,  failed  ,2025-12-01 10:00:00\n"
        )
        ruta_csv = dir_raw / "transactions_20251201.csv"
        ruta_csv.write_text(contenido_csv)

        ruta_salida = clean_daily_transactions(fecha_ejecucion, dir_raw, dir_clean)
        df = pd.read_parquet(ruta_salida)

        # Todos los valores de status deberían estar en minúsculas
        assert set(df["status"].unique()) == {"completed", "pending", "failed"}

    def test_transaction_date_derivado(self, directorios_temporales):
        """Verifica que transaction_date se deriva de transaction_ts."""
        dir_raw, dir_clean = directorios_temporales
        fecha_ejecucion = date(2025, 12, 1)

        contenido_csv = (
            "transaction_id,customer_id,amount,status,transaction_ts\n"
            "1,1001,100.0,completed,2025-12-01 08:00:00\n"
            "2,1002,200.0,pending,2025-12-05 09:00:00\n"
        )
        ruta_csv = dir_raw / "transactions_20251201.csv"
        ruta_csv.write_text(contenido_csv)

        ruta_salida = clean_daily_transactions(fecha_ejecucion, dir_raw, dir_clean)
        df = pd.read_parquet(ruta_salida)

        # La columna transaction_date debería existir
        assert "transaction_date" in df.columns
        assert df.loc[df["transaction_id"] == 1, "transaction_date"].iloc[0] == date(2025, 12, 1)
        assert df.loc[df["transaction_id"] == 2, "transaction_date"].iloc[0] == date(2025, 12, 5)

    def test_archivo_no_encontrado_lanza_error(self, directorios_temporales):
        """Verifica que se lanza FileNotFoundError cuando el archivo raw no existe."""
        dir_raw, dir_clean = directorios_temporales
        fecha_ejecucion = date(2025, 12, 25)  # El archivo para esta fecha no existe

        with pytest.raises(FileNotFoundError) as exc_info:
            clean_daily_transactions(fecha_ejecucion, dir_raw, dir_clean)

        assert "Raw data not found" in str(exc_info.value)
        assert "20251225" in str(exc_info.value)

    def test_filas_con_amount_invalido_eliminadas(self, directorios_temporales):
        """Verifica que las filas con montos inválidos se eliminan."""
        dir_raw, dir_clean = directorios_temporales
        fecha_ejecucion = date(2025, 12, 1)

        contenido_csv = (
            "transaction_id,customer_id,amount,status,transaction_ts\n"
            "1,1001,100.0,completed,2025-12-01 08:00:00\n"
            "2,1002,invalid,pending,2025-12-01 09:00:00\n"
            "3,1003,N/A,failed,2025-12-01 10:00:00\n"
        )
        ruta_csv = dir_raw / "transactions_20251201.csv"
        ruta_csv.write_text(contenido_csv)

        ruta_salida = clean_daily_transactions(fecha_ejecucion, dir_raw, dir_clean)
        df = pd.read_parquet(ruta_salida)

        # Solo la primera fila debería permanecer (las otras tienen montos inválidos)
        assert len(df) == 1
        assert df["transaction_id"].iloc[0] == 1

    def test_filas_con_status_invalido_eliminadas(self, directorios_temporales):
        """Verifica que las filas con valores de status inválidos se eliminan."""
        dir_raw, dir_clean = directorios_temporales
        fecha_ejecucion = date(2025, 12, 1)

        contenido_csv = (
            "transaction_id,customer_id,amount,status,transaction_ts\n"
            "1,1001,100.0,completed,2025-12-01 08:00:00\n"
            "2,1002,200.0,status_invalido,2025-12-01 09:00:00\n"
            "3,1003,300.0,desconocido,2025-12-01 10:00:00\n"
        )
        ruta_csv = dir_raw / "transactions_20251201.csv"
        ruta_csv.write_text(contenido_csv)

        ruta_salida = clean_daily_transactions(fecha_ejecucion, dir_raw, dir_clean)
        df = pd.read_parquet(ruta_salida)

        # Solo la primera fila debería permanecer (las otras tienen status inválido)
        assert len(df) == 1
        assert df["transaction_id"].iloc[0] == 1

    def test_directorio_clean_se_crea_si_no_existe(self, directorios_temporales):
        """Verifica que el directorio clean se crea si no existe."""
        dir_raw, _ = directorios_temporales
        fecha_ejecucion = date(2025, 12, 1)

        # Crear una nueva ruta de directorio clean que no existe
        dir_clean = dir_raw.parent / "nuevo_dir_clean" / "anidado"

        contenido_csv = (
            "transaction_id,customer_id,amount,status,transaction_ts\n"
            "1,1001,100.0,completed,2025-12-01 08:00:00\n"
        )
        ruta_csv = dir_raw / "transactions_20251201.csv"
        ruta_csv.write_text(contenido_csv)

        ruta_salida = clean_daily_transactions(fecha_ejecucion, dir_raw, dir_clean)

        assert dir_clean.exists()
        assert ruta_salida.exists()

    def test_filas_con_timestamp_invalido_eliminadas(self, directorios_temporales):
        """Verifica que las filas con timestamps inválidos se eliminan."""
        dir_raw, dir_clean = directorios_temporales
        fecha_ejecucion = date(2025, 12, 1)

        contenido_csv = (
            "transaction_id,customer_id,amount,status,transaction_ts\n"
            "1,1001,100.0,completed,2025-12-01 08:00:00\n"
            "2,1002,200.0,pending,no_es_timestamp\n"
            "3,1003,300.0,failed,invalido\n"
        )
        ruta_csv = dir_raw / "transactions_20251201.csv"
        ruta_csv.write_text(contenido_csv)

        ruta_salida = clean_daily_transactions(fecha_ejecucion, dir_raw, dir_clean)
        df = pd.read_parquet(ruta_salida)

        # Solo la primera fila debería permanecer (las otras tienen timestamps inválidos)
        assert len(df) == 1
        assert df["transaction_id"].iloc[0] == 1
