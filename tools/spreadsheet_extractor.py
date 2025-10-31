import io
import json
import logging
import re
from collections.abc import Generator
from typing import Any, List, Dict
from dify_plugin.file.file import File
from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage
from dify_plugin.config.logger_format import plugin_logger_handler
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(plugin_logger_handler)


def handler_input(table_fields: str) -> dict:
    normalized = re.sub(r'[\s\u00A0]+', ' ', table_fields)
    normalized = normalized.strip()
    logger.info(f"Processing table_fields JSON: {normalized}")
    if not table_fields or not table_fields.strip():
        raise ValueError("Empty table_fields input")

    try:
        field_mapping = json.loads(normalized)
        if not isinstance(field_mapping, dict):
            raise ValueError("Input must be a JSON object")

        result = {}
        for field, alias in field_mapping.items():
            if not isinstance(field, str) or not field.strip():
                raise ValueError(f"Invalid field: {field}")
            if not isinstance(alias, str) or not alias.strip():
                raise ValueError(f"Invalid alias for '{field}': {alias}")
            result[field.strip()] = alias.strip()
        logger.info(f"Parsed {len(result)} field mappings: {result}")
        return result

    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format: {str(e)}")
    except Exception as e:
        raise ValueError(f"Failed to parse table_fields: {str(e)}")


def read_csv_with_encoding(content: bytes, encodings=('utf-8', 'gbk')) -> pd.DataFrame:
    for encoding in encodings:
        try:
            text = content.decode(encoding)
            return pd.read_csv(io.StringIO(text), dtype=str)
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    raise RuntimeError(f"Failed to decode CSV with encodings: {encodings}")


def clean_column_name(name: str) -> str:
    if not isinstance(name, str):
        return str(name).strip()
    cleaned = re.sub(r'[\s\u00A0\u3000\r\n\t]+', ' ', name)
    return cleaned.strip()


def read_table_file_to_objects(file: File, field_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
    if not isinstance(file, File):
        raise TypeError("Invalid file format, expected File object")

    ext = file.extension.lower()
    content = file.blob

    try:
        if ext == '.csv':
            df = read_csv_with_encoding(content)
        elif ext in ('.xlsx', '.xls'):
            df = pd.read_spreadsheet(io.BytesIO(content), dtype=str)
        else:
            raise ValueError(f"Unsupported file format: {ext}. Only .csv, .xlsx, and .xls are supported.")
    except Exception as e:
        raise RuntimeError(f"Failed to parse file: {e}") from e

    # Clean the columns
    original_columns = df.columns.tolist()
    cleaned_columns = [clean_column_name(col) for col in original_columns]
    df.columns = cleaned_columns

    # Verify if the field exists
    missing_cols = set(field_mapping.keys()) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns in file: {missing_cols}. Available: {list(df.columns)}")

    df = df[list(field_mapping.keys())].rename(columns=field_mapping)
    # Convert NaN values to None
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient='records')


class SpreadsheetExtractorTool(Tool):
    def _invoke(self, tool_parameters: Dict[str, Any]) -> Generator[ToolInvokeMessage]:
        try:
            field_mapping = handler_input(tool_parameters["table_fields"])
            result = read_table_file_to_objects(tool_parameters["file"], field_mapping)
            output = {"result": result}
            yield self.create_json_message(output)
            yield self.create_text_message(json.dumps(output, ensure_ascii=False))
        except Exception as e:
            yield self.create_text_message(f"Error: {str(e)}")
