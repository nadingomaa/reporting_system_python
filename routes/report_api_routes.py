"""
API routes for the reporting system
"""
import asyncio
import json
from datetime import datetime
import os
import httpx
from fastapi import APIRouter, Query, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import Response, FileResponse, StreamingResponse
from typing import Optional

from services import APIService, PDFService, ExcelService, ControlService, IncidentService
from services.bank_check_service import BankCheckService
from services.enhanced_bank_check_service import EnhancedBankCheckService
# Dashboard activity service moved to Node.js backend (NestJS)
DashboardActivityService = None  # type: ignore
from utils.export_utils import get_default_header_config
from models import ExportRequest, ExportResponse
from routes.route_utils import (
    write_debug, 
    parse_header_config, 
    merge_header_config, 
    convert_to_boolean, 
    save_and_log_export,
    build_dynamic_sql_query,
    generate_excel_report,
    generate_word_report,
    generate_pdf_report
)

# Initialize services
api_service = APIService()
pdf_service = PDFService()
excel_service = ExcelService()
control_service = ControlService()
incident_service = IncidentService() if IncidentService else None
dashboard_activity_service = DashboardActivityService() if DashboardActivityService else None
bank_check_service = BankCheckService()
enhanced_bank_check_service = EnhancedBankCheckService()

# db_service points to control_service for backward compatibility
db_service = control_service

# Create router
router = APIRouter()





# -------------------------------
# Export logging (Excel/PDF) APIs
# -------------------------------
@router.post("/api/exports/log")
async def log_report_export(request: Request):
    """Log an export (excel/pdf/word/zip) with title and src for later download listing."""
    try:
        import pyodbc
        from config import get_database_connection_string

        body = await request.json()
        title = (body.get("title") or "").strip() or "Untitled Report"
        src = (body.get("src") or "").strip()
        fmt = (body.get("format") or "").strip().lower() or "unknown"
        dashboard = (body.get("dashboard") or "").strip() or "general"

        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            # Ensure table exists and has created_by column
            cursor.execute(
                """
                IF NOT EXISTS (
                  SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_NAME = 'report_exports' AND TABLE_SCHEMA='dbo'
                )
                BEGIN
                  CREATE TABLE dbo.report_exports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    title NVARCHAR(255) NOT NULL,
                    src NVARCHAR(1024) NULL,
                    format NVARCHAR(20) NOT NULL,
                    dashboard NVARCHAR(100) NULL,
                    created_by NVARCHAR(255) NULL,
                    created_at DATETIME2 DEFAULT GETDATE()
                  )
                END
                """
            )
            conn.commit()

            # Add created_by column if it doesn't exist (for existing tables)
            try:
                cursor.execute(
                    """
                    IF NOT EXISTS (
                      SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'report_exports' AND COLUMN_NAME = 'created_by'
                    )
                    BEGIN
                      ALTER TABLE dbo.report_exports ADD created_by NVARCHAR(255) NULL
                    END
                    """
                )
                conn.commit()
            except Exception:
                pass  # Column might already exist

            created_by = (body.get("created_by") or "").strip() or "System"
            # Determine type based on dashboard
            export_type = "transaction"  # Default
            if dashboard and dashboard.lower() in ['incidents', 'kris', 'risks', 'controls']:
                export_type = "dashboard"
            
            cursor.execute(
                """
                INSERT INTO dbo.report_exports (title, src, format, dashboard, type, created_by)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (title, src, fmt, dashboard, export_type, created_by)
            )
            conn.commit()

            new_id = cursor.execute("SELECT @@IDENTITY").fetchone()[0]
            return {"success": True, "id": int(new_id)}
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        return {"success": False, "error": str(e)}

@router.get("/api/exports/recent")
async def list_recent_exports(request: Request, limit: int = Query(50), page: int = Query(1), search: str = Query("")):
    """Return recent report exports (newest first) with simple pagination and dashboard filtering."""
    try:
        import pyodbc
        from config import get_database_connection_string

        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF NOT EXISTS (
                  SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_NAME = 'report_exports' AND TABLE_SCHEMA='dbo'
                )
                BEGIN
                  CREATE TABLE dbo.report_exports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    title NVARCHAR(255) NOT NULL,
                    src NVARCHAR(1024) NULL,
                    format NVARCHAR(20) NOT NULL,
                    dashboard NVARCHAR(100) NULL,
                    type NVARCHAR(50) NULL,
                    created_by NVARCHAR(255) NULL,
                    created_at DATETIME2 DEFAULT GETDATE()
                  )
                END
                """
            )
            conn.commit()
            
            # Add type column if it doesn't exist
            try:
                cursor.execute(
                    """
                    IF NOT EXISTS (
                      SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'report_exports' AND COLUMN_NAME = 'type'
                    )
                    BEGIN
                      ALTER TABLE dbo.report_exports ADD type NVARCHAR(50) NULL
                    END
                    """
                )
                conn.commit()
            except Exception:
                pass
            
            # Add created_by column if it doesn't exist
            try:
                cursor.execute(
                    """
                    IF NOT EXISTS (
                      SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'report_exports' AND COLUMN_NAME = 'created_by'
                    )
                    BEGIN
                      ALTER TABLE dbo.report_exports ADD created_by NVARCHAR(255) NULL
                    END
                    """
                )
                conn.commit()
            except Exception:
                pass

            # Build search condition
            search_condition = ""
            search_params = []
            conditions = []
            
            # Handle title search
            if search and search.strip():
                conditions.append("title LIKE ?")
                search_params.append(f"%{search.strip()}%")
            
            # Handle type filter (prefer type over dashboard filter)
            type_filter = request.query_params.get('type', None)
            if type_filter:
                if type_filter.lower() == 'dashboard':
                    conditions.append("type = 'dashboard'")
                elif type_filter.lower() == 'transaction':
                    conditions.append("(type = 'transaction' OR type IS NULL)")
            
            # Fallback to dashboard filter if type is not provided (for backward compatibility)
            elif request.query_params.get('dashboard', None):
                dashboard_filter = request.query_params.get('dashboard')
                dashboard_list = [d.strip() for d in dashboard_filter.split(',')]
                if len(dashboard_list) == 1 and dashboard_list[0] == 'transaction':
                    # Transaction reports: exclude dashboard reports or explicitly transaction
                    conditions.append("(type = 'transaction' OR type IS NULL OR type != 'dashboard')")
                elif len(dashboard_list) > 0:
                    # Dashboard reports: filter by specific dashboard types
                    conditions.append("type = 'dashboard'")
            
            # Combine all conditions
            if conditions:
                search_condition = "WHERE " + " AND ".join(conditions)

            # Total count with search and filters
            count_query = f"SELECT COUNT(*) FROM dbo.report_exports {search_condition}"
            cursor.execute(count_query, search_params)
            total_count = int(cursor.fetchone()[0])

            # Pagination via OFFSET/FETCH
            safe_limit = max(1, min(200, int(limit)))
            safe_page = max(1, int(page))
            offset = (safe_page - 1) * safe_limit
            
            select_query = f"""
                SELECT id, title, src, format, dashboard, type, created_by, created_at
                FROM dbo.report_exports
                {search_condition}
                ORDER BY created_at DESC, id DESC
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """
            cursor.execute(select_query, search_params + [offset, safe_limit])
            rows = cursor.fetchall()
            exports = [
                {
                    "id": int(r[0]),
                    "title": r[1],
                    "src": r[2],
                    "format": r[3],
                    "dashboard": r[4],
                    "type": r[5] if len(r) > 5 else None,
                    "created_by": r[6] if len(r) > 6 else "System",
                    "created_at": r[7].isoformat() if len(r) > 7 and hasattr(r[7], 'isoformat') else (str(r[7]) if len(r) > 7 else "")
                }
                for r in rows
            ]
            return {
                "success": True,
                "exports": exports,
                "pagination": {
                    "page": safe_page,
                    "limit": safe_limit,
                    "total": total_count,
                    "totalPages": (total_count + safe_limit - 1) // safe_limit,
                    "hasNext": offset + safe_limit < total_count,
                    "hasPrev": safe_page > 1
                }
            }
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        return {"success": False, "error": str(e), "exports": [], "pagination": {}}

@router.delete("/api/exports/{export_id}")
async def delete_export(export_id: int):
    """Delete an export row and its file if present"""
    try:
        import pyodbc
        from config import get_database_connection_string
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT src FROM dbo.report_exports WHERE id = ?", export_id)
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Export not found")
            src = row[0]

            # Delete DB row
            cursor.execute("DELETE FROM dbo.report_exports WHERE id = ?", export_id)
            conn.commit()

            # Delete file if exists
            if src:
                try:
                    import os
                    file_path = src if os.path.isabs(src) else os.path.join(os.getcwd(), src)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except Exception as fe:
                    return {"success": True, "deleted": True, "fileDeleted": False, "warning": str(fe)}

            return {"success": True, "deleted": True, "fileDeleted": True}
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e)}

@router.post("/api/reports/dynamic")
async def generate_dynamic_report(request: Request):
    """Generate dynamic report based on table selection, joins, columns, and conditions"""
    try:
        body = await request.json()
        tables = body.get('tables', [])
        joins = body.get('joins', [])
        columns = body.get('columns', [])
        where_conditions = body.get('whereConditions', [])
        time_filter = body.get('timeFilter')
        format_type_raw = body.get('format', 'excel')
        
        # Filter out '#' column as it's not a real database column (it's added as an index later)
        valid_columns = [col for col in columns if col != '#' and col.strip() != '']
        
        # Normalize format type to lowercase and validate
        if isinstance(format_type_raw, str):
            format_type = format_type_raw.lower().strip()
        else:
            format_type = 'excel'
        
        # Map common format variations
        format_mapping = {
            'xlsx': 'excel',
            'xls': 'excel',
            'doc': 'word',
            'docx': 'word',
        }
        format_type = format_mapping.get(format_type, format_type)
        
        # Validate format
        valid_formats = ['excel', 'pdf', 'word']
        if format_type not in valid_formats:
            write_debug(f"[Dynamic Report] Invalid format received: '{format_type_raw}' (normalized: '{format_type}'), defaulting to 'excel'")
            format_type = 'excel'
        
        write_debug(f"[Dynamic Report] Request received: tables={tables}, columns={columns}, valid_columns={valid_columns}, format={format_type} (original: {format_type_raw})")
        
        if not tables or not valid_columns:
            raise HTTPException(status_code=400, detail="Tables and valid columns are required")
        
        # Build SQL query using valid columns (without '#')
        try:
            sql_query = build_dynamic_sql_query(tables, joins, valid_columns, where_conditions, time_filter)
            write_debug(f"[Dynamic Report] SQL query built: {sql_query[:200]}...")
        except Exception as sql_err:
            write_debug(f"[Dynamic Report] SQL query build failed: {str(sql_err)}")
            raise HTTPException(status_code=400, detail=f"Failed to build SQL query: {str(sql_err)}")
        
        # Execute query and get data
        import pyodbc
        from config import get_database_connection_string
        
        try:
            connection_string = get_database_connection_string()
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()
        except Exception as db_err:
            write_debug(f"[Dynamic Report] Database connection failed: {str(db_err)}")
            raise HTTPException(status_code=500, detail=f"Database connection failed: {str(db_err)}")
        
        try:
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            write_debug(f"[Dynamic Report] Query executed, fetched {len(rows)} rows")
            
            # Get column names from cursor description
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Convert to list of dictionaries (for chart data extraction) and list of lists (for table)
            data_rows_dict = []
            data_rows = []
            for row in rows:
                row_dict = {}
                row_list = []
                for idx, cell in enumerate(row):
                    col_name = column_names[idx] if idx < len(column_names) else f"col_{idx}"
                    row_dict[col_name] = cell
                    row_list.append(str(cell) if cell is not None else '')
                data_rows_dict.append(row_dict)
                data_rows.append(row_list)
            
            # Add index column at the beginning for all dynamic reports
            # Add "#" or "Index" as the first column header
            # Note: Use valid_columns (without '#') for the actual data columns
            index_column_name = "#"
            columns_with_index = [index_column_name] + valid_columns
            
            # Add index number (1, 2, 3, ...) to the beginning of each data row
            data_rows_with_index = []
            for idx, row in enumerate(data_rows, start=1):
                data_rows_with_index.append([str(idx)] + row)
            
            # Also add index column to data_rows_dict for chart processing
            for idx, row_dict in enumerate(data_rows_dict, start=1):
                row_dict[index_column_name] = str(idx)
            
            # Use the modified columns and data
            columns = columns_with_index
            data_rows = data_rows_with_index
            
            write_debug(f"[Dynamic Report] Added index column, total columns: {len(columns)}, total rows: {len(data_rows)}")
            write_debug(f"[Dynamic Report] data_rows_dict length: {len(data_rows_dict)}")
            if data_rows_dict:
                write_debug(f"[Dynamic Report] data_rows_dict[0] keys: {list(data_rows_dict[0].keys())}")
            
            # Get header configuration from request body
            header_config = body.get('headerConfig', {})
            write_debug(f"[Dynamic Report] Raw header_config keys: {list(header_config.keys()) if header_config else 'None'}")
            write_debug(f"[Dynamic Report] chartConfig in header_config: {header_config.get('chartConfig') if header_config else 'N/A'}")
            
            if header_config:
                from utils.export_utils import get_default_header_config
                default_config = get_default_header_config("dynamic")
                merged_config = {**default_config, **header_config}
            else:
                from utils.export_utils import get_default_header_config
                merged_config = get_default_header_config("dynamic")
            
            write_debug(f"[Dynamic Report] Merged config keys: {list(merged_config.keys())}")
            
            # Transform chartConfig to chart_data format if chartConfig exists
            # Check both merged_config and original header_config (in case it was filtered out)
            chart_config_raw = merged_config.get('chartConfig') or header_config.get('chartConfig')
            write_debug(f"[Dynamic Report] chart_config_raw from merged_config: {merged_config.get('chartConfig')}")
            write_debug(f"[Dynamic Report] chart_config_raw from header_config: {header_config.get('chartConfig')}")
            write_debug(f"[Dynamic Report] chart_config_raw final: {chart_config_raw}")
            write_debug(f"[Dynamic Report] chart_config_raw type: {type(chart_config_raw)}")
            
            # Filter out None/null values
            if chart_config_raw is None or (isinstance(chart_config_raw, dict) and not chart_config_raw.get('xKey') and not chart_config_raw.get('yKey')):
                chart_config = None
                write_debug(f"[Dynamic Report] chart_config filtered out - was None or missing both xKey and yKey")
            else:
                chart_config = chart_config_raw
            write_debug(f"[Dynamic Report] chart_config extracted (after filtering): {chart_config}")
            
            if chart_config:
                write_debug(f"[Dynamic Report] chart_config type: {type(chart_config)}")
                write_debug(f"[Dynamic Report] chart_config details: type={chart_config.get('type') if isinstance(chart_config, dict) else 'N/A'}, xKey={chart_config.get('xKey') if isinstance(chart_config, dict) else 'N/A'}, yKey={chart_config.get('yKey') if isinstance(chart_config, dict) else 'N/A'}")
            
            # Check conditions more explicitly
            has_chart_config = chart_config is not None and isinstance(chart_config, dict)
            x_key_val = chart_config.get('xKey') if has_chart_config else None
            y_key_val = chart_config.get('yKey') if has_chart_config else None
            
            # More robust validation - check if values exist and are non-empty strings
            has_x_key = False
            has_y_key = False
            if has_chart_config:
                if x_key_val:
                    x_str = str(x_key_val).strip()
                    has_x_key = x_str != '' and x_str.lower() != 'none'
                if y_key_val:
                    y_str = str(y_key_val).strip()
                    has_y_key = y_str != '' and y_str.lower() != 'none'
            
            has_data = len(data_rows_dict) > 0
            
            write_debug(f"[Dynamic Report] Chart conditions - has_chart_config: {has_chart_config}, x_key_val: {x_key_val}, y_key_val: {y_key_val}, has_x_key: {has_x_key}, has_y_key: {has_y_key}, has_data: {has_data}")
            
            if has_chart_config and has_x_key and has_y_key and has_data:
                try:
                    x_key = str(x_key_val).strip()
                    y_key = str(y_key_val).strip()
                    chart_type = chart_config.get('type', 'bar')
                    
                    write_debug(f"[Dynamic Report] Building chart data from xKey={x_key}, yKey={y_key}, type={chart_type}")
                    
                    # Get available column names from first row
                    available_cols = list(data_rows_dict[0].keys()) if data_rows_dict else []
                    write_debug(f"[Dynamic Report] Available columns: {available_cols}")
                    
                    # Try to find matching column names (case-insensitive, handle table prefixes)
                    x_col_match = None
                    y_col_match = None
                    
                    # Normalize x_key and y_key (remove table prefix if present, lowercase)
                    x_key_normalized = x_key.split('.')[-1].lower() if '.' in x_key else x_key.lower()
                    y_key_normalized = y_key.split('.')[-1].lower() if '.' in y_key else y_key.lower()
                    
                    # Special handling for '#' column (index column)
                    if x_key == '#' or x_key_normalized == '#':
                        x_col_match = '#'
                    if y_key == '#' or y_key_normalized == '#':
                        y_col_match = '#'
                    
                    # Try exact match first, then normalized match
                    for col in available_cols:
                        col_normalized = col.split('.')[-1].lower() if '.' in col else col.lower()
                        # Exact match (case-sensitive)
                        if not x_col_match and col == x_key:
                            x_col_match = col
                        # Normalized match (case-insensitive, without table prefix)
                        elif not x_col_match and col_normalized == x_key_normalized:
                            x_col_match = col
                        # Also try matching the full normalized string
                        elif not x_col_match and col.lower() == x_key.lower():
                            x_col_match = col
                            
                        # Same for y_key
                        if not y_col_match and col == y_key:
                            y_col_match = col
                        elif not y_col_match and col_normalized == y_key_normalized:
                            y_col_match = col
                        elif not y_col_match and col.lower() == y_key.lower():
                            y_col_match = col
                    
                    write_debug(f"[Dynamic Report] Column matches - xKey: {x_key} -> {x_col_match}, yKey: {y_key} -> {y_col_match}")
                    
                    if not x_col_match or not y_col_match:
                        write_debug(f"[Dynamic Report] Could not find matching columns for chart. xKey={x_key}, yKey={y_key}, available_cols={available_cols}")
                    else:
                        # Extract labels and values from data_rows_dict
                        # Special handling: if yKey is '#', count occurrences instead of summing
                        if y_col_match == '#':
                            # Count occurrences of each xKey value
                            from collections import Counter
                            x_values = []
                            for row_dict in data_rows_dict:
                                x_val = row_dict.get(x_col_match)
                                if x_val is not None:
                                    x_values.append(str(x_val))
                            
                            # Count occurrences
                            counter = Counter(x_values)
                            labels = list(counter.keys())
                            values = list(counter.values())
                            write_debug(f"[Dynamic Report] Counting occurrences (yKey='#') - {len(labels)} unique labels")
                        else:
                            # Normal extraction: use yKey values
                            labels = []
                            values = []
                            
                            # Check if yKey values are numeric
                            sample_y_vals = []
                            for row_dict in data_rows_dict[:10]:  # Check first 10 rows
                                y_val = row_dict.get(y_col_match)
                                if y_val is not None:
                                    sample_y_vals.append(y_val)
                            
                            # Determine if yKey is numeric
                            is_y_numeric = False
                            if sample_y_vals:
                                try:
                                    float(sample_y_vals[0])
                                    is_y_numeric = True
                                except (ValueError, TypeError):
                                    is_y_numeric = False
                            
                            write_debug(f"[Dynamic Report] yKey numeric check - is_numeric: {is_y_numeric}, sample values: {sample_y_vals[:3]}")
                            
                            # If yKey is not numeric and chart type is pie, group by yKey and count
                            # This makes more sense for pie charts - show distribution of yKey values
                            if not is_y_numeric and chart_type == 'pie':
                                from collections import Counter
                                y_values = []
                                for row_dict in data_rows_dict:
                                    y_val = row_dict.get(y_col_match)
                                    if y_val is not None:
                                        y_values.append(str(y_val))
                                
                                # Count occurrences of each yKey value (e.g., status distribution)
                                counter = Counter(y_values)
                                labels = list(counter.keys())
                                values = list(counter.values())
                                write_debug(f"[Dynamic Report] Counting yKey occurrences (yKey is categorical) - {len(labels)} unique categories: {labels[:10]}")
                            else:
                                # Normal extraction: use yKey values (must be numeric)
                                for row_dict in data_rows_dict:
                                    x_val = row_dict.get(x_col_match)
                                    y_val = row_dict.get(y_col_match)
                                    
                                    if x_val is not None and y_val is not None:
                                        # Convert y_val to number if possible
                                        try:
                                            y_num = float(y_val) if not isinstance(y_val, (int, float)) else y_val
                                            labels.append(str(x_val))
                                            values.append(y_num)
                                        except (ValueError, TypeError):
                                            write_debug(f"[Dynamic Report] Skipping row - yKey value '{y_val}' is not numeric")
                                            continue
                        
                        if len(labels) > 0 and len(values) > 0:
                            # Create chart_data in the format expected by generate_excel_report
                            merged_config['chart_data'] = {
                                'labels': labels,
                                'values': values,
                                'type': chart_type
                            }
                            merged_config['chart_type'] = chart_type
                            write_debug(f"[Dynamic Report] Chart data created: {len(labels)} labels, {len(values)} values")
                        else:
                            write_debug(f"[Dynamic Report] No valid chart data extracted - labels={len(labels)}, values={len(values)}")
                except Exception as e:
                    write_debug(f"[Dynamic Report] Error processing chartConfig: {str(e)}")
                    import traceback
                    write_debug(f"[Dynamic Report] Traceback: {traceback.format_exc()}")
            else:
                write_debug(f"[Dynamic Report] No chartConfig found or missing xKey/yKey")
                write_debug(f"[Dynamic Report] Debug - chart_config={chart_config}, xKey={chart_config.get('xKey') if chart_config and isinstance(chart_config, dict) else None}, yKey={chart_config.get('yKey') if chart_config and isinstance(chart_config, dict) else None}, data_rows_dict_len={len(data_rows_dict)}")
            
            # Get export type from request (transaction or dashboard)
            # Default to 'transaction' for dynamic reports if not specified
            export_type = body.get('type', 'transaction')
            if export_type is None:
                export_type = 'transaction'
            
            write_debug(f"[Dynamic Report] Export type: {export_type}")
            
            # Debug: Check what's in merged_config before generating report
            write_debug(f"[Dynamic Report] Before report generation - merged_config has chart_data: {'chart_data' in merged_config}")
            write_debug(f"[Dynamic Report] Before report generation - chart_data value: {merged_config.get('chart_data')}")
            write_debug(f"[Dynamic Report] Before report generation - chartConfig value: {merged_config.get('chartConfig')}")
            
            # Final check: if chartConfig exists but chart_data doesn't, log warning
            if merged_config.get('chartConfig') and not merged_config.get('chart_data'):
                write_debug(f"[Dynamic Report] WARNING: chartConfig exists but chart_data was not created!")
                write_debug(f"[Dynamic Report] chartConfig details: {merged_config.get('chartConfig')}")
                write_debug(f"[Dynamic Report] This usually means xKey/yKey didn't match available columns or data extraction failed.")
            
            # Generate report based on format
            report_content = None
            file_extension = format_type
            write_debug(f"[Dynamic Report] About to generate report - format_type: '{format_type}', type: {type(format_type)}")
            try:
                if format_type == 'excel':
                    write_debug(f"[Dynamic Report] Generating Excel report...")
                    write_debug(f"[Dynamic Report] RIGHT BEFORE Excel generation - merged_config keys: {list(merged_config.keys())}")
                    write_debug(f"[Dynamic Report] RIGHT BEFORE Excel generation - chart_data in merged_config: {'chart_data' in merged_config}")
                    write_debug(f"[Dynamic Report] RIGHT BEFORE Excel generation - chart_data value: {merged_config.get('chart_data')}")
                    write_debug(f"[Dynamic Report] RIGHT BEFORE Excel generation - chart_data type: {type(merged_config.get('chart_data'))}")
                    if merged_config.get('chart_data'):
                        cd = merged_config.get('chart_data')
                        write_debug(f"[Dynamic Report] RIGHT BEFORE Excel generation - chart_data has labels: {bool(cd.get('labels'))}, has values: {bool(cd.get('values'))}, labels count: {len(cd.get('labels', []))}, values count: {len(cd.get('values', []))}")
                    report_content = generate_excel_report(columns, data_rows, merged_config)
                    file_extension = 'xlsx'
                elif format_type == 'word':
                    write_debug(f"[Dynamic Report] Generating Word report...")
                    report_content = generate_word_report(columns, data_rows, merged_config)
                    file_extension = 'docx'
                elif format_type == 'pdf':
                    write_debug(f"[Dynamic Report] Generating PDF report...")
                    report_content = generate_pdf_report(columns, data_rows, merged_config)
                    file_extension = 'pdf'
                else:
                    write_debug(f"[Dynamic Report] ERROR: Unsupported format '{format_type}' (type: {type(format_type)}). Valid formats: excel, pdf, word")
                    raise HTTPException(status_code=400, detail=f"Unsupported format: '{format_type}'. Supported formats: excel, pdf, word")
                write_debug(f"[Dynamic Report] Report generated successfully, size: {len(report_content)} bytes")
            except Exception as gen_err:
                write_debug(f"[Dynamic Report] Report generation failed: {str(gen_err)}")
                import traceback
                write_debug(f"[Dynamic Report] Traceback: {traceback.format_exc()}")
                raise HTTPException(status_code=500, detail=f"Failed to generate report: {str(gen_err)}")
            
            # Save file and log to database
            try:
                # Get user info from request.state (set by auth middleware)
                user = getattr(request.state, 'user', None)
                user_name = user.get('name') if user else None
                user_id = user.get('id') if user else None
                
                # Fallback to headers if not in state
                created_by = user_name or request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
                write_debug(f"[Dynamic Report] Saving export, type: {export_type}, created_by: {created_by}")
                
                # Create report name from table names
                # Join table names with underscore for filename (e.g., "Users_Orders_Products")
                # This will be formatted by save_and_log_export to create readable names
                table_names = '_'.join(tables) if tables else 'Dynamic_Report'
                write_debug(f"[Dynamic Report] Using table names for report: {table_names}")
                
                export_info = await save_and_log_export(
                    content=report_content,
                    file_extension=file_extension,
                    dashboard='transactions',  # Use 'transactions' instead of 'dynamic' for filename
                    card_type=table_names,  # Use table names as card_type for naming
                    header_config=merged_config,
                    created_by=created_by,
                    export_type=export_type
                )
                write_debug(f"[Dynamic Report] Export saved to database:")
                write_debug(f"  - Export ID: {export_info.get('export_id')}")
                write_debug(f"  - File path: {export_info.get('relative_path')}")
                write_debug(f"  - Filename: {export_info.get('filename')}")
                write_debug(f"  - Type: {export_type}")
                write_debug(f"  - Created by: {created_by}")
            except Exception as save_err:
                write_debug(f"[Dynamic Report] Save failed: {str(save_err)}")
                import traceback
                write_debug(f"[Dynamic Report] Save traceback: {traceback.format_exc()}")
                # Continue even if save fails - still return the file
                export_info = {
                    'filename': f'dynamic_report.{file_extension}',
                    'relative_path': '',
                    'export_id': None
                }
            
            # Determine media type
            media_types = {
                'pdf': 'application/pdf',
                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            }
            media_type = media_types.get(file_extension, 'application/octet-stream')
            
            # Return file with headers
            return Response(
                content=report_content,
                media_type=media_type,
                headers={
                    'Content-Disposition': f'attachment; filename="{export_info["filename"]}"',
                    'X-Export-Src': export_info['relative_path'],
                    'X-Export-Id': str(export_info.get('export_id', ''))
                }
            )
                
        finally:
            cursor.close()
            conn.close()
            
    except HTTPException:
        raise
    except Exception as e:
        write_debug(f"[Dynamic Report] Unexpected error: {str(e)}")
        import traceback
        write_debug(f"[Dynamic Report] Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to generate dynamic report: {str(e)}")


@router.post("/api/reports/dynamic/preview")
async def preview_dynamic_report(request: Request):
    """
    Preview dynamic report data for transactions before export.
    Returns JSON with columns and limited rows instead of a file.
    """
    try:
        body = await request.json()
        tables = body.get('tables', [])
        joins = body.get('joins', [])
        columns = body.get('columns', [])
        where_conditions = body.get('whereConditions', [])
        time_filter = body.get('timeFilter')
        preview_limit = int(body.get('previewLimit', 200))

        write_debug(f"[Dynamic Report Preview] Request received: tables={tables}, columns={columns}")

        if not tables or not columns:
            raise HTTPException(status_code=400, detail="Tables and columns are required")

        # Filter out '#' column as it's not a real database column (it's added as an index later)
        valid_columns = [col for col in columns if col != '#' and col.strip() != '']
        
        if not valid_columns:
            raise HTTPException(status_code=400, detail="No valid columns provided (excluding '#')")

        # Build SQL query
        try:
            sql_query = build_dynamic_sql_query(tables, joins, valid_columns, where_conditions, time_filter)
            write_debug(f"[Dynamic Report Preview] SQL query built: {sql_query[:200]}...")
        except Exception as sql_err:
            write_debug(f"[Dynamic Report Preview] SQL query build failed: {str(sql_err)}")
            raise HTTPException(status_code=400, detail=f"Failed to build SQL query: {str(sql_err)}")

        # Execute query and get data
        import pyodbc
        from config import get_database_connection_string

        try:
            connection_string = get_database_connection_string()
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()
        except Exception as db_err:
            write_debug(f"[Dynamic Report Preview] Database connection failed: {str(db_err)}")
            raise HTTPException(status_code=500, detail=f"Database connection failed: {str(db_err)}")

        try:
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            total_rows = len(rows)
            write_debug(f"[Dynamic Report Preview] Query executed, fetched {total_rows} rows")

            # Add index column like the export endpoint
            index_column_name = "#"
            columns_with_index = [index_column_name] + columns

            preview_rows = []
            for idx, row in enumerate(rows[:preview_limit], start=1):
                row_values = [str(cell) if cell is not None else '' for cell in row]
                row_with_index = [str(idx)] + row_values
                # Convert to dict keyed by column names for easier consumption on frontend
                preview_rows.append(dict(zip(columns_with_index, row_with_index)))

            return {
                "success": True,
                "columns": columns_with_index,
                "rows": preview_rows,
                "total": total_rows,
                "previewLimit": preview_limit,
            }
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        write_debug(f"[Dynamic Report Preview] Unexpected error: {str(e)}")
        import traceback
        write_debug(f"[Dynamic Report Preview] Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to preview dynamic report: {str(e)}")


@router.get("/api/exports/{export_id}/download")
async def download_export(export_id: int):
    """Download a saved export file by ID"""
    try:
        import pyodbc
        from config import get_database_connection_string
        import os
        
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT src, format FROM dbo.report_exports WHERE id = ?", export_id)
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Export not found")
            
            src = row[0]
            fmt = row[1] or 'pdf'
            
            if not src:
                raise HTTPException(status_code=404, detail="Export file not found")
            
            # Build file path
            base_dir = os.path.dirname(os.path.dirname(__file__))
            file_path = os.path.join(base_dir, src)
            
            if not os.path.exists(file_path):
                raise HTTPException(status_code=404, detail="Export file not found on disk")
            
            # Determine media type
            media_types = {
                'pdf': 'application/pdf',
                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'xls': 'application/vnd.ms-excel',
                'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                'doc': 'application/msword'
            }
            media_type = media_types.get(fmt.lower(), 'application/octet-stream')
            
            return FileResponse(
                file_path,
                media_type=media_type,
                filename=os.path.basename(file_path)
            )
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to download export: {str(e)}")


@router.post("/api/reports/schedule")
async def save_report_schedule(request: Request):
    """Save scheduled report configuration"""
    try:
        body = await request.json()
        report_config = body.get('reportConfig', {})
        schedule = body.get('schedule', {})
        
        # Save to database (you can create a scheduled_reports table)
        import pyodbc
        from config import get_database_connection_string
        
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        try:
            # Create table if not exists
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='scheduled_reports' and xtype='U')
                CREATE TABLE scheduled_reports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    report_config NVARCHAR(MAX) NOT NULL,
                    schedule_config NVARCHAR(MAX) NOT NULL,
                    is_active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE()
                );
            """)
            conn.commit()
            
            # Insert schedule
            import json
            cursor.execute("""
                INSERT INTO scheduled_reports (report_config, schedule_config)
                VALUES (?, ?)
            """, json.dumps(report_config), json.dumps(schedule))
            conn.commit()
            
            return {"success": True, "message": "Schedule saved successfully"}
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save schedule: {str(e)}")


@router.post("/api/reports/dynamic-dashboard/save-chart")
async def save_chart_to_dynamic_dashboard(request: Request):
    """Save chart configuration to dynamic dashboard"""
    try:
        body = await request.json()
        
        # Extract chart configuration
        chart_config = {
            'tables': body.get('tables', []),
            'joins': body.get('joins', []),
            'columns': body.get('columns', []),
            'whereConditions': body.get('whereConditions', []),
            'timeFilter': body.get('timeFilter'),
            'xKey': body.get('xKey'),
            'yKey': body.get('yKey'),
            'chartType': body.get('chartType', 'bar'),
            'visibleColumns': body.get('visibleColumns', []),
            'title': body.get('title', 'Dynamic Chart'),
        }
        
        # Get user from request.state (set by auth middleware)
        user = getattr(request.state, 'user', None)
        user_id = user.get('id') if user else None
        user_name = user.get('name') or user.get('userName') or request.headers.get('X-User-Name') or "System"
        
        from config import get_database_connection_string
        import pyodbc
        import json
        
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string, timeout=30)
        cursor = conn.cursor()
        
        try:
            # Create table if not exists
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dynamic_dashboard_charts' and xtype='U')
                CREATE TABLE dynamic_dashboard_charts (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    user_id NVARCHAR(500),
                    user_name NVARCHAR(500),
                    chart_config NVARCHAR(MAX) NOT NULL,
                    title NVARCHAR(1000),
                    chart_type NVARCHAR(100),
                    is_active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE(),
                    updated_at DATETIME2 DEFAULT GETDATE()
                );
            """)
            conn.commit()
            
            # Alter existing table columns if they're too small (for existing tables)
            try:
                cursor.execute("""
                    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                               WHERE TABLE_NAME = 'dynamic_dashboard_charts' AND COLUMN_NAME = 'title' 
                               AND CHARACTER_MAXIMUM_LENGTH < 1000)
                    ALTER TABLE dynamic_dashboard_charts ALTER COLUMN title NVARCHAR(1000);
                """)
                cursor.execute("""
                    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                               WHERE TABLE_NAME = 'dynamic_dashboard_charts' AND COLUMN_NAME = 'chart_type' 
                               AND CHARACTER_MAXIMUM_LENGTH < 100)
                    ALTER TABLE dynamic_dashboard_charts ALTER COLUMN chart_type NVARCHAR(100);
                """)
                cursor.execute("""
                    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                               WHERE TABLE_NAME = 'dynamic_dashboard_charts' AND COLUMN_NAME = 'user_id' 
                               AND CHARACTER_MAXIMUM_LENGTH < 500)
                    ALTER TABLE dynamic_dashboard_charts ALTER COLUMN user_id NVARCHAR(500);
                """)
                cursor.execute("""
                    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                               WHERE TABLE_NAME = 'dynamic_dashboard_charts' AND COLUMN_NAME = 'user_name' 
                               AND CHARACTER_MAXIMUM_LENGTH < 500)
                    ALTER TABLE dynamic_dashboard_charts ALTER COLUMN user_name NVARCHAR(500);
                """)
                conn.commit()
            except Exception as alter_err:
                write_debug(f"Note: Could not alter table columns (may not exist yet): {str(alter_err)}")
            
            # Insert chart configuration
            # Truncate fields to fit database constraints
            chart_config_json = json.dumps(chart_config)
            # NVARCHAR(MAX) can hold up to 2GB, but let's ensure it's reasonable
            if len(chart_config_json) > 10000000:  # 10MB limit
                write_debug(f"Warning: chart_config JSON is very large ({len(chart_config_json)} bytes), truncating")
                chart_config_json = chart_config_json[:10000000]
            
            title = (chart_config.get('title', 'Dynamic Chart') or 'Dynamic Chart')[:1000]  # Limit to 1000 chars
            chart_type = (chart_config.get('chartType', 'bar') or 'bar')[:100]  # Limit to 100 chars
            user_id_str = (user_id or '')[:500] if user_id else None  # Limit to 500 chars
            user_name_str = (user_name or 'System')[:500]  # Limit to 500 chars
            
            cursor.execute("""
                INSERT INTO dynamic_dashboard_charts (user_id, user_name, chart_config, title, chart_type)
                VALUES (?, ?, ?, ?, ?)
            """, user_id_str, user_name_str, chart_config_json, title, chart_type)
            conn.commit()
            
            # Get the inserted ID
            cursor.execute("SELECT SCOPE_IDENTITY()")
            chart_id = cursor.fetchone()[0]
            
            return {
                "success": True,
                "message": "Chart saved to dynamic dashboard successfully",
                "chartId": chart_id
            }
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        write_debug(f"Error saving chart to dynamic dashboard: {str(e)}")
        import traceback
        write_debug(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to save chart: {str(e)}")


@router.get("/api/reports/dynamic-dashboard/charts")
async def get_dynamic_dashboard_charts(request: Request):
    """Get all saved charts for dynamic dashboard"""
    try:
        # Get user from request.state (set by auth middleware)
        user = getattr(request.state, 'user', None)
        user_id = user.get('id') if user else None
        
        from config import get_database_connection_string
        import pyodbc
        import json
        
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string, timeout=30)
        cursor = conn.cursor()
        
        try:
            # Check if table exists
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = 'dynamic_dashboard_charts'
            """)
            table_exists = cursor.fetchone()[0] > 0
            
            if not table_exists:
                return {"success": True, "charts": []}
            
            # Get all active charts (show all charts for now, can filter by user later if needed)
            # For now, show all charts to all users so they can see shared charts
            cursor.execute("""
                SELECT id, user_id, user_name, chart_config, title, chart_type, created_at, updated_at
                FROM dynamic_dashboard_charts
                WHERE is_active = 1
                ORDER BY created_at DESC
            """)
            
            rows = cursor.fetchall()
            write_debug(f"[Dynamic Dashboard] Found {len(rows)} charts in database")
            charts = []
            
            for row in rows:
                chart_id, db_user_id, db_user_name, chart_config_json, title, chart_type, created_at, updated_at = row
                try:
                    chart_config = json.loads(chart_config_json) if chart_config_json else {}
                    charts.append({
                        'id': chart_id,
                        'userId': db_user_id,
                        'userName': db_user_name,
                        'title': title or chart_config.get('title', 'Dynamic Chart'),
                        'chartType': chart_type or chart_config.get('chartType', 'bar'),
                        'config': chart_config,
                        'createdAt': created_at.isoformat() if created_at else None,
                        'updatedAt': updated_at.isoformat() if updated_at else None,
                    })
                except json.JSONDecodeError:
                    write_debug(f"Error parsing chart config for chart ID {chart_id}")
                    continue
            
            return {"success": True, "charts": charts}
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        write_debug(f"Error getting dynamic dashboard charts: {str(e)}")
        import traceback
        write_debug(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to get charts: {str(e)}")


@router.delete("/api/reports/dynamic-dashboard/charts/{chart_id}")
async def delete_dynamic_dashboard_chart(chart_id: int, request: Request):
    """Delete a chart from dynamic dashboard"""
    try:
        # Get user from request.state (set by auth middleware)
        user = getattr(request.state, 'user', None)
        user_id = user.get('id') if user else None
        
        from config import get_database_connection_string
        import pyodbc
        
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string, timeout=30)
        cursor = conn.cursor()
        
        try:
            # Soft delete (set is_active = 0)
            if user_id:
                cursor.execute("""
                    UPDATE dynamic_dashboard_charts
                    SET is_active = 0, updated_at = GETDATE()
                    WHERE id = ? AND (user_id = ? OR user_id IS NULL)
                """, chart_id, user_id)
            else:
                cursor.execute("""
                    UPDATE dynamic_dashboard_charts
                    SET is_active = 0, updated_at = GETDATE()
                    WHERE id = ?
                """, chart_id)
            
            conn.commit()
            deleted = cursor.rowcount > 0
            
            if not deleted:
                raise HTTPException(status_code=404, detail="Chart not found")
            
            return {"success": True, "message": "Chart deleted successfully"}
            
        finally:
            cursor.close()
            conn.close()
            
    except HTTPException:
        raise
    except Exception as e:
        write_debug(f"Error deleting chart: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete chart: {str(e)}")


def convert_value_safely(value):
    """Safely convert a database value to string"""
    try:
        if value is None:
            return ''
        elif isinstance(value, (str, int, float, bool)):
            return str(value)
        elif isinstance(value, (datetime, date)):
            return value.isoformat()
        else:
            # Try to convert to string, fallback to repr or placeholder
            try:
                return str(value)
            except (TypeError, ValueError, UnicodeEncodeError):
                try:
                    return repr(value)
                except:
                    return '[Unsupported Type]'
    except Exception as e:
        return '[Conversion Error]'


@router.post("/api/reports/execute-sql")
async def execute_sql_query(request: Request):
    """
    Execute a custom SQL query and return results.
    Only SELECT statements are allowed for security.
    """
    try:
        body = await request.json()
        sql_query = body.get('sql', '').strip()
        limit = int(body.get('limit', 1000))
        
        write_debug(f"[Execute SQL] Received query: {sql_query[:200]}...")
        
        if not sql_query:
            raise HTTPException(status_code=400, detail="SQL query is required")
        
        # Security: Only allow SELECT statements
        sql_upper = sql_query.upper().strip()
        if not sql_upper.startswith('SELECT'):
            write_debug(f"[Execute SQL] Rejected: Query does not start with SELECT")
            raise HTTPException(status_code=400, detail="Only SELECT statements are allowed")
        
        # Prevent dangerous operations
        # Use word boundaries to avoid matching keywords inside other words
        # (e.g., "CREATE" should not match inside "createdAt" or "CAST(createdAt")
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE', 'EXEC', 'EXECUTE']
        
        import re
        for keyword in dangerous_keywords:
            # Use word boundary regex to match only whole words
            # \b ensures we match "CREATE" as a word, not "CREATE" inside "createdAt"
            pattern = rf'\b{re.escape(keyword)}\b'
            if re.search(pattern, sql_upper):
                write_debug(f"[Execute SQL] Rejected: Found dangerous keyword '{keyword}'")
                raise HTTPException(status_code=400, detail=f"Operation '{keyword}' is not allowed")
        
        # Automatically wrap common datetime column names with CAST to avoid unsupported type errors
        # This handles columns like createdAt, created_at, updatedAt, updated_at, date, timestamp, etc.
        datetime_column_patterns = [
            r'\bcreatedAt\b', r'\bcreated_at\b', r'\bupdatedAt\b', r'\bupdated_at\b',
            r'\bdeletedAt\b', r'\bdeleted_at\b', r'\bdate\b', r'\btimestamp\b',
            r'\bdatetime\b', r'\bdatetime2\b', r'\bdatetimeoffset\b'
        ]
        
        import re
        modified_query = sql_query
        for pattern in datetime_column_patterns:
            # Find columns that match datetime patterns and aren't already CAST
            matches = re.finditer(pattern, sql_query, re.IGNORECASE)
            for match in matches:
                col_name = match.group(0)
                # Check if it's already wrapped in CAST
                start_pos = match.start()
                end_pos = match.end()
                
                # Look backwards and forwards to see if it's already in a CAST
                before = sql_query[max(0, start_pos-10):start_pos].upper()
                after = sql_query[end_pos:min(len(sql_query), end_pos+10)].upper()
                
                if 'CAST' not in before and 'AS VARCHAR' not in after:
                    # Wrap the column name with CAST
                    # This is a simple replacement - for complex queries, user should CAST manually
                    # We'll only do this for simple SELECT column patterns
                    if re.search(rf'\b{re.escape(col_name)}\b', sql_query, re.IGNORECASE):
                        # Replace in SELECT clause only (simple case)
                        # For complex queries, we'll let the error happen and show helpful message
                        pass
        
        # For now, we'll let the query run and handle errors gracefully
        # Users can CAST datetime columns manually: CAST(createdAt AS VARCHAR(MAX)) AS createdAt
        
        write_debug(f"[Execute SQL] Executing query: {sql_query[:200]}...")
        
        # Execute query
        import pyodbc
        from config import get_database_connection_string
        from datetime import datetime, date
        
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string, timeout=30)
        cursor = conn.cursor()
        
        # Set output converter to handle unsupported types by converting to string
        def handle_unsupported_type(value):
            """Converter for unsupported SQL types"""
            try:
                return str(value) if value is not None else None
            except:
                return '[Unsupported Type]'
        
        # Register converter for all SQL types that might cause issues
        # This will catch types that pyodbc doesn't natively support
        try:
            # Try to set a default converter (this might not work in all pyodbc versions)
            # Instead, we'll handle it in the fetch logic
            pass
        except:
            pass
        
        try:
            cursor.execute(sql_query)
            
            # Get column names
            columns = [column[0] for column in cursor.description] if cursor.description else []
            
            # Fetch rows one by one to handle unsupported data types gracefully
            result_rows = []
            row_count = 0
            max_rows = limit  # Limit the number of rows returned
            
            try:
                # Try to fetch all at once first (faster for supported types)
                rows = cursor.fetchall()
                for row in rows[:max_rows]:
                    row_dict = {}
                    for idx, col in enumerate(columns):
                        value = row[idx]
                        row_dict[col] = convert_value_safely(value)
                    result_rows.append(row_dict)
                    row_count += 1
            except Exception as fetch_err:
                # If fetchall fails due to unsupported types, fetch row by row with new cursor
                write_debug(f"[Execute SQL] fetchall failed, trying row-by-row: {str(fetch_err)}")
                cursor.close()
                cursor = conn.cursor()
                cursor.execute(sql_query)
                
                while row_count < max_rows:
                    try:
                        row = cursor.fetchone()
                        if row is None:
                            break
                        
                        row_dict = {}
                        # Try to access each column, skip problematic ones
                        for idx, col in enumerate(columns):
                            try:
                                # Try to access the column value
                                value = row[idx]
                                row_dict[col] = convert_value_safely(value)
                            except (TypeError, ValueError, AttributeError, IndexError) as col_err:
                                # Column has unsupported type or access error
                                write_debug(f"[Execute SQL] Error accessing column {col} (index {idx}): {str(col_err)}")
                                row_dict[col] = '[Unsupported Type]'
                            except Exception as col_err:
                                # Other errors
                                write_debug(f"[Execute SQL] Unexpected error with column {col}: {str(col_err)}")
                                row_dict[col] = '[Error]'
                        
                        # Only add row if we got at least some data
                        if row_dict:
                            result_rows.append(row_dict)
                            row_count += 1
                    except Exception as row_err:
                        write_debug(f"[Execute SQL] Error fetching row {row_count}: {str(row_err)}")
                        # If we can't fetch this row at all, stop trying
                        # (likely all remaining rows have the same issue)
                        break
            
            write_debug(f"[Execute SQL] Query executed successfully, returned {len(result_rows)} rows")
            
            return {
                "success": True,
                "columns": columns,
                "rows": result_rows,
                "total": len(result_rows)
            }
            
        finally:
            cursor.close()
            conn.close()
            
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        write_debug(f"[Execute SQL] Error: {error_msg}")
        import traceback
        write_debug(f"[Execute SQL] Traceback: {traceback.format_exc()}")
        
        # Provide helpful error message for unsupported SQL types
        if 'ODBC SQL type' in error_msg and 'is not yet supported' in error_msg:
            # Try to extract column index if available
            import re
            col_match = re.search(r'column-index=(\d+)', error_msg)
            col_index = col_match.group(1) if col_match else None
            
            # Try to get column name from the query
            col_name = None
            try:
                if col_index and 'cursor' in locals() and cursor.description:
                    col_idx = int(col_index)
                    if col_idx < len(cursor.description):
                        col_name = cursor.description[col_idx][0]
            except:
                pass
            
            # Check if the query contains common datetime column names
            datetime_columns = []
            sql_lower = sql_query.lower()
            datetime_patterns = ['createdat', 'created_at', 'updatedat', 'updated_at', 'deletedat', 'deleted_at', 'date', 'timestamp']
            for pattern in datetime_patterns:
                if pattern in sql_lower:
                    # Extract the actual column name from the query
                    col_match = re.search(rf'\b(\w*{pattern}\w*)\b', sql_query, re.IGNORECASE)
                    if col_match:
                        datetime_columns.append(col_match.group(1))
            
            suggestion = "This error occurs when selecting datetime/datetimeoffset columns. "
            if datetime_columns:
                examples = []
                for col in set(datetime_columns[:3]):  # Show max 3 examples
                    examples.append(f"CAST({col} AS VARCHAR(MAX)) AS {col}")
                suggestion += f"Please CAST these columns in your SELECT: {', '.join(examples)}. "
                suggestion += "Example query: SELECT name, " + examples[0] + " FROM table_name"
            elif col_name:
                suggestion += f"Please CAST the '{col_name}' column: CAST({col_name} AS VARCHAR(MAX)) AS {col_name}"
            else:
                suggestion += "Please CAST datetime columns to VARCHAR(MAX). Example: SELECT CAST(createdAt AS VARCHAR(MAX)) AS createdAt FROM table_name"
            
            raise HTTPException(
                status_code=500, 
                detail=f"Unsupported data type error (datetime/datetimeoffset). {suggestion}"
            )
        
        raise HTTPException(status_code=500, detail=f"Failed to execute SQL query: {error_msg}")





