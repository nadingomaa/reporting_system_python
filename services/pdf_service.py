import asyncio
import aiohttp
from typing import Dict, Any, Optional, List
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.units import inch
from reportlab.platypus import Image
from reportlab.lib.utils import ImageReader
import io
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
from datetime import datetime

# Import PDF utilities
from utils.pdf_utils import shape_text_for_arabic, generate_pdf_report

def write_debug(msg):
    """Write debug message to file and stderr with timestamp"""
    from datetime import datetime
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    msg_with_time = f"[{timestamp}] {msg}"
    with open('debug_log.txt', 'a', encoding='utf-8') as f:
        f.write(f"{msg_with_time}\n")
        f.flush()
    import sys
    sys.stderr.write(f"{msg_with_time}\n")
    sys.stderr.flush()

class PDFService:
    def __init__(self):
        self.api_service = None
        self.database_service = None

    def set_services(self, api_service, database_service):
        self.api_service = api_service
        self.database_service = database_service

    async def generate_incidents_pdf(self, incidents_data: Dict[str, Any], start_date: str, end_date: str, header_config: Dict[str, Any], card_type: str = None, only_card: bool = False, only_overall_table: bool = False, only_chart: bool = False) -> bytes:
        """Generate incidents PDF report mirroring risks/controls behavior."""
        import re
        write_debug("DEBUG: ===== generate_incidents_pdf START =====")
        try:
            if not header_config:
                from utils.export_utils import get_default_header_config
                header_config = get_default_header_config("incidents")

            write_debug(f"  - card_type: {card_type}")
            write_debug(f"  - only_card: {only_card}")
            write_debug(f"  - only_chart: {only_chart}")
            write_debug(f"  - only_overall_table: {only_overall_table}")

            if not incidents_data:
                raise ValueError("No incidents_data provided")

            # Resolve data by card_type
            data = []
            if card_type:
                data = incidents_data.get(card_type) or incidents_data.get('list') or []

            write_debug(f"DEBUG: incidents_data keys: {list(incidents_data.keys()) if isinstance(incidents_data, dict) else 'N/A'}")
            write_debug(f"DEBUG: card_type: {card_type}")
            write_debug(f"DEBUG: data type: {type(data)}")
            write_debug(f"DEBUG: data length: {len(data) if isinstance(data, (list, dict)) else 'N/A'}")
            if isinstance(data, list) and data:
                write_debug(f"DEBUG: first item: {data[0] if data else 'N/A'}")
                write_debug(f"DEBUG: first item keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'N/A'}")

            columns: List[str] = []
            data_rows: List[List[str]] = []

            # CHART EXPORT
            if only_chart and card_type:
                def fmt(k: str) -> str:
                    return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', k).title()
                
                chart_type_override = None
                try:
                    chart_type_override = header_config.get("chartType") or header_config.get("chart_type")
                except Exception:
                    chart_type_override = None
                
                # Handle multiBar charts (like monthlyTrendByType)
                if chart_type_override == "multiBar" or card_type == "monthlyTrendByType":
                    chart_data = {"labels": [], "series": []}
                    if isinstance(data, list) and data:
                        first_item = data[0]
                        if isinstance(first_item, dict):
                            # Get all keys except 'period' or 'name' for the label
                            keys = list(first_item.keys())
                            # Find the label key (usually 'period' or 'name')
                            label_key = None
                            value_keys = []
                            for key in keys:
                                if key.lower() in ['period', 'name', 'month', 'date']:
                                    label_key = key
                                else:
                                    value_keys.append(key)
                            
                            if label_key:
                                # Create columns: Period + all value columns
                                columns = [fmt(label_key)] + [fmt(k) for k in value_keys]
                                
                                # Prepare labels and series for stacked bar chart
                                labels = []
                                series_list = []
                                
                                for item in data:
                                    row = [str(item.get(label_key, "N/A"))]
                                    labels.append(str(item.get(label_key, "N/A")))
                                    for k in value_keys:
                                        row.append(str(item.get(k, 0)))
                                    data_rows.append(row)
                                
                                # Create series for each value column (for stacked bar chart)
                                for value_key in value_keys:
                                    series_values = [float(item.get(value_key, 0) or 0) for item in data]
                                    series_list.append({
                                        'name': fmt(value_key),
                                        'values': series_values
                                    })
                                
                                chart_data["labels"] = labels
                                chart_data["series"] = series_list
                            else:
                                # Fallback: use first key as label, rest as values
                                label_key = keys[0]
                                value_keys = keys[1:]
                                columns = [fmt(label_key)] + [fmt(k) for k in value_keys]
                                
                                labels = []
                                series_list = []
                                for item in data:
                                    row = [str(item.get(label_key, "N/A"))]
                                    labels.append(str(item.get(label_key, "N/A")))
                                    for k in value_keys:
                                        row.append(str(item.get(k, 0)))
                                    data_rows.append(row)
                                
                                # Create series for each value column
                                for value_key in value_keys:
                                    series_values = [float(item.get(value_key, 0) or 0) for item in data]
                                    series_list.append({
                                        'name': fmt(value_key),
                                        'values': series_values
                                    })
                                
                                chart_data["labels"] = labels
                                chart_data["series"] = series_list
                        else:
                            data_rows = [[str(first_item), "0"]]
                            columns = ["Label", "Value"]
                            chart_data["labels"] = [str(first_item)]
                            chart_data["series"] = []
                    else:
                        data_rows = [["No data available", "0"]]
                        columns = ["Label", "Value"]
                    header_config["chart_data"] = chart_data
                    header_config["chart_type"] = "multiBar"
                else:
                    # Handle regular 2-column charts
                    chart_data = {"labels": [], "values": []}
                    if isinstance(data, list) and data:
                        first_item = data[0]
                        if isinstance(first_item, dict):
                            keys = list(first_item.keys())
                            if len(keys) >= 2:
                                key1 = keys[0]
                                key2 = keys[-1]
                                columns = [fmt(key1), fmt(key2)]
                                for item in data:
                                    name = item.get(key1, "N/A")
                                    value = item.get(key2, 0)
                                    chart_data["labels"].append(name)
                                    chart_data["values"].append(value)
                                    data_rows.append([name, str(value)])
                            else:
                                columns = ["Label", "Value"]
                                for item in data:
                                    name = str(item)
                                    chart_data["labels"].append(name)
                                    chart_data["values"].append(0)
                                    data_rows.append([name, "0"])
                        else:
                            columns = ["Label", "Value"]
                            data_rows.append([str(first_item), "0"])
                            chart_data["labels"].append(str(first_item))
                            chart_data["values"].append(0)
                    else:
                        columns = ["Label", "Value"]
                        data_rows.append(["No data available", "0"])

                    default_type_by_card = {
                        "byCategory": "bar",
                        "byStatus": "pie",
                        "monthlyTrend": "line",
                        "netLossAndRecovery": "bar",
                        "topFinancialImpacts": "bar",
                        "incidentsByEventType": "pie",
                        "incidentsByFinancialImpact": "pie",
                    }
                    valid_types = {"bar", "line", "pie"}
                    if chart_type_override in valid_types:
                        resolved_chart_type = chart_type_override
                    else:
                        resolved_chart_type = default_type_by_card.get(card_type or chart_type_override, "bar")
                    header_config["chart_data"] = chart_data
                    header_config["chart_type"] = resolved_chart_type

            # TABLE EXPORT
            elif only_overall_table:
                write_debug("DEBUG: ===== incidents only_overall_table START =====")
                # Use incidents list for overall table
                table_rows = []
                if card_type == 'overallStatuses':
                    table_rows = incidents_data.get('overallStatuses') or incidents_data.get('statusOverview') or []
                else:
                    table_rows = incidents_data.get(card_type) or []

                if isinstance(table_rows, list) and table_rows:
                    first_item = table_rows[0]
                    if isinstance(first_item, dict):
                        raw_keys = list(first_item.keys())
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        for i, row in enumerate(table_rows, 1):
                            values = [str(row.get(k, '')) for k in raw_keys]
                            data_rows.append([str(i)] + values)
                    elif isinstance(first_item, (list, tuple)):
                        num_cols = len(first_item)
                        columns = ['#'] + [f'C{idx+1}' for idx in range(num_cols)]
                        for i, row in enumerate(table_rows, 1):
                            vals = [str(v) for v in (row if isinstance(row, (list, tuple)) else [row])]
                            data_rows.append([str(i)] + vals)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [["1", str(first_item)]]
                else:
                    columns = ['#', 'Value']
                    data_rows = [["1", 'No data available']]

            # CARD SUMMARY EXPORT
            elif only_card and card_type:
                write_debug("DEBUG: ===== incidents only_card START =====")
                write_debug(f"DEBUG: data: {data}")
                write_debug(f"DEBUG: data type: {type(data)}")
                
                # Build a simple table from the card data
                if isinstance(data, list) and data:
                    first_item = data[0]
                    write_debug(f"DEBUG: first_item: {first_item}")
                    write_debug(f"DEBUG: first_item type: {type(first_item)}")
                    
                    if isinstance(first_item, dict):
                        write_debug(f"DEBUG: first_item keys: {list(first_item.keys())}")
                        
                        # Special handling for status-based card types (similar to controls)
                        if card_type in ['pendingPreparer', 'pendingChecker', 'pendingReviewer', 'pendingAcceptance']:
                            write_debug(f"DEBUG: Creating columns for status card type: ['#', 'Code', 'Title', 'Status', 'Created At']")
                            columns = ["#", "Code", "Title", "Status", "Created At"]
                            data_rows = []
                            for i, item in enumerate(data, 1):
                                data_rows.append([
                                    str(i),
                                    str(item.get('code', 'N/A')),
                                    str(item.get('title', 'N/A')),
                                    str(item.get('status', 'N/A')),
                                    str(item.get('createdAt', 'N/A'))
                                ])
                            write_debug(f"DEBUG: Created {len(data_rows)} rows with columns: {columns}")
                        else:
                            # Generic handling for other card types
                            def nice(k: str) -> str:
                                return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                            raw_keys = list(first_item.keys())
                            columns = ['#'] + [nice(k) for k in raw_keys]
                            write_debug(f"DEBUG: Generic handling - columns: {columns}")
                            for i, item in enumerate(data, 1):
                                values = [str(item.get(k, 'N/A')) for k in raw_keys]
                                data_rows.append([str(i)] + values)
                            write_debug(f"DEBUG: Created {len(data_rows)} rows")
                    else:
                        columns = ['#', 'Value']
                        data_rows = [["1", str(first_item)]]
                elif isinstance(data, list) and not data:
                    # Empty list
                    write_debug("DEBUG: data is an empty list - showing 'No data available'")
                    columns = ["Metric", "Value"]
                    data_rows = [["No data available", "No incidents found matching the criteria"]]
                elif isinstance(data, dict):
                    columns = ["Metric", "Value"]
                    data_rows = [[k, str(v)] for k, v in data.items()]
                else:
                    write_debug(f"DEBUG: data is not list or dict - type: {type(data)}, value: {data}")
                    columns = ["Metric", "Value"]
                    data_rows = [["No data available", "N/A"]]
            else:
                # Default simple report
                data_rows = [['Incidents Dashboard Report', 'Generated Successfully']]
                columns = ['Title', 'Status']

            # Merge header config for incidents
            from utils.export_utils import merge_header_config
            final_config = merge_header_config(header_config, "incidents")
            write_debug(f"DEBUG: Using PDF config (incidents): {final_config}")

            result = generate_pdf_report(columns, data_rows, final_config)
            if not result:
                raise ValueError("PDF generation returned None")
            write_debug(f"DEBUG: Incidents PDF generated successfully for {card_type}")
            return result

        except Exception as e:
            write_debug(f"ERROR: Failed to generate incidents PDF for {card_type} - {str(e)}")
            import traceback
            traceback.print_exc()
            # Fallback simple PDF
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            content = [Paragraph("Error generating incidents report", getSampleStyleSheet()['Normal'])]
            doc.build(content)
            buffer.seek(0)
            return buffer.getvalue()

    async def generate_kris_pdf(self, data: Dict[str, Any], start_date: str, end_date: str, header_config: Dict[str, Any], card_type: str = None, only_card: bool = False, only_overall_table: bool = False, only_chart: bool = False) -> bytes:
        """Generate KRI PDF report mirroring incidents behavior."""
        import re
        write_debug("DEBUG: ===== generate_kris_pdf START =====")
        try:
            if not header_config:
                from utils.export_utils import get_default_header_config
                header_config = get_default_header_config("kris")

            write_debug(f"  - card_type: {card_type}")
            write_debug(f"  - only_card: {only_card}")
            write_debug(f"  - only_chart: {only_chart}")
            write_debug(f"  - only_overall_table: {only_overall_table}")

            if not data:
                raise ValueError("No KRI data provided")

            # Resolve data by card_type
            kris_data = []
            if card_type:
                kris_data = data.get(card_type) or data.get('list') or []

            columns: List[str] = []
            data_rows: List[List[str]] = []

            # CHART EXPORT
            if only_chart and card_type:
                # Special handling for stacked monthly assessment chart
                if card_type == "kriMonthlyAssessment" and isinstance(kris_data, list) and kris_data:
                    # Transform from long format to wide format (pivot)
                    grouped: dict = {}
                    assessment_levels = set()
                    
                    def parse_month(month_val):
                        """Parse month to datetime object and return formatted string"""
                        try:
                            from datetime import datetime
                            dt = None
                            if isinstance(month_val, str):
                                # Try parsing various date formats
                                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S.%f']:
                                    try:
                                        dt = datetime.strptime(month_val.split('.')[0], fmt)
                                        break
                                    except:
                                        continue
                            elif hasattr(month_val, 'strftime'):
                                # Already a datetime object
                                dt = month_val
                            
                            if dt:
                                return dt, dt.strftime('%b %Y')  # Format as "Jan 2025"
                            return None, str(month_val)
                        except:
                            return None, str(month_val)
                    
                    # Use dict with date key for proper sorting
                    month_dict = {}  # Maps formatted string to date object for sorting
                    
                    for item in kris_data:
                        # Get month from createdAt, month, or month_year
                        month = item.get('month') or item.get('month_year') or item.get('createdAt')
                        if month:
                            dt, month_str = parse_month(month)
                            
                            assessment = item.get('assessment') or item.get('level') or 'Unknown'
                            count = float(item.get('count', 0))
                            
                            assessment_levels.add(assessment)
                            month_dict[month_str] = dt  # Store date for sorting
                            
                            if month_str not in grouped:
                                grouped[month_str] = {}
                            grouped[month_str][assessment] = (grouped[month_str].get(assessment, 0) + count)
                    
                    # Create columns: Month + each assessment level
                    sorted_levels = sorted(list(assessment_levels))
                    columns = ["Month"] + sorted_levels
                    
                    # Sort months by date value (not string)
                    from datetime import datetime
                    sorted_months = sorted(grouped.keys(), key=lambda m: month_dict.get(m) or datetime(1970, 1, 1))
                    chart_data = {"labels": [], "values": []}
                    
                    for month in sorted_months:
                        row = [month]
                        for level in sorted_levels:
                            value = grouped[month].get(level, 0)
                            row.append(str(value))
                        data_rows.append(row)
                        chart_data["labels"].append(month)
                    
                    # For stacked charts, we need multi-series data
                    # Store all series in chart_data
                    chart_series = []
                    for level in sorted_levels:
                        series_values = []
                        for month in sorted_months:
                            value = grouped[month].get(level, 0)
                            series_values.append(float(value))
                        chart_series.append({"name": level, "values": series_values})
                    
                    chart_data["series"] = chart_series
                    chart_data["labels"] = sorted_months
                    header_config["chart_data"] = chart_data
                    header_config["chart_type"] = "bar"  # Stacked bar for stacked charts
                    header_config["stacked"] = True
                else:
                    # Default handling for other charts
                    chart_data = {"labels": [], "values": []}
                    if isinstance(kris_data, list) and kris_data:
                        first_item = kris_data[0]
                        if isinstance(first_item, dict):
                            keys = list(first_item.keys())
                            if len(keys) >= 2:
                                key1 = keys[0]
                                key2 = keys[-1]
                                def fmt(k: str) -> str:
                                    return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', k).title()
                                columns = [fmt(key1), fmt(key2)]
                                for item in kris_data:
                                    name = item.get(key1, "N/A")
                                    value = item.get(key2, 0)
                                    chart_data["labels"].append(name)
                                    chart_data["values"].append(value)
                                    data_rows.append([name, str(value)])
                            else:
                                columns = ["Label", "Value"]
                                for item in kris_data:
                                    name = str(item)
                                    chart_data["labels"].append(name)
                                    chart_data["values"].append(0)
                                    data_rows.append([name, "0"])
                        else:
                            columns = ["Label", "Value"]
                            data_rows.append([str(first_item), "0"])
                    else:
                        columns = ["Label", "Value"]
                        data_rows.append(["No data available", "0"])

                    default_type_by_card = {
                        "krisByStatus": "pie",
                        "krisByLevel": "pie",
                        "breachedKRIsByDepartment": "bar",
                        "kriAssessmentCount": "bar",
                        "kriOverdueStatusCounts": "pie",
                        "kriMonthlyAssessment": "bar",
                    }
                    chart_type_override = None
                    try:
                        chart_type_override = header_config.get("chartType") or header_config.get("chart_type")
                    except Exception:
                        chart_type_override = None
                    valid_types = {"bar", "line", "pie"}
                    if chart_type_override in valid_types:
                        resolved_chart_type = chart_type_override
                    else:
                        resolved_chart_type = default_type_by_card.get(card_type or chart_type_override, "bar")
                    header_config["chart_data"] = chart_data
                    header_config["chart_type"] = resolved_chart_type

            # TABLE EXPORT
            elif only_overall_table:
                write_debug("DEBUG: ===== kris only_overall_table START =====")
                table_rows = data.get(card_type) or []

                if isinstance(table_rows, list) and table_rows:
                    first_item = table_rows[0]
                    if isinstance(first_item, dict):
                        raw_keys = list(first_item.keys())
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        for i, row in enumerate(table_rows, 1):
                            values = [str(row.get(k, '')) for k in raw_keys]
                            data_rows.append([str(i)] + values)
                    elif isinstance(first_item, (list, tuple)):
                        num_cols = len(first_item)
                        columns = ['#'] + [f'C{idx+1}' for idx in range(num_cols)]
                        for i, row in enumerate(table_rows, 1):
                            vals = [str(v) for v in (row if isinstance(row, (list, tuple)) else [row])]
                            data_rows.append([str(i)] + vals)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [["1", str(first_item)]]
                else:
                    columns = ['#', 'Value']
                    data_rows = [["1", 'No data available']]

            # CARD SUMMARY EXPORT
            elif only_card and card_type:
                write_debug("DEBUG: ===== kris only_card START =====")
                write_debug(f"DEBUG: kris_data type={type(kris_data)}, len={len(kris_data) if isinstance(kris_data, (list, dict)) else 'N/A'}")
                # Build a simple table from the card data
                if isinstance(kris_data, list):
                    if kris_data:  # Non-empty list
                        first_item = kris_data[0]
                        if isinstance(first_item, dict):
                            def nice(k: str) -> str:
                                return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                            raw_keys = list(first_item.keys())
                            columns = ['#'] + [nice(k) for k in raw_keys]
                            for i, item in enumerate(kris_data, 1):
                                values = [str(item.get(k, 'N/A')) for k in raw_keys]
                                data_rows.append([str(i)] + values)
                        else:
                            columns = ['#', 'Value']
                            data_rows = [["1", str(first_item)]]
                    else:  # Empty list
                        write_debug("DEBUG: kris_data is an empty list - showing 'No data available'")
                        columns = ["Metric", "Value"]
                        data_rows = [["No data available", "No KRIs found matching the criteria"]]
                elif isinstance(kris_data, dict):
                    if kris_data:  # Non-empty dict
                        columns = ["Metric", "Value"]
                        data_rows = [[k, str(v)] for k, v in kris_data.items()]
                    else:  # Empty dict
                        columns = ["Metric", "Value"]
                        data_rows = [["No data available", "No KRIs found matching the criteria"]]
                else:
                    columns = ["Metric", "Value"]
                    data_rows = [["No data available", "No KRIs found matching the criteria"]]
            else:
                # Default simple report
                data_rows = [['KRIs Dashboard Report', 'Generated Successfully']]
                columns = ['Title', 'Status']

            # Merge header config for kris
            from utils.export_utils import merge_header_config
            final_config = merge_header_config(header_config, "kris")
            write_debug(f"DEBUG: Using PDF config (kris): {final_config}")

            result = generate_pdf_report(columns, data_rows, final_config)
            if not result:
                raise ValueError("PDF generation returned None")
            write_debug(f"DEBUG: KRIs PDF generated successfully for {card_type}")
            return result

        except Exception as e:
            write_debug(f"ERROR: Failed to generate KRIs PDF for {card_type} - {str(e)}")
            import traceback
            traceback.print_exc()
            # Fallback simple PDF
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            content = [Paragraph("Error generating KRIs report", getSampleStyleSheet()['Normal'])]
            doc.build(content)
            buffer.seek(0)
            return buffer.getvalue()

    async def generate_pdf_report(self, report_data: Dict[str, Any], report_config: Dict[str, Any]) -> bytes:
        """Generate PDF report from data and configuration"""
        try:
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            content = []
            
            # Add title
            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=16,
                spaceAfter=30,
                alignment=1
            )
            
            content.append(Paragraph("Controls Dashboard Report", title_style))
            content.append(Spacer(1, 20))
            
            # Add basic content
            content.append(Paragraph("Report generated successfully.", styles['Normal']))
            
            doc.build(content)
            buffer.seek(0)
            
            return buffer.getvalue()
            
        except Exception as e:
            print(f"Error generating PDF report: {e}")
            raise e


    async def generate_controls_pdf(self, controls_data: Dict[str, Any], startDate: str, endDate: str, header_config: Dict[str, Any], cardType: str = None, onlyCard: bool = False, onlyOverallTable: bool = False, onlyChart: bool = False) -> bytes:
        
        """
        Generate PDF report for controls dashboard.
        Simplified to use provided data instead of fetching or reconstructing.
        Each cardType data is passed directly from caller.
        """
        # generate_pdf_report is already imported at top of file from pdf_utils
        import re
        write_debug(f"DEBUG: ===== generate_controls_pdf START =====")

        try:
            # Basic info logs
            write_debug(f"  - cardType: {cardType}")
            write_debug(f"  - onlyCard: {onlyCard}")
            write_debug(f"  - onlyChart: {onlyChart}")
            write_debug(f"  - onlyOverallTable: {onlyOverallTable}")


            if not controls_data:
                raise ValueError("No controls_data provided")

            data = controls_data.get(cardType, [])
            
            # Safety defaults
            columns = []
            data_rows = []

            # CHART EXPORT
            if onlyChart and cardType:
                chart_data = {"labels": [], "values": []}

                if isinstance(data, list) and data:
                    # Dynamically extract keys from the first item
                    first_item = data[0] if data else {}
                    if isinstance(first_item, dict):
                        keys = list(first_item.keys())
                        
                        # For most queries, we expect 2 columns: a label/key and a count/value
                        if len(keys) >= 2:
                            # First key is usually the label/name column
                            # Last key is usually the count/value column
                            key1 = keys[0]
                            key2 = keys[-1]
                            
                            # Create human-readable column headers
                            def format_column_name(key):
                                # Convert snake_case or camelCase to Title Case
                                # Replace underscores/camelCase with spaces
                                formatted = re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', key)
                                return formatted.title()
                            
                            label1 = format_column_name(key1)
                            label2 = format_column_name(key2)
                            columns = [label1, label2]
                            
                            
                            
                            for item in data:
                                name = item.get(key1, "N/A")
                                value = item.get(key2, 0)
                                chart_data["labels"].append(name)
                                chart_data["values"].append(value)
                                data_rows.append([name, str(value)])
                        else:
                            # Fallback if unexpected structure: build single-column names safely
                            write_debug(f"  - Unexpected key count: {len(keys)}, using first key only")
                            key1 = keys[0]
                            columns = [format_column_name(key1), "Value"]
                            # Do NOT append duplicate rows; leave values unknown
                            for item in data:
                                name = item.get(key1, "N/A")
                                chart_data["labels"].append(name)
                                # Keep value as 0 to avoid N/A duplicates in table
                                chart_data["values"].append(0)
                                data_rows.append([name, 0])
                    else:
                        
                        data_rows.append(["No data available", "0"])
                        columns = ["Label", "Value"]
                else:
                    data_rows.append(["No data available", "0"])
                    columns = ["Label", "Value"]
                
                # Decide chart type: header_config overrides mapping
                chart_type_override = None
                try:
                    chart_type_override = header_config.get("chartType") or header_config.get("chart_type")
                except Exception:
                    chart_type_override = None

                # Map cardType to default chart types (matching frontend config)
                default_type_by_card = {
                    "quarterlyControlCreationTrend": "line",
                    "controlsByType": "pie",
                    "antiFraudDistribution": "pie",
                    "controlsPerLevel": "bar",
                    "controlExecutionFrequency": "bar",
                    "numberOfControlsPerComponent": "bar",
                    "departmentDistribution": "bar",
                    "numberOfControlsByIcofrStatus": "pie",
                }

                # If override is not a known matplotlib type, fall back to mapping
                valid_types = {"bar", "line", "pie"}
                if chart_type_override and chart_type_override in valid_types:
                    resolved_chart_type = chart_type_override
                else:
                    # Also handle when chartType was sent as a card key (e.g., 'quarterlyControlCreationTrend')
                    resolved_chart_type = default_type_by_card.get(cardType or chart_type_override, "bar")

                header_config["chart_data"] = chart_data
                header_config["chart_type"] = resolved_chart_type
            
            # TABLE EXPORT
            elif onlyOverallTable:
                write_debug(f"DEBUG: ===== onlyOverallTable START =====")
                write_debug(f"  - data type: {type(data)}")
                
                # Generic table builder: derive columns from keys of first row and add index column
                if isinstance(data, list) and len(data) > 0:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        raw_keys = list(first_item.keys())
                        # Human-readable column labels
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        data_rows = []
                        for i, row in enumerate(data, 1):
                            values = [str(row.get(k, '')) for k in raw_keys]
                            data_rows.append([str(i)] + values)
                    elif isinstance(first_item, (list, tuple)):
                        # If rows are arrays, label columns generically C1..Cn
                        num_cols = len(first_item)
                        columns = ['#'] + [f'C{idx+1}' for idx in range(num_cols)]
                        data_rows = []
                        for i, row in enumerate(data, 1):
                            vals = [str(v) for v in (row if isinstance(row, (list, tuple)) else [row])]
                            data_rows.append([str(i)] + vals)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [[str(i+1), str(v)] for i, v in enumerate(data)]
                else:
                    columns = ['#', 'Value']
                    data_rows = [['1', 'No data available']]

            # CARD SUMMARY EXPORT
            elif onlyCard and cardType:
                write_debug(f"DEBUG: ===== onlyCard START =====")
                write_debug(f"  - data: {data}")
                write_debug(f"  - data type: {type(data)}")
                
                # Handle both dict and list data
                if isinstance(data, list) and data:
                    first_item = data[0] if data else {}
                    
                    # Check if data is list of strings (for totalControls)
                    if isinstance(first_item, str):
                        write_debug(f"  - Data is list of strings")
                        write_debug(f"  - Creating columns: ['#', 'Control Name']")
                        columns = ["#", "Control Name"]
                        data_rows = []
                        for i, item in enumerate(data, 1):
                            data_rows.append([
                                str(i),
                                str(item)  # Just the control name
                            ])
                        write_debug(f"  - Created {len(data_rows)} rows")
                    elif isinstance(first_item, dict):
                        # Data is list of dicts
                        write_debug(f"  - Data is list of dicts")
                        write_debug(f"  - first_item keys: {list(first_item.keys())}")
                        # Determine columns based on cardType
                        if cardType in ['pendingPreparer', 'pendingChecker', 'pendingReviewer', 'pendingAcceptance', 'testsPendingPreparer', 'testsPendingChecker', 'testsPendingReviewer', 'testsPendingAcceptance','unmappedControls','unmappedIcofrControls','unmappedNonIcofrControls','totalControls']:
                            write_debug(f"  - Creating columns: ['#', 'Control Code', 'Control Name']")
                            columns = ["#", "Control Code", "Control Name"]
                            data_rows = []
                            for i, item in enumerate(data, 1):
                                data_rows.append([
                                    str(i),
                                    item.get('control_code', item.get('code', 'N/A')),
                                    item.get('control_name', item.get('name', 'N/A'))
                                ])
                            write_debug(f"  - Created {len(data_rows)} rows with columns: {columns}")
                        else:
                            # Generic handling for other card types
                            write_debug(f"  - Generic handling")
                            columns = ["#", "Code", "Name"]
                            data_rows = []
                            for i, item in enumerate(data, 1):
                                data_rows.append([
                                    str(i),
                                    item.get('code', 'N/A'),
                                    item.get('name', 'N/A')
                                ])
                            write_debug(f"  - Created {len(data_rows)} rows with columns: {columns}")
                elif isinstance(data, dict):
                    columns = ["Metric", "Value"]
                    data_rows = [[key, str(value)] for key, value in data.items()]
                else:
                    columns = ["Metric", "Value"]
                    data_rows = [["No data available", "N/A"]]

            # Apply default PDF design settings before generation
            # Uses centralized configuration system from utils/export_utils.py
            # All defaults are defined in ONE place - modify them there!
            
            from utils.export_utils import merge_header_config
            
            # Merge user config with centralized defaults
            final_config = merge_header_config(header_config, "controls")
            
            write_debug(f"DEBUG: Using PDF config: {final_config}")
            
            # Generate PDF with merged config
            result = generate_pdf_report(columns, data_rows, final_config)
            if not result:
                raise ValueError("PDF generation returned None")

            write_debug(f"DEBUG: PDF generated successfully for {cardType}")
            return result
                
        except Exception as e:
            write_debug(f"ERROR: Failed to generate PDF for {cardType} - {str(e)}")
            import traceback
            traceback.print_exc()
            raise e

        

    async def generate_risks_pdf(self, risks_data: Dict[str, Any], start_date: str, end_date: str, header_config: Dict[str, Any], card_type: str = None, only_card: bool = False, only_overall_table: bool = False, only_chart: bool = False) -> bytes:
        """Generate risks PDF report with the same structure as controls PDF generation."""
        import re
        write_debug("DEBUG: ===== generate_risks_pdf START =====")
        try:
            # Ensure header_config has risks defaults (handle None or empty dict)
            if not header_config or (isinstance(header_config, dict) and len(header_config) == 0):
                from utils.export_utils import get_default_header_config
                header_config = get_default_header_config("risks")
            
            write_debug(f"  - header_config tableHeaderBgColor (before merge): {header_config.get('tableHeaderBgColor', 'NOT SET')}")
            write_debug(f"  - card_type: {card_type}")
            write_debug(f"  - only_card: {only_card}")
            write_debug(f"  - only_chart: {only_chart}")
            write_debug(f"  - only_overall_table: {only_overall_table}")

            if not risks_data:
                raise ValueError("No risks_data provided")

            data = risks_data.get(card_type, [])

            columns: List[str] = []
            data_rows: List[List[str]] = []

            # CHART EXPORT (mirrors controls behavior)
            if only_chart and card_type:
                chart_data = {"labels": [], "values": []}
                if isinstance(data, list) and data:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        keys = list(first_item.keys())
                        def fmt(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', k).title()
                        
                        # Special handling for charts with multiple value columns
                        if card_type == "createdDeletedRisksPerQuarter" and len(keys) >= 3:
                            # For created/deleted charts, include all numeric columns
                            label_key = "name"
                            value_keys = [k for k in keys if k != label_key]
                            write_debug(f"[RISKS PDF] Multi-column chart: label_key={label_key}, value_keys={value_keys}")
                            columns = [fmt(label_key)] + [fmt(k) for k in value_keys]
                            chart_data["labels"] = []
                            chart_data["values"] = []
                            for item in data:
                                name = item.get(label_key, "N/A")
                                row = [name]
                                values_for_chart = []
                                for vk in value_keys:
                                    val = item.get(vk, 0)
                                    row.append(str(val))
                                    values_for_chart.append(val)
                                chart_data["labels"].append(name)
                                # Use first value series for chart (created)
                                chart_data["values"].append(values_for_chart[0] if values_for_chart else 0)
                                data_rows.append(row)
                            write_debug(f"[RISKS PDF] Generated {len(data_rows)} rows, columns={columns}")
                        elif len(keys) >= 2:
                            key1 = keys[0]
                            key2 = keys[-1]
                            columns = [fmt(key1), fmt(key2)]
                            for item in data:
                                name = item.get(key1, "N/A")
                                value = item.get(key2, 0)
                                chart_data["labels"].append(name)
                                chart_data["values"].append(value)
                                data_rows.append([name, str(value)])
                        else:
                            columns = ["Label", "Value"]
                            for item in data:
                                name = str(item)
                                chart_data["labels"].append(name)
                                chart_data["values"].append(0)
                                data_rows.append([name, "0"])
                    else:
                        columns = ["Label", "Value"]
                        data_rows.append([str(first_item), "0"])
                else:
                    columns = ["Label", "Value"]
                    data_rows.append(["No data available", "0"])

                # Risk default chart types (fallbacks)
                default_type_by_card = {
                    "risksByCategory": "bar",
                    "risksByEventType": "pie",
                    "createdDeletedRisksPerQuarter": "bar",
                    "quarterlyRiskCreationTrends": "line",
                    "riskApprovalStatusDistribution": "pie",
                    "riskDistributionByFinancialImpact": "pie",
                }
                chart_type_override = None
                try:
                    chart_type_override = header_config.get("chartType") or header_config.get("chart_type")
                except Exception:
                    chart_type_override = None
                valid_types = {"bar", "line", "pie"}
                if chart_type_override in valid_types:
                    resolved_chart_type = chart_type_override
                else:
                    resolved_chart_type = default_type_by_card.get(card_type or chart_type_override, "bar")
                header_config["chart_data"] = chart_data
                header_config["chart_type"] = resolved_chart_type

            # TABLE EXPORT
            elif only_overall_table:
                write_debug("DEBUG: ===== only_overall_table START =====")
                write_debug(f"  - data type: {type(data)}")
                if isinstance(data, list) and data:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        # Special formatting for allRisks table
                        if card_type == 'allRisks':
                            columns = ['#', 'Risk Name', 'Risk Description', 'Event', 'Inherent Value', 'Frequency', 'Financial Impact']
                            def map_frequency(val):
                                mapping = {1: 'Once in Three Years', 2: 'Annually', 3: 'Half Yearly', 4: 'Quarterly', 5: 'Monthly'}
                                try:
                                    num = int(val)
                                except Exception:
                                    return str(val)
                                return mapping.get(num, str(val))
                            def map_financial(val):
                                mapping = {1: '0 - 10,000', 2: '10,000 - 100,000', 3: '100,000 - 1,000,000', 4: '1,000,000 - 10,000,000', 5: '> 10,000,000'}
                                try:
                                    num = int(val)
                                except Exception:
                                    return str(val)
                                return mapping.get(num, str(val))
                            for i, row in enumerate(data, 1):
                                data_rows.append([
                                    str(i),
                                    str(row.get('RiskName', '')),
                                    str(row.get('RiskDesc', '')),
                                    str(row.get('RiskEventName', 'Unknown')),
                                    str(row.get('InherentValue', '')),
                                    map_frequency(row.get('InherentFrequency', '')),
                                    map_financial(row.get('InherentFinancialValue', '')),
                                ])
                        else:
                            raw_keys = list(first_item.keys())
                            def nice(k: str) -> str:
                                return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                            columns = ['#'] + [nice(k) for k in raw_keys]
                            for i, row in enumerate(data, 1):
                                values = [str(row.get(k, '')) for k in raw_keys]
                                data_rows.append([str(i)] + values)
                    elif isinstance(first_item, (list, tuple)):
                        num_cols = len(first_item)
                        columns = ['#'] + [f'C{idx+1}' for idx in range(num_cols)]
                        for i, row in enumerate(data, 1):
                            vals = [str(v) for v in (row if isinstance(row, (list, tuple)) else [row])]
                            data_rows.append([str(i)] + vals)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [["1", str(first_item)]]
                else:
                    columns = ['#', 'Value']
                    data_rows = [["1", 'No data available']]

            # CARD SUMMARY EXPORT
            elif only_card and card_type:
                write_debug("DEBUG: ===== only_card START =====")
                write_debug(f"  - data type: {type(data)}")
                if isinstance(data, list) and data:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        raw_keys = list(first_item.keys())
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        for i, item in enumerate(data, 1):
                            values = [str(item.get(k, 'N/A')) for k in raw_keys]
                            data_rows.append([str(i)] + values)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [["1", str(first_item)]]
                elif isinstance(data, dict):
                    columns = ["Metric", "Value"]
                    data_rows = [[k, str(v)] for k, v in data.items()]
                else:
                    columns = ["Metric", "Value"]
                    data_rows = [["No data available", "N/A"]]
            else:
                # Default simple report
                data_rows = [['Risks Dashboard Report', 'Generated Successfully']]
                columns = ['Title', 'Status']

            # Merge header config for risks
            from utils.export_utils import merge_header_config
            write_debug(f"  - header_config tableHeaderBgColor (before merge_header_config): {header_config.get('tableHeaderBgColor', 'NOT SET')}")
            final_config = merge_header_config(header_config, "risks")
            write_debug(f"  - final_config tableHeaderBgColor: {final_config.get('tableHeaderBgColor', 'NOT SET')}")
            write_debug(f"DEBUG: Using PDF config (risks): {final_config}")

            result = generate_pdf_report(columns, data_rows, final_config)
            if not result:
                raise ValueError("PDF generation returned None")
            write_debug(f"DEBUG: PDF generated successfully for {card_type}")
            return result
                
        except Exception as e:
            write_debug(f"ERROR: Failed to generate risks PDF for {card_type} - {str(e)}")
            import traceback
            traceback.print_exc()
            # Fallback simple PDF
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            content = [Paragraph("Error generating report", getSampleStyleSheet()['Normal'])]
            doc.build(content)
            buffer.seek(0)
            return buffer.getvalue()

    async def generate_comply_pdf(self, comply_data: Dict[str, Any], startDate: str, endDate: str, header_config: Dict[str, Any], cardType: str = None, onlyCard: bool = False, onlyOverallTable: bool = False, onlyChart: bool = False) -> bytes:
        """Generate comply PDF report - mirrors controls/risks PDF behavior"""
        import re
        write_debug("DEBUG: ===== generate_comply_pdf START =====")
        try:
            write_debug(f"  - cardType: {cardType}")
            write_debug(f"  - onlyCard: {onlyCard}")
            write_debug(f"  - onlyChart: {onlyChart}")
            write_debug(f"  - onlyOverallTable: {onlyOverallTable}")

            if not comply_data:
                raise ValueError("No comply_data provided")

            # Map frontend chart/card IDs to backend report names
            report_name_map = {
                # Chart IDs from frontend -> Backend report names
                'surveysByStatus': 'Surveys by Status',
                'complianceByStatus': 'Compliance per complianceStatus',
                'complianceByProgress': 'Compliance per progressStatus',
                'complianceByApproval': 'Compliance per approval_status',
                'avgScorePerSurvey': 'Average Score Per Survey',
                'complianceByControlCategory': 'Compliance by Control Category',
                'topFailedControls': 'Top Failed Controls',
                'controlsPerCategory': 'Controls no. per category',
                'risksPerCategory': 'Risks no. per category',
                'impactedAreasTrend': 'Impacted Areas Trend Over Time',
                # Card IDs from frontend -> Backend report names
                'totalSurveys': 'Surveys by Status',
                'totalCompliance': 'Compliance Details',
                'avgCompletionRate': 'Survey Completion Rate',
                'complianceWithoutEvidence': 'Compliance controls without evidence',
                # Table IDs from frontend -> Backend report names
                'complianceDetails': 'Compliance Details',
                'surveyCompletionRate': 'Survey Completion Rate',
                'bankQuestionsDetails': 'Bank Questions details',
                'risksPerCategoryDetails': 'Risks per category details',
                'controlsPerCategoryDetails': 'Controls per category details',
                'controlsPerDomainsDetails': 'Controls per domains Details',
                'questionsPerCategory': 'Questions Per Category',
                'impactedAreasByControls': 'Impacted Areas by Number of Linked Controls',
                'surveyParticipationByDepartment': 'Survey Participation by Department',
                'activeFunctions': 'Most Active vs Least Active Functions (Answer Count)',
                'surveyCoverageByCategory': 'Survey Coverage by Category (How many categories included per survey)',
                'complianceManagementDetails': 'Compliance managment details',
                # Additional chart mappings
                'questionsPerType': 'Questions no. per type',
                'questionsPerReferences': 'Questions no. per References',
                'controlNosPerDomains': 'Control Nos. per Domains',
            }

            # Get the actual report name from the map, or use cardType as-is
            report_name = report_name_map.get(cardType, cardType)
            
            # Try to get data from comply_data
            # First try cardType (route handler passes data with cardType as key)
            data = comply_data.get(cardType, [])
            
            # If not found, try report name (in case full comply_data was passed)
            if not data:
                data = comply_data.get(report_name, [])
            
            # If still no data, try to find it by partial match
            if not data and cardType:
                for key in comply_data.keys():
                    if isinstance(key, str) and cardType.lower() in key.lower():
                        data = comply_data[key]
                        report_name = key
                        break

            # Safety defaults
            columns = []
            data_rows = []

            # CHART EXPORT
            if onlyChart and cardType:
                chart_data = {"labels": [], "values": []}

                if isinstance(data, list) and data:
                    first_item = data[0] if data else {}
                    if isinstance(first_item, dict):
                        keys = list(first_item.keys())
                        
                        # Handle multi-column charts (like monthlyTrendByType)
                        if len(keys) >= 3 and any(k.lower() in ['period', 'month', 'date', 'name'] for k in keys):
                            # Find label key and value keys
                            label_key = None
                            value_keys = []
                            for key in keys:
                                if key.lower() in ['period', 'name', 'month', 'date']:
                                    label_key = key
                                else:
                                    value_keys.append(key)
                            
                            if label_key:
                                def format_column_name(key):
                                    return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(key)).title()
                                
                                columns = [format_column_name(label_key)] + [format_column_name(k) for k in value_keys]
                                
                                labels = []
                                series_list = []
                                for item in data:
                                    row = [str(item.get(label_key, "N/A"))]
                                    labels.append(str(item.get(label_key, "N/A")))
                                    for k in value_keys:
                                        row.append(str(item.get(k, 0)))
                                    data_rows.append(row)
                                
                                # Create series for stacked bar chart
                                for value_key in value_keys:
                                    series_values = [float(item.get(value_key, 0) or 0) for item in data]
                                    series_list.append({
                                        'name': format_column_name(value_key),
                                        'values': series_values
                                    })
                                
                                chart_data = {
                                    "labels": labels,
                                    "series": series_list
                                }
                                header_config["chart_type"] = "multiBar"
                        elif len(keys) >= 2:
                            # Standard 2-column chart
                            # For compliance charts, intelligently detect label vs value keys
                            label_key = None
                            value_key = None
                            
                            # Known status/label keys (exact matches first, then partial)
                            status_key_exact = ['complianceStatus', 'progressStatus', 'approval_status']
                            status_key_partial = ['status', 'name', 'label', 'category', 'type']
                            # Known value keys
                            value_key_exact = ['Compliance', 'count', 'total', 'value']
                            value_key_partial = ['amount', 'number', 'sum']
                            
                            # First pass: look for exact matches
                            for key in keys:
                                if key in status_key_exact:
                                    label_key = key
                                elif key in value_key_exact:
                                    value_key = key
                            
                            # Second pass: look for partial matches if exact not found
                            if label_key is None:
                                for key in keys:
                                    key_lower = key.lower()
                                    if any(sk in key_lower for sk in status_key_partial):
                                        label_key = key
                                        break
                            
                            if value_key is None:
                                for key in keys:
                                    key_lower = key.lower()
                                    if any(vk in key_lower for vk in value_key_partial) or key == 'Compliance':
                                        value_key = key
                                        break
                            
                            # Fallback: if still not found, use first as label, last as value
                            # But prioritize 'Compliance' as value if it exists
                            if value_key is None and 'Compliance' in keys:
                                value_key = 'Compliance'
                                label_key = [k for k in keys if k != 'Compliance'][0] if len(keys) > 1 else keys[0]
                            
                            if label_key is None:
                                label_key = keys[0]
                            if value_key is None:
                                value_key = keys[-1]
                            
                            write_debug(f"Chart key detection - keys={keys}, label_key={label_key}, value_key={value_key}")
                            
                            def format_column_name(key):
                                return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(key)).title()
                            
                            label1 = format_column_name(label_key)
                            label2 = format_column_name(value_key)
                            columns = [label1, label2]
                            
                            for item in data:
                                name = item.get(label_key, "N/A")
                                value = item.get(value_key, 0)
                                chart_data["labels"].append(name)
                                chart_data["values"].append(value)
                                data_rows.append([name, str(value)])
                        else:
                            key1 = keys[0]
                            def format_column_name(key):
                                return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(key)).title()
                            columns = [format_column_name(key1), "Value"]
                            for item in data:
                                name = item.get(key1, "N/A")
                                chart_data["labels"].append(name)
                                chart_data["values"].append(0)
                                data_rows.append([name, 0])
                    else:
                        data_rows.append(["No data available", "0"])
                        columns = ["Label", "Value"]
                else:
                    data_rows.append(["No data available", "0"])
                    columns = ["Label", "Value"]
                
                # Decide chart type: header_config overrides mapping
                chart_type_override = header_config.get("chartType") or header_config.get("chart_type")
                
                # Map cardType to default chart types
                default_type_by_card = {
                    "surveysByStatus": "pie",
                    "complianceByStatus": "pie",
                    "complianceByProgress": "pie",
                    "complianceByApproval": "pie",
                    "avgScorePerSurvey": "bar",
                    "complianceByControlCategory": "bar",
                    "topFailedControls": "bar",
                    "controlsPerCategory": "bar",
                    "risksPerCategory": "bar",
                    "impactedAreasTrend": "line",
                }

                valid_types = {"bar", "line", "pie", "multiBar"}
                if chart_type_override and chart_type_override in valid_types:
                    resolved_chart_type = chart_type_override
                else:
                    resolved_chart_type = default_type_by_card.get(cardType, "bar")

                header_config["chart_data"] = chart_data
                header_config["chart_type"] = resolved_chart_type
            
            # TABLE EXPORT
            elif onlyOverallTable and cardType:
                write_debug(f"DEBUG: ===== onlyOverallTable START =====")
                write_debug(f"  - data type: {type(data)}")
                
                if isinstance(data, list) and len(data) > 0:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        raw_keys = list(first_item.keys())
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        data_rows = []
                        for i, row in enumerate(data, 1):
                            values = [str(row.get(k, '')) for k in raw_keys]
                            data_rows.append([str(i)] + values)
                    elif isinstance(first_item, (list, tuple)):
                        num_cols = len(first_item)
                        columns = ['#'] + [f'C{idx+1}' for idx in range(num_cols)]
                        data_rows = []
                        for i, row in enumerate(data, 1):
                            vals = [str(v) for v in (row if isinstance(row, (list, tuple)) else [row])]
                            data_rows.append([str(i)] + vals)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [[str(i+1), str(v)] for i, v in enumerate(data)]
                else:
                    columns = ['#', 'Value']
                    data_rows = [['1', 'No data available']]

            # CARD SUMMARY EXPORT
            elif onlyCard and cardType:
                write_debug(f"DEBUG: ===== onlyCard START =====")
                write_debug(f"  - data: {data}")
                write_debug(f"  - data type: {type(data)}")
                
                if isinstance(data, list) and data:
                    first_item = data[0] if data else {}
                    
                    if isinstance(first_item, str):
                        columns = ["#", "Item Name"]
                        data_rows = []
                        for i, item in enumerate(data, 1):
                            data_rows.append([str(i), str(item)])
                    elif isinstance(first_item, dict):
                        raw_keys = list(first_item.keys())
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        data_rows = []
                        for i, item in enumerate(data, 1):
                            if isinstance(item, dict):
                                values = [str(item.get(k, 'N/A')) for k in raw_keys]
                                data_rows.append([str(i)] + values)
                            elif isinstance(item, (list, tuple)):
                                vals = [str(v) for v in item]
                                data_rows.append([str(i)] + vals)
                            else:
                                data_rows.append([str(i), str(item)])
                    else:
                        columns = ['#', 'Value']
                        data_rows = [[str(i+1), str(v)] for i, v in enumerate(data)]
                elif isinstance(data, dict):
                    columns = ["Metric", "Value"]
                    data_rows = [[key, str(value)] for key, value in data.items()]
                else:
                    columns = ["Metric", "Value"]
                    data_rows = [["No data available", "N/A"]]

            # Apply default PDF design settings before generation
            from utils.export_utils import merge_header_config
            
            # Merge user config with centralized defaults
            final_config = merge_header_config(header_config, "comply")
            
            write_debug(f"DEBUG: Using PDF config: {final_config}")
            
            # Generate PDF with merged config
            result = generate_pdf_report(columns, data_rows, final_config)
            if not result:
                raise ValueError("PDF generation returned None")

            write_debug(f"DEBUG: PDF generated successfully for {cardType}")
            return result
                
        except Exception as e:
            write_debug(f"ERROR: Failed to generate comply PDF for {cardType} - {str(e)}")
            import traceback
            traceback.print_exc()
            raise e