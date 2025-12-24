"""
API routes for comply dashboard exports
"""
import asyncio
import json
from datetime import datetime
import os
import httpx
from fastapi import APIRouter, Query, HTTPException, Request
from fastapi.responses import Response
from typing import Optional

from services import APIService, PDFService, ExcelService
from utils.export_utils import get_default_header_config
from routes.route_utils import write_debug, parse_header_config, merge_header_config, convert_to_boolean, save_and_log_export

# Initialize services
api_service = APIService()
pdf_service = PDFService()
excel_service = ExcelService()

# Create router
router = APIRouter()

@router.get("/api/grc/comply/test-save")
async def test_comply_save(request: Request):
    """Test function to verify database saving works for comply exports"""
    try:
        from datetime import datetime
        import os
        
        # Create a test file content
        test_content = b"Test PDF content for comply export"
        test_filename = f"test_comply_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        
        # Get user from request headers
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        write_debug(f"[TEST] Testing comply export save - user: {created_by}")
        
        # Try to save using save_and_log_export
        export_info = await save_and_log_export(
            content=test_content,
            file_extension='pdf',
            dashboard='comply',
            card_type='Test Export',
            header_config=None,
            created_by=created_by,
            date_range=None
        )
        
        filename = export_info.get('filename', 'N/A')
        relative_path = export_info.get('relative_path', 'N/A')
        export_id = export_info.get('export_id', None)
        file_path = export_info.get('file_path', 'N/A')
        
        # Verify file exists
        file_exists = os.path.exists(file_path) if file_path != 'N/A' else False
        
        result = {
            "success": True,
            "message": "Test save completed",
            "file_saved": file_exists,
            "database_logged": export_id is not None,
            "details": {
                "filename": filename,
                "relative_path": relative_path,
                "export_id": export_id,
                "file_path": file_path,
                "file_exists": file_exists,
                "file_size": os.path.getsize(file_path) if file_exists else 0
            }
        }
        
        write_debug(f"[TEST] Test save result: {result}")
        
        return result
        
    except Exception as e:
        write_debug(f"[TEST] Test save failed: {str(e)}")
        import traceback
        write_debug(f"[TEST] Traceback: {traceback.format_exc()}")
        return {
            "success": False,
            "message": f"Test save failed: {str(e)}",
            "error": str(e),
            "traceback": traceback.format_exc()
        }

@router.get("/api/grc/comply/export-pdf")
async def export_comply_pdf(
    request: Request,
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: str = Query("False"),
    onlyChart: str = Query("False"),
    chartType: str = Query(None),
    renderType: str = Query(None),
    tableType: str = Query(None),
    onlyOverallTable: str = Query("False"),
    functionId: str = Query(None, description="Filter by specific function ID")
):
    """Export comply report in PDF format"""
    
    try:
        # Parse and merge header configuration
        header_config = parse_header_config(headerConfig)
        if renderType:
            try:
                header_config["chartType"] = renderType
            except Exception:
                header_config = {"chartType": renderType}
        elif chartType:
            try:
                header_config["chartType"] = chartType
            except Exception:
                header_config = {"chartType": chartType}
        header_config = merge_header_config("comply", header_config)

        # Convert to boolean
        onlyCard = convert_to_boolean(onlyCard)
        onlyChart = convert_to_boolean(onlyChart)
        onlyOverallTable = convert_to_boolean(onlyOverallTable)
        
        if onlyChart and not cardType and chartType:
            cardType = chartType
         
        if onlyOverallTable and tableType:
            cardType = tableType
        
        # Require cardType for exports
        if not cardType:
            raise HTTPException(status_code=400, detail="cardType or chartType is required for exports")

        # Fetch comply data from Node backend
        node_api_url = os.getenv("NODE_API_BASE", "http://localhost:3002")
        comply_url = f"{node_api_url}/api/grc/comply/all"
        
        params = {}
        if startDate:
            params['startDate'] = startDate
        if endDate:
            params['endDate'] = endDate
        if functionId:
            params['functionId'] = functionId

        # Get auth token and cookies from request
        auth_header = request.headers.get('Authorization', '')
        cookies = dict(request.cookies)
        
        # Forward all cookies to Node.js API for authentication
        async with httpx.AsyncClient(cookies=cookies, follow_redirects=True) as client:
            headers = {}
            if auth_header:
                headers['Authorization'] = auth_header
            # Forward CSRF token if present
            csrf_token = request.headers.get('X-CSRF-Token') or request.cookies.get('csrfToken')
            if csrf_token:
                headers['X-CSRF-Token'] = csrf_token
            
            response = await client.get(
                comply_url,
                params=params,
                headers=headers,
                timeout=60.0
            )
            response.raise_for_status()
            comply_data = response.json()

        # Map frontend chart/card IDs to backend report names
        # Backend returns reports with descriptive names like "Survey Completion Rate", "Bank Questions details", etc.
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
        
        # Try to get data from comply_data using the report name
        # comply_data structure: { "Report Name": [data], ... }
        export_data = comply_data.get(report_name, comply_data.get(cardType, []))
        
        # If still no data, try to find it by partial match
        if not export_data and cardType:
            for key in comply_data.keys():
                if isinstance(key, str) and cardType.lower() in key.lower():
                    export_data = comply_data[key]
                    report_name = key
                    break
        
        # Log for debugging
        write_debug(f"Comply PDF export - cardType={cardType}, report_name={report_name}, data found={len(export_data) if isinstance(export_data, list) else 'N/A'}")
        
        # Prepare data for PDF generation
        # Use cardType as the key so export service finds it immediately
        # Also include report_name in case service needs it for fallback
        comply_export_data = {cardType: export_data or []}
        # Add report_name as well for service fallback lookup
        if report_name != cardType:
            comply_export_data[report_name] = export_data or []

        # Generate PDF
        write_debug(f"Calling generate_comply_pdf with onlyCard={onlyCard}, onlyChart={onlyChart}, onlyOverallTable={onlyOverallTable}")
        pdf_content = await pdf_service.generate_comply_pdf(
            comply_export_data,
            startDate,
            endDate,
            header_config,
            cardType,
            onlyCard=onlyCard,
            onlyOverallTable=onlyOverallTable,
            onlyChart=onlyChart
        )

        if not pdf_content:
            raise HTTPException(status_code=500, detail="PDF generation failed")

        # Get user from request headers
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Store readable report_name in header_config for filename generation
        # Use cardType for database (like risks/controls dashboard)
        if report_name and report_name != cardType:
            if not header_config:
                header_config = {}
            header_config['export_title'] = report_name  # Store readable name for filename
        
        write_debug(f"[COMPLY PDF] Saving export - cardType={cardType}, report_name={report_name}")
        write_debug(f"[COMPLY PDF] PDF content size: {len(pdf_content) if pdf_content else 0} bytes")
        
        # Save file and log to database - use cardType for database (like risks/controls)
        try:
            export_info = await save_and_log_export(
                content=pdf_content,
                file_extension='pdf',
                dashboard='comply',
                card_type=cardType,  # Use cardType for database (matches risks/controls pattern)
                header_config=header_config,
                created_by=created_by,
                date_range={'startDate': startDate, 'endDate': endDate}
            )
            
            filename = export_info['filename']
            relative_path = export_info.get('relative_path', '')
            export_id = export_info.get('export_id', '')
            
            write_debug(f"[COMPLY PDF] Export saved successfully:")
            write_debug(f"  - Filename: {filename}")
            write_debug(f"  - Relative path: {relative_path}")
            write_debug(f"  - Export ID: {export_id}")
            write_debug(f"PDF generated successfully for {cardType}: {filename}")
        except Exception as save_err:
            write_debug(f"[COMPLY PDF] Error saving export: {str(save_err)}")
            import traceback
            write_debug(f"[COMPLY PDF] Save error traceback: {traceback.format_exc()}")
            raise

        # Return PDF as file download
        return Response(
            content=pdf_content,
            media_type='application/pdf',
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"',
                'X-Export-Src': export_info['relative_path'],
                'X-Export-Id': str(export_info.get('export_id', ''))
            }
        )

    except Exception as e:
        write_debug(f"Error during export_comply_pdf: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@router.get("/api/grc/comply/export-excel")
async def export_comply_excel(
    request: Request,
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: str = Query("False"),
    onlyChart: str = Query("False"),
    chartType: str = Query(None),
    renderType: str = Query(None),
    tableType: str = Query(None),
    onlyOverallTable: str = Query("False"),
    functionId: str = Query(None, description="Filter by specific function ID")
):
    """Export comply report in Excel format"""
    try:
        # Parse header config
        header_config = parse_header_config(headerConfig)
        if renderType:
            try:
                header_config["chartType"] = renderType
            except Exception:
                header_config = {"chartType": renderType}
        elif chartType:
            try:
                header_config["chartType"] = chartType
            except Exception:
                header_config = {"chartType": chartType}
        header_config = merge_header_config("comply", header_config)
        
        # Convert to boolean
        onlyCard = convert_to_boolean(onlyCard)
        onlyChart = convert_to_boolean(onlyChart)
        onlyOverallTable = convert_to_boolean(onlyOverallTable)
        
        if onlyChart and not cardType and chartType:
            cardType = chartType
         
        if onlyOverallTable and tableType:
            cardType = tableType
        
        # Require cardType for exports
        if not cardType:
            raise HTTPException(status_code=400, detail="cardType or chartType is required for exports")

        # Fetch comply data from Node backend
        node_api_url = os.getenv("NODE_API_BASE", "http://localhost:3002")
        comply_url = f"{node_api_url}/api/grc/comply/all"
        
        params = {}
        if startDate:
            params['startDate'] = startDate
        if endDate:
            params['endDate'] = endDate
        if functionId:
            params['functionId'] = functionId

        # Get auth token and cookies from request
        auth_header = request.headers.get('Authorization', '')
        cookies = dict(request.cookies)
        
        # Forward all cookies to Node.js API for authentication
        async with httpx.AsyncClient(cookies=cookies, follow_redirects=True) as client:
            headers = {}
            if auth_header:
                headers['Authorization'] = auth_header
            # Forward CSRF token if present
            csrf_token = request.headers.get('X-CSRF-Token') or request.cookies.get('csrfToken')
            if csrf_token:
                headers['X-CSRF-Token'] = csrf_token
            
            response = await client.get(
                comply_url,
                params=params,
                headers=headers,
                timeout=60.0
            )
            response.raise_for_status()
            comply_data = response.json()

        # Map frontend chart/card IDs to backend report names
        # Backend returns reports with descriptive names like "Survey Completion Rate", "Bank Questions details", etc.
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
        
        # Try to get data from comply_data using the report name
        # comply_data structure: { "Report Name": [data], ... }
        export_data = comply_data.get(report_name, comply_data.get(cardType, []))
        
        # If still no data, try to find it by partial match
        if not export_data and cardType:
            for key in comply_data.keys():
                if isinstance(key, str) and cardType.lower() in key.lower():
                    export_data = comply_data[key]
                    report_name = key
                    break
        
        # Log for debugging
        write_debug(f"Comply Excel export - cardType={cardType}, report_name={report_name}, data found={len(export_data) if isinstance(export_data, list) else 'N/A'}")
        
        # Prepare data for Excel generation
        # Use cardType as the key so export service finds it immediately
        # Also include report_name in case service needs it for fallback
        comply_export_data = {cardType: export_data or []}
        # Add report_name as well for service fallback lookup
        if report_name != cardType:
            comply_export_data[report_name] = export_data or []

        # Generate Excel
        write_debug(f"Calling generate_comply_excel with onlyCard={onlyCard}, onlyChart={onlyChart}, onlyOverallTable={onlyOverallTable}")
        excel_content = await excel_service.generate_comply_excel(
            comply_export_data,
            startDate,
            endDate,
            header_config,
            cardType,
            onlyCard=onlyCard,
            onlyOverallTable=onlyOverallTable,
            onlyChart=onlyChart
        )

        if not excel_content:
            raise HTTPException(status_code=500, detail="Excel generation failed")

        # Get user from request headers
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Store readable report_name in header_config for filename generation
        # Use cardType for database (like risks/controls dashboard)
        if report_name and report_name != cardType:
            if not header_config:
                header_config = {}
            header_config['export_title'] = report_name  # Store readable name for filename
        
        write_debug(f"[COMPLY EXCEL] Saving export - cardType={cardType}, report_name={report_name}")
        write_debug(f"[COMPLY EXCEL] Excel content size: {len(excel_content) if excel_content else 0} bytes")
        
        # Save file and log to database - use cardType for database (like risks/controls)
        try:
            export_info = await save_and_log_export(
                content=excel_content,
                file_extension='xlsx',
                dashboard='comply',
                card_type=cardType,  # Use cardType for database (matches risks/controls pattern)
                header_config=header_config,
                created_by=created_by,
                date_range={'startDate': startDate, 'endDate': endDate}
            )
            
            filename = export_info['filename']
            relative_path = export_info.get('relative_path', '')
            export_id = export_info.get('export_id', '')
            
            write_debug(f"[COMPLY EXCEL] Export saved successfully:")
            write_debug(f"  - Filename: {filename}")
            write_debug(f"  - Relative path: {relative_path}")
            write_debug(f"  - Export ID: {export_id}")
            write_debug(f"Excel generated successfully for {cardType}: {filename}")
        except Exception as save_err:
            write_debug(f"[COMPLY EXCEL] Error saving export: {str(save_err)}")
            import traceback
            write_debug(f"[COMPLY EXCEL] Save error traceback: {traceback.format_exc()}")
            raise

        # Return Excel as file download
        return Response(
            content=excel_content,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"',
                'X-Export-Src': export_info['relative_path'],
                'X-Export-Id': str(export_info.get('export_id', ''))
            }
        )

    except Exception as e:
        write_debug(f"Error during export_comply_excel: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

