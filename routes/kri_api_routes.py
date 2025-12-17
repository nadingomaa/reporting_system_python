"""
API routes for the reporting system
"""
import asyncio
import json
from datetime import datetime
import os
import httpx
from fastapi import APIRouter, Query, HTTPException, Request, UploadFile, File, Form
from fastapi import Request
from fastapi.responses import Response, FileResponse, StreamingResponse
from typing import Optional

from services import APIService, PDFService, ExcelService, ControlService, IncidentService, KRIService
from services.bank_check_service import BankCheckService
from services.enhanced_bank_check_service import EnhancedBankCheckService
# Dashboard activity service moved to Node.js backend (NestJS)
DashboardActivityService = None  # type: ignore
from utils.export_utils import get_default_header_config
from models import ExportRequest, ExportResponse
from routes.route_utils import write_debug, parse_header_config, merge_header_config, convert_to_boolean, save_and_log_export
from services.user_function_access_service import user_function_access_service, UserFunctionAccess

# Initialize services
api_service = APIService()
pdf_service = PDFService()
excel_service = ExcelService()

# Initialize KRI service
try:
    from services.kri_service import KriService
    kri_service = KriService()
except Exception as e:
    write_debug(f"Failed to initialize KRI service: {e}")
    import traceback
    traceback.print_exc()
    kri_service = None

incident_service = IncidentService() if IncidentService else None
dashboard_activity_service = DashboardActivityService() if DashboardActivityService else None
bank_check_service = BankCheckService()
enhanced_bank_check_service = EnhancedBankCheckService()

# db_service points to kri_service for KRI-related database calls
db_service = kri_service

# Create router
router = APIRouter()



@router.get("/api/grc/kris/export/pdf")
async def export_kris_pdf(
    request: Request,
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: str = Query("False"),
    onlyChart: str = Query("False"),
    chartType: str = Query(None),
    onlyOverallTable: str = Query("False"),
    tableType: str = Query(None),
    functionId: str = Query(None, description="Filter by specific function ID")
):
    """Export KRIs dashboard to PDF (service-backed like incidents)."""
    global kri_service
    try:
        # Get user function access for filtering
        user = getattr(request.state, 'user', None)
        user_id = user.get('id') if user else None
        group_name = user.get('groupName') if user else None
        
        access = user_function_access_service.get_user_function_access(user_id, group_name) if user_id else UserFunctionAccess(is_super_admin=True)
        function_filter = user_function_access_service.build_kri_function_filter('k', access, functionId)
        write_debug(f"[KRIS PDF] User {user_id}, group {group_name}, function_filter: {function_filter}")
        
        write_debug(f"[KRIS PDF] startDate={startDate} endDate={endDate}")
        write_debug(f"[KRIS PDF] cardType={cardType} onlyCard={onlyCard} onlyChart={onlyChart}")
        write_debug(f"[KRIS PDF] chartType={chartType} onlyOverallTable={onlyOverallTable} tableType={tableType}")

        # Parse and merge header configuration
        header_config = parse_header_config(headerConfig)
        # Allow chartType as separate query param
        renderType = request.query_params.get('renderType')
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
        header_config = merge_header_config("kris", header_config)
        
        # Normalize parameters
        if chartType and not cardType:
            cardType = chartType
        if onlyOverallTable and tableType:
            cardType = tableType

        # Normalize booleans
        try:
            only_card_bool = convert_to_boolean(onlyCard)
        except Exception:
            only_card_bool = str(onlyCard).lower() == 'true'
        
        try:
            only_chart_bool = convert_to_boolean(onlyChart)
        except Exception:
            only_chart_bool = str(onlyChart).lower() == 'true'
        
        try:
            only_overall_table_bool = convert_to_boolean(onlyOverallTable)
        except Exception:
            only_overall_table_bool = str(onlyOverallTable).lower() == 'true'

        # Require cardType
        if not cardType:
            raise HTTPException(status_code=400, detail="cardType or chartType is required for exports")

        write_debug(f"[KRIS PDF] normalized cardType={cardType}")

        # Fetch data via kri_service - reinitialize if needed
        if not kri_service:
            write_debug("[KRIS PDF] ERROR: kri_service is None - attempting to reinitialize")
            try:
                from services.kri_service import KriService
                kri_service = KriService()
                write_debug("[KRIS PDF] Successfully reinitialized kri_service")
            except Exception as init_err:
                write_debug(f"[KRIS PDF] Failed to reinitialize kri_service: {init_err}")
                import traceback
                traceback.print_exc()
                raise HTTPException(status_code=500, detail=f"KRI service not available: {str(init_err)}")
        
        if not kri_service:
            raise HTTPException(status_code=500, detail="KRI service not available after initialization attempt")

        data = None
        
        # Status counts (metrics) - return lists for card export - with function filtering
        if cardType == 'totalKris' or cardType == 'krisList':
            write_debug('Fetching totalKris/krisList')
            data = await kri_service.get_kris_list(startDate, endDate, function_filter)
        elif cardType == 'pendingPreparer':
            data = await kri_service.get_kris_by_status_detail('pendingPreparer', startDate, endDate, function_filter)
        elif cardType == 'pendingChecker':
            data = await kri_service.get_kris_by_status_detail('pendingChecker', startDate, endDate, function_filter)
        elif cardType == 'pendingReviewer':
            data = await kri_service.get_kris_by_status_detail('pendingReviewer', startDate, endDate, function_filter)
        elif cardType == 'pendingAcceptance':
            data = await kri_service.get_kris_by_status_detail('pendingAcceptance', startDate, endDate, function_filter)
        elif cardType == 'approved':
            data = await kri_service.get_kris_by_status_detail('Approved', startDate, endDate, function_filter)
        
        # Charts - with function filtering
        elif cardType == 'krisByStatus':
            data = await kri_service.get_kris_by_status(startDate, endDate, function_filter)
        elif cardType == 'krisByLevel':
            data = await kri_service.get_kris_by_level_detailed(startDate, endDate, function_filter)
        elif cardType == 'breachedKRIsByDepartment':
            data = await kri_service.get_breached_kris_by_department_detailed(startDate, endDate, function_filter)
        elif cardType == 'kriAssessmentCount':
            data = await kri_service.get_kri_assessment_count_detailed(startDate, endDate, function_filter)
        elif cardType == 'kriMonthlyAssessment':
            data = await kri_service.get_kri_monthly_assessment(startDate, endDate, function_filter)
        elif cardType == 'newlyCreatedKrisPerMonth':
            data = await kri_service.get_newly_created_kris_per_month(startDate, endDate, function_filter)
        elif cardType == 'deletedKrisPerMonth':
            data = await kri_service.get_deleted_kris_per_month(startDate, endDate, function_filter)
        elif cardType == 'kriOverdueStatusCounts':
            data = await kri_service.get_kri_overdue_status_counts(startDate, endDate, function_filter)
        elif cardType == 'kriCountsByMonthYear':
            data = await kri_service.get_kri_counts_by_month_year(startDate, endDate, function_filter)
        elif cardType == 'kriCountsByFrequency':
            data = await kri_service.get_kri_counts_by_frequency(startDate, endDate, function_filter)
        elif cardType == 'kriRisksByKriName':
            data = await kri_service.get_kri_risks_by_kri_name(startDate, endDate, function_filter)
        
        # Tables - with function filtering
        elif cardType == 'overallKris' or cardType == 'kriStatus':
            data = await kri_service.get_overall_kri_statuses(startDate, endDate, function_filter)
        elif cardType == 'kriHealth':
            data = await kri_service.get_kri_health(startDate, endDate, function_filter)
        elif cardType == 'activeKrisDetails':
            data = await kri_service.get_active_kris_details(startDate, endDate, function_filter)
        elif cardType == 'overdueKrisByDepartment':
            data = await kri_service.get_overdue_kris_by_department(startDate, endDate, function_filter)
        elif cardType == 'allKrisSubmittedByFunction':
            data = await kri_service.get_all_kris_submitted_by_function(startDate, endDate, function_filter)
        elif cardType == 'kriRiskRelationships':
            data = await kri_service.get_kri_risk_relationships(startDate, endDate, function_filter)
        elif cardType == 'kriWithoutLinkedRisks' or cardType == 'krisWithoutLinkedRisks':
            data = await kri_service.get_kris_without_linked_risks(startDate, endDate, function_filter)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown cardType: {cardType}")

        kris_data = {cardType: data}
        write_debug(f"kris_data: {kris_data}")
        try:
            data_len = len(data) if isinstance(data, list) else (len(data.keys()) if isinstance(data, dict) else 1)
            write_debug(f"[KRIS PDF] data_type={type(data)} data_len={data_len}")
        except Exception:
            write_debug(f"[KRIS PDF] data_type={type(data)} data_len=N/A")
        
        # Generate PDF
        try:
            pdf_content = await pdf_service.generate_kris_pdf(
                kris_data, startDate, endDate, header_config, cardType, only_card_bool, only_overall_table_bool, only_chart_bool
            )
        except Exception as gen_err:
            write_debug(f"[KRIS PDF] generate_kris_pdf error: {gen_err}")
            raise
        
        # Get user from request headers (if available)
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Save file and log to database
        export_info = await save_and_log_export(
            content=pdf_content,
            file_extension='pdf',
            dashboard='kris',
            card_type=cardType,
            header_config=header_config,
            created_by=created_by,
            date_range={'startDate': startDate, 'endDate': endDate}
        )
        
        filename = export_info['filename']
        
        return Response(
            content=pdf_content,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                'X-Export-Src': export_info['relative_path'],
                'X-Export-Id': str(export_info.get('export_id', ''))
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        write_debug(f"[KRIS PDF] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {str(e)}")

@router.get("/api/grc/kris/export/excel")
async def export_kris_excel(
    request: Request,
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: str = Query("False"),
    onlyChart: str = Query("False"),
    chartType: str = Query(None),
    onlyOverallTable: str = Query("False"),
    tableType: str = Query(None),
    functionId: str = Query(None, description="Filter by specific function ID")
):
    """Export KRIs dashboard to Excel (service-backed like incidents)."""
    global kri_service
    try:
        # Get user function access for filtering
        user = getattr(request.state, 'user', None)
        user_id = user.get('id') if user else None
        group_name = user.get('groupName') if user else None
        
        access = user_function_access_service.get_user_function_access(user_id, group_name) if user_id else UserFunctionAccess(is_super_admin=True)
        function_filter = user_function_access_service.build_kri_function_filter('k', access, functionId)
        write_debug(f"[KRIS EXCEL] User {user_id}, group {group_name}, function_filter: {function_filter}")
        
        write_debug(f"Exporting KRIs report in Excel format for {startDate} to {endDate}")
        write_debug(f"cardType: {cardType}")
        write_debug(f"onlyCard: {onlyCard}")
        write_debug(f"onlyChart: {onlyChart}")
        write_debug(f"chartType: {chartType}")

        # Parse and merge header configuration
        header_config = parse_header_config(headerConfig)
        # Allow chartType as separate query param
        renderType = request.query_params.get('renderType')
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
        header_config = merge_header_config("kris", header_config)

        # Normalize parameters
        if chartType and not cardType:
            cardType = chartType
        if onlyOverallTable and tableType:
            cardType = tableType

        # Normalize booleans
        try:
            only_card_bool = convert_to_boolean(onlyCard)
        except Exception:
            only_card_bool = str(onlyCard).lower() == 'true'
        
        try:
            only_chart_bool = convert_to_boolean(onlyChart)
        except Exception:
            only_chart_bool = str(onlyChart).lower() == 'true'
        
        try:
            only_overall_table_bool = convert_to_boolean(onlyOverallTable)
        except Exception:
            only_overall_table_bool = str(onlyOverallTable).lower() == 'true'

        if not cardType:
            raise HTTPException(status_code=400, detail="cardType or chartType is required for exports")

        if not kri_service:
            write_debug("[KRIS EXCEL] ERROR: kri_service is None - attempting to reinitialize")
            try:
                from services.kri_service import KriService
                kri_service = KriService()
                write_debug("[KRIS EXCEL] Successfully reinitialized kri_service")
            except Exception as init_err:
                write_debug(f"[KRIS EXCEL] Failed to reinitialize kri_service: {init_err}")
                import traceback
                traceback.print_exc()
                raise HTTPException(status_code=500, detail=f"KRI service not available: {str(init_err)}")
        
        if not kri_service:
            raise HTTPException(status_code=500, detail="KRI service not available after initialization attempt")

        data = None
        
        # Status counts (metrics) - return lists for card export - with function filtering
        if cardType == 'totalKris' or cardType == 'krisList':
            data = await kri_service.get_kris_list(startDate, endDate, function_filter)
        elif cardType == 'pendingPreparer':
            data = await kri_service.get_kris_by_status_detail('pendingPreparer', startDate, endDate, function_filter)
        elif cardType == 'pendingChecker':
            data = await kri_service.get_kris_by_status_detail('pendingChecker', startDate, endDate, function_filter)
        elif cardType == 'pendingReviewer':
            data = await kri_service.get_kris_by_status_detail('pendingReviewer', startDate, endDate, function_filter)
        elif cardType == 'pendingAcceptance':
            data = await kri_service.get_kris_by_status_detail('pendingAcceptance', startDate, endDate, function_filter)
        elif cardType == 'approved':
            data = await kri_service.get_kris_by_status_detail('Approved', startDate, endDate, function_filter)
        
        # Charts - with function filtering
        elif cardType == 'krisByStatus':
            data = await kri_service.get_kris_by_status(startDate, endDate, function_filter)
        elif cardType == 'krisByLevel':
            data = await kri_service.get_kris_by_level_detailed(startDate, endDate, function_filter)
        elif cardType == 'breachedKRIsByDepartment':
            data = await kri_service.get_breached_kris_by_department_detailed(startDate, endDate, function_filter)
        elif cardType == 'kriAssessmentCount':
            data = await kri_service.get_kri_assessment_count_detailed(startDate, endDate, function_filter)
        elif cardType == 'kriMonthlyAssessment':
            data = await kri_service.get_kri_monthly_assessment(startDate, endDate, function_filter)
        elif cardType == 'newlyCreatedKrisPerMonth':
            data = await kri_service.get_newly_created_kris_per_month(startDate, endDate, function_filter)
        elif cardType == 'deletedKrisPerMonth':
            data = await kri_service.get_deleted_kris_per_month(startDate, endDate, function_filter)
        elif cardType == 'kriOverdueStatusCounts':
            data = await kri_service.get_kri_overdue_status_counts(startDate, endDate, function_filter)
        elif cardType == 'kriCountsByMonthYear':
            data = await kri_service.get_kri_counts_by_month_year(startDate, endDate, function_filter)
        elif cardType == 'kriCountsByFrequency':
            data = await kri_service.get_kri_counts_by_frequency(startDate, endDate, function_filter)
        elif cardType == 'kriRisksByKriName':
            data = await kri_service.get_kri_risks_by_kri_name(startDate, endDate, function_filter)
        
        # Tables - with function filtering
        elif cardType == 'overallKris' or cardType == 'kriStatus':
            data = await kri_service.get_overall_kri_statuses(startDate, endDate, function_filter)
        elif cardType == 'kriHealth':
            data = await kri_service.get_kri_health(startDate, endDate, function_filter)
        elif cardType == 'activeKrisDetails':
            data = await kri_service.get_active_kris_details(startDate, endDate, function_filter)
        elif cardType == 'overdueKrisByDepartment':
            data = await kri_service.get_overdue_kris_by_department(startDate, endDate, function_filter)
        elif cardType == 'allKrisSubmittedByFunction':
            data = await kri_service.get_all_kris_submitted_by_function(startDate, endDate, function_filter)
        elif cardType == 'kriRiskRelationships':
            data = await kri_service.get_kri_risk_relationships(startDate, endDate, function_filter)
        elif cardType == 'kriWithoutLinkedRisks' or cardType == 'krisWithoutLinkedRisks':
            data = await kri_service.get_kris_without_linked_risks(startDate, endDate, function_filter)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown cardType: {cardType}")

        kris_data = {cardType: data}
        write_debug(f"KRIs Excel export - card_type={cardType}, data type={type(data)}, len={len(data) if isinstance(data, list) else 'N/A'}")
        
        # Generate Excel
        excel_content = await excel_service.generate_kris_excel(
            kris_data, startDate, endDate, header_config, cardType, only_card_bool, only_overall_table_bool, only_chart_bool
        )
        
        # Get user from request headers (if available)
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Save file and log to database
        export_info = await save_and_log_export(
            content=excel_content,
            file_extension='xlsx',
            dashboard='kris',
            card_type=cardType,
            header_config=header_config,
            created_by=created_by,
            date_range={'startDate': startDate, 'endDate': endDate}
        )
        
        filename = export_info['filename']
        
        return Response(
            content=excel_content,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                'X-Export-Src': export_info['relative_path'],
                'X-Export-Id': str(export_info.get('export_id', ''))
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        write_debug(f"[KRIS EXCEL] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate Excel: {str(e)}")


