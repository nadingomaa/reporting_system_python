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
from routes.route_utils import write_debug, parse_header_config, merge_header_config, convert_to_boolean, save_and_log_export
from services.user_function_access_service import user_function_access_service, UserFunctionAccess

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
# All control-related database calls will use control_service
db_service = control_service

# Create router
router = APIRouter()




@router.get("/api/grc/controls/export-pdf")
async def export_controls_pdf(
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
    source: str = Query(None, description="Set to 'db' to force database source"),
    functionId: str = Query(None, description="Filter by specific function ID")
):
    """Export controls report in PDF format"""
    
    try:
        # Get user function access for filtering
        user = getattr(request.state, 'user', None)
        user_id = user.get('id') if user else None
        group_name = user.get('groupName') if user else None
        
        access = user_function_access_service.get_user_function_access(user_id, group_name) if user_id else UserFunctionAccess(is_super_admin=True)
        function_filter = user_function_access_service.build_control_function_filter('c', access, functionId)
        write_debug(f"[CONTROLS PDF] User {user_id}, group {group_name}, function_filter: {function_filter}")
        # Parse and merge header configuration
        header_config = parse_header_config(headerConfig)
        # Allow chartType as separate query param
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
        header_config = merge_header_config("controls", header_config)

        # Convert to boolean
        onlyCard = convert_to_boolean(onlyCard)
        onlyChart = convert_to_boolean(onlyChart)
        onlyOverallTable = convert_to_boolean(onlyOverallTable)
        
        # If onlyChart is true, use chartType as cardType
        if onlyChart and not cardType and chartType:
            cardType = chartType
         
        if onlyOverallTable and tableType :

            cardType = tableType
        
        # Require cardType for exports
        if not cardType:
            raise HTTPException(status_code=400, detail="cardType or chartType is required for exports")

        # Initialize container for data
        controls_data = {}

        # Map tableType for overall tables
        if onlyOverallTable and tableType:
            cardType = tableType
      

        # Fetch data for the requested cardType with function filtering
        # Always use DB-backed condition block to ensure parity with Node queries
        card_data = None
        if not card_data:
            # SQL Fallbacks - pass function_filter to service methods
            if cardType == 'unmappedControls':
                card_data = await control_service.get_unmapped_controls(startDate, endDate, function_filter)
            elif cardType == 'pendingPreparer':
                card_data = await control_service.get_pending_controls('preparer', startDate, endDate, function_filter)
            elif cardType == 'pendingChecker':
                card_data = await control_service.get_pending_controls('checker', startDate, endDate, function_filter)
            elif cardType == 'pendingReviewer':
                card_data = await control_service.get_pending_controls('reviewer', startDate, endDate, function_filter)
            elif cardType == 'pendingAcceptance':
                card_data = await control_service.get_pending_controls('acceptance', startDate, endDate, function_filter)
            elif cardType == 'testsPendingPreparer':
                card_data = await control_service.get_tests_pending_controls('preparer', startDate, endDate, function_filter)
            elif cardType == 'testsPendingChecker':
                card_data = await control_service.get_tests_pending_controls('checker', startDate, endDate, function_filter)
            elif cardType == 'testsPendingReviewer':
                card_data = await control_service.get_tests_pending_controls('reviewer', startDate, endDate, function_filter)
            elif cardType == 'testsPendingAcceptance':
                card_data = await control_service.get_tests_pending_controls('acceptance', startDate, endDate, function_filter)
            elif cardType == 'unmappedIcofrControls':
                card_data = await control_service.get_unmapped_icofr_controls(startDate, endDate, function_filter)
            elif cardType == 'unmappedNonIcofrControls':
                card_data = await control_service.get_unmapped_non_icofr_controls(startDate, endDate, function_filter)
            
            elif cardType == 'department':
                card_data = await control_service.get_controls_by_department(startDate, endDate, function_filter)
            elif cardType == 'risk':
                card_data = await control_service.get_controls_by_risk_response(startDate, endDate, function_filter)
            elif cardType == 'quarterlyControlCreationTrend':
                card_data = await control_service.get_quarterly_control_creation_trend(startDate, endDate, function_filter)
            elif cardType == 'controlsByType':
                card_data = await control_service.get_controls_by_type(startDate, endDate, function_filter)
            elif cardType == 'antiFraudDistribution':
                card_data = await control_service.get_anti_fraud_distribution(startDate, endDate, function_filter)
            elif cardType == 'controlsPerLevel':
                card_data = await control_service.get_controls_per_level(startDate, endDate, function_filter)
            elif cardType == 'controlExecutionFrequency':
                card_data = await control_service.get_control_execution_frequency(startDate, endDate, function_filter)
            elif cardType == 'numberOfControlsByIcofrStatus':
                card_data = await control_service.get_number_of_controls_by_icofr_status(startDate, endDate, function_filter)
            elif cardType == 'numberOfFocusPointsPerPrinciple':
                card_data = await control_service.get_number_of_focus_points_per_principle(startDate, endDate, function_filter)
            elif cardType == 'numberOfFocusPointsPerComponent':
                card_data = await control_service.get_number_of_focus_points_per_component(startDate, endDate, function_filter)
            elif cardType == 'actionPlansStatus':
                card_data = await control_service.get_action_plans_status(startDate, endDate, function_filter)
            elif cardType == 'numberOfControlsPerComponent':
                card_data = await control_service.get_number_of_controls_per_component(startDate, endDate, function_filter)
           
            elif cardType == 'controlsNotMappedToPrinciples':
                card_data = await control_service.get_controls_not_mapped_to_principles(startDate, endDate, function_filter)
            elif cardType == 'controlsNotMappedToAssertions':
                card_data = await control_service.get_controls_not_mapped_to_assertions(startDate, endDate, function_filter)
            elif cardType == 'functionsWithFullyTestedControlTests':
                card_data = await control_service.get_functions_with_fully_tested_control_tests(startDate, endDate, function_filter)
            elif cardType == 'controlSubmissionStatusByQuarterFunction':
                card_data = await control_service.get_control_submission_status_by_quarter_function(startDate, endDate, function_filter)
            elif cardType == 'actionPlanForEffectiveness':
                card_data = await control_service.get_action_plan_for_effectiveness(startDate, endDate, function_filter)
            elif cardType == 'actionPlanForAdequacy':
                card_data = await control_service.get_action_plan_for_adequacy(startDate, endDate, function_filter)
            elif cardType == 'icofrControlCoverageByCoso':
                card_data = await control_service.get_icofr_control_coverage_by_coso(startDate, endDate, function_filter)
            elif cardType == 'controlCountByAssertionName':
                card_data = await control_service.get_control_count_by_assertion_name(startDate, endDate, function_filter)
            elif cardType == 'keyNonKeyControlsPerBusinessUnit':
                card_data = await control_service.get_key_non_key_controls_per_business_unit(startDate, endDate, function_filter)  
            elif cardType == 'keyNonKeyControlsPerProcess':
                card_data = await control_service.get_key_non_key_controls_per_process(startDate, endDate, function_filter)  
            elif cardType == 'keyNonKeyControlsPerDepartment':
                card_data = await control_service.get_key_non_key_controls_per_department(startDate, endDate, function_filter)  
            elif cardType == 'controlsByFunction':
                card_data = await control_service.get_controls_by_function(startDate, endDate, function_filter)
            elif cardType == 'controlsTestingApprovalCycle':
                card_data = await control_service.get_controls_testing_approval_cycle(startDate, endDate, function_filter)
            elif cardType == 'overallStatuses':
                card_data = await control_service.get_status_overview(startDate, endDate, function_filter)
           
            elif cardType == 'totalControls':
                card_data = await control_service.get_total_controls(startDate, endDate, function_filter)

                

            
        controls_data[cardType] = card_data

        

        # Generate PDF
        write_debug(f"Calling generate_controls_pdf with onlyCard={onlyCard}, onlyChart={onlyChart}, onlyOverallTable={onlyOverallTable}")
        pdf_content = await pdf_service.generate_controls_pdf(
            controls_data,
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

        # Get user from request headers (if available)
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Save file and log to database
        export_info = await save_and_log_export(
            content=pdf_content,
            file_extension='pdf',
            dashboard='controls',
            card_type=cardType,
            header_config=header_config,
            created_by=created_by,
            date_range={'startDate': startDate, 'endDate': endDate}
        )
        
        filename = export_info['filename']
        
        write_debug(f"PDF generated successfully for {cardType}: {filename}")

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
        write_debug(f"Error during export_controls_pdf: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")




        print(f"Error in get_tests_pending_acceptance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/grc/controls/export-excel")
async def export_controls_excel(
    request: Request,
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: str = Query("False"),
    onlyChart: str = Query("False"),
    chartType: str = Query(None),
    renderType: str = Query(None),
    onlyOverallTable: str = Query("False"),
    tableType: str = Query(None),
    functionId: str = Query(None, description="Filter by specific function ID")
):
    """Export controls report in Excel format"""
    try:
        # Get user function access for filtering
        user = getattr(request.state, 'user', None)
        user_id = user.get('id') if user else None
        group_name = user.get('groupName') if user else None
        
        access = user_function_access_service.get_user_function_access(user_id, group_name) if user_id else UserFunctionAccess(is_super_admin=True)
        function_filter = user_function_access_service.build_control_function_filter('c', access, functionId)
        write_debug(f"[CONTROLS EXCEL] User {user_id}, group {group_name}, function_filter: {function_filter}")
        # Parse header config
        header_config = parse_header_config(headerConfig)
        # Allow chartType/renderType as separate query params
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
        header_config = merge_header_config("controls", header_config)
        
        # Get table type from query params
        table_type = request.query_params.get('tableType', 'overallStatuses')
        if tableType:
            table_type = tableType
        
        # Convert string parameters to boolean
        onlyCard = convert_to_boolean(onlyCard)
        onlyChart = convert_to_boolean(onlyChart)
        onlyOverallTable = convert_to_boolean(onlyOverallTable)
        
        # If onlyChart is true, use chartType as cardType
        if onlyChart and not cardType and chartType:
            cardType = chartType
        
        if onlyOverallTable and tableType:
            cardType = tableType
        
        # Require cardType for exports
        if not cardType:
            raise HTTPException(status_code=400, detail="cardType or chartType is required for exports")

        # Initialize container for data

        write_debug(f"Fetching totalControls for {startDate} to {endDate}")


        controls_data = {}

        # Fetch data for the requested cardType (same as PDF export) with function filtering
        card_data = None
        if not card_data:
            # SQL Fallbacks - pass function_filter to service methods
            if cardType == 'unmappedControls':
                write_debug(f"Fetching unmappedControls for {startDate} to {endDate}")
                card_data = await control_service.get_unmapped_controls(startDate, endDate, function_filter)
            elif cardType == 'pendingPreparer':
                write_debug(f"Fetching pendingPreparer for {startDate} to {endDate}")
                card_data = await control_service.get_pending_controls('preparer', startDate, endDate, function_filter)
            elif cardType == 'pendingChecker':
                write_debug(f"Fetching pendingChecker for {startDate} to {endDate}")
                card_data = await control_service.get_pending_controls('checker', startDate, endDate, function_filter)
            elif cardType == 'pendingReviewer':
                card_data = await control_service.get_pending_controls('reviewer', startDate, endDate, function_filter)
            elif cardType == 'pendingAcceptance':
                card_data = await control_service.get_pending_controls('acceptance', startDate, endDate, function_filter)
            elif cardType == 'testsPendingPreparer':
                card_data = await control_service.get_tests_pending_controls('preparer', startDate, endDate, function_filter)
            elif cardType == 'testsPendingChecker':
                card_data = await control_service.get_tests_pending_controls('checker', startDate, endDate, function_filter)
            elif cardType == 'testsPendingReviewer':
                card_data = await control_service.get_tests_pending_controls('reviewer', startDate, endDate, function_filter)
            elif cardType == 'testsPendingAcceptance':
                card_data = await control_service.get_tests_pending_controls('acceptance', startDate, endDate, function_filter)
            elif cardType == 'unmappedIcofrControls':
                card_data = await control_service.get_unmapped_icofr_controls(startDate, endDate, function_filter)
            elif cardType == 'unmappedNonIcofrControls':
                card_data = await control_service.get_unmapped_non_icofr_controls(startDate, endDate, function_filter)
            
            elif cardType == 'department':
               card_data = await control_service.get_controls_by_department(startDate, endDate, function_filter)
            elif cardType == 'risk':
                card_data = await control_service.get_controls_by_risk_response(startDate, endDate, function_filter)
            elif cardType == 'quarterlyControlCreationTrend':
                card_data = await control_service.get_quarterly_control_creation_trend(startDate, endDate, function_filter)
            elif cardType == 'controlsByType':
                card_data = await control_service.get_controls_by_type(startDate, endDate, function_filter)
            elif cardType == 'antiFraudDistribution':
                card_data = await control_service.get_anti_fraud_distribution(startDate, endDate, function_filter)
            elif cardType == 'controlsPerLevel':
                card_data = await control_service.get_controls_per_level(startDate, endDate, function_filter)
            elif cardType == 'controlExecutionFrequency':
                card_data = await control_service.get_control_execution_frequency(startDate, endDate, function_filter)
            elif cardType == 'numberOfControlsByIcofrStatus':
                card_data = await control_service.get_number_of_controls_by_icofr_status(startDate, endDate, function_filter)
            elif cardType == 'numberOfFocusPointsPerPrinciple':
                card_data = await control_service.get_number_of_focus_points_per_principle(startDate, endDate, function_filter)
            elif cardType == 'numberOfFocusPointsPerComponent':
                card_data = await control_service.get_number_of_focus_points_per_component(startDate, endDate, function_filter)
            elif cardType == 'actionPlansStatus':
                card_data = await control_service.get_action_plans_status(startDate, endDate, function_filter)
            elif cardType == 'numberOfControlsPerComponent':
                card_data = await control_service.get_number_of_controls_per_component(startDate, endDate, function_filter)
           
            elif cardType == 'controlsNotMappedToPrinciples':
                card_data = await control_service.get_controls_not_mapped_to_principles(startDate, endDate, function_filter)
            elif cardType == 'controlsNotMappedToAssertions':
                card_data = await control_service.get_controls_not_mapped_to_assertions(startDate, endDate, function_filter)
            elif cardType == 'functionsWithFullyTestedControlTests':
                card_data = await control_service.get_functions_with_fully_tested_control_tests(startDate, endDate, function_filter)
            elif cardType == 'controlSubmissionStatusByQuarterFunction':
                card_data = await control_service.get_control_submission_status_by_quarter_function(startDate, endDate, function_filter)
            elif cardType == 'actionPlanForEffectiveness':
                card_data = await control_service.get_action_plan_for_effectiveness(startDate, endDate, function_filter)
            elif cardType == 'actionPlanForAdequacy':
                card_data = await control_service.get_action_plan_for_adequacy(startDate, endDate, function_filter)
            elif cardType == 'icofrControlCoverageByCoso':
                card_data = await control_service.get_icofr_control_coverage_by_coso(startDate, endDate, function_filter)
            elif cardType == 'controlCountByAssertionName':
                card_data = await control_service.get_control_count_by_assertion_name(startDate, endDate, function_filter)
            elif cardType == 'keyNonKeyControlsPerBusinessUnit':
                card_data = await control_service.get_key_non_key_controls_per_business_unit(startDate, endDate, function_filter)  
            elif cardType == 'keyNonKeyControlsPerProcess':
                card_data = await control_service.get_key_non_key_controls_per_process(startDate, endDate, function_filter)  
            elif cardType == 'keyNonKeyControlsPerDepartment':
                card_data = await control_service.get_key_non_key_controls_per_department(startDate, endDate, function_filter)  
            elif cardType == 'controlsByFunction':
                card_data = await control_service.get_controls_by_function(startDate, endDate, function_filter)
            elif cardType == 'controlsTestingApprovalCycle':
                card_data = await control_service.get_controls_testing_approval_cycle(startDate, endDate, function_filter)
            elif cardType == 'overallStatuses':
                card_data = await control_service.get_status_overview(startDate, endDate, function_filter)
           
            elif cardType == 'totalControls':
                card_data = await control_service.get_total_controls(startDate, endDate, function_filter)

            
            controls_data[cardType] = card_data
        
        # Data fetching complete via service methods (no inline SQL)
        
        # Generate Excel
        excel_content = await excel_service.generate_controls_excel(
            controls_data, startDate, endDate, header_config, cardType, onlyCard, onlyOverallTable, onlyChart
        )
        
        # Get user from request headers (if available)
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Determine card type for filename
        export_card_type = cardType
        if onlyOverallTable and table_type:
            export_card_type = table_type
        elif onlyChart and chartType:
            export_card_type = chartType
        
        # Save file and log to database
        export_info = await save_and_log_export(
            content=excel_content,
            file_extension='xlsx',
            dashboard='controls',
            card_type=export_card_type or cardType,
            header_config=header_config,
            created_by=created_by,
            date_range={'startDate': startDate, 'endDate': endDate}
        )
        
        filename = export_info['filename']
        
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
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

