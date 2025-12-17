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

from services import APIService, PDFService, ExcelService, IncidentService
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
incident_service = IncidentService() if IncidentService else None
dashboard_activity_service = DashboardActivityService() if DashboardActivityService else None
bank_check_service = BankCheckService()
enhanced_bank_check_service = EnhancedBankCheckService()

# db_service points to incident_service for incident-related database calls
db_service = incident_service 

# Create router
router = APIRouter()


# Incidents: PDF export


@router.get("/api/grc/incidents/export-pdf")
async def export_incidents_pdf(
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
    """Export incidents report in PDF format (service-backed like controls)."""
    try:
        # Get user function access for filtering
        user = getattr(request.state, 'user', None)
        write_debug(f"[INCIDENTS PDF] Raw user from request.state: {user}")
        user_id = user.get('id') if user else None
        group_name = user.get('groupName') if user else None
        
        # If no user info, treat as super admin (no filter) for now
        if not user_id:
            access = UserFunctionAccess(is_super_admin=True)
            function_filter = ""
            write_debug(f"[INCIDENTS PDF] No user found, treating as super admin (no filter)")
        else:
            access = user_function_access_service.get_user_function_access(user_id, group_name)
            function_filter = user_function_access_service.build_incident_function_filter('i', access, functionId)
        write_debug(f"[INCIDENTS PDF] User {user_id}, group {group_name}, is_super_admin={access.is_super_admin}, function_ids={access.function_ids}, function_filter: {function_filter}")
        
        write_debug(f"[INCIDENTS PDF] startDate={startDate} endDate={endDate}")
        write_debug(f"[INCIDENTS PDF] cardType={cardType} onlyCard={onlyCard} onlyChart={onlyChart}")
        write_debug(f"[INCIDENTS PDF] chartType={chartType} onlyOverallTable={onlyOverallTable} tableType={tableType}")

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
        header_config = merge_header_config("incidents", header_config)
        
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

        write_debug(f"[INCIDENTS PDF] normalized cardType={cardType}")

        # Fetch data via incident_service with function filtering
        if not incident_service:
            raise HTTPException(status_code=500, detail="Incident service not available")

        data = None
        # Metrics
        if cardType == 'totalIncidents':
            data = await incident_service.get_incidents_list(startDate, endDate, function_filter)
        elif cardType == 'pendingPreparer':
            write_debug(f"[INCIDENTS PDF] fetching pending preparer incidents for {startDate} to {endDate}")
            data = await incident_service.get_incidents_by_status('pendingPreparer', startDate, endDate, function_filter)
        elif cardType == 'pendingChecker':
            data = await incident_service.get_incidents_by_status('pendingChecker', startDate, endDate, function_filter)
        elif cardType == 'pendingReviewer':
            data = await incident_service.get_incidents_by_status('pendingReviewer', startDate, endDate, function_filter)
        elif cardType == 'pendingAcceptance':
            data = await incident_service.get_incidents_by_status('pendingAcceptance', startDate, endDate, function_filter)
            
      
        # Charts
        elif cardType == 'byCategory':
            data = await incident_service.get_incidents_by_category(startDate, endDate, function_filter)
        
        elif cardType == 'byStatus':
            data = await incident_service.get_incidents_by_status_distribution(startDate, endDate, function_filter)
        
        elif cardType == 'monthlyTrend':
            data = await incident_service.get_incidents_monthly_trend(startDate, endDate, function_filter)
       
        elif cardType == 'incidentsTimeSeries':
            data = await incident_service.get_incidents_time_series(startDate, endDate, function_filter)
        
        elif cardType == 'topFinancialImpacts':
            data = await incident_service.get_incidents_top_financial_impacts(startDate, endDate, function_filter)
       
       
        elif cardType == 'incidentsByEventType':
            data = await incident_service.get_incidents_by_event_type(startDate, endDate, function_filter)
       
       
        elif cardType == 'incidentsByFinancialImpact':
            data = await incident_service.get_incidents_by_financial_impact(startDate, endDate, function_filter)
        
        elif cardType == 'netLossAndRecovery':
            data = await incident_service.get_incidents_net_loss_recovery(startDate, endDate, function_filter)
       
        #tables
        elif cardType == 'overallStatuses':
            data = await incident_service.get_incidents_status_overview(startDate, endDate, function_filter)
        elif cardType == 'incidentsFinancialDetails':
            data = await incident_service.get_incidents_financial_details(startDate, endDate, function_filter)
        elif cardType == 'incidentsWithTimeframe':
            data = await incident_service.get_incidents_with_timeframe(startDate, endDate, function_filter)
        elif cardType == 'incidentsWithFinancialAndFunction':
            data = await incident_service.get_incidents_with_financial_and_function(startDate, endDate, function_filter)
        
        # Operational Loss Metrics - Cards
        elif cardType == 'atmTheftCount':
            data = await incident_service.get_atm_theft_incidents(startDate, endDate, function_filter)
        elif cardType == 'avgRecognitionTime':
            data = await incident_service.get_incidents_with_recognition_time(startDate, endDate, function_filter)
        elif cardType == 'internalFraudCount':
            data = await incident_service.get_internal_fraud_incidents(startDate, endDate, function_filter)
        elif cardType == 'externalFraudCount':
            data = await incident_service.get_external_fraud_incidents(startDate, endDate, function_filter)
        elif cardType == 'physicalAssetDamageCount':
            data = await incident_service.get_physical_asset_damage_incidents(startDate, endDate, function_filter)
        elif cardType == 'peopleErrorCount':
            data = await incident_service.get_people_error_incidents(startDate, endDate, function_filter)
        elif cardType == 'internalFraudLoss':
            data = await incident_service.get_internal_fraud_incidents(startDate, endDate, function_filter)
        elif cardType == 'externalFraudLoss':
            data = await incident_service.get_external_fraud_incidents(startDate, endDate, function_filter)
        elif cardType == 'physicalAssetLoss':
            data = await incident_service.get_physical_asset_damage_incidents(startDate, endDate, function_filter)
        elif cardType == 'peopleErrorLoss':
            data = await incident_service.get_people_error_incidents(startDate, endDate, function_filter)
        
        # Operational Loss Metrics - Charts
        elif cardType == 'operationalLossValue':
            data = await incident_service.get_operational_loss_value_monthly(startDate, endDate, function_filter)
        elif cardType == 'monthlyTrendByType':
            data = await incident_service.get_monthly_trend_by_incident_type(startDate, endDate, function_filter)
        
        # Operational Loss Metrics - Tables
        elif cardType == 'lossByRiskCategory':
            data = await incident_service.get_loss_by_risk_category(startDate, endDate, function_filter)
        elif cardType == 'comprehensiveOperationalLoss':
            data = await incident_service.get_comprehensive_operational_loss(startDate, endDate, function_filter)
          
        """
        elif cardType == 'createdDeletedIncidentsPerQuarter':
            data = await incident_service.get_created_deleted_incidents_per_quarter(startDate, endDate)
        elif cardType == 'quarterlyIncidentCreationTrends':
            data = await incident_service.get_quarterly_incident_creation_trends(startDate, endDate)
        elif cardType == 'incidentApprovalStatusDistribution':
            data = await incident_service.get_incident_approval_status_distribution(startDate, endDate)
        
        # Tables
        elif cardType == 'incidentsPerDepartment':
            data = await incident_service.get_incidents_per_department(startDate, endDate)
        elif cardType == 'incidentsPerBusinessProcess':
            data = await incident_service.get_incidents_per_business_process(startDate, endDate)
        elif cardType == 'inherentResidualIncidentComparison':
            data = await incident_service.get_inherent_residual_incident_comparison(startDate, endDate)
        elif cardType == 'highResidualIncidentOverview':
            data = await incident_service.get_high_residual_incident_overview(startDate, endDate)
        elif cardType == 'incidentsAndControlsCount':
            data = await incident_service.get_incidents_and_controls_count(startDate, endDate)
        elif cardType == 'controlsAndIncidentCount':
            data = await incident_service.get_controls_and_incident_count(startDate, endDate)
        elif cardType == 'allIncidents':
            data = await incident_service.get_incidents_details(startDate, endDate)
        """

        incidents_data = {cardType: data}
        write_debug(f"incidents_data: {incidents_data}")
        try:
            data_len = len(data) if isinstance(data, list) else (len(data.keys()) if isinstance(data, dict) else 1)
            write_debug(f"[INCIDENTS PDF] data_type={type(data).__name__} data_len={data_len}")
        except Exception:
            pass
        
        # Generate PDF
        try:
            pdf_content = await pdf_service.generate_incidents_pdf(  # ← FIXED: Proper indentation
                incidents_data, startDate, endDate, header_config, cardType, only_card_bool, only_overall_table_bool, only_chart_bool
        )
        except Exception as gen_err:
            write_debug(f"[INCIDENTS PDF] generate_incidents_pdf error: {gen_err}")
            raise
        
        # Get user from request headers (if available)
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Save file and log to database
        export_info = await save_and_log_export(
            content=pdf_content,
            file_extension='pdf',
            dashboard='incidents',
            card_type=cardType,
            header_config=header_config,
            created_by=created_by,
            date_range={'startDate': startDate, 'endDate': endDate}
        )
        
        filename = export_info['filename']
        
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
        write_debug(f"[INCIDENTS PDF] Export failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

@router.get("/api/grc/incidents/export-excel")
async def export_incidents_excel(
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
    """Export incidents report in Excel format (service-backed like controls)."""
   
    try:
        # Get user function access for filtering
        user = getattr(request.state, 'user', None)
        write_debug(f"[INCIDENTS EXCEL] Raw user from request.state: {user}")
        user_id = user.get('id') if user else None
        group_name = user.get('groupName') if user else None
        
        # If no user info, treat as super admin (no filter) for now
        if not user_id:
            access = UserFunctionAccess(is_super_admin=True)
            function_filter = ""
            write_debug(f"[INCIDENTS EXCEL] No user found, treating as super admin (no filter)")
        else:
            access = user_function_access_service.get_user_function_access(user_id, group_name)
            function_filter = user_function_access_service.build_incident_function_filter('i', access, functionId)
        write_debug(f"[INCIDENTS EXCEL] User {user_id}, group {group_name}, is_super_admin={access.is_super_admin}, function_ids={access.function_ids}, function_filter: {function_filter}")
        
        write_debug(f"Exporting incidents report in Excel format for {startDate} to {endDate}")
        write_debug(f"cardType: {cardType}")
        write_debug(f"onlyCard: {onlyCard}")
        write_debug(f"onlyChart: {onlyChart}")
        write_debug(f"chartType: {chartType}")
        write_debug(f"onlyOverallTable: {onlyOverallTable}")
        write_debug(f"tableType: {tableType}")

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
        header_config = merge_header_config("incidents", header_config)

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

        
        if not incident_service:
            raise HTTPException(status_code=500, detail="Incident service not available")

        data = None
        
        # All service calls with function filtering
        if cardType == 'totalIncidents':
            data = await incident_service.get_incidents_list(startDate, endDate, function_filter)
        elif cardType == 'pendingPreparer':
            write_debug(f"[INCIDENTS EXCEL] fetching pending preparer incidents for {startDate} to {endDate}")
            data = await incident_service.get_incidents_by_status('pendingPreparer', startDate, endDate, function_filter)
        elif cardType == 'pendingChecker':
            data = await incident_service.get_incidents_by_status('pendingChecker', startDate, endDate, function_filter)
        elif cardType == 'pendingReviewer':
            data = await incident_service.get_incidents_by_status('pendingReviewer', startDate, endDate, function_filter)
        elif cardType == 'pendingAcceptance':
            data = await incident_service.get_incidents_by_status('pendingAcceptance', startDate, endDate, function_filter)
            
      
        # Charts
        elif cardType == 'byCategory':
            data = await incident_service.get_incidents_by_category(startDate, endDate, function_filter)
        elif cardType == 'byStatus':
            data = await incident_service.get_incidents_by_status_distribution(startDate, endDate, function_filter)
        elif cardType == 'monthlyTrend':
            data = await incident_service.get_incidents_monthly_trend(startDate, endDate, function_filter)
        elif cardType == 'incidentsTimeSeries':
            data = await incident_service.get_incidents_time_series(startDate, endDate, function_filter)
        elif cardType == 'topFinancialImpacts':
            data = await incident_service.get_incidents_top_financial_impacts(startDate, endDate, function_filter)
        
        elif cardType == 'incidentsByEventType':
            data = await incident_service.get_incidents_by_event_type(startDate, endDate, function_filter)
        elif cardType == 'incidentsByFinancialImpact':
            data = await incident_service.get_incidents_by_financial_impact(startDate, endDate, function_filter)
        elif cardType == 'netLossAndRecovery':
            data = await incident_service.get_incidents_net_loss_recovery(startDate, endDate, function_filter)
        # Pending buckets and totals/list
      
        #tables
        elif cardType == 'overallStatuses':
            data = await incident_service.get_incidents_status_overview(startDate, endDate, function_filter)
        elif cardType == 'incidentsFinancialDetails':
            data = await incident_service.get_incidents_financial_details(startDate, endDate, function_filter)
        elif cardType == 'incidentsWithTimeframe':
            data = await incident_service.get_incidents_with_timeframe(startDate, endDate, function_filter)
        elif cardType == 'incidentsWithFinancialAndFunction':
            data = await incident_service.get_incidents_with_financial_and_function(startDate, endDate, function_filter)
        
        # Operational Loss Metrics - Cards
        elif cardType == 'atmTheftCount':
            data = await incident_service.get_atm_theft_incidents(startDate, endDate, function_filter)
        elif cardType == 'avgRecognitionTime':
            data = await incident_service.get_incidents_with_recognition_time(startDate, endDate, function_filter)
        elif cardType == 'internalFraudCount':
            data = await incident_service.get_internal_fraud_incidents(startDate, endDate, function_filter)
        elif cardType == 'externalFraudCount':
            data = await incident_service.get_external_fraud_incidents(startDate, endDate, function_filter)
        elif cardType == 'physicalAssetDamageCount':
            data = await incident_service.get_physical_asset_damage_incidents(startDate, endDate, function_filter)
        elif cardType == 'peopleErrorCount':
            data = await incident_service.get_people_error_incidents(startDate, endDate, function_filter)
        elif cardType == 'internalFraudLoss':
            data = await incident_service.get_internal_fraud_incidents(startDate, endDate, function_filter)
        elif cardType == 'externalFraudLoss':
            data = await incident_service.get_external_fraud_incidents(startDate, endDate, function_filter)
        elif cardType == 'physicalAssetLoss':
            data = await incident_service.get_physical_asset_damage_incidents(startDate, endDate, function_filter)
        elif cardType == 'peopleErrorLoss':
            data = await incident_service.get_people_error_incidents(startDate, endDate, function_filter)
        
        # Operational Loss Metrics - Charts
        elif cardType == 'operationalLossValue':
            data = await incident_service.get_operational_loss_value_monthly(startDate, endDate, function_filter)
        elif cardType == 'monthlyTrendByType':
            data = await incident_service.get_monthly_trend_by_incident_type(startDate, endDate, function_filter)
        
        # Operational Loss Metrics - Tables
        elif cardType == 'lossByRiskCategory':
            data = await incident_service.get_loss_by_risk_category(startDate, endDate, function_filter)
        elif cardType == 'comprehensiveOperationalLoss':
            data = await incident_service.get_comprehensive_operational_loss(startDate, endDate, function_filter)

       
        elif cardType == 'incidentsReduced':
            data = await incident_service.get_incidents_reduced(startDate, endDate, function_filter)
        elif cardType == 'newIncidents':
            write_debug(f"[INCIDENTS EXCEL] fetching new incidents for {startDate} to {endDate}")
            data = await incident_service.get_new_incidents(startDate, endDate, function_filter)


        
        else:
            # Fallback to aggregated API data
            incidents_data = await api_service.get_incidents_data(startDate, endDate)
            data = incidents_data.get(cardType) or incidents_data.get('statusOverview') or []

        incidents_data_wrapped = {cardType or 'overallStatuses': data}

        excel_bytes = await excel_service.generate_incidents_excel(
            incidents_data=incidents_data_wrapped,
            start_date=startDate,
            end_date=endDate,
            header_config=header_config,
            card_type=cardType or 'overallStatuses',
            only_card=only_card_bool,
            only_overall_table=only_overall_table_bool,
            only_chart=only_chart_bool
        )

        # Get user from request headers (if available)
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Save file and log to database
        export_info = await save_and_log_export(
            content=excel_bytes,
            file_extension='xlsx',
            dashboard='incidents',
            card_type=cardType or 'overallStatuses',
            header_config=header_config,
            created_by=created_by,
            date_range={'startDate': startDate, 'endDate': endDate}
        )
        
        filename = export_info['filename']
        
        return Response(
            content=excel_bytes,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"',
                'X-Export-Src': export_info['relative_path'],
                'X-Export-Id': str(export_info.get('export_id', ''))
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


