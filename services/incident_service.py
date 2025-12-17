"""
Incident service for incident operations
"""
import pyodbc
import asyncio
from typing import List, Dict, Any, Optional
from config import get_database_connection_string

def write_debug(msg):
    """Write debug message to file with timestamp"""
    from datetime import datetime
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    msg_with_time = f"[{timestamp}] {msg}"
    with open('debug_log.txt', 'a', encoding='utf-8') as f:
        f.write(f"{msg_with_time}\n")
        f.flush()
    import sys
    sys.stderr.write(f"{msg_with_time}\n")
    sys.stderr.flush()

class IncidentService:

    """Service for incident operations"""
    
    def __init__(self):
        self.connection_string = get_database_connection_string()

    def get_fully_qualified_table_name(self, table_name: str) -> str:
        """Get fully qualified table name using configuration"""
        from config import DATABASE_CONFIG
        database_name = DATABASE_CONFIG.get('database', 'NEWDCC-V4-UAT')
        return f"[{database_name}].dbo.[{table_name}]"
    
    async def execute_query(self, query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results"""
        try:
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._execute_sync_query, query, params)
            return result
        except Exception as e:
            return []
    
    def _execute_sync_query(self, query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """Execute synchronous database query"""
        try:
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Get column names
                columns = [column[0] for column in cursor.description]
                
                # Fetch all results
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                result = []
                for row in rows:
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[columns[i]] = value
                    result.append(row_dict)
                
                return result
        except Exception as e:
            return []

       # ===== Incidents fallbacks =====
  
    async def get_incidents_list(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents detailed list (basic fields)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt
        FROM Incidents i
        WHERE i.isDeleted = 0 AND i.deletedAt IS NULL {date_filter} {function_filter}
        ORDER BY i.createdAt DESC
        """
        write_debug(f"[INCIDENTS LIST] query: {query}")
        return await self.execute_query(query)

    async def get_incidents_status_overview(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents status overview list with computed status (matches Node.js statusOverview)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
          i.code,
          i.title,
          CASE 
            WHEN ISNULL(i.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
            WHEN ISNULL(i.preparerStatus, '') = 'sent' AND ISNULL(i.checkerStatus, '') <> 'approved' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
            WHEN ISNULL(i.checkerStatus, '') = 'approved' AND ISNULL(i.reviewerStatus, '') <> 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
            WHEN ISNULL(i.reviewerStatus, '') = 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
            WHEN ISNULL(i.acceptanceStatus, '') = 'approved' THEN 'Approved'
            ELSE 'Other'
          END as status,
          FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt
        FROM Incidents i
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
          {function_filter}
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_incidents_by_status(self, status: str, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents rows filtered by computed status label (not counts)."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        # Build query that computes the label and filters to requested status
        query = f"""
        WITH IncidentStatus AS (
            SELECT 
                i.code,
                i.title,
                CASE 
                    -- 1) Pending preparer: preparerStatus is anything other than 'sent'
                    WHEN ISNULL(i.preparerStatus, '') <> 'sent' THEN 'pendingPreparer'
                    -- 2) Pending checker: preparer sent AND checker not approved AND acceptance not approved
                    WHEN ISNULL(i.preparerStatus, '') = 'sent' AND ISNULL(i.checkerStatus, '') <> 'approved' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'pendingChecker'
                    -- 3) Pending reviewer: checker approved AND reviewer not approved AND acceptance not approved
                    WHEN ISNULL(i.checkerStatus, '') = 'approved' AND ISNULL(i.reviewerStatus, '') <> 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'pendingReviewer'
                    -- 4) Pending acceptance: reviewer approved AND acceptance not approved
                    WHEN ISNULL(i.reviewerStatus, '') = 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'pendingAcceptance'
                    -- 5) Fully approved
                    WHEN ISNULL(i.acceptanceStatus, '') = 'approved' THEN 'Approved'
                    ELSE 'Other'
                END AS status,
                FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt
            FROM Incidents i
            WHERE i.isDeleted = 0 AND i.deletedAt IS NULL {date_filter} {function_filter}
        )
        SELECT *
        FROM IncidentStatus
        WHERE status = '{status}'
        ORDER BY createdAt DESC;
        """

        write_debug(f"[INCIDENTS BY STATUS] query: {query}")
        return await self.execute_query(query)

   
    async def get_incidents_by_category(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
       
        write_debug(f"[INCIDENTS BY CATEGORY] fetching incidents by category for {start_date} to {end_date}")
        """Return incidents count by category (excludes NULL and deleted categories)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            ISNULL(c.name, 'Unknown') as category_name,
            COUNT(i.id) as count
        FROM Incidents i
        LEFT JOIN Categories c ON i.category_id = c.id
            AND c.isDeleted = 0
            AND c.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
        GROUP BY ISNULL(c.name, 'Unknown')
        ORDER BY COUNT(i.id) DESC
        """
        write_debug(f"[INCIDENTS BY CATEGORY] query: {query}")
        return await self.execute_query(query)

    async def get_incidents_by_status_distribution(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents count by status (distribution for charts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"
        
        query = f"""
        WITH IncidentStatus AS (
            SELECT 
                i.id,
                CASE 
                    WHEN ISNULL(i.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
                    WHEN ISNULL(i.preparerStatus, '') = 'sent' AND ISNULL(i.checkerStatus, '') <> 'approved' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
                    WHEN ISNULL(i.checkerStatus, '') = 'approved' AND ISNULL(i.reviewerStatus, '') <> 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
                    WHEN ISNULL(i.reviewerStatus, '') = 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
                    WHEN ISNULL(i.acceptanceStatus, '') = 'approved' THEN 'Approved'
                    ELSE 'Other'
                END AS status
            FROM Incidents i
            WHERE i.isDeleted = 0 AND i.deletedAt IS NULL {date_filter} {function_filter}
        ),
        StatusCounts AS (
            SELECT 
                status as status_name,
                COUNT(*) as count
            FROM IncidentStatus
            GROUP BY status
        ),
        AllStatuses AS (
            SELECT 'Pending Preparer' AS status_name
            UNION ALL SELECT 'Pending Checker'
            UNION ALL SELECT 'Pending Reviewer'
            UNION ALL SELECT 'Pending Acceptance'
            UNION ALL SELECT 'Approved'
            UNION ALL SELECT 'Other'
        )
        SELECT 
            a.status_name,
            ISNULL(s.count, 0) as count
        FROM AllStatuses a
        LEFT JOIN StatusCounts s ON a.status_name = s.status_name
        ORDER BY s.count DESC, a.status_name
        """
        return await self.execute_query(query)
 
    async def get_incidents_monthly_trend(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents monthly trend counts grouped by occurrence_date"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            FORMAT(i.createdAt, 'MMM yyyy') as month_year,
            COUNT(i.id) as incident_count
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND i.createdAt IS NOT NULL
        GROUP BY FORMAT(i.createdAt, 'MMM yyyy')
        ORDER BY MIN(i.createdAt)
        """
        return await self.execute_query(query)
    
    async def get_incidents_time_series(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents time series by month (matches grc-incidents.service.ts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND createdAt <= '{end_date}'"

        query = f"""
        WITH month_series AS (
          SELECT  
            DATEFROMPARTS(YEAR(MIN(createdAt)), MONTH(MIN(createdAt)), 1) AS start_month, 
            DATEFROMPARTS(YEAR(MAX(createdAt)), MONTH(MAX(createdAt)), 1) AS end_month 
          FROM Incidents 
          WHERE isDeleted = 0 AND deletedAt IS NULL {date_filter}
        ), 
        months AS ( 
          SELECT start_month AS month_date 
          FROM month_series 
          UNION ALL 
          SELECT DATEADD(MONTH, 1, month_date) 
          FROM months, month_series 
          WHERE DATEADD(MONTH, 1, month_date) <= (SELECT end_month FROM month_series) 
        ) 
        SELECT 
          m.month_date AS month, 
          COUNT(i.id) AS total_incidents 
        FROM months AS m 
        LEFT JOIN Incidents AS i 
          ON YEAR(i.createdAt) = YEAR(m.month_date) 
          AND MONTH(i.createdAt) = MONTH(m.month_date)
          AND i.isDeleted = 0
          AND i.deletedAt IS NULL
        GROUP BY  
          m.month_date 
        ORDER BY  
          m.month_date 
        OPTION (MAXRECURSION 0)
        """
        return await self.execute_query(query)

   
   
   
   
   
    async def get_incidents_top_financial_impacts(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return top financial impacts grouped by category with total net loss"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            ISNULL(fi.name, 'Unknown') as financial_impact_name,
            ISNULL(SUM(i.net_loss), 0) as net_loss
        FROM Incidents i
        LEFT JOIN FinancialImpacts fi ON i.financial_impact_id = fi.id
            AND fi.isDeleted = 0
            AND fi.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND i.net_loss IS NOT NULL
            AND i.net_loss > 0
        GROUP BY fi.name
        ORDER BY net_loss DESC
        """
        return await self.execute_query(query)
    
    async def get_incidents_net_loss_recovery(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return net loss and recovery data for incidents"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            i.title as incident_title,
            ISNULL(i.net_loss, 0) as net_loss,
            ISNULL(i.recovery_amount, 0) as recovery_amount
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND i.net_loss > 0
        ORDER BY i.net_loss DESC
        """
        return await self.execute_query(query)

   
    async def get_incidents_by_event_type(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents count by event type (excludes NULL and deleted event types)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
          ISNULL(ie.name, 'Unknown') AS event_type, 
          COUNT(i.id) AS incident_count 
        FROM Incidents i 
        LEFT JOIN IncidentEvents ie ON i.event_type_id = ie.id
          AND ie.isDeleted = 0
          AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
        GROUP BY ISNULL(ie.name, 'Unknown')
        ORDER BY COUNT(i.id) DESC
        """
        return await self.execute_query(query)

    async def get_incidents_by_financial_impact(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents count by financial impact (excludes NULL and deleted financial impacts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
          ISNULL(fi.name, 'Unknown') AS financial_impact_name, 
          COUNT(i.id) AS incident_count 
        FROM Incidents i
        LEFT JOIN FinancialImpacts fi ON i.financial_impact_id = fi.id
          AND fi.isDeleted = 0
          AND fi.deletedAt IS NULL
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
        GROUP BY ISNULL(fi.name, 'Unknown')
        ORDER BY COUNT(i.id) DESC
        """
        return await self.execute_query(query)

    
    async def get_new_incidents_by_month(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return new incidents count per month (matches grc-incidents.service.ts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
          CAST(
            DATEFROMPARTS(
              YEAR(i.createdAt), 
              MONTH(i.createdAt), 
              1 
            ) AS datetime2
          ) AS month, 
          COUNT(*) AS new_incidents 
        FROM Incidents i
        WHERE i.isDeleted = 0 {date_filter}
          AND i.deletedAt IS NULL
        GROUP BY 
          CAST(
            DATEFROMPARTS(
              YEAR(i.createdAt), 
              MONTH(i.createdAt), 
              1 
            ) AS datetime2
          )
        ORDER BY month ASC
        """
        return await self.execute_query(query)

    async def get_incidents_with_timeframe(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents with timeframe values (matches grc-incidents.service.ts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
          i.title AS incident_name, 
          i.timeFrame AS time_frame 
        FROM Incidents i
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
        ORDER BY i.timeFrame DESC
        """
        return await self.execute_query(query)

    async def get_incidents_financial_details(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents financial details with net loss, recovery, and gross amount (matches Node.js)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
          i.title AS incident_name, 
          i.rootCause AS rootCause, 
          f.name AS function_name, 
          i.net_loss AS netLoss, 
          i.total_loss AS totalLoss, 
          i.recovery_amount AS recoveryAmount, 
          (ISNULL(i.total_loss, 0) + ISNULL(i.recovery_amount, 0)) AS grossAmount, 
          CASE 
            WHEN ISNULL(i.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
            WHEN ISNULL(i.preparerStatus, '') = 'sent' AND ISNULL(i.checkerStatus, '') <> 'approved' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
            WHEN ISNULL(i.checkerStatus, '') = 'approved' AND ISNULL(i.reviewerStatus, '') <> 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
            WHEN ISNULL(i.reviewerStatus, '') = 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
            WHEN ISNULL(i.acceptanceStatus, '') = 'approved' THEN 'Approved'
            ELSE 'Other'
          END AS status 
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
        """
        return await self.execute_query(query)

    # ===== Operational Loss Metrics =====
    
    async def get_atm_theft_incidents(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return ATM theft incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND sc.name = N'ATM issue'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_internal_fraud_incidents(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return internal fraud incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND ie.name = N'Internal Fraud'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_external_fraud_incidents(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return external fraud incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND ie.name = N'External Fraud'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_physical_asset_damage_incidents(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return physical asset damage incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND ie.name = N'Damage to Physical Assets'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_people_error_incidents(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return people error incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND sc.name = N'Human Mistake'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_incidents_with_recognition_time(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents with recognition time calculation (last 12 months)"""
        date_filter = "AND i.occurrence_date >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND i.occurrence_date BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.occurrence_date >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.occurrence_date <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(i.occurrence_date, 'yyyy-MM-dd') as occurrence_date,
            FORMAT(i.reported_date, 'yyyy-MM-dd') as reported_date,
            DATEDIFF(DAY, i.occurrence_date, i.reported_date) AS recognition_days,
            CAST(DATEDIFF(DAY, i.occurrence_date, i.reported_date) AS FLOAT) / 30.44 AS recognition_months
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND i.occurrence_date IS NOT NULL
            AND i.reported_date IS NOT NULL
            AND i.reported_date >= i.occurrence_date
        ORDER BY recognition_months DESC
        """
        return await self.execute_query(query)

    async def get_operational_loss_value_monthly(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return operational loss value by month (last 12 months)"""
        date_filter = "AND i.occurrence_date >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND i.occurrence_date BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.occurrence_date >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.occurrence_date <= '{end_date}'"

        query = f"""
        SELECT 
            YEAR(i.occurrence_date) AS year,
            MONTH(i.occurrence_date) AS month,
            CAST(SUM(i.net_loss) AS DECIMAL(18,2)) AS totalLossValue,
            COUNT(*) AS incidentCount
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND i.net_loss IS NOT NULL
        GROUP BY YEAR(i.occurrence_date), MONTH(i.occurrence_date)
        ORDER BY year, month
        """
        return await self.execute_query(query)

    async def get_monthly_trend_by_incident_type(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return monthly trend analysis by incident type (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            FORMAT(COALESCE(i.occurrence_date, i.createdAt), 'yyyy-MM') AS Period,
            SUM(CASE WHEN ie.name = N'Internal Fraud' THEN 1 ELSE 0 END) AS InternalFrauds,
            SUM(CASE WHEN ie.name = N'External Fraud' THEN 1 ELSE 0 END) AS ExternalFrauds,
            SUM(CASE WHEN ie.name = N'Damage to Physical Assets' THEN 1 ELSE 0 END) AS PhysicalAssetDamages,
            SUM(CASE WHEN sc.name = N'Human Mistake' THEN 1 ELSE 0 END) AS HumanErrors,
            SUM(CASE WHEN sc.name = N'ATM issue' THEN 1 ELSE 0 END) AS ATMIssues,
            SUM(CASE WHEN sc.name IN (N'System Error', N'Prime System Issue', N'Transaction system error (TRX BUG)') THEN 1 ELSE 0 END) AS SystemErrors
        FROM Incidents i
        LEFT JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        LEFT JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
        GROUP BY FORMAT(COALESCE(i.occurrence_date, i.createdAt), 'yyyy-MM')
        ORDER BY Period
        """
        result = await self.execute_query(query)
        # Transform to match Node.js format (lowercase/camelCase)
        return [
            {
                'period': row.get('Period', ''),
                'internalFrauds': row.get('InternalFrauds', 0) or 0,
                'externalFrauds': row.get('ExternalFrauds', 0) or 0,
                'physicalAssetDamages': row.get('PhysicalAssetDamages', 0) or 0,
                'humanErrors': row.get('HumanErrors', 0) or 0,
                'atmIssues': row.get('ATMIssues', 0) or 0,
                'systemErrors': row.get('SystemErrors', 0) or 0
            }
            for row in result
        ]

    async def get_loss_by_risk_category(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return loss analysis by risk category (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            c.name AS riskCategory,
            COUNT(*) AS incidentCount,
            CAST(SUM(ISNULL(i.net_loss, 0)) AS DECIMAL(18,2)) AS totalLoss,
            CAST(AVG(NULLIF(i.net_loss, 0)) AS DECIMAL(18,2)) AS averageLoss
        FROM Incidents i
        INNER JOIN Categories c ON i.category_id = c.id
            AND c.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND i.net_loss IS NOT NULL
        GROUP BY c.name
        ORDER BY totalLoss DESC
        """
        return await self.execute_query(query)

    async def get_comprehensive_operational_loss(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return comprehensive operational loss dashboard metrics (last 12 months)"""
        date_filter = "COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            'Total Operational Loss Incidents' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL

        UNION ALL

        SELECT 
            'ATM Issues' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            AND sc.name = N'ATM issue'

        UNION ALL

        SELECT 
            'Internal Fraud' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            AND ie.name = N'Internal Fraud'

        UNION ALL

        SELECT 
            'External Fraud' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            AND ie.name = N'External Fraud'

        UNION ALL

        SELECT 
            'Human Mistakes' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            AND sc.name = N'Human Mistake'

        UNION ALL

        SELECT 
            'System Errors' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            AND sc.name IN (N'System Error', N'Prime System Issue', N'Transaction system error (TRX BUG)')
        """
        return await self.execute_query(query)

    async def get_incidents_with_financial_and_function(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents with financial impact and function details (matches grc-incidents.service.ts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
          i.title AS title, 
          ISNULL(fi.name, 'Unknown') AS financial_impact_name, 
          ISNULL(f.name, 'Unknown') AS function_name 
        FROM Incidents i
        LEFT JOIN FinancialImpacts fi ON i.financial_impact_id = fi.id
          AND fi.isDeleted = 0
          AND fi.deletedAt IS NULL
        LEFT JOIN Functions f ON i.function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
        """
        return await self.execute_query(query)

    async def get_incidents_reduced(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return reduced incidents count"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT COUNT(*) as count
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            AND i.acceptanceStatus = 'approved'
            {date_filter}
            {function_filter}
        """
        return await self.execute_query(query)

    async def get_new_incidents(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return new incidents list"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    # ===== Operational Loss Metrics (Second Set) =====
    
    async def get_atm_theft_incidents_v2(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return ATM theft incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND sc.name = N'ATM issue'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_internal_fraud_incidents(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return internal fraud incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND ie.name = N'Internal Fraud'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_external_fraud_incidents(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return external fraud incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND ie.name = N'External Fraud'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_physical_asset_damage_incidents(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return physical asset damage incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND ie.name = N'Damage to Physical Assets'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_people_error_incidents(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return people error incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND sc.name = N'Human Mistake'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_incidents_with_recognition_time(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return incidents with recognition time calculation (last 12 months)"""
        date_filter = "AND i.occurrence_date >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND i.occurrence_date BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.occurrence_date >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.occurrence_date <= '{end_date}'"

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(i.occurrence_date, 'yyyy-MM-dd') as occurrence_date,
            FORMAT(i.reported_date, 'yyyy-MM-dd') as reported_date,
            DATEDIFF(DAY, i.occurrence_date, i.reported_date) AS recognition_days,
            CAST(DATEDIFF(DAY, i.occurrence_date, i.reported_date) AS FLOAT) / 30.44 AS recognition_months
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND i.occurrence_date IS NOT NULL
            AND i.reported_date IS NOT NULL
            AND i.reported_date >= i.occurrence_date
        ORDER BY recognition_months DESC
        """
        return await self.execute_query(query)

    async def get_operational_loss_value_monthly(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return operational loss value by month (last 12 months)"""
        date_filter = "AND i.occurrence_date >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND i.occurrence_date BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.occurrence_date >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.occurrence_date <= '{end_date}'"

        query = f"""
        SELECT 
            YEAR(i.occurrence_date) AS year,
            MONTH(i.occurrence_date) AS month,
            CAST(SUM(i.net_loss) AS DECIMAL(18,2)) AS totalLossValue,
            COUNT(*) AS incidentCount
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND i.net_loss IS NOT NULL
        GROUP BY YEAR(i.occurrence_date), MONTH(i.occurrence_date)
        ORDER BY year, month
        """
        return await self.execute_query(query)

    async def get_monthly_trend_by_incident_type(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return monthly trend analysis by incident type (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            FORMAT(COALESCE(i.occurrence_date, i.createdAt), 'yyyy-MM') AS Period,
            SUM(CASE WHEN ie.name = N'Internal Fraud' THEN 1 ELSE 0 END) AS InternalFrauds,
            SUM(CASE WHEN ie.name = N'External Fraud' THEN 1 ELSE 0 END) AS ExternalFrauds,
            SUM(CASE WHEN ie.name = N'Damage to Physical Assets' THEN 1 ELSE 0 END) AS PhysicalAssetDamages,
            SUM(CASE WHEN sc.name = N'Human Mistake' THEN 1 ELSE 0 END) AS HumanErrors,
            SUM(CASE WHEN sc.name = N'ATM issue' THEN 1 ELSE 0 END) AS ATMIssues,
            SUM(CASE WHEN sc.name IN (N'System Error', N'Prime System Issue', N'Transaction system error (TRX BUG)') THEN 1 ELSE 0 END) AS SystemErrors
        FROM Incidents i
        LEFT JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        LEFT JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
        GROUP BY FORMAT(COALESCE(i.occurrence_date, i.createdAt), 'yyyy-MM')
        ORDER BY Period
        """
        result = await self.execute_query(query)
        # Transform to match Node.js format (lowercase/camelCase)
        return [
            {
                'period': row.get('Period', ''),
                'internalFrauds': row.get('InternalFrauds', 0) or 0,
                'externalFrauds': row.get('ExternalFrauds', 0) or 0,
                'physicalAssetDamages': row.get('PhysicalAssetDamages', 0) or 0,
                'humanErrors': row.get('HumanErrors', 0) or 0,
                'atmIssues': row.get('ATMIssues', 0) or 0,
                'systemErrors': row.get('SystemErrors', 0) or 0
            }
            for row in result
        ]

    async def get_loss_by_risk_category(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return loss analysis by risk category (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            c.name AS riskCategory,
            COUNT(*) AS incidentCount,
            CAST(SUM(ISNULL(i.net_loss, 0)) AS DECIMAL(18,2)) AS totalLoss,
            CAST(AVG(NULLIF(i.net_loss, 0)) AS DECIMAL(18,2)) AS averageLoss
        FROM Incidents i
        INNER JOIN Categories c ON i.category_id = c.id
            AND c.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            AND i.net_loss IS NOT NULL
        GROUP BY c.name
        ORDER BY totalLoss DESC
        """
        return await self.execute_query(query)

    async def get_comprehensive_operational_loss(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return comprehensive operational loss dashboard metrics (last 12 months)"""
        date_filter = "COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        query = f"""
        SELECT 
            'Total Operational Loss Incidents' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL

        UNION ALL

        SELECT 
            'ATM Issues' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            AND sc.name = N'ATM issue'

        UNION ALL

        SELECT 
            'Internal Fraud' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            AND ie.name = N'Internal Fraud'

        UNION ALL

        SELECT 
            'External Fraud' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            AND ie.name = N'External Fraud'

        UNION ALL

        SELECT 
            'Human Mistakes' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            AND sc.name = N'Human Mistake'

        UNION ALL

        SELECT 
            'System Errors' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            AND sc.name IN (N'System Error', N'Prime System Issue', N'Transaction system error (TRX BUG)')
        """
        return await self.execute_query(query)