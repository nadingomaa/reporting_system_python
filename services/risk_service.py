"""
Risk service for risk operations
"""
import pyodbc
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
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

class RiskService:

    """Service for risk operations"""
    
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
            try:
                write_debug(f"[RiskService] execute_query OK: rows={len(result)}")
            except Exception:
                pass
            return result
        except Exception as e:
            try:
                write_debug(f"[RiskService] execute_query ERROR: {e}")
            except Exception:
                pass
            return []
    
    def _execute_sync_query(self, query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """Execute synchronous database query"""
        try:
            try:
                write_debug(f"[RiskService] _execute_sync_query SQL: {query}")
            except Exception:
                pass
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
            try:
                write_debug(f"[RiskService] _execute_sync_query ERROR: {e}")
            except Exception:
                pass
            return []

    async def get_risks_by_category(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get risks grouped by category"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.created_at BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.created_at >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.created_at <= '{end_date}'"
        
        query = f"""
        SELECT 
            c.name as category_name,
            COUNT(*) as risk_count
        FROM {self.get_fully_qualified_table_name('Risks')} r
        INNER JOIN dbo.[RiskCategories] rc ON r.id = rc.risk_id
        INNER JOIN dbo.[Categories] c ON rc.category_id = c.id
        WHERE r.isDeleted = 0 
        {date_filter}
        GROUP BY c.name
        ORDER BY risk_count DESC
        """
        
        return await self.execute_query(query)
    
    async def get_risks_by_event_type(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get risks grouped by event type"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            ISNULL(et.name, 'Unknown') as name,
            COUNT(r.id) as value
        FROM {self.get_fully_qualified_table_name('Risks')} r
        LEFT JOIN dbo.[EventTypes] et ON r.event = et.id
        WHERE r.isDeleted = 0 
        {date_filter}
        GROUP BY et.name
        ORDER BY value DESC
        """
        
        return await self.execute_query(query)

    async def get_risks_list_by_inherent_level(self, level: str, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get list of risks filtered by inherent_value level (e.g., 'High', 'Medium', 'Low')."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
          r.id,
          r.name,
          r.description,
          r.inherent_value,
          r.residual_value,
          r.createdAt
        FROM {self.get_fully_qualified_table_name('Risks')} r
        WHERE r.isDeleted = 0 AND r.inherent_value = '{level}' {date_filter}
        ORDER BY r.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_all_risks_list(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get list of all risks (filtered by date if provided)."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
          r.id,
          r.name,
          r.description,
          r.inherent_value,
          r.residual_value,
          r.createdAt
        FROM {self.get_fully_qualified_table_name('Risks')} r
        WHERE r.isDeleted = 0 {date_filter}
        ORDER BY r.createdAt DESC
        """
        return await self.execute_query(query)

    # Dashboard-specific methods based on dashboard-config.service.ts
    async def get_total_risks(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get total risks count"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND createdAt <= '{end_date}'"
        
        query = f"""
        SELECT code ,name ,FORMAT(CONVERT(datetime, createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt
        FROM {self.get_fully_qualified_table_name('Risks')}
        WHERE isDeleted = 0 AND deletedAt IS NULL {date_filter} 
        ORDER BY createdAt DESC
        """
        
        return await self.execute_query(query)

    async def get_high_risks(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get high risks count"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND createdAt <= '{end_date}'"
        
        query = f"""
        SELECT code ,name, inherent_value
        FROM {self.get_fully_qualified_table_name('Risks')}
        WHERE isDeleted = 0 AND deletedAt IS NULL AND inherent_value = 'High' {date_filter}
        ORDER BY createdAt DESC
        """
        
        return await self.execute_query(query)

    async def get_medium_risks(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get medium risks count"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND createdAt <= '{end_date}'"
        
        query = f"""
        SELECT code ,name, inherent_value
        FROM {self.get_fully_qualified_table_name('Risks')}
        WHERE isDeleted = 0 AND deletedAt IS NULL AND inherent_value = 'Medium' {date_filter}
        ORDER BY createdAt DESC
        """
        
        return await self.execute_query(query)

    async def get_low_risks(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get low risks count"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND createdAt <= '{end_date}'"
        
        query = f"""
        SELECT code ,name, inherent_value
        FROM {self.get_fully_qualified_table_name('Risks')}
        WHERE isDeleted = 0 AND deletedAt IS NULL AND inherent_value = 'Low' {date_filter}
        ORDER BY createdAt DESC
        """

        
        
        return await self.execute_query(query)
    
    async def get_risks_reduced_count(self, start_date: str = None, end_date: str = None):
        query = """
        SELECT COUNT(*) as total 
        FROM Risks r
        INNER JOIN ResidualRisks rr ON r.id = rr.riskId
        WHERE r.isDeleted = 0 
        AND rr.isDeleted = 0
        AND rr.quarter = @current_quarter
        AND rr.year = YEAR(GETDATE())
        AND (
            CASE WHEN r.inherent_value = 'High' THEN 3 
                WHEN r.inherent_value = 'Medium' THEN 2 
                WHEN r.inherent_value = 'Low' THEN 1 ELSE 0 END
            - 
            CASE WHEN rr.residual_value = 'High' THEN 3 
                WHEN rr.residual_value = 'Medium' THEN 2 
                WHEN rr.residual_value = 'Low' THEN 1 ELSE 0 END
        ) > 0
        """
        
        params = {
            'current_quarter': self.get_current_quarter()  # Move logic to function
        }
        
        # Add date filters safely
        if start_date and end_date:
            query += " AND r.createdAt BETWEEN @start_date AND @end_date"
            params.update({'start_date': start_date, 'end_date': end_date})
        elif start_date:
            query += " AND r.createdAt >= @start_date"
            params['start_date'] = start_date
        elif end_date:
            query += " AND r.createdAt <= @end_date" 
            params['end_date'] = end_date
        
        return await self.execute_query(query, params)


    async def get_new_risks(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get new risks this month count"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND createdAt <= '{end_date}'"
        
        query = f"""
        SELECT code, name, FORMAT(CONVERT(datetime, createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt
        FROM {self.get_fully_qualified_table_name('Risks')}
        WHERE isDeleted = 0 AND deletedAt IS NULL AND DATEDIFF(month, createdAt, GETDATE()) = 0 {date_filter}
        ORDER BY createdAt DESC
        """

        write_debug(f"[RiskService] get_new_risks SQL: {query}")
        result = await self.execute_query(query)
        try:
            write_debug(f"[RiskService] get_new_risks result count: {len(result)}")
            if result:
                write_debug(f"[RiskService] get_new_risks first row keys: {list(result[0].keys()) if result else 'N/A'}")
        except Exception:
            pass
        return result

    async def get_risks_by_category(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get risks grouped by category"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            ISNULL(c.name, 'Uncategorized') as name,
            COUNT(r.id) as value
        FROM {self.get_fully_qualified_table_name('Risks')} r
        LEFT JOIN dbo.RiskCategories rc ON r.id = rc.risk_id AND rc.isDeleted = 0
        LEFT JOIN dbo.Categories c ON rc.category_id = c.id AND c.isDeleted = 0
        WHERE r.isDeleted = 0 {date_filter}
        GROUP BY c.name
        ORDER BY value DESC
        """
        
        return await self.execute_query(query)

    async def get_risks_by_event_type_chart(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get risks by event type for charts"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            ISNULL(et.name, 'Unknown') as name,
            COUNT(r.id) as value
        FROM {self.get_fully_qualified_table_name('Risks')} r
        LEFT JOIN dbo.[EventTypes] et ON r.event = et.id
        WHERE r.isDeleted = 0 {date_filter}
        GROUP BY et.name
        ORDER BY value DESC
        """
        
        return await self.execute_query(query)

    async def get_created_deleted_risks_per_quarter(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get created and deleted risks per quarter"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        # Compute current year for labeling and filtering
        current_year = datetime.now().year
        
        query = f"""
        WITH AllQuarters AS (
        SELECT 1 AS quarter_num, 'Q1 {current_year}' AS quarter_label
        UNION ALL SELECT 2, 'Q2 {current_year}'
        UNION ALL SELECT 3, 'Q3 {current_year}' 
        UNION ALL SELECT 4, 'Q4 {current_year}'
        ),
        QuarterData AS (
        SELECT 
            DATEPART(quarter, r.createdAt) AS quarter_num,
            'Q' + CAST(DATEPART(quarter, r.createdAt) AS VARCHAR(1)) + ' {current_year}' AS quarter_label,
            COUNT(CASE WHEN r.isDeleted = 0 THEN 1 END) AS created,
            COUNT(CASE WHEN r.isDeleted = 1 THEN 1 END) AS deleted
        FROM {self.get_fully_qualified_table_name('Risks')} r
        WHERE YEAR(r.createdAt) = {current_year} {date_filter}
        GROUP BY DATEPART(quarter, r.createdAt), 
                'Q' + CAST(DATEPART(quarter, r.createdAt) AS VARCHAR(1)) + ' {current_year}'
        )
        SELECT 
        q.quarter_label AS name,
        ISNULL(qd.created, 0) AS created,
        ISNULL(qd.deleted, 0) AS deleted
        FROM AllQuarters q
        LEFT JOIN QuarterData qd ON q.quarter_num = qd.quarter_num
        ORDER BY q.quarter_num ASC
        """
        
        write_debug(query)
        return await self.execute_query(query)

    async def get_quarterly_risk_creation_trends(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get quarterly risk creation trends"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          creation_quarter AS creation_quarter, 
          SUM(risk_count) AS [SUM(risk_count)] 
        FROM ( 
          SELECT 
            CONCAT(YEAR(r.createdAt), '-Q', DATEPART(QUARTER, r.createdAt)) AS creation_quarter, 
            COUNT(r.id) AS risk_count 
          FROM {self.get_fully_qualified_table_name('Risks')} r 
          WHERE r.isDeleted = 0 {date_filter}
          GROUP BY YEAR(r.createdAt), DATEPART(QUARTER, r.createdAt) 
        ) AS virtual_table 
        GROUP BY creation_quarter 
        ORDER BY creation_quarter
        """
        
        return await self.execute_query(query)

    async def get_risk_approval_status_distribution(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get risk approval status distribution"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          CASE 
            WHEN rr.preparerResidualStatus = 'sent' AND rr.acceptanceResidualStatus = 'approved' THEN 'Approved'
            ELSE 'Not Approved'
          END AS approve,
          COUNT(*) AS count
        FROM {self.get_fully_qualified_table_name('Risks')}r
        INNER JOIN dbo.[ResidualRisks] rr ON r.id = rr.riskId
        WHERE r.isDeleted = 0 {date_filter}
        GROUP BY 
          CASE 
            WHEN rr.preparerResidualStatus = 'sent' AND rr.acceptanceResidualStatus = 'approved' THEN 'Approved'
            ELSE 'Not Approved'
          END
        ORDER BY approve ASC
        """
        
        return await self.execute_query(query)

    async def get_risk_distribution_by_financial_impact(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get risk distribution by financial impact level"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          CASE 
            WHEN r.inherent_financial_value <= 2 THEN 'Low' 
            WHEN r.inherent_financial_value = 3 THEN 'Medium' 
            WHEN r.inherent_financial_value >= 4 THEN 'High' 
            ELSE 'Unknown' 
          END AS [Financial Status],
          COUNT(*) AS count
        FROM {self.get_fully_qualified_table_name('Risks')}r
        WHERE r.isDeleted = 0 {date_filter}
        GROUP BY 
          CASE 
            WHEN r.inherent_financial_value <= 2 THEN 'Low' 
            WHEN r.inherent_financial_value = 3 THEN 'Medium' 
            WHEN r.inherent_financial_value >= 4 THEN 'High' 
            ELSE 'Unknown' 
          END
        ORDER BY [Financial Status] ASC
        """
        
        return await self.execute_query(query)

    # Table data methods
    async def get_risks_per_department(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get total number of risks per department"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT
          f.name AS [Functions__name], 
          COUNT(*) AS [count] 
        FROM {self.get_fully_qualified_table_name('Risks')}r
        LEFT JOIN dbo.[RiskFunctions] rf ON r.id = rf.risk_id
        LEFT JOIN dbo.[Functions] f ON rf.function_id = f.id
        WHERE r.isDeleted = 0 {date_filter}
        GROUP BY f.name
        ORDER BY [count] DESC, f.name ASC
        """
        
        return await self.execute_query(query)

    async def get_risks_per_business_process(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get number of risks per business process"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          p.name AS process_name, 
          COUNT(rp.risk_id) AS risk_count 
        FROM dbo.[RiskProcesses] rp 
        JOIN dbo.[Processes] p ON rp.process_id = p.id 
        JOIN {self.get_fully_qualified_table_name('Risks')} r ON rp.risk_id = r.id
        WHERE r.isDeleted = 0 {date_filter}
        GROUP BY p.name 
        ORDER BY risk_count DESC, p.name ASC
        """
        
        return await self.execute_query(query)

    async def get_inherent_residual_risk_comparison(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get inherent risk & residual risk comparison"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          r.name AS [Risk Name], 
          d.name AS [Department Name], 
          r.inherent_value AS [Inherent Value], 
          rr.residual_value AS [Residual Value] 
        FROM {self.get_fully_qualified_table_name('Risks')}r
        JOIN dbo.[ResidualRisks] rr ON rr.riskId = r.id 
        LEFT JOIN dbo.[Departments] d ON r.departmentId = d.id 
        WHERE r.isDeleted = 0 AND rr.isDeleted = 0 {date_filter}
        ORDER BY r.createdAt DESC
        """
        
        return await self.execute_query(query)

    async def get_high_residual_risk_overview(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get high residual risk overview"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          risk_name AS [Risk Name], 
          residual_level AS [Residual Level], 
          inherent_value AS [Inherent Value], 
          inherent_frequency_label AS [Inherent Frequency], 
          inherent_financial_label AS [Inherent Financial],
          residual_frequency_label AS [Residual Frequency], 
          residual_financial_label AS [Residual Financial],
          quarter AS [Quarter],
          year AS [Year]
        FROM ( 
          SELECT 
            r.name AS risk_name, 
            rr.residual_value AS residual_level, 
            r.inherent_value AS inherent_value,
            r.inherent_frequency AS inherent_frequency,
            r.inherent_financial_value AS inherent_financial_value,
            rr.residual_frequency AS residual_frequency,
            rr.residual_financial_value AS residual_financial_value,
            rr.quarter AS quarter,
            rr.year AS year,
            -- Inherent Frequency Labels
            CASE 
              WHEN r.inherent_frequency = 1 THEN 'Once in Three Years'
              WHEN r.inherent_frequency = 2 THEN 'Annually'
              WHEN r.inherent_frequency = 3 THEN 'Half Yearly'
              WHEN r.inherent_frequency = 4 THEN 'Quarterly'
              WHEN r.inherent_frequency = 5 THEN 'Monthly'
              ELSE 'Unknown'
            END AS inherent_frequency_label,
            -- Inherent Financial Labels
            CASE 
              WHEN r.inherent_financial_value = 1 THEN '0 - 10,000'
              WHEN r.inherent_financial_value = 2 THEN '10,000 - 100,000'
              WHEN r.inherent_financial_value = 3 THEN '100,000 - 1,000,000'
              WHEN r.inherent_financial_value = 4 THEN '1,000,000 - 10,000,000'
              WHEN r.inherent_financial_value = 5 THEN '> 10,000,000'
              ELSE 'Unknown'
            END AS inherent_financial_label,
            -- Residual Frequency Labels
            CASE 
              WHEN rr.residual_frequency = 1 THEN 'Once in Three Years'
              WHEN rr.residual_frequency = 2 THEN 'Annually'
              WHEN rr.residual_frequency = 3 THEN 'Half Yearly'
              WHEN rr.residual_frequency = 4 THEN 'Quarterly'
              WHEN rr.residual_frequency = 5 THEN 'Monthly'
              ELSE 'Unknown'
            END AS residual_frequency_label,
            -- Residual Financial Labels
            CASE 
              WHEN rr.residual_financial_value = 1 THEN '0 - 10,000'
              WHEN rr.residual_financial_value = 2 THEN '10,000 - 100,000'
              WHEN rr.residual_financial_value = 3 THEN '100,000 - 1,000,000'
              WHEN rr.residual_financial_value = 4 THEN '1,000,000 - 10,000,000'
              WHEN rr.residual_financial_value = 5 THEN '> 10,000,000'
              ELSE 'Unknown'
            END AS residual_financial_label
          FROM dbo.[ResidualRisks] rr 
        JOIN {self.get_fully_qualified_table_name('Risks')} r ON rr.riskId = r.id 
          WHERE r.isDeleted = 0 AND rr.residual_value = 'High' {date_filter}
        ) AS virtual_table
        ORDER BY year DESC, quarter DESC, inherent_value DESC
        """
        
        return await self.execute_query(query)

    async def get_risks_and_controls_count(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get risks and their controls count"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          r.name AS risk_name, 
          COUNT(DISTINCT rc.control_id) AS control_count 
        FROM {self.get_fully_qualified_table_name('Risks')} r 
        LEFT JOIN dbo.[RiskControls] rc ON r.id = rc.risk_id 
        LEFT JOIN dbo.[Controls] c ON rc.control_id = c.id 
        WHERE r.isDeleted = 0 AND r.deletedAt IS NULL AND c.isDeleted = 0 AND c.deletedAt IS NULL {date_filter}
        GROUP BY r.name 
        ORDER BY control_count DESC
        """
        
        return await self.execute_query(query)

    async def get_controls_and_risk_count(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get controls and risk count"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          c.name AS [Controls__name], 
          COUNT(DISTINCT rc.risk_id) AS [count] 
        FROM dbo.[Controls] c
        LEFT JOIN dbo.[RiskControls] rc ON c.id = rc.control_id 
        LEFT JOIN {self.get_fully_qualified_table_name('Risks')} r ON rc.risk_id = r.id AND r.isDeleted = 0
        WHERE c.isDeleted = 0 {date_filter}
        GROUP BY c.name 
        ORDER BY [count] DESC, c.name ASC
        """
        
        return await self.execute_query(query)

    async def get_risks_details(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get all risks details - matches frontend allRisks table structure"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT
          r.name AS [RiskName], 
          r.description AS [RiskDesc], 
          ISNULL(et.name, 'Unknown') AS [RiskEventName], 
          r.inherent_value AS [InherentValue], 
          r.inherent_frequency AS [InherentFrequency], 
          r.inherent_financial_value AS [InherentFinancialValue]
        FROM {self.get_fully_qualified_table_name('Risks')} r 
        LEFT JOIN dbo.[EventTypes] et ON et.id = r.event 
        WHERE r.isDeleted = 0 {date_filter}
        ORDER BY r.createdAt DESC
        """
        
        write_debug(f"[RiskService] get_risks_details SQL: {query}")
        result = await self.execute_query(query)
        try:
            write_debug(f"[RiskService] get_risks_details result count: {len(result)}")
            if result:
                write_debug(f"[RiskService] get_risks_details first row keys: {list(result[0].keys()) if result else 'N/A'}")
        except Exception:
            pass
        return result
