"""
Control service for control operations
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

class ControlService:

    """Service for control operations"""
    
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
            write_debug(f"Error executing query: {e}")
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
                    for i in range(len(columns)):
                        try:
                            value = row[i]
                            # Handle unsupported SQL types by converting to string
                            if value is not None:
                                row_dict[columns[i]] = value
                            else:
                                row_dict[columns[i]] = None
                        except Exception as e:
                            # If we can't read the value (e.g., unsupported SQL type), set to None
                            write_debug(f"Could not read column '{columns[i]}': {e}")
                            row_dict[columns[i]] = None
                    result.append(row_dict)
                
                return result
        except Exception as e:
            write_debug(f"DB ERROR (sync): {type(e).__name__}: {e}")
            write_debug(f"Offending query: {query}")
            return []
    
    async def get_total_controls(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get total controls data"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            c.name, 
            c.code, 
            CASE WHEN c.createdAt IS NOT NULL THEN CONVERT(VARCHAR(20), CAST(c.createdAt AS DATETIME), 105) ELSE NULL END AS createdAt
        FROM {self.get_fully_qualified_table_name('Controls')} c
        WHERE c.isDeleted = 0 AND c.deletedAt IS NULL {date_filter}
        ORDER BY c.createdAt DESC
        """
        return await self.execute_query(query)

   
    async def get_pending_controls(self, role: str, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get pending controls for a given role: preparer/checker/reviewer/acceptance (using standardized sequential approval logic)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"

        # Use standardized staged workflow pattern matching Node.js base-dashboard.service.ts
        if role == 'preparer':
            where_clause = "(ISNULL(c.preparerStatus, '') <> 'sent')"
            status_field = 'preparerStatus'
        elif role == 'checker':
            where_clause = "(ISNULL(c.preparerStatus, '') = 'sent' AND ISNULL(c.checkerStatus, '') <> 'approved' AND ISNULL(c.acceptanceStatus, '') <> 'approved')"
            status_field = 'checkerStatus'
        elif role == 'reviewer':
            where_clause = "(ISNULL(c.checkerStatus, '') = 'approved' AND ISNULL(c.reviewerStatus, '') <> 'sent' AND ISNULL(c.acceptanceStatus, '') <> 'approved')"
            status_field = 'reviewerStatus'
        elif role == 'acceptance':
            where_clause = "(ISNULL(c.reviewerStatus, '') = 'sent' AND ISNULL(c.acceptanceStatus, '') <> 'approved')"
            status_field = 'acceptanceStatus'
        else:
            return []

        query = f"""
        SELECT 
            c.code as control_code,
            c.name as control_name,
            c.{status_field} as status
        FROM dbo.[Controls] c
        WHERE c.isDeleted = 0 AND c.deletedAt IS NULL {date_filter}
          AND {where_clause}
        ORDER BY c.createdAt DESC, c.name
        """
        write_debug(f"SQL Query: {query}")
        return await self.execute_query(query)
    
    async def get_unmapped_icofr_controls(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get unmapped ICOFR controls data for export"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT c.id, c.name as control_name, c.code as control_code, a.name as assertion_name, a.account_type as assertion_type,
          'Not Mapped' as coso_component,
          'Not Mapped' as coso_point
        FROM {self.get_fully_qualified_table_name('Controls')} c 
        JOIN {self.get_fully_qualified_table_name('Assertions')} a ON c.icof_id = a.id 
        WHERE c.isDeleted = 0 AND c.icof_id IS NOT NULL 
        AND NOT EXISTS (SELECT 1 FROM {self.get_fully_qualified_table_name('ControlCosos')} ccx WHERE ccx.control_id = c.id AND ccx.deletedAt IS NULL) 
        AND ((a.C = 1 OR a.E = 1 OR a.A = 1 OR a.V = 1 OR a.O = 1 OR a.P = 1) 
             AND a.account_type IN ('Balance Sheet', 'Income Statement')) 
        AND a.isDeleted = 0 {date_filter}
        ORDER BY c.createdAt DESC
        """
        return await self.execute_query(query)
    
    async def get_tests_pending_controls(self, status_type: str, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get controls with pending test status (using ControlDesignTests table like Node.js frontend)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        # Map status types to database columns
        status_column_map = {
            'preparer': 'preparerStatus',
            'checker': 'checkerStatus', 
            'reviewer': 'reviewerStatus',
            'acceptance': 'acceptanceStatus'
        }
        
        status_column = status_column_map.get(status_type, 'preparerStatus')
        
        # Use standardized staged workflow pattern matching Node.js base-dashboard.service.ts
        if status_type == 'preparer':
            where_clause = "(ISNULL(t.preparerStatus, '') <> 'sent')"
        elif status_type == 'checker':
            where_clause = "(ISNULL(t.preparerStatus, '') = 'sent' AND ISNULL(t.checkerStatus, '') <> 'approved' AND ISNULL(t.acceptanceStatus, '') <> 'approved')"
        elif status_type == 'reviewer':
            where_clause = "(ISNULL(t.checkerStatus, '') = 'approved' AND ISNULL(t.reviewerStatus, '') <> 'sent' AND ISNULL(t.acceptanceStatus, '') <> 'approved')"
        elif status_type == 'acceptance':
            where_clause = "(ISNULL(t.reviewerStatus, '') = 'sent' AND ISNULL(t.acceptanceStatus, '') <> 'approved')"
        else:
            return []
        
        query = f"""
        SELECT 
            t.id,
            c.code,
            c.name as control_name,
            t.{status_column} as status,
            f.name as function_name
        FROM {self.get_fully_qualified_table_name('ControlDesignTests')} t
        INNER JOIN {self.get_fully_qualified_table_name('Controls')} c ON c.id = t.control_id
        INNER JOIN {self.get_fully_qualified_table_name('Functions')} f ON t.function_id = f.id
        WHERE {where_clause}
          AND t.function_id IS NOT NULL 
          AND c.isDeleted = 0
          AND c.deletedAt IS NULL
          AND t.deletedAt IS NULL
        {date_filter}
        ORDER BY c.createdAt DESC, c.name
        """
        return await self.execute_query(query)

    async def get_unmapped_controls(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get unmapped controls based on Node.js config logic"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        # Mirror Node query: controls with no ControlCosos mapping
        query = f"""
        SELECT 
            c.code as control_code,
            c.name as control_name
        FROM {self.get_fully_qualified_table_name('Controls')} c 
        WHERE c.isDeleted = 0 {date_filter}
          AND NOT EXISTS (
            SELECT 1 FROM {self.get_fully_qualified_table_name('ControlCosos')} ccx 
            WHERE ccx.control_id = c.id AND ccx.deletedAt IS NULL
          )
        ORDER BY c.createdAt DESC, c.name
        """
        return await self.execute_query(query)

    async def get_unmapped_non_icofr_controls(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get unmapped Non-ICOFR controls data for export"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT c.id, c.name as control_name, c.code as control_code, a.name as assertion_name, a.account_type as assertion_type,
          'Not Mapped' as coso_component,
          'Not Mapped' as coso_point
        FROM {self.get_fully_qualified_table_name('Controls')} c 
        LEFT JOIN {self.get_fully_qualified_table_name('Assertions')} a ON c.icof_id = a.id 
        WHERE c.isDeleted = 0 
        AND NOT EXISTS (SELECT 1 FROM {self.get_fully_qualified_table_name('ControlCosos')} ccx WHERE ccx.control_id = c.id AND ccx.deletedAt IS NULL) 
        AND (c.icof_id IS NULL OR ((a.C IS NULL OR a.C = 0) AND (a.E IS NULL OR a.E = 0) AND (a.A IS NULL OR a.A = 0) 
             AND (a.V IS NULL OR a.V = 0) AND (a.O IS NULL OR a.O = 0) AND (a.P IS NULL OR a.P = 0) 
             OR a.account_type NOT IN ('Balance Sheet', 'Income Statement'))) 
        AND (a.isDeleted = 0 OR a.id IS NULL) {date_filter}
        ORDER BY c.createdAt DESC
        """
        return await self.execute_query(query)
   
    async def get_controls_by_department(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get controls grouped by function (many-to-many relationship via ControlFunctions table)"""
        write_debug(f"Fetching controls by function - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        controls_table = self.get_fully_qualified_table_name('Controls')
        functions_table = self.get_fully_qualified_table_name('Functions')
        control_functions_table = self.get_fully_qualified_table_name('ControlFunctions')
        
        # Use ControlFunctions junction table for many-to-many relationship
        query = f"""
        SELECT 
            f.name as function_name,
            COUNT(DISTINCT c.id) as control_count
        FROM {controls_table} c
        INNER JOIN {control_functions_table} cf ON c.id = cf.control_id
        INNER JOIN {functions_table} f ON cf.function_id = f.id
        WHERE c.isDeleted = 0 
        {date_filter}
        GROUP BY f.name
        ORDER BY control_count DESC
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result
    
    async def get_controls_by_risk_response(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get controls grouped by risk response"""
        write_debug(f"Fetching controls by risk response - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        controls_table = self.get_fully_qualified_table_name('Controls')
        
        query = f"""
        SELECT 
            c.risk_response as name,
            COUNT(c.id) as value
        FROM {controls_table} c
        WHERE c.isDeleted = 0 
        {date_filter}
        GROUP BY c.risk_response
        ORDER BY COUNT(c.id) DESC
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result
    
    async def get_quarterly_control_creation_trend(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get controls created by quarter"""
        write_debug(f"Fetching quarterly control creation trend - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        controls_table = self.get_fully_qualified_table_name('Controls')

        query = f"""
        SELECT 
            CONCAT('Q', DATEPART(QUARTER, c.createdAt), ' ', YEAR(c.createdAt)) AS name,
            COUNT(c.id) AS value
        FROM {controls_table} c
        WHERE c.isDeleted = 0 
        {date_filter}
        GROUP BY YEAR(c.createdAt), DATEPART(QUARTER, c.createdAt)
        ORDER BY YEAR(c.createdAt), DATEPART(QUARTER, c.createdAt)
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result
    
    async def get_controls_by_type(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get controls grouped by type"""
        write_debug(f"Fetching controls by type - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        controls_table = self.get_fully_qualified_table_name('Controls')

        query = f"""
        SELECT 
            CASE 
                WHEN c.type IS NULL OR c.type = '' THEN 'Not Specified'
                ELSE c.type
            END AS name,
            COUNT(c.id) AS value
        FROM {controls_table} c
        WHERE c.isDeleted = 0 
        {date_filter}
        GROUP BY c.type
        ORDER BY COUNT(c.id) DESC
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result
    
    async def get_anti_fraud_distribution(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get controls by anti-fraud distribution"""
        write_debug(f"Fetching anti-fraud distribution - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        controls_table = self.get_fully_qualified_table_name('Controls')

        query = f"""
        SELECT 
            CASE 
                WHEN c.AntiFraud = 1 THEN 'Anti-Fraud'
                WHEN c.AntiFraud = 0 THEN 'Non-Anti-Fraud'
                ELSE 'Unknown'
            END AS name,
            COUNT(c.id) AS value
        FROM {controls_table} c
        WHERE c.isDeleted = 0 
        {date_filter}
        GROUP BY c.AntiFraud
        ORDER BY COUNT(c.id) DESC
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result
    
    async def get_controls_per_level(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get controls per control level"""
        write_debug(f"Fetching controls per level - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        controls_table = self.get_fully_qualified_table_name('Controls')

        query = f"""
        SELECT 
            CASE 
                WHEN c.entityLevel IS NULL OR c.entityLevel = '' THEN 'Not Specified'
                ELSE c.entityLevel
            END AS name,
            COUNT(c.id) AS value
        FROM {controls_table} c
        WHERE c.isDeleted = 0 
        {date_filter}
        GROUP BY c.entityLevel
        ORDER BY COUNT(c.id) DESC
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result
    
    async def get_control_execution_frequency(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get controls by execution frequency"""
        write_debug(f"Fetching control execution frequency - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        controls_table = self.get_fully_qualified_table_name('Controls')
        
        query = f"""
        SELECT 
            CASE 
                WHEN c.frequency = 'Daily' THEN 'Daily'
                WHEN c.frequency = 'Event Base' THEN 'Event Base'
                WHEN c.frequency = 'Weekly' THEN 'Weekly'
                WHEN c.frequency = 'Monthly' THEN 'Monthly'
                WHEN c.frequency = 'Quarterly' THEN 'Quarterly'
                WHEN c.frequency = 'Semi Annually' THEN 'Semi Annually'
                WHEN c.frequency = 'Annually' THEN 'Annually'
                WHEN c.frequency IS NULL OR c.frequency = '' THEN 'Not Specified'
                ELSE c.frequency
            END AS name,
            COUNT(c.id) AS value
        FROM {controls_table} c
        WHERE c.isDeleted = 0 
        {date_filter}
        GROUP BY c.frequency
        ORDER BY COUNT(c.id) DESC
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result
    
    async def get_number_of_controls_by_icofr_status(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get number of controls by ICOFR status"""
        write_debug(f"Fetching controls by ICOFR status - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        controls_table = self.get_fully_qualified_table_name('Controls')
        assertions_table = self.get_fully_qualified_table_name('Assertions')

        query = f"""
        SELECT 
            CASE 
                WHEN a.id IS NULL THEN 'Non-ICOFR'
                WHEN (a.C = 1 OR a.E = 1 OR a.A = 1 OR a.V = 1 OR a.O = 1 OR a.P = 1)
                     AND (a.account_type IN ('Balance Sheet', 'Income Statement')) 
                  THEN 'ICOFR' 
                ELSE 'Non-ICOFR' 
            END AS name,
            COUNT(c.id) AS value
        FROM {controls_table} c
        LEFT JOIN {assertions_table} a ON c.icof_id = a.id AND a.isDeleted = 0
        WHERE c.isDeleted = 0 
        {date_filter}
        GROUP BY 
            CASE 
                WHEN a.id IS NULL THEN 'Non-ICOFR'
                WHEN (a.C = 1 OR a.E = 1 OR a.A = 1 OR a.V = 1 OR a.O = 1 OR a.P = 1)
                     AND (a.account_type IN ('Balance Sheet', 'Income Statement')) 
                  THEN 'ICOFR' 
                ELSE 'Non-ICOFR' 
            END
        ORDER BY COUNT(c.id) DESC
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result
    
    async def get_number_of_focus_points_per_principle(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get number of focus points per principle"""
        write_debug(f"Fetching focus points per principle - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND prin.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND prin.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND prin.createdAt <= '{end_date}'"
        
        principles_table = self.get_fully_qualified_table_name('CosoPrinciples')
        points_table = self.get_fully_qualified_table_name('CosoPoints')
        
        query = f"""
        SELECT 
            prin.name AS name,
            COUNT(point.id) AS value
        FROM {principles_table} prin
        LEFT JOIN {points_table} point ON prin.id = point.principle_id
        WHERE prin.deletedAt IS NULL 
        {date_filter}
        GROUP BY prin.name
        ORDER BY COUNT(point.id) DESC, prin.name
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result
    
    async def get_number_of_focus_points_per_component(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get number of focus points per component"""
        write_debug(f"Fetching focus points per component - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND comp.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND comp.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND comp.createdAt <= '{end_date}'"
        
        components_table = self.get_fully_qualified_table_name('CosoComponents')
        principles_table = self.get_fully_qualified_table_name('CosoPrinciples')
        points_table = self.get_fully_qualified_table_name('CosoPoints')
        
        query = f"""
        SELECT 
            comp.name AS name,
            COUNT(point.id) AS value
        FROM {components_table} comp
        JOIN {principles_table} prin ON prin.component_id = comp.id
        LEFT JOIN {points_table} point ON point.principle_id = prin.id
        WHERE comp.deletedAt IS NULL AND prin.deletedAt IS NULL 
        {date_filter}
        GROUP BY comp.name
        ORDER BY COUNT(point.id) DESC
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result
    
    async def get_action_plans_status(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get action plans status"""
        write_debug(f"Fetching action plans status - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND a.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND a.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND a.createdAt <= '{end_date}'"
        
        actionplans_table = self.get_fully_qualified_table_name('Actionplans')
        
        query = f"""
        SELECT 
            CASE 
                WHEN a.done = 0 AND a.implementation_date < GETDATE() THEN 'Overdue'
                ELSE 'Not Overdue'
            END AS name,
            COUNT(a.id) AS value
        FROM {actionplans_table} a
        WHERE a.deletedAt IS NULL 
        {date_filter}
        GROUP BY 
            CASE 
                WHEN a.done = 0 AND a.implementation_date < GETDATE() THEN 'Overdue'
                ELSE 'Not Overdue'
            END
        ORDER BY COUNT(a.id) DESC
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result
    
    async def get_number_of_controls_per_component(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Get number of controls per component"""
        write_debug(f"Fetching controls per component - start_date: {start_date}, end_date: {end_date}")
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        controls_table = self.get_fully_qualified_table_name('Controls')
        control_cosos_table = self.get_fully_qualified_table_name('ControlCosos')
        points_table = self.get_fully_qualified_table_name('CosoPoints')
        principles_table = self.get_fully_qualified_table_name('CosoPrinciples')
        components_table = self.get_fully_qualified_table_name('CosoComponents')
        
        query = f"""
        SELECT 
            cc.name AS name,
            COUNT(DISTINCT c.id) AS value
        FROM {controls_table} c
        JOIN {control_cosos_table} ccx ON c.id = ccx.control_id
        JOIN {points_table} cp ON ccx.coso_id = cp.id
        JOIN {principles_table} pr ON cp.principle_id = pr.id
        JOIN {components_table} cc ON pr.component_id = cc.id
        WHERE c.isDeleted = 0 
            AND ccx.deletedAt IS NULL 
            AND cp.deletedAt IS NULL 
            AND pr.deletedAt IS NULL 
            AND cc.deletedAt IS NULL 
        {date_filter}
        GROUP BY cc.name
        ORDER BY COUNT(DISTINCT c.id) DESC
        """
        
        write_debug(f"SQL Query: {query}")
        result = await self.execute_query(query)
        write_debug(f"Query result: {result} (count: {len(result)})")
        return result

    
    async def get_status_overview(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Status overview table matching Node config (controls with statuses)."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            c.code as code,
            c.name as name,
            c.preparerStatus,
            c.checkerStatus,
            c.reviewerStatus,
            c.acceptanceStatus
        FROM {self.get_fully_qualified_table_name('Controls')} c
        WHERE c.isDeleted = 0 AND c.deletedAt IS NULL
        {date_filter}
        ORDER BY c.createdAt DESC
        """
        write_debug(f"SQL Query: {query}")
        return await self.execute_query(query)

    async def get_controls_by_function(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            f.name as function_name,
            c.id as control_id,
            c.name as control_name,
            c.code as control_code
        FROM {self.get_fully_qualified_table_name('Controls')} c
        JOIN {self.get_fully_qualified_table_name('ControlFunctions')} cf ON c.id = cf.control_id
        JOIN {self.get_fully_qualified_table_name('Functions')} f ON cf.function_id = f.id
        WHERE c.isDeleted = 0 {date_filter}
        ORDER BY c.createdAt DESC, f.name, c.name
        """
        write_debug(f"SQL Query: {query}")
        return await self.execute_query(query)

    async def get_controls_testing_approval_cycle(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            c.name AS [Control Name],
            c.createdAt AS [Created At],
            c.id AS [Control ID],
            c.code AS [Code],
            t.preparerStatus AS [Preparer Status],
            t.checkerStatus AS [Checker Status],
            t.reviewerStatus AS [Reviewer Status],
            t.acceptanceStatus AS [Acceptance Status],
            f.name AS [Business Unit],
            CASE 
              WHEN ISNULL(t.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
              WHEN ISNULL(t.preparerStatus, '') = 'sent' AND ISNULL(t.checkerStatus, '') <> 'approved' AND ISNULL(t.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
              WHEN ISNULL(t.checkerStatus, '') = 'approved' AND ISNULL(t.reviewerStatus, '') <> 'sent' AND ISNULL(t.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
              WHEN ISNULL(t.reviewerStatus, '') = 'sent' AND ISNULL(t.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
              WHEN ISNULL(t.acceptanceStatus, '') = 'approved' THEN 'Approved'
              ELSE 'Other'
            END AS [Current Status]
        FROM {self.get_fully_qualified_table_name('ControlDesignTests')} AS t
        INNER JOIN {self.get_fully_qualified_table_name('Controls')} AS c ON t.control_id = c.id
        INNER JOIN {self.get_fully_qualified_table_name('Functions')} AS f ON t.function_id = f.id
        WHERE c.isDeleted = 0 AND (t.deletedAt IS NULL) AND t.function_id IS NOT NULL {date_filter}
        ORDER BY c.createdAt DESC, c.name
        """
        write_debug(f"SQL Query: {query}")
        return await self.execute_query(query)
    
    async def get_key_non_key_controls_per_department(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            COALESCE(jt.name, 'Unassigned Department') AS [Department],
            SUM(CASE WHEN c.keyControl = 1 THEN 1 ELSE 0 END) AS [Key Controls],
            SUM(CASE WHEN c.keyControl = 0 THEN 1 ELSE 0 END) AS [Non-Key Controls],
            COUNT(c.id) AS [Total Controls]
        FROM {self.get_fully_qualified_table_name('Controls')} c
        LEFT JOIN {self.get_fully_qualified_table_name('JobTitles')} jt ON c.departmentId = jt.id
        WHERE c.isDeleted = 0 {date_filter}
        GROUP BY COALESCE(jt.name, 'Unassigned Department'), c.departmentId
        ORDER BY COUNT(c.id) DESC, COALESCE(jt.name, 'Unassigned Department')
        """
        write_debug(f"SQL Query: {query}")
        return await self.execute_query(query)

    async def get_key_non_key_controls_per_process(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            CASE 
              WHEN p.name IS NULL THEN 'Unassigned Process'
              ELSE p.name
            END AS [Process],
            SUM(CASE WHEN c.keyControl = 1 THEN 1 ELSE 0 END) AS [Key Controls],
            SUM(CASE WHEN c.keyControl = 0 THEN 1 ELSE 0 END) AS [Non-Key Controls],
            COUNT(c.id) AS [Total Controls]
        FROM {self.get_fully_qualified_table_name('Controls')} c
        LEFT JOIN {self.get_fully_qualified_table_name('ControlProcesses')} cp ON c.id = cp.control_id
        LEFT JOIN {self.get_fully_qualified_table_name('Processes')} p ON cp.process_id = p.id
        WHERE c.isDeleted = 0 {date_filter}
        GROUP BY 
            CASE 
              WHEN p.name IS NULL THEN 'Unassigned Process'
              ELSE p.name
            END
        ORDER BY COUNT(c.id) DESC,
            CASE 
              WHEN p.name IS NULL THEN 'Unassigned Process'
              ELSE p.name
            END
        """
        write_debug(f"SQL Query: {query}")
        return await self.execute_query(query)
    
    async def get_key_non_key_controls_per_business_unit(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            f.name AS [Business Unit],
            SUM(CASE WHEN c.keyControl = 1 THEN 1 ELSE 0 END) AS [Key Controls],
            SUM(CASE WHEN c.keyControl = 0 THEN 1 ELSE 0 END) AS [Non-Key Controls],
            COUNT(c.id) AS [Total Controls]
        FROM {self.get_fully_qualified_table_name('ControlFunctions')} cf
        JOIN {self.get_fully_qualified_table_name('Functions')} f ON cf.function_id = f.id
        JOIN {self.get_fully_qualified_table_name('Controls')} c ON cf.control_id = c.id
        WHERE c.isDeleted = 0 {date_filter}
        GROUP BY f.name
        ORDER BY COUNT(c.id) DESC, f.name
        """
        write_debug(f"SQL Query: {query}")
        return await self.execute_query(query)

    async def get_control_count_by_assertion_name(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            COALESCE(a.name, 'Unassigned Assertion') AS [Assertion Name],
            COALESCE(a.account_type, 'Not Specified') AS [Type],
            COUNT(c.id) AS [Control Count]
        FROM {self.get_fully_qualified_table_name('Controls')} c
        LEFT JOIN {self.get_fully_qualified_table_name('Assertions')} a ON c.icof_id = a.id AND a.isDeleted = 0
        WHERE c.isDeleted = 0 {date_filter}
        GROUP BY a.name, a.account_type
        ORDER BY COUNT(c.id) DESC, a.name
        """
        write_debug(f"SQL Query: {query}")
        return await self.execute_query(query)
    
    async def get_icofr_control_coverage_by_coso(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"

        q = f"""
        SELECT 
            comp.name AS [Component], 
            CASE 
              WHEN c.icof_id IS NOT NULL 
                AND (a.C = 1 OR a.E = 1 OR a.A = 1 OR a.V = 1 OR a.O = 1 OR a.P = 1) 
                AND (a.account_type IN ('Balance Sheet', 'Income Statement')) 
              THEN 'ICOFR' 
              WHEN c.icof_id IS NULL 
                OR ((a.C IS NULL OR a.C = 0) AND (a.E IS NULL OR a.E = 0) AND (a.A IS NULL OR a.A = 0) 
                    AND (a.V IS NULL OR a.V = 0) AND (a.O IS NULL OR a.O = 0) AND (a.P IS NULL OR a.P = 0)) 
                OR a.account_type NOT IN ('Balance Sheet', 'Income Statement')
              THEN 'Non-ICOFR'
              ELSE 'Other'
            END AS [IcofrStatus], 
            COUNT(DISTINCT c.id) AS [Control Count]
        FROM {self.get_fully_qualified_table_name('Controls')} c
        LEFT JOIN {self.get_fully_qualified_table_name('Assertions')} a ON c.icof_id = a.id AND (a.isDeleted = 0 OR a.id IS NULL)
        JOIN {self.get_fully_qualified_table_name('ControlCosos')} ccx ON c.id = ccx.control_id AND ccx.deletedAt IS NULL
        JOIN {self.get_fully_qualified_table_name('CosoPoints')} point ON ccx.coso_id = point.id AND point.deletedAt IS NULL
        JOIN {self.get_fully_qualified_table_name('CosoPrinciples')} prin ON point.principle_id = prin.id AND prin.deletedAt IS NULL
        JOIN {self.get_fully_qualified_table_name('CosoComponents')} comp ON prin.component_id = comp.id AND comp.deletedAt IS NULL
        WHERE c.isDeleted = 0 {date_filter}
        GROUP BY comp.name, 
            CASE 
              WHEN c.icof_id IS NOT NULL 
                AND (a.C = 1 OR a.E = 1 OR a.A = 1 OR a.V = 1 OR a.O = 1 OR a.P = 1) 
                AND (a.account_type IN ('Balance Sheet', 'Income Statement')) 
              THEN 'ICOFR' 
              WHEN c.icof_id IS NULL 
                OR ((a.C IS NULL OR a.C = 0) AND (a.E IS NULL OR a.E = 0) AND (a.A IS NULL OR a.A = 0) 
                    AND (a.V IS NULL OR a.V = 0) AND (a.O IS NULL OR a.O = 0) AND (a.P IS NULL OR a.P = 0)) 
                OR a.account_type NOT IN ('Balance Sheet', 'Income Statement')
              THEN 'Non-ICOFR'
              ELSE 'Other'
            END
        ORDER BY comp.name, [IcofrStatus]
        """
        write_debug(f"SQL Query: {q}")
        res = await self.execute_query(q)
        write_debug(f"Query result: {len(res)} rows for actionPlanForAdequacy")
        if not res:
            # Fallback 2: fetch from Actionplans only (no join requirements), show available columns
            q_min = f"""
            SELECT 
                ap.control_procedure AS [Control Procedure],
                ap.[type] AS [Control Procedure Type],
                ap.factor AS [Factor],
                ap.riskType AS [Risk Treatment],
                ap.responsible AS [Action Plan Owner],
                ap.expected_cost AS [Expected Cost],
                ap.business_unit AS [Business Unit Status],
                ap.meeting_date AS [Meeting Date],
                ap.implementation_date AS [Expected Implementation Date],
                ap.not_attend AS [Did Not Attend]
            FROM {self.get_fully_qualified_table_name('Actionplans')} ap
            WHERE ap.[from] = 'adequacy' AND ap.deletedAt IS NULL
        {date_filter}
            ORDER BY ap.createdAt DESC
            """
            write_debug(f"SQL Query (minimal): {q_min}")
            res = await self.execute_query(q_min)
            write_debug(f"Query result (minimal): {len(res)} rows for actionPlanForAdequacy")
        return res

    async def get_controls_not_mapped_to_principles(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Controls not mapped to any Principles (match Node SQL)."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            c.name AS [Control Name], 
            f.name AS [Function Name]
        FROM {self.get_fully_qualified_table_name('Controls')} c
        LEFT JOIN {self.get_fully_qualified_table_name('ControlFunctions')} cf ON cf.control_id = c.id 
        LEFT JOIN {self.get_fully_qualified_table_name('Functions')} f ON f.id = cf.function_id 
        LEFT JOIN {self.get_fully_qualified_table_name('ControlCosos')} ccx ON ccx.control_id = c.id AND ccx.deletedAt IS NULL 
        WHERE ccx.control_id IS NULL AND c.isDeleted = 0 {date_filter}
        ORDER BY c.createdAt DESC
        """
        write_debug(f"SQL Query: {query}")
        return await self.execute_query(query)
    
    async def get_controls_not_mapped_to_assertions(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Controls not mapped to any Assertions (ICOFR account) - matches Node SQL."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            c.name AS [Control Name], 
            f.name AS [Function Name]
        FROM {self.get_fully_qualified_table_name('Controls')} c
        LEFT JOIN {self.get_fully_qualified_table_name('ControlFunctions')} cf ON cf.control_id = c.id 
        LEFT JOIN {self.get_fully_qualified_table_name('Functions')} f ON f.id = cf.function_id 
        WHERE c.icof_id IS NULL AND c.isDeleted = 0 {date_filter}
        ORDER BY c.createdAt DESC
        """
        write_debug(f"SQL Query: {query}")
        return await self.execute_query(query)

    async def get_action_plan_for_adequacy(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        write_debug(f"Fetching actionPlanForAdequacy for {start_date} to {end_date}")
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND ap.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND ap.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND ap.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            COALESCE(c.name, 'N/A') AS [Control Name], 
            COALESCE(f.name, 'N/A') AS [Function Name], 
            ap.factor AS [Factor], 
            ap.riskType AS [Risk Treatment], 
            ap.control_procedure AS [Control Procedure], 
            ap.[type] AS [Control Procedure Type], 
            ap.responsible AS [Action Plan Owner], 
            ap.expected_cost AS [Expected Cost], 
            ap.business_unit AS [Business Unit Status], 
            CASE WHEN ap.meeting_date IS NOT NULL THEN CONVERT(VARCHAR(20), CAST(ap.meeting_date AS DATETIME), 105) ELSE NULL END AS [Meeting Date], 
            CASE WHEN ap.implementation_date IS NOT NULL THEN CONVERT(VARCHAR(20), CAST(ap.implementation_date AS DATETIME), 105) ELSE NULL END AS [Expected Implementation Date], 
            ap.not_attend AS [Did Not Attend]
        FROM {self.get_fully_qualified_table_name('Actionplans')} ap
        LEFT JOIN {self.get_fully_qualified_table_name('ControlDesignTests')} cdt ON ap.controlDesignTest_id = cdt.id AND cdt.deletedAt IS NULL
        LEFT JOIN {self.get_fully_qualified_table_name('Controls')} c ON cdt.control_id = c.id AND c.isDeleted = 0
        LEFT JOIN {self.get_fully_qualified_table_name('Functions')} f ON cdt.function_id = f.id AND f.deletedAt IS NULL
        WHERE ap.[from] = 'adequacy' 
            AND ap.deletedAt IS NULL AND ap.controlDesignTest_id IS NOT NULL {date_filter}
        ORDER BY ap.createdAt DESC
        """
        write_debug(f"SQL Query: {query}")
        res = await self.execute_query(query)
        write_debug(f"Query result: {len(res)} rows for actionPlanForAdequacy")
        return res

    async def get_action_plan_for_effectiveness(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND ap.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND ap.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND ap.createdAt <= '{end_date}'"

        q = f"""
        SELECT 
            COALESCE(c.name, 'N/A') AS [Control Name], 
            COALESCE(f.name, 'N/A') AS [Function Name], 
            ap.factor AS [Factor], 
            ap.riskType AS [Risk Treatment], 
            ap.control_procedure AS [Control Procedure], 
            ap.[type] AS [Control Procedure Type], 
            ap.responsible AS [Action Plan Owner], 
            ap.expected_cost AS [Expected Cost], 
            ap.business_unit AS [Business Unit Status], 
            CASE WHEN ap.meeting_date IS NOT NULL THEN CONVERT(VARCHAR(20), CAST(ap.meeting_date AS DATETIME), 105) ELSE NULL END AS [Meeting Date], 
            CASE WHEN ap.implementation_date IS NOT NULL THEN CONVERT(VARCHAR(20), CAST(ap.implementation_date AS DATETIME), 105) ELSE NULL END AS [Expected Implementation Date], 
            ap.not_attend AS [Did Not Attend]
        FROM {self.get_fully_qualified_table_name('Actionplans')} ap
        LEFT JOIN {self.get_fully_qualified_table_name('ControlDesignTests')} cdt ON ap.controlDesignTest_id = cdt.id AND cdt.deletedAt IS NULL
        LEFT JOIN {self.get_fully_qualified_table_name('Controls')} c ON cdt.control_id = c.id AND c.isDeleted = 0
        LEFT JOIN {self.get_fully_qualified_table_name('ControlFunctions')} cf ON c.id = cf.control_id
        LEFT JOIN {self.get_fully_qualified_table_name('Functions')} f ON cf.function_id = f.id
        WHERE ap.[from] = 'effective' 
            AND ap.deletedAt IS NULL AND ap.controlDesignTest_id IS NOT NULL {date_filter}
        ORDER BY ap.createdAt DESC
        """
        write_debug(f"SQL Query: {q}")
        return await self.execute_query(q)

    async def get_control_submission_status_by_quarter_function(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"

        q = f"""
        SELECT 
            c.name AS [Control Name], 
            f.name AS [Function Name], 
            CASE WHEN cdt.quarter = 'quarterOne' THEN 1 
                 WHEN cdt.quarter = 'quarterTwo' THEN 2 
                 WHEN cdt.quarter = 'quarterThree' THEN 3 
                 WHEN cdt.quarter = 'quarterFour' THEN 4 
                 ELSE NULL END AS [Quarter], 
            cdt.year AS [Year], 
            CASE WHEN ( c.preparerStatus = 'sent' AND c.acceptanceStatus = 'approved' ) 
                 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS [Control Submitted?], 
            CASE WHEN ( cdt.preparerStatus = 'sent' AND cdt.acceptanceStatus = 'approved' ) 
                 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS [Test Approved?] 
        FROM {self.get_fully_qualified_table_name('ControlDesignTests')} cdt 
        JOIN {self.get_fully_qualified_table_name('Controls')} c ON cdt.control_id = c.id 
        JOIN {self.get_fully_qualified_table_name('Functions')} f ON cdt.function_id = f.id 
        WHERE c.isDeleted = 0 AND cdt.deletedAt IS NULL {date_filter}
        ORDER BY c.createdAt DESC
        """
        write_debug(f"SQL Query: {q}")
        return await self.execute_query(q)

    async def get_functions_with_fully_tested_control_tests(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"

        q = f"""
        SELECT 
            f.name AS [Function Name],
            CASE WHEN cdt.quarter = 'quarterOne' THEN 1 
                 WHEN cdt.quarter = 'quarterTwo' THEN 2 
                 WHEN cdt.quarter = 'quarterThree' THEN 3 
                 WHEN cdt.quarter = 'quarterFour' THEN 4 
                 ELSE NULL END AS [Quarter],
            cdt.year AS [Year],
            COUNT(DISTINCT c.id) AS [Total Controls],
            COUNT(DISTINCT CASE WHEN (c.preparerStatus = 'sent' AND c.acceptanceStatus = 'approved') THEN c.id END) AS [Controls Submitted],
            COUNT(DISTINCT CASE WHEN (cdt.preparerStatus = 'sent' AND cdt.acceptanceStatus = 'approved') THEN c.id END) AS [Tests Approved]
        FROM {self.get_fully_qualified_table_name('Functions')} AS f 
        JOIN {self.get_fully_qualified_table_name('ControlFunctions')} AS cf ON f.id = cf.function_id 
        JOIN {self.get_fully_qualified_table_name('Controls')} AS c ON cf.control_id = c.id AND c.isDeleted = 0 
        LEFT JOIN {self.get_fully_qualified_table_name('ControlDesignTests')} AS cdt ON cdt.control_id = c.id AND cdt.deletedAt IS NULL 
        WHERE 1=1 {date_filter}
        GROUP BY f.name, cdt.quarter, cdt.year
        ORDER BY f.name, cdt.year, cdt.quarter
        """
        write_debug(f"SQL Query: {q}")
        return await self.execute_query(q)

