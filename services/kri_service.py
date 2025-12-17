"""
KRI service for KRI operations
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

class KriService:
    """Service for kri operations"""

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
    
   
 
    # KRI Database Methods
    async def get_kris_by_status(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRIs grouped by status"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            CASE 
                WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
                WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' AND ISNULL(k.checkerStatus, '') <> 'approved' THEN 'Pending Checker'
                WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' THEN 'Pending Reviewer'
                WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
                WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved'
                ELSE 'Other'
            END as status,
            COUNT(*) as count
        FROM Kris k
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
        GROUP BY 
            CASE 
                WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
                WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' AND ISNULL(k.checkerStatus, '') <> 'approved' THEN 'Pending Checker'
                WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' THEN 'Pending Reviewer'
                WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
                WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved'
                ELSE 'Other'
            END
        ORDER BY count DESC
        """
        return await self.execute_query(query)

    async def get_kris_by_level(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRIs grouped by risk level"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            k.kri_level as level,
            COUNT(*) as count
        FROM Kris k
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
        AND k.kri_level IS NOT NULL
        GROUP BY k.kri_level
        ORDER BY count DESC
        """
        return await self.execute_query(query)

    async def get_breached_kris_by_department(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return breached KRIs by department"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            ISNULL(f.name, 'Unknown') as function_name,
            COUNT(k.id) as breached_count
        FROM Kris k
        LEFT JOIN Functions f ON k.related_function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
        AND k.status = 'Breached'
        GROUP BY f.name
        ORDER BY breached_count DESC
        """
        return await self.execute_query(query)

    async def get_kri_assessment_count(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRI assessment count by department"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            ISNULL(f.name, 'Unknown') as function_name,
            COUNT(k.id) as assessment_count
        FROM Kris k
        LEFT JOIN Functions f ON k.related_function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
        GROUP BY f.name
        ORDER BY assessment_count DESC
        """
        return await self.execute_query(query)

    async def get_kris_list(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return list of all KRIs"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            k.code,
            k.kriName as kri_name,
            k.threshold,
            k.isAscending as is_ascending,
            k.kri_level,
            k.status,
            FORMAT(CONVERT(datetime, k.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            ISNULL(f.name, 'Unknown') as function_name
        FROM Kris k
        LEFT JOIN Functions f ON k.related_function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL
          {date_filter}
        ORDER BY k.createdAt DESC
        """
        write_debug(f"Query: {query}")
        return await self.execute_query(query)

    async def get_kris_by_status_detail(self, status: str, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRIs rows filtered by computed status label (not counts, matches incidents pattern)"""
        write_debug(f"Getting KRIS by status detail: {status}")
       
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        # Build query that computes the label and filters to requested status
        query = f"""
        WITH KrisStatus AS (
            SELECT 
                k.code,
                k.kriName as kri_name,
                CASE 
                    -- 1) Pending preparer: preparerStatus is anything other than 'sent'
                    WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'pendingPreparer'
                    -- 2) Pending checker: preparer sent AND checker not approved AND acceptance not approved
                    WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.checkerStatus, '') <> 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingChecker'
                    -- 3) Pending reviewer: checker approved AND reviewer not approved AND acceptance not approved
                    WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingReviewer'
                    -- 4) Pending acceptance: reviewer approved AND acceptance not approved
                    WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingAcceptance'
                    -- 5) Fully approved
                    WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved'
                    ELSE 'Other'
                END AS status,
                FORMAT(CONVERT(datetime, k.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt
            FROM Kris k
            WHERE k.isDeleted = 0 AND k.deletedAt IS NULL {date_filter}
        )
        SELECT *
        FROM KrisStatus
        WHERE status = '{status}'
        ORDER BY createdAt DESC;
        """
        write_debug(f"Query: {query}")
        return await self.execute_query(query)

    async def get_kris_status_counts(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, int]:
        """Return KRIs status counts (independent counts, matches Node.js logic)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        WITH KrisStatus AS (
          SELECT 
            CASE 
              WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'pendingPreparer'
              WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.checkerStatus, '') <> 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingChecker'
              WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingReviewer'
              WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingAcceptance'
              WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'approved'
              ELSE 'Other'
            END AS status
          FROM Kris k
          WHERE k.isDeleted = 0 AND k.deletedAt IS NULL {date_filter}
        )
        SELECT 
          CAST(SUM(CASE WHEN status = 'pendingPreparer' THEN 1 ELSE 0 END) AS INT) AS pendingPreparer,
          CAST(SUM(CASE WHEN status = 'pendingChecker' THEN 1 ELSE 0 END) AS INT) AS pendingChecker,
          CAST(SUM(CASE WHEN status = 'pendingReviewer' THEN 1 ELSE 0 END) AS INT) AS pendingReviewer,
          CAST(SUM(CASE WHEN status = 'pendingAcceptance' THEN 1 ELSE 0 END) AS INT) AS pendingAcceptance,
          CAST(SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS INT) AS approved
        FROM KrisStatus
        """
        result = await self.execute_query(query)
        return result[0] if result else {}

    async def get_overall_kri_statuses(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return all KRIs with their combined status (for Overall KRI Statuses table)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT
          k.code             AS code,
          k.kriName          AS kri_name,
          ISNULL(COALESCE(fkf.name, frel.name), 'Unknown') AS function_name,
          CASE 
            WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
            WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.checkerStatus, '') <> 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
            WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
            WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
            WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved'
            ELSE 'Unknown'
          END AS status
        FROM Kris k
        LEFT JOIN KriFunctions kf ON k.id = kf.kri_id
          AND kf.deletedAt IS NULL
        LEFT JOIN Functions fkf ON fkf.id = kf.function_id
          AND fkf.isDeleted = 0
          AND fkf.deletedAt IS NULL
        LEFT JOIN Functions frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        WHERE
          k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
        ORDER BY k.kriName
        """
        return await self.execute_query(query)

    async def get_kris_by_level_detailed(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRIs by level with derived logic from latest values (matches Node.js)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        WITH LatestKV AS (
          SELECT kv.kriId,
                 kv.value,
                 ROW_NUMBER() OVER (PARTITION BY kv.kriId ORDER BY COALESCE(CONVERT(datetime, CONCAT(kv.[year], '-', kv.[month], '-01')), kv.createdAt) DESC) rn
          FROM KriValues kv
          WHERE kv.deletedAt IS NULL
        ),
        K AS (
          SELECT k.id,
                 k.kri_level,
                 CAST(k.isAscending AS int) AS isAscending,
                 TRY_CONVERT(float, k.medium_from) AS med_thr,
                 TRY_CONVERT(float, k.high_from) AS high_thr
          FROM Kris k
          WHERE k.isDeleted = 0 AND k.deletedAt IS NULL {date_filter}
        ),
        KL AS (
          SELECT K.id, K.kri_level, K.isAscending, K.med_thr, K.high_thr,
                 TRY_CONVERT(float, kv.value) AS val
          FROM K
          LEFT JOIN LatestKV kv ON kv.kriId = K.id AND kv.rn = 1
        ),
        Derived AS (
          SELECT CASE
                   WHEN kri_level IS NOT NULL AND LTRIM(RTRIM(kri_level)) <> '' THEN kri_level
                   WHEN val IS NULL OR med_thr IS NULL OR high_thr IS NULL THEN 'Unknown'
                   WHEN isAscending = 1 AND val >= high_thr THEN 'High'
                   WHEN isAscending = 1 AND val >= med_thr THEN 'Medium'
                   WHEN isAscending = 1 THEN 'Low'
                   WHEN isAscending = 0 AND val <= high_thr THEN 'High'
                   WHEN isAscending = 0 AND val <= med_thr THEN 'Medium'
                   ELSE 'Low'
                 END AS level_bucket
          FROM KL
        )
        SELECT level_bucket AS level, COUNT(*) AS count
        FROM Derived
        GROUP BY level_bucket
        ORDER BY count DESC
        """
        return await self.execute_query(query)

    async def get_breached_kris_by_department_detailed(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return count of KRIs by function (simplified - just count KRIs per function)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          ISNULL(COALESCE(fkf.name, frel.name), 'Unknown') AS function_name,
          COUNT(k.id) AS breached_count
        FROM Kris k
        LEFT JOIN KriFunctions kf ON kf.kri_id = k.id
          AND kf.deletedAt IS NULL
        LEFT JOIN Functions fkf ON fkf.id = kf.function_id
          AND fkf.isDeleted = 0
          AND fkf.deletedAt IS NULL
        LEFT JOIN Functions frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
        GROUP BY ISNULL(COALESCE(fkf.name, frel.name), 'Unknown')
        ORDER BY breached_count DESC
        """
        return await self.execute_query(query)

    async def get_kri_health(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRI health status list"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT
          k.kriName,
          k.status,
          COALESCE(k.kri_level, 'Unknown') AS kri_level,
          COALESCE(fkf.name, frel.name, 'Unknown') AS function_name,
          k.threshold,
          k.frequency
        FROM Kris k
        LEFT JOIN KriFunctions kf ON k.id = kf.kri_id
          AND kf.deletedAt IS NULL
        LEFT JOIN Functions fkf ON fkf.id = kf.function_id
          AND fkf.isDeleted = 0
          AND fkf.deletedAt IS NULL
        LEFT JOIN Functions frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
        ORDER BY k.createdAt DESC
        """
        return await self.execute_query(query)
    
    async def get_kri_assessment_count_detailed(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRI assessment count by function (count assessments from KriValues table)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND kv.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND kv.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND kv.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT
          ISNULL(COALESCE(fkf.name, frel.name), 'Unknown') AS function_name,
          COUNT(kv.id) AS assessment_count
        FROM KriValues kv
        INNER JOIN Kris k ON kv.kriId = k.id
          AND k.isDeleted = 0 
          AND k.deletedAt IS NULL
        LEFT JOIN KriFunctions kf ON k.id = kf.kri_id
          AND kf.deletedAt IS NULL
        LEFT JOIN Functions fkf ON fkf.id = kf.function_id
          AND fkf.isDeleted = 0
          AND fkf.deletedAt IS NULL
        LEFT JOIN Functions frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        WHERE kv.deletedAt IS NULL {date_filter}
        GROUP BY ISNULL(COALESCE(fkf.name, frel.name), 'Unknown')
        ORDER BY assessment_count DESC
        """
        return await self.execute_query(query)

    async def get_kri_monthly_assessment(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return monthly KRI counts grouped by assessment"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND kv.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND kv.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND kv.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT
          CAST(DATEADD(month, DATEPART(month, kv.createdAt) - 1, DATEFROMPARTS(YEAR(kv.createdAt), 1, 1)) AS datetime2) AS createdAt,
          kv.assessment AS assessment,
          COUNT(kv.id) AS count
        FROM Kris AS k
        INNER JOIN KriValues AS kv ON kv.kriId = k.id AND kv.deletedAt IS NULL
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL
          AND kv.assessment IS NOT NULL {date_filter}
        GROUP BY
          CAST(DATEADD(month, DATEPART(month, kv.createdAt) - 1, DATEFROMPARTS(YEAR(kv.createdAt), 1, 1)) AS datetime2),
          kv.assessment
        ORDER BY createdAt ASC, assessment ASC
        """
        return await self.execute_query(query)

    async def get_newly_created_kris_per_month(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return number of newly created KRIs per month"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          CAST(DATEFROMPARTS(YEAR(k.createdAt), MONTH(k.createdAt), 1) AS datetime2) AS createdAt,
          COUNT(*) AS count
        FROM Kris k
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
        GROUP BY CAST(DATEFROMPARTS(YEAR(k.createdAt), MONTH(k.createdAt), 1) AS datetime2)
        ORDER BY createdAt ASC
        """
        return await self.execute_query(query)

    async def get_deleted_kris_per_month(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return number of deleted KRIs by month"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          CAST(DATEFROMPARTS(YEAR(k.createdAt), MONTH(k.createdAt), 1) AS datetime2) AS createdAt,
          COUNT(*) AS count
        FROM Kris k
        WHERE (k.isDeleted = 1 OR k.deletedAt IS NOT NULL) {date_filter}
        GROUP BY YEAR(k.createdAt), MONTH(k.createdAt)
        ORDER BY YEAR(k.createdAt) ASC, MONTH(k.createdAt) ASC
        """
        return await self.execute_query(query)

    async def get_kri_overdue_status_counts(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRIs overdue vs not overdue based on related Action Plans"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        WITH classified AS (
          SELECT
            k.id,
            CASE
              WHEN EXISTS (
                SELECT 1
                FROM Actionplans ap
                WHERE ap.kri_id = k.id
                  AND ap.deletedAt IS NULL
                  AND ap.implementation_date < GETDATE()
                  AND (ap.done = 0 OR ap.done IS NULL)
              ) THEN 'Overdue'
              ELSE 'Not Overdue'
            END AS KRIStatus
          FROM Kris AS k
          WHERE k.isDeleted = 0
            AND k.deletedAt IS NULL {date_filter}
        )
        SELECT
          KRIStatus AS status,
          COUNT(*) AS count
        FROM classified
        GROUP BY KRIStatus
        """
        return await self.execute_query(query)

    async def get_overdue_kris_by_department(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return overdue KRIs with department from Actionplans or linked Function"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT DISTINCT 
          k.id AS kriId, 
          k.kriName AS kriName, 
          ISNULL(COALESCE(fkf.name, frel.name), 'Unknown') AS function_name
        FROM Kris AS k
        INNER JOIN Actionplans AS ap ON ap.kri_id = k.id
          AND ap.deletedAt IS NULL
          AND ap.implementation_date < GETDATE()
          AND (ap.done = 0 OR ap.done IS NULL)
        LEFT JOIN KriFunctions AS kf ON k.id = kf.kri_id
          AND kf.deletedAt IS NULL
        LEFT JOIN Functions AS fkf ON fkf.id = kf.function_id
          AND fkf.isDeleted = 0
          AND fkf.deletedAt IS NULL
        LEFT JOIN Functions AS frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
        ORDER BY function_name, kriName
        """
        return await self.execute_query(query)

    async def get_all_kris_submitted_by_function(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return all KRIs submitted by function"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT
          ISNULL(COALESCE(fkf.name, frel.name), 'Unknown') AS function_name,
          COUNT(k.id) AS total_kris,
          COUNT(CASE
            WHEN ISNULL(k.preparerStatus, '') = 'sent'
            THEN 1 END) AS submitted_kris,
          CASE
            WHEN COUNT(k.id) = COUNT(CASE
              WHEN ISNULL(k.preparerStatus, '') = 'sent'
              THEN 1 END)
            THEN 'Yes' ELSE 'No'
          END AS all_submitted
        FROM Kris AS k
        LEFT JOIN KriFunctions AS kf ON k.id = kf.kri_id
          AND kf.deletedAt IS NULL
        LEFT JOIN Functions AS fkf ON fkf.id = kf.function_id
          AND fkf.isDeleted = 0
          AND fkf.deletedAt IS NULL
        LEFT JOIN Functions AS frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
        GROUP BY ISNULL(COALESCE(fkf.name, frel.name), 'Unknown')
        ORDER BY ISNULL(COALESCE(fkf.name, frel.name), 'Unknown')
        """
        return await self.execute_query(query)

    async def get_kri_counts_by_month_year(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRI counts by month and year"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT  
          FORMAT(k.createdAt, 'MMM yyyy') AS month_year,
          DATENAME(month, k.createdAt) AS month_name, 
          YEAR(k.createdAt) AS year, 
          COUNT(*) AS kri_count 
        FROM Kris k 
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
        GROUP BY FORMAT(k.createdAt, 'MMM yyyy'), YEAR(k.createdAt), DATENAME(month, k.createdAt), MONTH(k.createdAt) 
        ORDER BY YEAR(k.createdAt), MONTH(k.createdAt)
        """
        return await self.execute_query(query)

    async def get_kri_counts_by_frequency(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRI counts by frequency"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          ISNULL(k.frequency, 'Unknown') AS frequency, 
          COUNT(*) AS count 
        FROM Kris k
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
        GROUP BY ISNULL(k.frequency, 'Unknown')
        ORDER BY frequency ASC
        """
        return await self.execute_query(query)

    async def get_kri_risks_by_kri_name(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return risks linked to KRIs (count per KRI name)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          k.kriName AS kriName,
          COUNT(*) AS count
        FROM Risks r
        INNER JOIN KriRisks kr ON r.id = kr.risk_id
          AND kr.deletedAt IS NULL
        INNER JOIN Kris k ON kr.kri_id = k.id
          AND k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
        WHERE r.isDeleted = 0
          AND r.deletedAt IS NULL
          AND k.kriName IS NOT NULL
        GROUP BY k.kriName
        ORDER BY k.kriName ASC
        """
        return await self.execute_query(query)

    async def get_kri_risk_relationships(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRI and Risk relationships (detailed list)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
          k.code AS kri_code,
          k.kriName AS kri_name, 
          r.code AS risk_code,
          r.name AS risk_name
        FROM Kris k
        INNER JOIN KriRisks kr ON kr.kri_id = k.id
          AND kr.deletedAt IS NULL
        INNER JOIN Risks r ON r.id = kr.risk_id
          AND r.isDeleted = 0
          AND r.deletedAt IS NULL
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
        ORDER BY k.kriName, r.name
        """
        return await self.execute_query(query)

    async def get_kris_without_linked_risks(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return KRIs without linked risks"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT  
        k.code AS kriCode
        k.kriName AS kriName, 
         
        FROM Kris AS k
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          AND NOT EXISTS (
            SELECT 1
            FROM KriRisks AS kr
            WHERE kr.kri_id = k.id
              AND kr.deletedAt IS NULL
          )
        ORDER BY k.kriName
        """
        return await self.execute_query(query)

    async def get_active_kris_details(self, start_date: Optional[str] = None, end_date: Optional[str] = None, function_filter: str = "") -> List[Dict[str, Any]]:
        """Return active KRIs details"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT
          k.kriName AS kriName,
          CASE 
            WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
            WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.checkerStatus, '') <> 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
            WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
            WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
            WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved'
            ELSE 'Unknown'
          END AS combined_status,
          u.name AS assignedPersonId,
          u2.name AS addedBy,
          k.status AS status,
          k.frequency AS frequency,
          k.threshold AS threshold,
          k.high_from AS high_from,
          k.medium_from AS medium_from,
          k.low_from AS low_from,
          ISNULL(f.name, NULL) AS function_name
        FROM Kris k
        LEFT JOIN KriFunctions kf ON k.id = kf.kri_id
          AND kf.deletedAt IS NULL
        LEFT JOIN Functions f ON f.id = kf.function_id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        LEFT JOIN users u ON k.assignedPersonId = u.id
          AND u.deletedAt IS NULL
        LEFT JOIN users u2 ON k.addedBy = u2.id
          AND u2.deletedAt IS NULL
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          AND k.status = 'active'
        """
        return await self.execute_query(query)