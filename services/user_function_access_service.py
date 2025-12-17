"""
User Function Access Service - filters data by user's assigned functions.
Mirrors the Node.js implementation in reporting_system_node.
"""
from typing import Dict, Any, List, Optional
import pyodbc
import os

# Database connection string
DB_CONNECTION_STRING = os.getenv(
    "DB_CONNECTION_STRING",
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.1.100;DATABASE=GRC;UID=sa;PWD=P@ssw0rd123"
)


class UserFunctionAccess:
    """Data class for user function access info."""
    def __init__(self, is_super_admin: bool = False, function_ids: List[str] = None):
        self.is_super_admin = is_super_admin
        self.function_ids = function_ids or []


class UserFunctionAccessService:
    """Service to manage user function-based data filtering."""
    
    # Super admin groups that bypass function filtering (case-insensitive, partial match)
    SUPER_ADMIN_GROUPS = ['super admin', 'superadmin', 'super_admin', 'admin']
    
    def execute_query(self, query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results."""
        try:
            conn = pyodbc.connect(DB_CONNECTION_STRING)
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            columns = [column[0] for column in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            print(f"[UserFunctionAccessService] execute_query ERROR: {e}")
            return []
    
    def get_user_function_access(self, user_id: str, group_name: Optional[str] = None) -> UserFunctionAccess:
        """
        Get user's function access info.
        Returns UserFunctionAccess with is_super_admin flag and list of function IDs.
        """
        # Check if user is super admin (case-insensitive partial match)
        is_super_admin = False
        if group_name:
            group_lower = group_name.lower().replace('_', ' ').replace('-', ' ')
            for admin_group in self.SUPER_ADMIN_GROUPS:
                if admin_group in group_lower or group_lower in admin_group:
                    is_super_admin = True
                    break
        
        if is_super_admin:
            return UserFunctionAccess(is_super_admin=True, function_ids=[])
        
        if not user_id:
            return UserFunctionAccess(is_super_admin=False, function_ids=[])
        
        # Get user's assigned function IDs
        query = """
            SELECT uf.function_id
            FROM [UserFunction] uf
            INNER JOIN [Functions] f ON f.id = uf.function_id
            WHERE uf.user_id = ? AND uf.deletedAt IS NULL AND f.deletedAt IS NULL
        """
        
        results = self.execute_query(query, [user_id])
        function_ids = [str(row['function_id']) for row in results if row.get('function_id')]
        
        return UserFunctionAccess(is_super_admin=False, function_ids=function_ids)
    
    def build_control_function_filter(
        self, 
        table_alias: str, 
        access: UserFunctionAccess, 
        selected_function_id: Optional[str] = None
    ) -> str:
        """
        Build SQL filter for Controls via ControlFunctions join table.
        Returns SQL fragment like: " AND EXISTS (SELECT 1 FROM ControlFunctions cf WHERE ...)"
        """
        if selected_function_id:
            # Filter by specific function
            if not access.is_super_admin and selected_function_id not in access.function_ids:
                return " AND 1 = 0"  # User doesn't have access to this function
            return f" AND EXISTS (SELECT 1 FROM [ControlFunctions] cf WHERE cf.control_id = {table_alias}.id AND cf.function_id = '{selected_function_id}' AND cf.deletedAt IS NULL)"
        
        if access.is_super_admin:
            return ""  # No filter for super admin
        
        if not access.function_ids:
            return " AND 1 = 0"  # User has no functions, return no data
        
        function_ids_str = "','".join(access.function_ids)
        return f" AND EXISTS (SELECT 1 FROM [ControlFunctions] cf WHERE cf.control_id = {table_alias}.id AND cf.function_id IN ('{function_ids_str}') AND cf.deletedAt IS NULL)"
    
    def build_risk_function_filter(
        self, 
        table_alias: str, 
        access: UserFunctionAccess, 
        selected_function_id: Optional[str] = None
    ) -> str:
        """
        Build SQL filter for Risks via RiskFunctions join table.
        Returns SQL fragment like: " AND EXISTS (SELECT 1 FROM RiskFunctions rf WHERE ...)"
        """
        if selected_function_id:
            if not access.is_super_admin and selected_function_id not in access.function_ids:
                return " AND 1 = 0"
            return f" AND EXISTS (SELECT 1 FROM [RiskFunctions] rf WHERE rf.risk_id = {table_alias}.id AND rf.function_id = '{selected_function_id}' AND rf.deletedAt IS NULL)"
        
        if access.is_super_admin:
            return ""
        
        if not access.function_ids:
            return " AND 1 = 0"
        
        function_ids_str = "','".join(access.function_ids)
        return f" AND EXISTS (SELECT 1 FROM [RiskFunctions] rf WHERE rf.risk_id = {table_alias}.id AND rf.function_id IN ('{function_ids_str}') AND rf.deletedAt IS NULL)"
    
    def build_kri_function_filter(
        self, 
        table_alias: str, 
        access: UserFunctionAccess, 
        selected_function_id: Optional[str] = None
    ) -> str:
        """
        Build SQL filter for KRIs via related_function_id column.
        Returns SQL fragment like: " AND k.related_function_id IN (...)"
        """
        if selected_function_id:
            if not access.is_super_admin and selected_function_id not in access.function_ids:
                return " AND 1 = 0"
            return f" AND {table_alias}.related_function_id = '{selected_function_id}'"
        
        if access.is_super_admin:
            return ""
        
        if not access.function_ids:
            return " AND 1 = 0"
        
        function_ids_str = "','".join(access.function_ids)
        return f" AND {table_alias}.related_function_id IN ('{function_ids_str}')"
    
    def build_incident_function_filter(
        self, 
        table_alias: str, 
        access: UserFunctionAccess, 
        selected_function_id: Optional[str] = None
    ) -> str:
        """
        Build SQL filter for Incidents via function_id column.
        Returns SQL fragment like: " AND i.function_id IN (...)"
        """
        if selected_function_id:
            if not access.is_super_admin and selected_function_id not in access.function_ids:
                return " AND 1 = 0"
            return f" AND {table_alias}.function_id = '{selected_function_id}'"
        
        if access.is_super_admin:
            return ""
        
        if not access.function_ids:
            return " AND 1 = 0"
        
        function_ids_str = "','".join(access.function_ids)
        return f" AND {table_alias}.function_id IN ('{function_ids_str}')"


# Singleton instance
user_function_access_service = UserFunctionAccessService()

