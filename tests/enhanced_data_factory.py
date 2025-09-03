"""
Enhanced Data Factory for Real-World Edge Cases
Generates test data that matches various company naming conventions and edge cases
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string
from typing import Dict, List, Optional, Union

class EnhancedAutoDQDataFactory:
    """Enhanced factory for generating realistic edge case test data"""
    
    # Real-world table naming patterns from various companies/industries
    REAL_WORLD_TABLE_PATTERNS = {
        'enterprise': [
            'dim_customer', 'fact_sales', 'staging_orders', 'raw_inventory',
            'cleansed_products', 'master_suppliers', 'reference_geography'
        ],
        'saas': [
            'users', 'organizations', 'subscriptions', 'events', 'metrics',
            'feature_flags', 'audit_logs', 'billing_invoices'
        ],
        'ecommerce': [
            'customers', 'orders', 'order_items', 'products', 'categories',
            'reviews', 'cart_sessions', 'payment_transactions'
        ],
        'financial': [
            'accounts', 'transactions', 'portfolios', 'securities', 'trades',
            'risk_metrics', 'compliance_reports', 'regulatory_filings'
        ],
        'healthcare': [
            'patients', 'encounters', 'diagnoses', 'procedures', 'medications',
            'lab_results', 'imaging_studies', 'provider_network'
        ],
        'manufacturing': [
            'parts', 'assemblies', 'work_orders', 'quality_inspections',
            'supplier_deliveries', 'production_schedules', 'maintenance_logs'
        ]
    }
    
    # Real-world column naming patterns
    REAL_WORLD_COLUMN_PATTERNS = {
        'ids': [
            'id', 'uuid', 'guid', 'key', 'primary_key', 'surrogate_key',
            'natural_key', 'business_key', 'external_id', 'legacy_id'
        ],
        'names': [
            'name', 'title', 'description', 'label', 'display_name',
            'full_name', 'short_name', 'code', 'abbreviation', 'alias'
        ],
        'dates': [
            'created_at', 'updated_at', 'deleted_at', 'effective_date',
            'expiration_date', 'start_date', 'end_date', 'due_date',
            'birth_date', 'hire_date', 'termination_date'
        ],
        'amounts': [
            'amount', 'price', 'cost', 'value', 'balance', 'total',
            'subtotal', 'tax', 'discount', 'fee', 'commission'
        ],
        'flags': [
            'is_active', 'is_deleted', 'is_primary', 'is_default',
            'enabled', 'visible', 'published', 'approved', 'verified'
        ],
        'international': [
            'país', 'nome', 'preço', 'データ', '顧客名', '注文日',
            'название', 'цена', 'дата', 'nom', 'prix', 'date'
        ]
    }
    
    # Problematic naming patterns that companies actually use
    PROBLEMATIC_PATTERNS = {
        'reserved_words': [
            'order', 'select', 'from', 'where', 'group', 'having',
            'union', 'join', 'table', 'column', 'index', 'view'
        ],
        'special_chars': [
            'field-with-dashes', 'field_with_underscores', 'field with spaces',
            'field@with@symbols', 'field#with#hash', 'field$with$dollar'
        ],
        'case_variations': [
            'CamelCaseField', 'snake_case_field', 'kebab-case-field',
            'UPPERCASE_FIELD', 'lowercase_field', 'MiXeD_CaSe_FiElD'
        ],
        'numeric_prefixes': [
            '1st_field', '2nd_column', '3_way_join', '4th_quarter',
            '123_numeric_start', '999_high_number'
        ],
        'long_names': [
            'a' * 64,   # Standard long name
            'b' * 128,  # Very long name
            'c' * 255,  # Maximum identifier length
            'extremely_long_descriptive_field_name_that_someone_thought_was_a_good_idea_but_makes_queries_hard_to_read'
        ]
    }
    
    @classmethod
    def create_realistic_validation_data(
        cls,
        num_rows: int = 100,
        industry: str = 'mixed',
        include_edge_cases: bool = True,
        failure_rate: float = 0.2
    ) -> pd.DataFrame:
        """
        Create realistic validation data with industry-specific naming patterns
        
        Args:
            num_rows: Number of rows to generate
            industry: Industry type ('enterprise', 'saas', 'ecommerce', etc.)
            include_edge_cases: Whether to include problematic naming patterns
            failure_rate: Proportion of validations that should fail
        """
        # Select table and column patterns based on industry
        if industry == 'mixed':
            all_tables = []
            for pattern_list in cls.REAL_WORLD_TABLE_PATTERNS.values():
                all_tables.extend(pattern_list)
            tables = random.sample(all_tables, min(10, len(all_tables)))
        else:
            tables = cls.REAL_WORLD_TABLE_PATTERNS.get(industry, ['generic_table'])
        
        # Build column mappings
        columns_by_table = {}
        for table in tables:
            base_columns = []
            # Add different types of columns
            base_columns.extend(random.sample(cls.REAL_WORLD_COLUMN_PATTERNS['ids'], 1))
            base_columns.extend(random.sample(cls.REAL_WORLD_COLUMN_PATTERNS['names'], 2))
            base_columns.extend(random.sample(cls.REAL_WORLD_COLUMN_PATTERNS['dates'], 1))
            base_columns.extend(random.sample(cls.REAL_WORLD_COLUMN_PATTERNS['amounts'], 1))
            base_columns.extend(random.sample(cls.REAL_WORLD_COLUMN_PATTERNS['flags'], 1))
            
            # Add edge cases if requested
            if include_edge_cases:
                if random.random() < 0.3:  # 30% chance of problematic names
                    problematic_type = random.choice(list(cls.PROBLEMATIC_PATTERNS.keys()))
                    base_columns.extend(random.sample(cls.PROBLEMATIC_PATTERNS[problematic_type], 1))
                
                if random.random() < 0.2:  # 20% chance of international names
                    base_columns.extend(random.sample(cls.REAL_WORLD_COLUMN_PATTERNS['international'], 1))
            
            columns_by_table[table] = base_columns
        
        # Generate realistic validation rules
        realistic_rules = [
            'No Nulls', 'Unique Values', 'Primary Key Present', 'Foreign Key Valid',
            'Range OK', 'Valid Type', 'Format Match', 'Column Present',
            'Allowed Values', 'Valid Date', 'Email Format', 'Phone Format',
            'Credit Card Format', 'SSN Format', 'ZIP Code Format',
            'Currency Format', 'Percentage Range', 'Positive Values Only',
            'Future Date Only', 'Past Date Only', 'Business Hours Only',
            'Working Days Only', 'Length Validation', 'Pattern Match',
            'Custom Business Rule', 'Data Consistency Check'
        ]
        
        realistic_metrics = [
            'Column Values Not Null', 'Uniqueness Check', 'Primary Key Validation',
            'Foreign Key Validation', 'Value Range Check', 'Data Type Validation',
            'Format Validation', 'Column Existence Check', 'Domain Value Check',
            'Date Format Validation', 'Email Format Check', 'Phone Number Validation',
            'Credit Card Number Check', 'Social Security Number Validation',
            'Postal Code Validation', 'Currency Amount Check', 'Percentage Validation',
            'Positive Number Check', 'Future Date Validation', 'Historical Date Check',
            'Business Hours Validation', 'Weekday Validation', 'String Length Check',
            'Regular Expression Match', 'Business Logic Validation', 'Cross-Field Validation'
        ]
        
        data = []
        for i in range(num_rows):
            table = random.choice(tables)
            column = random.choice(columns_by_table.get(table, ['unknown_column']))
            rule = random.choice(realistic_rules)
            metric = random.choice(realistic_metrics)
            
            # Determine status based on failure rate
            status = 'Failed' if random.random() < failure_rate else 'Passed'
            
            # Generate realistic timestamps
            days_back = random.randint(0, 30)  # Last 30 days
            hours_back = random.randint(0, 23)
            minutes_back = random.randint(0, 59)
            timestamp = datetime.now() - timedelta(days=days_back, hours=hours_back, minutes=minutes_back)
            
            # Generate realistic failed values
            failed_value = None
            failed_row_id = None
            if status == 'Failed':
                failed_value = cls._generate_realistic_failed_value(rule, column)
                failed_row_id = random.randint(1, 1000000)
            
            data.append({
                'Run_Timestamp': timestamp,
                'Table': table,
                'Column': column,
                'Rule': rule,
                'Status': status,
                'Metric': metric,
                'Failed_Value': failed_value,
                'Failed_Row_ID': failed_row_id
            })
        
        return pd.DataFrame(data)
    
    @classmethod
    def create_edge_case_scenarios(cls) -> Dict[str, pd.DataFrame]:
        """Create specific edge case scenarios that companies encounter"""
        scenarios = {}
        
        # Scenario 1: Unicode and International Data
        scenarios['unicode_data'] = pd.DataFrame({
            'Run_Timestamp': [datetime.now()] * 8,
            'Table': ['客户表', 'заказы', 'productos', 'données', 'データ', '테이블', 'جدول', 'טבלה'],
            'Column': ['客户_id', 'заказ_номер', 'precio€', 'nom_français', 'データ値', '필드명', 'قيمة', 'ערך'],
            'Rule': ['No Nulls'] * 8,
            'Status': ['Failed'] * 8,
            'Metric': ['Column Values Not Null'] * 8,
            'Failed_Value': ['', None, '无效值', 'недействительный', 'inválido', 'invalide', '無効', '무효'],
            'Failed_Row_ID': list(range(1, 9))
        })
        
        # Scenario 2: SQL Reserved Words and Special Characters
        scenarios['sql_reserved_words'] = pd.DataFrame({
            'Run_Timestamp': [datetime.now()] * 10,
            'Table': ['order', 'select', 'from', 'where', 'group', 'table-name', 'field with spaces', 'UPPERCASE', 'MixedCase', '123_numeric'],
            'Column': ['order', 'select', 'from-field', 'where_clause', 'group by', 'field@symbol', 'field with spaces', 'FIELD', 'fieldName', '1st_column'],
            'Rule': ['No Nulls'] * 10,
            'Status': ['Failed'] * 10,
            'Metric': ['Column Values Not Null'] * 10,
            'Failed_Value': [None] * 10,
            'Failed_Row_ID': list(range(1, 11))
        })
        
        # Scenario 3: Extreme Values and Data Types
        scenarios['extreme_values'] = pd.DataFrame({
            'Run_Timestamp': [datetime.now()] * 12,
            'Table': ['numeric_extremes'] * 12,
            'Column': ['extreme_values'] * 12,
            'Rule': ['Range OK'] * 12,
            'Status': ['Failed'] * 12,
            'Metric': ['Value Range Check'] * 12,
            'Failed_Value': [
                '999999999999999999999',    # Very large number
                '-999999999999999999999',   # Very large negative
                '1e308',                    # Scientific notation
                '1e-308',                   # Very small number
                'inf',                      # Infinity
                '-inf',                     # Negative infinity
                'nan',                      # Not a number
                '',                         # Empty string
                '   ',                      # Whitespace only
                None,                       # Null
                'not_a_number',            # Invalid format
                '123.456.789'              # Multiple decimal points
            ],
            'Failed_Row_ID': list(range(1, 13))
        })
        
        # Scenario 4: Date and Time Edge Cases
        scenarios['date_edge_cases'] = pd.DataFrame({
            'Run_Timestamp': [datetime.now()] * 15,
            'Table': ['date_tests'] * 15,
            'Column': ['date_field'] * 15,
            'Rule': ['Valid Date'] * 15,
            'Status': ['Failed'] * 15,
            'Metric': ['Date Format Validation'] * 15,
            'Failed_Value': [
                '2023-13-45',              # Invalid month/day
                '2023-02-29',              # Invalid leap year
                '0000-01-01',              # Edge date
                '9999-12-31',              # Far future
                '2023/02/30',              # Invalid day for month
                '32/01/2023',              # Invalid day
                '01/13/2023',              # Invalid month
                '2023-1-1',                # Single digit month/day
                '23-01-01',                # Two-digit year
                '2023-01-01T25:00:00',     # Invalid hour
                '2023-01-01 24:60:60',     # Invalid minute/second
                'Jan 1, 2023',             # Text format
                'not-a-date',              # Invalid format
                '',                        # Empty
                None                       # Null
            ],
            'Failed_Row_ID': list(range(1, 16))
        })
        
        # Scenario 5: Long Field Names and Values
        scenarios['long_names'] = pd.DataFrame({
            'Run_Timestamp': [datetime.now()] * 5,
            'Table': [
                'a' * 64,   # Long table name
                'b' * 128,  # Very long table name
                'c' * 255,  # Maximum length table name
                'extremely_long_descriptive_table_name_that_exceeds_normal_limits',
                'table_with_very_long_name_that_might_cause_issues_in_some_systems'
            ],
            'Column': [
                'x' * 64,   # Long column name
                'y' * 128,  # Very long column name
                'z' * 255,  # Maximum length column name
                'extremely_long_descriptive_column_name_that_exceeds_normal_limits',
                'column_with_very_long_name_that_might_cause_issues_in_some_systems'
            ],
            'Rule': ['No Nulls'] * 5,
            'Status': ['Failed'] * 5,
            'Metric': ['Column Values Not Null'] * 5,
            'Failed_Value': [
                'x' * 1000,  # Very long value
                'y' * 5000,  # Extremely long value
                'z' * 10000, # Maximum long value
                ''.join(random.choices(string.ascii_letters + string.digits, k=2000)),
                ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation, k=3000))
            ],
            'Failed_Row_ID': list(range(1, 6))
        })
        
        return scenarios
    
    @classmethod
    def _generate_realistic_failed_value(cls, rule: str, column: str) -> str:
        """Generate realistic failed values based on rule and column context"""
        rule_patterns = {
            'No Nulls': [None, '', '   ', 'NULL', 'null', 'None'],
            'Email Format': ['invalid-email', 'user@', '@domain.com', 'user.domain.com', 'user@domain'],
            'Phone Format': ['123', '123-45', '123-456-78901', 'abc-def-ghij', '(555) 123'],
            'Date Format': ['2023-13-45', '32/01/2023', 'not-a-date', '2023/02/30', '99/99/99'],
            'Credit Card Format': ['1234', '1234-5678-9012', '4111-1111-1111-111', 'invalid-card'],
            'Range OK': ['-999', '999999999', '0', '-1', 'out-of-range'],
            'Valid Type': ['text-not-number', '123abc', 'true/false', 'invalid-type'],
            'Format Match': ['invalid-format', '123-abc', 'WRONG_CASE', 'missing-parts'],
            'Unique Values': ['duplicate-value', 'repeated-entry', 'same-value'],
            'Allowed Values': ['invalid-option', 'not-in-list', 'wrong-choice', 'bad-enum']
        }
        
        # Context-aware failed values based on column name
        if 'email' in column.lower():
            return random.choice(['invalid@', 'user.domain.com', 'not-an-email'])
        elif 'phone' in column.lower():
            return random.choice(['123', '123-45-678901', 'abc-def-ghij'])
        elif 'date' in column.lower() or 'time' in column.lower():
            return random.choice(['2023-13-45', 'not-a-date', '32/01/2023'])
        elif 'price' in column.lower() or 'amount' in column.lower():
            return random.choice(['-999', '999999999', 'not-a-number'])
        else:
            return random.choice(rule_patterns.get(rule, ['generic-error-value']))


# Convenience functions
def get_realistic_test_data(industry: str = 'mixed', rows: int = 100) -> pd.DataFrame:
    """Get realistic test data for a specific industry"""
    return EnhancedAutoDQDataFactory.create_realistic_validation_data(
        num_rows=rows,
        industry=industry,
        include_edge_cases=True
    )

def get_edge_case_scenarios() -> Dict[str, pd.DataFrame]:
    """Get all edge case scenarios"""
    return EnhancedAutoDQDataFactory.create_edge_case_scenarios()


if __name__ == "__main__":
    # Generate sample realistic data
    print("Generating realistic test data samples...")
    
    industries = ['enterprise', 'saas', 'ecommerce', 'financial']
    for industry in industries:
        data = get_realistic_test_data(industry, 50)
        print(f"\n{industry.upper()} Industry Sample:")
        print(f"Tables: {data['Table'].unique()}")
        print(f"Sample columns: {data['Column'].unique()[:5]}")
    
    # Generate edge case scenarios
    edge_cases = get_edge_case_scenarios()
    print(f"\nGenerated {len(edge_cases)} edge case scenarios:")
    for scenario_name in edge_cases.keys():
        print(f"- {scenario_name}")
    
    print("\nRealistic test data generation complete!")
