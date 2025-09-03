"""
Test Data Factory for AutoDQ Testing
Creates realistic test datasets that mimic your Databricks table structures
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import Dict, List, Optional


class AutoDQTestDataFactory:
    """Factory class for generating test data that matches AutoDQ's expected schemas"""
    
    @staticmethod
    def create_validation_results_data(
        num_rows: int = 100,
        tables: List[str] = None,
        failure_rate: float = 0.2,
        date_range_days: int = 7
    ) -> pd.DataFrame:
        """
        Create test data matching gx_validation_results_cleaned_combined table structure
        
        Args:
            num_rows: Number of rows to generate
            tables: List of table names to use (defaults to common ones)
            failure_rate: Proportion of validations that should fail (0.0 to 1.0)
            date_range_days: Number of days back to generate timestamps
            
        Returns:
            DataFrame with validation results structure
        """
        if tables is None:
            tables = ['customers', 'orders', 'products', 'inventory', 'payments', 'users']
        
        columns_by_table = {
            'customers': ['customer_id', 'email', 'phone', 'registration_date', 'status'],
            'orders': ['order_id', 'customer_id', 'order_date', 'total_amount', 'status'],
            'products': ['product_id', 'name', 'price', 'category', 'stock_quantity'],
            'inventory': ['product_id', 'location', 'quantity', 'last_updated'],
            'payments': ['payment_id', 'order_id', 'amount', 'payment_date', 'method'],
            'users': ['user_id', 'username', 'email', 'created_at', 'is_active']
        }
        
        rules = [
            'No Nulls', 'Unique Values', 'Primary Key Present', 'Foreign Key Valid',
            'Range OK', 'Valid Type', 'Format Match', 'Column Present', 
            'Allowed Values', 'Valid Date'
        ]
        
        metrics = [
            'Column Values Not Null', 'Uniqueness Check', 'Primary Key Validation',
            'Foreign Key Validation', 'Value Range Check', 'Data Type Validation',
            'Format Validation', 'Column Existence Check', 'Domain Value Check',
            'Date Format Validation'
        ]
        
        data = []
        for i in range(num_rows):
            table = random.choice(tables)
            column = random.choice(columns_by_table.get(table, ['unknown_column']))
            rule = random.choice(rules)
            metric = random.choice(metrics)
            
            # Determine status based on failure rate
            status = 'Failed' if random.random() < failure_rate else 'Passed'
            
            # Generate timestamp within date range
            days_back = random.randint(0, date_range_days)
            hours_back = random.randint(0, 23)
            timestamp = datetime.now() - timedelta(days=days_back, hours=hours_back)
            
            # Generate failed values for failed checks
            failed_value = None
            failed_row_id = None
            if status == 'Failed':
                failed_value = AutoDQTestDataFactory._generate_failed_value(rule)
                failed_row_id = random.randint(1, 100000)
            
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
    
    @staticmethod
    def create_user_defined_rules_data(
        num_rows: int = 20,
        tables: List[str] = None,
        failure_rate: float = 0.3
    ) -> pd.DataFrame:
        """
        Create test data for user-defined validation rules
        """
        if tables is None:
            tables = ['inventory', 'sales', 'customers', 'orders']
        
        user_rules = [
            'Custom Business Rule', 'Data Quality Check', 'Business Logic Validation',
            'Custom Range Check', 'Cross-Table Validation', 'Complex Business Rule'
        ]
        
        data = []
        for i in range(num_rows):
            table = random.choice(tables)
            rule = 'User Generated Rule'  # This is the marker for user rules
            metric = random.choice(user_rules)
            status = 'Failed' if random.random() < failure_rate else 'Passed'
            
            timestamp = datetime.now() - timedelta(hours=random.randint(1, 168))  # Last week
            
            failed_value = None
            failed_row_id = None
            if status == 'Failed':
                failed_value = f"custom_error_{i}"
                failed_row_id = random.randint(1, 50000)
            
            data.append({
                'Run_Timestamp': timestamp,
                'Table': table,
                'Column': f'custom_column_{i % 5}',
                'Rule': rule,
                'Status': status,
                'Metric': metric,
                'Failed_Value': failed_value,
                'Failed_Row_ID': failed_row_id
            })
        
        return pd.DataFrame(data)
    
    @staticmethod
    def create_anomaly_detection_data(
        num_rows: int = 1000,
        anomaly_rate: float = 0.05
    ) -> pd.DataFrame:
        """
        Create test data suitable for anomaly detection testing
        """
        np.random.seed(42)  # For reproducible results
        
        # Generate normal data
        normal_size = int(num_rows * (1 - anomaly_rate))
        anomaly_size = num_rows - normal_size
        
        # Normal data distribution
        normal_values = np.random.normal(50, 10, normal_size)
        normal_row_ids = np.random.randint(1, 100000, normal_size)
        
        # Anomalous data (outliers)
        anomaly_values = np.concatenate([
            np.random.normal(200, 20, anomaly_size // 2),  # High outliers
            np.random.normal(-50, 15, anomaly_size - anomaly_size // 2)  # Low outliers
        ])
        anomaly_row_ids = np.random.randint(1, 100000, anomaly_size)
        
        # Combine data
        all_values = np.concatenate([normal_values, anomaly_values])
        all_row_ids = np.concatenate([normal_row_ids, anomaly_row_ids])
        
        # Shuffle to mix normal and anomalous data
        indices = np.random.permutation(len(all_values))
        all_values = all_values[indices]
        all_row_ids = all_row_ids[indices]
        
        data = {
            'Failed_Value': all_values.astype(str),  # String format as it comes from DB
            'Failed_Row_ID': all_row_ids.astype(str),
            'Table': np.random.choice(['customers', 'orders', 'products'], num_rows),
            'Column': np.random.choice(['amount', 'quantity', 'score', 'rating'], num_rows),
            'Status': ['Failed'] * num_rows,  # All are failed for anomaly detection
            'Run_Timestamp': [
                datetime.now() - timedelta(hours=random.randint(1, 24))
                for _ in range(num_rows)
            ]
        }
        
        return pd.DataFrame(data)
    
    @staticmethod
    def create_data_cleaning_test_data(num_rows: int = 50) -> pd.DataFrame:
        """
        Create test data with various data quality issues for cleaning tests
        """
        data = {
            'id': list(range(1, num_rows + 1)) + [num_rows - 1],  # Add duplicate
            'name': [
                'Alice Johnson', 'BOB SMITH', None, 'charlie brown', 'DAVID WILSON',
                'emma davis', 'FRANK MILLER', 'grace taylor', None, 'HENRY CLARK'
            ] + [f'Person_{i}' for i in range(10, num_rows)] + ['DAVID WILSON'],
            'email': [
                'alice@example.com', 'BOB@EXAMPLE.COM', None, 'charlie@test.com',
                'david@company.org', 'emma@domain.net', 'FRANK@SITE.COM',
                'grace@email.co', None, 'henry@web.org'
            ] + [f'person{i}@test.com' for i in range(10, num_rows)] + ['david@company.org'],
            'age': [25, 30, None, 35, 40, 28, 45, 33, None, 50] + 
                  [random.randint(18, 80) for _ in range(10, num_rows)] + [40],
            'score': [85.5, 92.0, None, 78.5, 95.0, 88.2, 91.7, 79.3, None, 87.8] +
                    [round(random.uniform(60, 100), 1) for _ in range(10, num_rows)] + [95.0],
            'category': ['A', 'b', None, 'C', 'a', 'B', 'c', 'A', None, 'b'] +
                       [random.choice(['A', 'B', 'C', 'a', 'b', 'c']) for _ in range(10, num_rows)] + ['a']
        }
        
        return pd.DataFrame(data)
    
    @staticmethod
    def _generate_failed_value(rule: str) -> str:
        """Generate realistic failed values based on the validation rule"""
        failed_values = {
            'No Nulls': None,
            'Unique Values': 'duplicate_value',
            'Primary Key Present': None,
            'Foreign Key Valid': 'invalid_foreign_key_999999',
            'Range OK': '-999',
            'Valid Type': 'invalid_type_string',
            'Format Match': 'invalid-format-123',
            'Column Present': 'missing_column',
            'Allowed Values': 'invalid_domain_value',
            'Valid Date': '2023-13-45'  # Invalid date
        }
        
        return failed_values.get(rule, 'generic_error_value')
    
    @staticmethod
    def create_comprehensive_test_suite() -> Dict[str, pd.DataFrame]:
        """
        Create a comprehensive set of test datasets for all AutoDQ components
        """
        return {
            'validation_results': AutoDQTestDataFactory.create_validation_results_data(200),
            'user_rules': AutoDQTestDataFactory.create_user_defined_rules_data(30),
            'anomaly_data': AutoDQTestDataFactory.create_anomaly_detection_data(500),
            'cleaning_data': AutoDQTestDataFactory.create_data_cleaning_test_data(100),
            'empty_data': pd.DataFrame(),
            'single_row': AutoDQTestDataFactory.create_validation_results_data(1),
            'high_failure_rate': AutoDQTestDataFactory.create_validation_results_data(50, failure_rate=0.8),
            'recent_data_only': AutoDQTestDataFactory.create_validation_results_data(
                30, date_range_days=1
            )
        }
    
    @staticmethod
    def save_test_datasets(datasets: Dict[str, pd.DataFrame], output_dir: str = "test_data"):
        """
        Save test datasets to CSV files for use in testing
        """
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        for name, df in datasets.items():
            filepath = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(filepath, index=False)
            print(f"Saved {len(df)} rows to {filepath}")


# Convenience functions for quick test data generation
def get_sample_validation_data(rows: int = 50) -> pd.DataFrame:
    """Quick function to get sample validation data"""
    return AutoDQTestDataFactory.create_validation_results_data(rows)

def get_sample_anomaly_data(rows: int = 100) -> pd.DataFrame:
    """Quick function to get sample anomaly data"""
    return AutoDQTestDataFactory.create_anomaly_detection_data(rows)

def get_sample_cleaning_data(rows: int = 30) -> pd.DataFrame:
    """Quick function to get sample cleaning data"""
    return AutoDQTestDataFactory.create_data_cleaning_test_data(rows)


if __name__ == "__main__":
    # Generate and save comprehensive test datasets
    factory = AutoDQTestDataFactory()
    datasets = factory.create_comprehensive_test_suite()
    factory.save_test_datasets(datasets)
    print("Test datasets generated successfully!")
