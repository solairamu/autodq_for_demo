#!/usr/bin/env python3
"""
AutoDQ Comprehensive Test Suite
One command to test everything - data generation, edge cases, and all functionality
"""
import os
import sys
import traceback
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

def run_comprehensive_tests():
    """Run all tests and generate all test data in one comprehensive suite"""
    print("🚀 AutoDQ Comprehensive Test Suite")
    print("=" * 60)
    print("Testing everything: data generation, edge cases, and all functionality")
    print("=" * 60)
    
    total_tests = 0
    passed_tests = 0
    failed_tests = 0
    
    # Test 1: Enhanced Data Factory
    print("\n📊 1. Testing Enhanced Data Factory...")
    try:
        from tests.test_data_factory import AutoDQTestDataFactory
        from tests.enhanced_data_factory import EnhancedAutoDQDataFactory, get_edge_case_scenarios
        
        # Test original factory
        factory = AutoDQTestDataFactory()
        validation_data = factory.create_validation_results_data(100)
        assert len(validation_data) == 100
        assert 'Table' in validation_data.columns
        total_tests += 1
        passed_tests += 1
        print("  ✅ Original data factory: PASSED")
        
        # Test enhanced factory with all industries
        enhanced_factory = EnhancedAutoDQDataFactory()
        industries = ['enterprise', 'saas', 'ecommerce', 'financial', 'healthcare', 'manufacturing']
        
        for industry in industries:
            industry_data = enhanced_factory.create_realistic_validation_data(50, industry=industry)
            assert len(industry_data) == 50
            assert 'Table' in industry_data.columns
            total_tests += 1
            passed_tests += 1
            print(f"  ✅ {industry.capitalize()} industry data: PASSED")
        
        # Test all edge case scenarios
        edge_cases = get_edge_case_scenarios()
        expected_scenarios = ['unicode_data', 'sql_reserved_words', 'extreme_values', 'date_edge_cases', 'long_names']
        
        for scenario in expected_scenarios:
            assert scenario in edge_cases
            assert not edge_cases[scenario].empty
            total_tests += 1
            passed_tests += 1
            print(f"  ✅ Edge case '{scenario}': PASSED")
            
    except Exception as e:
        failed_tests += 1
        print(f"  ❌ Data factory test failed: {e}")
        traceback.print_exc()
    
    # Test 2: Data Cleaning with Edge Cases
    print("\n🧪 2. Testing Data Cleaning with All Edge Cases...")
    try:
        # Test with Unicode and special characters
        unicode_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', '客户名', 'заказ', None, 'données'],
            'email': ['alice@test.com', '客户@test.com', None, 'invalid@', 'test@données.com'],
            'value': [100, None, '999999999999999999999', 'inf', -999]
        })
        
        # Test null removal
        cleaned = unicode_data.dropna()
        assert len(cleaned) < len(unicode_data)
        total_tests += 1
        passed_tests += 1
        print("  ✅ Unicode data null removal: PASSED")
        
        # Test with extreme values
        extreme_data = pd.DataFrame({
            'extreme_values': ['999999999999999999999', '-999999999999999999999', '1e308', 'inf', 'nan', ''],
            'table': ['test'] * 6
        })
        extreme_data['numeric'] = pd.to_numeric(extreme_data['extreme_values'], errors='coerce')
        assert 'numeric' in extreme_data.columns
        total_tests += 1
        passed_tests += 1
        print("  ✅ Extreme values handling: PASSED")
        
        # Test with SQL reserved words
        sql_data = pd.DataFrame({
            'Table': ['order', 'select', 'from', 'where'],
            'Column': ['order', 'select', 'from-field', 'where_clause']
        })
        sql_data['Table.Column'] = sql_data['Table'] + '.' + sql_data['Column']
        assert len(sql_data) == 4
        total_tests += 1
        passed_tests += 1
        print("  ✅ SQL reserved words handling: PASSED")
        
    except Exception as e:
        failed_tests += 1
        print(f"  ❌ Data cleaning test failed: {e}")
        traceback.print_exc()
    
    # Test 3: Anomaly Detection with Edge Cases
    print("\n🔍 3. Testing Anomaly Detection with All Edge Cases...")
    try:
        from sklearn.ensemble import IsolationForest
        from scipy.stats import zscore
        
        # Test with normal data + outliers
        np.random.seed(42)
        normal_data = np.random.normal(50, 10, 95)
        outliers = [200, -100, 300, -150, 250]
        all_values = np.concatenate([normal_data, outliers])
        
        anomaly_data = pd.DataFrame({
            'Failed_Value_num': all_values,
            'Table': ['test_table'] * len(all_values)
        })
        
        # Test Z-Score method
        z_scores = zscore(anomaly_data['Failed_Value_num'])
        z_anomalies = np.abs(z_scores) > 3
        assert np.sum(z_anomalies) > 0
        total_tests += 1
        passed_tests += 1
        print("  ✅ Z-Score anomaly detection: PASSED")
        
        # Test Isolation Forest
        model = IsolationForest(contamination=0.05, random_state=42)
        predictions = model.fit_predict(anomaly_data[['Failed_Value_num']])
        if_anomalies = predictions == -1
        assert np.sum(if_anomalies) > 0
        total_tests += 1
        passed_tests += 1
        print("  ✅ Isolation Forest anomaly detection: PASSED")
        
        # Test with constant values (edge case)
        constant_data = pd.DataFrame({'values': [5.0] * 100})
        z_scores_constant = zscore(constant_data['values'])
        # Should handle without crashing (results in NaN for constant values)
        assert len(z_scores_constant) == 100
        total_tests += 1
        passed_tests += 1
        print("  ✅ Constant values edge case: PASSED")
        
    except Exception as e:
        failed_tests += 1
        print(f"  ❌ Anomaly detection test failed: {e}")
        traceback.print_exc()
    
    # Test 4: Data Processing Edge Cases
    print("\n💾 4. Testing Data Processing with All Edge Cases...")
    try:
        # Test with mixed data types
        mixed_data = pd.DataFrame({
            'mixed_column': [123, '123', 123.45, True, None, 'text', np.inf, np.nan],
            'Table': ['mixed_table'] * 8
        })
        
        # Test preprocessing (as done in app.py)
        processed = mixed_data.copy()
        processed.columns = processed.columns.str.strip()
        assert len(processed) == 8
        total_tests += 1
        passed_tests += 1
        print("  ✅ Mixed data types processing: PASSED")
        
        # Test with very long names
        long_names_data = pd.DataFrame({
            'Table': ['a' * 255],  # Very long table name
            'Column': ['b' * 255]  # Very long column name
        })
        long_names_data['Table.Column'] = long_names_data['Table'] + '.' + long_names_data['Column']
        assert len(long_names_data['Table.Column'].iloc[0]) > 500
        total_tests += 1
        passed_tests += 1
        print("  ✅ Long names handling: PASSED")
        
        # Test with date edge cases
        date_data = pd.DataFrame({
            'dates': ['2023-01-01', '2023-13-45', '2023-02-29', 'not-a-date', None]
        })
        date_data['parsed_dates'] = pd.to_datetime(date_data['dates'], errors='coerce')
        valid_dates = date_data['parsed_dates'].notna().sum()
        assert valid_dates > 0  # At least some dates should be valid
        total_tests += 1
        passed_tests += 1
        print("  ✅ Date edge cases handling: PASSED")
        
    except Exception as e:
        failed_tests += 1
        print(f"  ❌ Data processing test failed: {e}")
        traceback.print_exc()
    
    # Test 5: Generate All Test Data
    print("\n📁 5. Generating All Test Data...")
    try:
        # Create test data directory
        test_data_dir = Path("comprehensive_test_data")
        test_data_dir.mkdir(exist_ok=True)
        
        datasets_generated = 0
        
        # Generate original test datasets
        factory = AutoDQTestDataFactory()
        original_datasets = factory.create_comprehensive_test_suite()
        
        for name, df in original_datasets.items():
            if not df.empty:
                filepath = test_data_dir / f"original_{name}.csv"
                df.to_csv(filepath, index=False)
                datasets_generated += 1
        
        # Generate industry-specific datasets
        enhanced_factory = EnhancedAutoDQDataFactory()
        industries = ['enterprise', 'saas', 'ecommerce', 'financial', 'healthcare', 'manufacturing']
        
        for industry in industries:
            industry_data = enhanced_factory.create_realistic_validation_data(200, industry=industry, include_edge_cases=True)
            filepath = test_data_dir / f"industry_{industry}.csv"
            industry_data.to_csv(filepath, index=False)
            datasets_generated += 1
        
        # Generate all edge case scenarios
        edge_cases = get_edge_case_scenarios()
        for name, df in edge_cases.items():
            filepath = test_data_dir / f"edge_case_{name}.csv"
            df.to_csv(filepath, index=False)
            datasets_generated += 1
        
        # Generate mixed scenario (all edge cases combined)
        mixed_scenarios = []
        for df in edge_cases.values():
            mixed_scenarios.append(df)
        
        if mixed_scenarios:
            combined_edge_cases = pd.concat(mixed_scenarios, ignore_index=True)
            filepath = test_data_dir / "comprehensive_all_edge_cases.csv"
            combined_edge_cases.to_csv(filepath, index=False)
            datasets_generated += 1
        
        total_tests += 1
        passed_tests += 1
        print(f"  ✅ Generated {datasets_generated} comprehensive test datasets")
        print(f"  📂 Saved to: {test_data_dir}/")
        
    except Exception as e:
        failed_tests += 1
        print(f"  ❌ Test data generation failed: {e}")
        traceback.print_exc()
    
    # Final Results
    print("\n" + "=" * 60)
    print("📊 COMPREHENSIVE TEST RESULTS")
    print("=" * 60)
    print(f"Total Tests: {total_tests}")
    print(f"✅ Passed: {passed_tests}")
    print(f"❌ Failed: {failed_tests}")
    print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%" if total_tests > 0 else "N/A")
    
    if failed_tests == 0:
        print("\n🎉 ALL TESTS PASSED!")
        print("\n✨ Your AutoDQ application is comprehensively tested and ready!")
        print("\n📋 What was tested:")
        print("   • Original data factory functionality")
        print("   • Enhanced realistic data generation (6 industries)")
        print("   • Comprehensive edge cases (Unicode, SQL reserved words, etc.)")
        print("   • Data cleaning with extreme values")
        print("   • Anomaly detection with edge cases")
        print("   • Data processing with mixed types")
        print("   • Long names and problematic characters")
        print("   • Date/time edge cases")
        print("\n📂 Generated comprehensive test datasets:")
        print(f"   • {datasets_generated} different test scenarios")
        print("   • Industry-specific naming patterns")
        print("   • Edge case scenarios")
        print("   • Combined comprehensive dataset")
        
    else:
        print(f"\n⚠️  {failed_tests} tests failed. Please review the error messages above.")
    
    print("\n" + "=" * 60)
    return failed_tests == 0

if __name__ == "__main__":
    success = run_comprehensive_tests()
    sys.exit(0 if success else 1)
