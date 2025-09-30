import pandas as pd
import numpy as np
import time

print("Testing pandas operations with different data sizes...")
print("=" * 50)

# Test 1: Simulate normal patient data size (~2,000 records)
print("Test 1: Normal patient size (~2,000 records)")
np.random.seed(42)
normal_size = 2000
normal_data = pd.DataFrame({
    'icustay_id': np.random.randint(200001, 200021, normal_size),
    'itemid': np.random.randint(1, 500, normal_size),  # Reduced items for realism
    'hours_in': np.random.randint(0, 100, normal_size),
    'value': np.random.randn(normal_size),
})

start_time = time.time()
grouped = normal_data.groupby(['icustay_id', 'itemid', 'hours_in'])['value'].agg(['mean', 'std', 'count'])
groupby_time = time.time() - start_time

start_time = time.time()
unstacked = grouped.unstack(level=['itemid'])
unstack_time = time.time() - start_time

print(f"  Groupby: {groupby_time:.3f}s, Unstack: {unstack_time:.3f}s")
print(f"  Final shape: {unstacked.shape}")

# Test 2: Simulate outlier patient data size (~12,000 records)  
print("\nTest 2: Outlier patient size (~12,000 records)")
outlier_size = 12000
outlier_data = pd.DataFrame({
    'icustay_id': [200017] * outlier_size,  # Single outlier patient
    'itemid': np.random.randint(1, 500, outlier_size),
    'hours_in': np.random.randint(0, 1400, outlier_size),  # 57 days worth
    'value': np.random.randn(outlier_size),
})

print(f"  Starting groupby with {len(outlier_data):,} records...")
start_time = time.time()
try:
    grouped_outlier = outlier_data.groupby(['icustay_id', 'itemid', 'hours_in'])['value'].agg(['mean', 'std', 'count'])
    groupby_time = time.time() - start_time
    print(f"  Groupby completed: {groupby_time:.3f}s, groups: {len(grouped_outlier):,}")
    
    print(f"  Starting unstack...")
    start_time = time.time()
    unstacked_outlier = grouped_outlier.unstack(level=['itemid'])
    unstack_time = time.time() - start_time
    print(f"  Unstack completed: {unstack_time:.3f}s")
    print(f"  Final shape: {unstacked_outlier.shape}")
    
except Exception as e:
    operation_time = time.time() - start_time
    print(f"  FAILED after {operation_time:.1f}s: {e}")

# Test 3: Multiple patients including outlier
print("\nTest 3: Multiple patients including outlier")
mixed_data_list = []

# Add normal patients
for patient_id in range(200001, 200021):  # 20 normal patients
    patient_data = pd.DataFrame({
        'icustay_id': [patient_id] * 100,  # ~100 records each
        'itemid': np.random.randint(1, 100, 100),
        'hours_in': np.random.randint(0, 200, 100),
        'value': np.random.randn(100),
    })
    mixed_data_list.append(patient_data)

# Add outlier patient
outlier_patient = pd.DataFrame({
    'icustay_id': [200017] * 5000,  # The problematic outlier
    'itemid': np.random.randint(1, 100, 5000),
    'hours_in': np.random.randint(0, 1400, 5000),
    'value': np.random.randn(5000),
})
mixed_data_list.append(outlier_patient)

mixed_data = pd.concat(mixed_data_list, ignore_index=True)

print(f"  Starting operations with {len(mixed_data):,} records...")
start_time = time.time()
try:
    grouped_mixed = mixed_data.groupby(['icustay_id', 'itemid', 'hours_in'])['value'].agg(['mean', 'std', 'count'])
    groupby_time = time.time() - start_time
    print(f"  Groupby completed: {groupby_time:.3f}s, groups: {len(grouped_mixed):,}")
    
    print(f"  Starting unstack...")
    start_time = time.time()
    unstacked_mixed = grouped_mixed.unstack(level=['itemid'])
    unstack_time = time.time() - start_time
    print(f"  Unstack completed: {unstack_time:.3f}s")
    print(f"  Final shape: {unstacked_mixed.shape}")
    
except Exception as e:
    operation_time = time.time() - start_time
    print(f"  FAILED after {operation_time:.1f}s: {e}")

print("\nConclusion:")
print("If Test 3 takes much longer than Tests 1&2, the pandas operations are the bottleneck.")
print("If all tests run quickly, the bottleneck is elsewhere in the pipeline.")
