#!/usr/bin/env python3

import subprocess
import threading
import time
import signal
import sys
import os

def monitor_extraction(pop_size):
    """Monitor the MIMIC extraction process with detailed timing"""
    
    print(f"=== MIMIC Extraction Monitor (pop_size={pop_size}) ===")
    print("Will monitor extraction and report timing at each stage")
    print("Timeout: 30 seconds per major operation")
    print("=" * 50)
    
    # Set up environment
    env = os.environ.copy()
    env['MIMIC_DB_PATH'] = '/Users/leis/Documents/Research/AttributionTransformer/MIMIC/MIMICExtractEasy/data/mimic3.db'
    env['OUTPUT_DIR'] = '/Users/leis/Documents/Research/AttributionTransformer/MIMIC'
    
    # Command to run
    cmd = [
        'python3', 'mimic_direct_extract.py',
        '--duckdb_database', env['MIMIC_DB_PATH'],
        '--extract_notes', '0',
        '--out_path', env['OUTPUT_DIR'],
        '--resource_path', './resources/',
        '--queries_path', './SQL_Queries/',
        '--duckdb_schema_name', 'main',
        '--pop_size', str(pop_size),
        '--plot_hist', '0'
    ]
    
    print(f"Command: {' '.join(cmd)}")
    print("Monitoring output...")
    print("-" * 50)
    
    start_time = time.time()
    last_output_time = start_time
    
    # Start the process
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        env=env,
        bufsize=1  # Line buffered
    )
    
    # Monitor output with timeout
    timeout_seconds = 30
    
    try:
        while True:
            # Check if process is still running
            if process.poll() is not None:
                remaining_output = process.stdout.read()
                if remaining_output:
                    print(remaining_output.rstrip())
                print(f"\nProcess completed with exit code: {process.poll()}")
                break
            
            # Check for timeout
            current_time = time.time()
            if current_time - last_output_time > timeout_seconds:
                print(f"\n!!! TIMEOUT: No output for {timeout_seconds} seconds !!!")
                print(f"Total runtime: {current_time - start_time:.1f} seconds")
                print("Last output was at: {:.1f}s".format(last_output_time - start_time))
                print("Killing process...")
                process.terminate()
                time.sleep(2)
                if process.poll() is None:
                    process.kill()
                return False
            
            # Read line with timeout
            line = process.stdout.readline()
            if line:
                current_time = time.time()
                elapsed = current_time - start_time
                since_last = current_time - last_output_time
                print(f"[{elapsed:6.1f}s] (+{since_last:4.1f}s) {line.rstrip()}")
                last_output_time = current_time
                
                # Look for specific milestones
                line_lower = line.lower()
                if 'starting db query' in line_lower:
                    print("    🔍 DATABASE QUERY PHASE STARTED")
                elif 'db query finished' in line_lower:
                    print("    ✅ DATABASE QUERY COMPLETED")
                elif 'building data from scratch' in line_lower:
                    print("    🔨 DATA BUILDING STARTED")
                elif 'loaded static_data' in line_lower:
                    print("    ✅ STATIC DATA LOADED")
                elif 'extracting' in line_lower:
                    print(f"    📊 EXTRACTION PHASE: {line.strip()}")
            else:
                time.sleep(0.1)  # Small delay if no output
            
    except KeyboardInterrupt:
        print("\nUser interrupted. Killing process...")
        process.terminate()
        return False
    
    total_time = time.time() - start_time
    print(f"\nTotal execution time: {total_time:.1f} seconds")
    return process.poll() == 0

if __name__ == "__main__":
    # Test both working and problematic cases
    print("Testing 20 subjects (known to work):")
    success_20 = monitor_extraction(20)
    
    if success_20:
        print("\n" + "="*60)
        print("Now testing 21 subjects (known to hang):")
        success_21 = monitor_extraction(21)
        sys.exit(0 if success_21 else 1)
    else:
        print("Even 20 subjects failed!")
        sys.exit(1)
