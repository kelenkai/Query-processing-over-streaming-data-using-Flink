#!/usr/bin/env python3
"""
TPC-H Stream Data Processor
Generates unified stream data from TPC-H table files for real-time processing
"""

import os
import time
from pathlib import Path


class TPCHStreamProcessor:
    """
    Processes TPC-H table files and generates unified stream data
    with insert/delete operations for sliding window simulation
    """
    
    def __init__(self):
        # Table configuration with TPC-H SF-1 specifications
        self.params = {
            'base_directory': '.',
            'sliding_window_capacity': 6001215,  # Window size for stream processing
            'data_scale': 1,                      # TPC-H scale factor
            'enable_lineitem_stream': True,
            'enable_orders_stream': True, 
            'enable_customer_stream': True,
            'result_filename': 'streamdata.csv'
        }
        
        # TPC-H table size ratios (scale factor 1)
        self.table_specifications = {
            'lineitem_records': self.params['data_scale'] * 6000000,
            'orders_records': self.params['data_scale'] * 1500000,
            'customer_records': self.params['data_scale'] * 150000
        }
        
        # Table metadata
        self.table_info = {
            'names': ['customer', 'lineitem', 'orders'],
            'expected_sizes': [
                self.table_specifications['customer_records'],
                self.table_specifications['lineitem_records'], 
                self.table_specifications['orders_records']
            ]
        }
        
    def validate_environment(self):
        """Validate input files and directory structure"""
        working_dir = self.params['base_directory']
        
        if not Path(working_dir).exists():
            raise FileNotFoundError("Working directory does not exist.")
            
        # Validate scale factor and window size
        if self.params['sliding_window_capacity'] < 0:
            raise ValueError("Sliding window capacity cannot be negative.")
            
        if self.params['data_scale'] <= 0:
            raise ValueError("Data scale factor must be positive.")
    
    def verify_input_files(self):
        """Check existence and content of TPC-H table files"""
        base_path = self.params['base_directory']
        
        # Construct file paths for .tbl format
        table_files = [Path(base_path) / f"{table_name}.tbl" 
                      for table_name in self.table_info['names']]
        
        # Verify file existence
        for file_path in table_files:
            if not file_path.exists():
                raise FileNotFoundError(f"Required file {file_path} not found.")
                
        # Verify file content
        for file_path in table_files:
            if file_path.stat().st_size == 0:
                raise ValueError(f"File {file_path} is empty.")
                
        # Validate record counts
        print("*************************************")
        for idx, file_path in enumerate(table_files):
            with open(file_path, 'r') as file_handle:
                actual_lines = sum(1 for _ in file_handle)
                
            expected_lines = self.table_info['expected_sizes'][idx]
            if actual_lines < expected_lines:
                raise ValueError(f"{file_path} contains {actual_lines} lines, expected {expected_lines}")
            else:
                print(f"{file_path} has {actual_lines} lines")
        
        return table_files
    
    def initialize_file_streams(self, table_files):
        """Open file streams for reading table data"""
        base_dir = self.params['base_directory']
        
        # Primary streams for insertion operations
        streams = {
            'lineitem_insert': open(Path(base_dir) / "lineitem.tbl", "r"),
            'orders_insert': open(Path(base_dir) / "orders.tbl", "r"),
            'customer_insert': open(Path(base_dir) / "customer.tbl", "r")
        }
        
        # Secondary streams for deletion operations  
        streams.update({
            'lineitem_delete': open(Path(base_dir) / "lineitem.tbl", "r"),
            'orders_delete': open(Path(base_dir) / "orders.tbl", "r"),
            'customer_delete': open(Path(base_dir) / "customer.tbl", "r")
        })
        
        return streams
    
    def setup_output_stream(self):
        """Initialize output file for stream data"""
        output_path = Path(self.params['base_directory']) / self.params['result_filename']
        return open(output_path, "w"), output_path
    
    def write_stream_record(self, writer, record_data):
        """Write formatted record to output stream"""
        writer.write(record_data)
    
    def process_unified_stream(self):
        """Main processing logic for generating unified stream data"""
        
        # Environment validation
        self.validate_environment()
        table_files = self.verify_input_files()
        
        # Initialize file streams
        file_streams = self.initialize_file_streams(table_files)
        output_writer, output_path = self.setup_output_stream()
        
        # Processing counters
        counters = {
            'lineitem_inserts': 0,
            'lineitem_deletes': 0 - self.params['sliding_window_capacity'],
            'orders_inserts': 0,
            'orders_deletes': 0,
            'customer_inserts': 0,
            'customer_deletes': 0
        }
        
        # Read initial records from all streams
        current_records = {
            'lineitem_insert': file_streams['lineitem_insert'].readline(),
            'orders_insert': file_streams['orders_insert'].readline(),
            'customer_insert': file_streams['customer_insert'].readline(),
            'lineitem_delete': file_streams['lineitem_delete'].readline(),
            'orders_delete': file_streams['orders_delete'].readline(),
            'customer_delete': file_streams['customer_delete'].readline()
        }
        
        # Phase 1: Process insertions with sliding window deletions
        while current_records['lineitem_insert']:
            counters['lineitem_inserts'] += 1
            counters['lineitem_deletes'] += 1
            
            # Process lineitem insertion
            if self.params['enable_lineitem_stream']:
                self.write_stream_record(output_writer, "+LI" + current_records['lineitem_insert'])
            current_records['lineitem_insert'] = file_streams['lineitem_insert'].readline()
            
            # Process lineitem deletion (when window is full)
            if counters['lineitem_deletes'] > 0:
                if self.params['enable_lineitem_stream']:
                    self.write_stream_record(output_writer, "-LI" + current_records['lineitem_delete'])
                current_records['lineitem_delete'] = file_streams['lineitem_delete'].readline()
            
            # Process orders based on ratio to lineitem
            orders_threshold = (counters['lineitem_inserts'] * 
                               self.table_specifications['orders_records'] / 
                               self.table_specifications['lineitem_records'])
            
            if orders_threshold > counters['orders_inserts'] and current_records['orders_insert']:
                counters['orders_inserts'] += 1
                if self.params['enable_orders_stream']:
                    self.write_stream_record(output_writer, "+OR" + current_records['orders_insert'])
                current_records['orders_insert'] = file_streams['orders_insert'].readline()
                
                # Orders deletion processing
                orders_delete_threshold = (counters['lineitem_deletes'] * 
                                          self.table_specifications['orders_records'] / 
                                          self.table_specifications['lineitem_records'])
                
                if (orders_delete_threshold > counters['orders_deletes'] and 
                    current_records['orders_delete']):
                    counters['orders_deletes'] += 1
                    if self.params['enable_orders_stream']:
                        self.write_stream_record(output_writer, "-OR" + current_records['orders_delete'])
                    current_records['orders_delete'] = file_streams['orders_delete'].readline()
            
            # Process customer based on ratio to lineitem
            customer_threshold = (counters['lineitem_inserts'] * 
                                 self.table_specifications['customer_records'] / 
                                 self.table_specifications['lineitem_records'])
            
            if customer_threshold > counters['customer_inserts'] and current_records['customer_insert']:
                counters['customer_inserts'] += 1
                if self.params['enable_customer_stream']:
                    self.write_stream_record(output_writer, "+CU" + current_records['customer_insert'])
                current_records['customer_insert'] = file_streams['customer_insert'].readline()
                
                # Customer deletion processing
                customer_delete_threshold = (counters['lineitem_deletes'] * 
                                           self.table_specifications['customer_records'] / 
                                           self.table_specifications['lineitem_records'])
                
                if (customer_delete_threshold > counters['customer_deletes'] and 
                    current_records['customer_delete']):
                    counters['customer_deletes'] += 1
                    if self.params['enable_customer_stream']:
                        self.write_stream_record(output_writer, "-CU" + current_records['customer_delete'])
                    current_records['customer_delete'] = file_streams['customer_delete'].readline()
        
        # Close insertion streams
        file_streams['lineitem_insert'].close()
        file_streams['orders_insert'].close()
        file_streams['customer_insert'].close()
        
        # Phase 2: Process remaining deletion operations
        while current_records['lineitem_delete']:
            counters['lineitem_deletes'] += 1
            if self.params['enable_lineitem_stream']:
                self.write_stream_record(output_writer, "-LI" + current_records['lineitem_delete'])
            current_records['lineitem_delete'] = file_streams['lineitem_delete'].readline()
            
            # Process remaining orders deletions
            orders_delete_threshold = (counters['lineitem_deletes'] * 
                                      self.table_specifications['orders_records'] / 
                                      self.table_specifications['lineitem_records'])
            
            if (orders_delete_threshold > counters['orders_deletes'] and 
                current_records['orders_delete']):
                counters['orders_deletes'] += 1
                if self.params['enable_orders_stream']:
                    self.write_stream_record(output_writer, "-OR" + current_records['orders_delete'])
                current_records['orders_delete'] = file_streams['orders_delete'].readline()
            
            # Process remaining customer deletions
            customer_delete_threshold = (counters['lineitem_deletes'] * 
                                       self.table_specifications['customer_records'] / 
                                       self.table_specifications['lineitem_records'])
            
            if (customer_delete_threshold > counters['customer_deletes'] and 
                current_records['customer_delete']):
                counters['customer_deletes'] += 1
                if self.params['enable_customer_stream']:
                    self.write_stream_record(output_writer, "-CU" + current_records['customer_delete'])
                current_records['customer_delete'] = file_streams['customer_delete'].readline()
        
        # Close deletion streams
        file_streams['lineitem_delete'].close()
        file_streams['orders_delete'].close()
        file_streams['customer_delete'].close()
        
        # Close output stream
        output_writer.close()
        
        # Generate processing summary
        self.generate_summary_report(counters, output_path)
    
    def generate_summary_report(self, counters, output_path):
        """Generate processing statistics and summary"""
        print("")
        print("*************************************")
        print("count: ", counters['lineitem_inserts'])
        print("delete_count: ", counters['lineitem_deletes'])
        print("customer_count: ", counters['customer_inserts'])
        print("customer_delete_count: ", counters['customer_deletes'])
        print("orders_count: ", counters['orders_inserts'])
        print("orders_delete_count: ", counters['orders_deletes'])
        
        total_operations = (counters['lineitem_inserts'] + counters['lineitem_deletes'] + 
                          counters['orders_inserts'] + counters['orders_deletes'] + 
                          counters['customer_inserts'] + counters['customer_deletes'])
        print("\ntotal count: ", total_operations)
        
        # Verify output file
        with open(output_path, 'r') as verification_file:
            output_lines = sum(1 for _ in verification_file)
        print("output file lines number: ", output_lines)
        print("finished")
        print("outputfile: ", output_path)


def execute_stream_processing():
    """Main execution function for stream data processing"""
    processor = TPCHStreamProcessor()
    processor.process_unified_stream()


if __name__ == '__main__':
    execution_start = time.time()
    execute_stream_processing()
    execution_end = time.time()
    print("cpu time: ", execution_end - execution_start) 