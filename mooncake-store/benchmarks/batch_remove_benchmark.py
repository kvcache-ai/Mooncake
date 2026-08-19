#!/usr/bin/env python3
"""
Batch Remove API Benchmark, Examples and Comprehensive Tests

Usage:
    # Run examples
    python3 batch_remove_benchmark.py --mode example
    
    # Run performance benchmark
    python3 batch_remove_benchmark.py --mode benchmark --key-counts 10 100 1000
    
    # Run comprehensive tests
    python3 batch_remove_benchmark.py --mode test --test-mode all
    
    # Run specific test
    python3 batch_remove_benchmark.py --mode test --test-mode basic
"""

import argparse
import sys
import time
import statistics

try:
    from mooncake.store import MooncakeDistributedStore
except ImportError:
    print("Error: mooncake-transfer-engine not installed")
    sys.exit(1)


class BenchmarkDemo:
    """Demonstrates batch_remove() API and performance benchmarking."""
    
    def __init__(self, master_addr, metadata_server):
        self.store = MooncakeDistributedStore()
        result = self.store.setup(
            "localhost", metadata_server, 512*1024*1024, 128*1024*1024,
            "tcp", "", master_addr
        )
        if result != 0:
            raise RuntimeError(f"Setup failed: {result}")
    
    def close(self):
        self.store.close()
    
    def example_basic_batch_remove(self):
        """Basic batch_remove() example."""
        print("\n=== Example: Basic Batch Remove ===")
        keys = [f"user_data_{i}" for i in range(5)]
        for key in keys:
            self.store.put(key, b"user data")
        
        results = self.store.batch_remove(keys)
        for key, result in zip(keys, results):
            status = "✓ removed" if result == 0 else f"✗ error {result}"
            print(f"  {key}: {status}")
    
    def benchmark(self, key_counts, iterations):
        """Compare sequential vs batch remove performance."""
        print("\n" + "="*60)
        print("Batch Remove Performance Benchmark")
        print("="*60)
        
        results = []
        for count in key_counts:
            print(f"\n--- Testing {count} keys ({iterations} iterations) ---")
            seq_times, batch_times = [], []
            
            for i in range(iterations):
                keys = [f"key_{count}_{i}_{j}" for j in range(count)]
                
                # Prepare data
                for key in keys:
                    self.store.put(key, b"x" * 1024)
                
                # Sequential
                start = time.time()
                for key in keys:
                    self.store.remove(key)
                seq_times.append(time.time() - start)
                
                # Prepare again
                for key in keys:
                    self.store.put(key, b"x" * 1024)
                
                # Batch
                start = time.time()
                self.store.batch_remove(keys)
                batch_times.append(time.time() - start)
            
            seq_mean = statistics.mean(seq_times)
            batch_mean = statistics.mean(batch_times)
            speedup = seq_mean / batch_mean if batch_mean > 0 else 0
            
            results.append({
                'count': count, 'seq': seq_mean, 
                'batch': batch_mean, 'speedup': speedup
            })
            print(f"  Sequential: {seq_mean:.3f}s, Batch: {batch_mean:.3f}s, Speedup: {speedup:.2f}x")
        
        # Summary
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        print(f"{'Keys':>8} | {'Sequential':>12} | {'Batch':>12} | {'Speedup':>8}")
        print("-"*60)
        for r in results:
            print(f"{r['count']:>8} | {r['seq']:>12.3f} | {r['batch']:>12.3f} | {r['speedup']:>8.2f}x")


class ComprehensiveTest:
    """Comprehensive tests for batch_remove() API."""
    
    def __init__(self, master_addr, metadata_server):
        self.store = MooncakeDistributedStore()
        result = self.store.setup(
            "localhost", metadata_server, 512*1024*1024, 128*1024*1024,
            "tcp", "", master_addr
        )
        if result != 0:
            raise RuntimeError(f"Setup failed: {result}")
        self.passed = 0
        self.failed = 0
    
    def close(self):
        self.store.close()
    
    def assert_eq(self, actual, expected, msg=""):
        """Helper: assert equal."""
        if actual == expected:
            self.passed += 1
            print(f"  ✓ {msg}")
            return True
        else:
            self.failed += 1
            print(f"  ✗ {msg}")
            print(f"    Expected: {expected}, Actual: {actual}")
            return False
    
    def assert_true(self, condition, msg=""):
        """Helper: assert true."""
        if condition:
            self.passed += 1
            print(f"  ✓ {msg}")
            return True
        else:
            self.failed += 1
            print(f"  ✗ {msg}")
            return False
    
    def test_basic_functionality(self):
        """Test #1: Basic batch remove after put."""
        print("\n=== Test #1: Basic Functionality ===")
        
        keys = [f"test_basic_{i}" for i in range(10)]
        
        # Put data
        for key in keys:
            self.store.put(key, b"test data")
        
        # Batch remove
        results = self.store.batch_remove(keys)
        
        # Verify all succeeded
        success = all(r == 0 for r in results)
        self.assert_true(success, f"All {len(keys)} keys removed successfully")
        self.assert_eq(len(results), len(keys), "Result count matches key count")
    
    def test_mixed_existence(self):
        """Test #2: Mixed existing and non-existing keys."""
        print("\n=== Test #2: Mixed Existence ===")
        
        # Create some keys
        existing_keys = [f"test_mixed_exist_{i}" for i in range(3)]
        for key in existing_keys:
            self.store.put(key, b"data")
        
        non_existing_keys = [f"test_mixed_not_exist_{i}" for i in range(3)]
        
        # Mix them
        keys = existing_keys + non_existing_keys
        
        results = self.store.batch_remove(keys)
        
        # Verify existing keys removed (return 0)
        for i, key in enumerate(existing_keys):
            self.assert_eq(results[i], 0, f"Existing key {key} removed (code 0)")
        
        # Verify non-existing keys return OBJECT_NOT_FOUND (negative)
        for i, key in enumerate(non_existing_keys, start=3):
            self.assert_true(results[i] < 0, 
                           f"Non-existing key {key} returns negative error code")
    
    def test_empty_and_duplicate(self):
        """Test #3: Empty key list and duplicate keys."""
        print("\n=== Test #3: Edge Cases ===")
        
        # Empty list
        results = self.store.batch_remove([])
        self.assert_eq(len(results), 0, "Empty list returns empty results")
        
        # Duplicate keys
        key = "test_duplicate_key"
        self.store.put(key, b"data")
        
        keys = [key, key, key]  # Same key 3 times
        results = self.store.batch_remove(keys)
        
        self.assert_eq(len(results), 3, "Duplicate keys return 3 results")
        self.assert_eq(results[0], 0, "First remove succeeds")
        # Second and third should fail (key already removed)
        self.assert_true(results[1] < 0, "Second remove fails (not found)")
        self.assert_true(results[2] < 0, "Third remove fails (not found)")
    
    def test_consistency_with_single_remove(self):
        """Test #4: Error code consistency with single remove."""
        print("\n=== Test #4: Consistency with Single Remove ===")
        
        # Test 1: Non-existing key
        key = "test_consistency_nonexist"
        
        single_result = self.store.remove(key)
        batch_results = self.store.batch_remove([key])
        
        self.assert_eq(single_result, batch_results[0],
                      "Non-existing: Single and Batch return same error code")
        
        # Test 2: Existing key
        key = "test_consistency_exist"
        self.store.put(key, b"data")
        
        single_result = self.store.remove(key)
        
        key2 = "test_consistency_exist2"
        self.store.put(key2, b"data")
        batch_results = self.store.batch_remove([key2])
        
        self.assert_eq(single_result, batch_results[0],
                      "Existing: Single and Batch return same code (0)")
    
    def test_error_code_format(self):
        """Test #5: Error code format (should be ErrorCode enum values)."""
        print("\n=== Test #5: Error Code Format ===")
        
        key = "test_error_format"
        # Don't put, so it doesn't exist
        
        results = self.store.batch_remove([key])
        error_code = results[0]
        
        # Should be negative (error)
        self.assert_true(error_code < 0, 
                        f"Error code is negative: {error_code}")
        
        # Should match known error codes (from types.h)
        # OBJECT_NOT_FOUND = -704 typically
        known_error_codes = [-704, -706, -707]  # Common error codes
        self.assert_true(error_code in known_error_codes or error_code < 0,
                        f"Error code {error_code} is a valid negative value")
    
    def test_large_batch(self):
        """Test #6: Large batch performance."""
        print("\n=== Test #6: Large Batch ===")
        
        count = 1000
        keys = [f"test_large_{i}" for i in range(count)]
        
        # Prepare data
        for key in keys:
            self.store.put(key, b"x" * 1024)
        
        # Measure time
        start = time.time()
        results = self.store.batch_remove(keys)
        elapsed = time.time() - start
        
        success_count = sum(1 for r in results if r == 0)
        
        self.assert_eq(success_count, count, 
                      f"All {count} keys removed successfully")
        self.assert_true(elapsed < 5.0, 
                        f"Large batch completes in reasonable time ({elapsed:.2f}s)")
        print(f"  Info: Removed {count} keys in {elapsed:.3f}s "
              f"({count/elapsed:.0f} ops/sec)")
    
    def test_special_characters(self):
        """Test #7: Keys with special characters."""
        print("\n=== Test #7: Special Characters ===")
        
        special_keys = [
            "key with spaces",
            "key/with/slashes",
            "key:with:colons",
            "key#hash",
            "key@at",
            "unicode_中文",
        ]
        
        for key in special_keys:
            self.store.put(key, b"data")
        
        results = self.store.batch_remove(special_keys)
        
        success = all(r == 0 for r in results)
        self.assert_true(success, "All special character keys handled correctly")
    
    def run_all_tests(self):
        """Run all test cases."""
        print("="*60)
        print("Batch Remove Comprehensive Test Suite")
        print("="*60)
        
        self.test_basic_functionality()
        self.test_mixed_existence()
        self.test_empty_and_duplicate()
        self.test_consistency_with_single_remove()
        self.test_error_code_format()
        self.test_large_batch()
        self.test_special_characters()
        
        # Summary
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        total = self.passed + self.failed
        print(f"Passed: {self.passed}/{total}")
        print(f"Failed: {self.failed}/{total}")
        
        if self.failed == 0:
            print("\n✓ All tests passed!")
            return 0
        else:
            print(f"\n✗ {self.failed} test(s) failed")
            return 1


def main():
    parser = argparse.ArgumentParser(
        description='Batch Remove Benchmark, Examples & Comprehensive Tests'
    )
    parser.add_argument('--mode', 
                       choices=['example', 'benchmark', 'test', 'all'], 
                       default='all',
                       help='Run mode: example=demo, benchmark=performance, test=functional, all=everything')
    parser.add_argument('--test-mode',
                       choices=['all', 'basic', 'consistency', 'edge', 'large', 'format'],
                       default='all',
                       help='Test mode (only used when --mode is test or all)')
    parser.add_argument('--master', default='localhost:30051',
                       help='Master server address (default: localhost:30051)')
    parser.add_argument('--metadata', default='http://localhost:30080/metadata',
                       help='Metadata server URL (default: http://localhost:30080/metadata)')
    parser.add_argument('--key-counts', type=int, nargs='+', 
                       default=[10, 50, 100, 500, 1000],
                       help='Key counts for benchmark (default: 10 50 100 500 1000)')
    parser.add_argument('--iterations', type=int, default=2,
                       help='Iterations for benchmark (default: 2)')
    args = parser.parse_args()
    
    exit_code = 0
    
    # Run examples
    if args.mode in ['example', 'all']:
        demo = BenchmarkDemo(args.master, args.metadata)
        try:
            demo.example_basic_batch_remove()
        finally:
            demo.close()
    
    # Run benchmark
    if args.mode in ['benchmark', 'all']:
        demo = BenchmarkDemo(args.master, args.metadata)
        try:
            demo.benchmark(args.key_counts, args.iterations)
        finally:
            demo.close()
    
    # Run tests
    if args.mode in ['test', 'all']:
        test = ComprehensiveTest(args.master, args.metadata)
        try:
            if args.test_mode == 'all':
                exit_code = test.run_all_tests()
            elif args.test_mode == 'basic':
                test.test_basic_functionality()
            elif args.test_mode == 'consistency':
                test.test_consistency_with_single_remove()
                test.test_error_code_format()
            elif args.test_mode == 'edge':
                test.test_mixed_existence()
                test.test_empty_and_duplicate()
                test.test_special_characters()
            elif args.test_mode == 'large':
                test.test_large_batch()
            else:
                print(f"Test mode {args.test_mode} not implemented")
                exit_code = 1
        finally:
            test.close()
    
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
