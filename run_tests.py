"""
Script untuk menjalankan test suite dengan coverage report
"""

import subprocess
import sys
import os

def run_tests():
    """Jalankan test suite dengan coverage"""
    print("=" * 60)
    print("RUNNING TEST SUITE WITH COVERAGE")
    print("=" * 60)
    
    # Jalankan pytest dengan coverage
    result = subprocess.run([
        sys.executable, "-m", "pytest", 
        "tests/", 
        "-v", 
        "--cov=utils",
        "--cov-report=term",
        "--cov-report=html"
    ])
    
    print("\n" + "=" * 60)
    print("COVERAGE COMPLETE")
    print("=" * 60)
    
    if result.returncode == 0:
        print("✓ All tests passed!")
    else:
        print("✗ Some tests failed!")
    
    return result.returncode

if __name__ == "__main__":
    exit_code = run_tests()
    sys.exit(exit_code)