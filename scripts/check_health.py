#!/usr/bin/env python3
"""
Verify Parser Installation and Run Health Checks

This script validates that the parser is correctly installed and configured.
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd: list, description: str) -> bool:
    """Run a command and return success status."""
    print(f"\n{'='*60}")
    print(f"✓ {description}")
    print(f"{'='*60}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        print(f"✅ {description}: PASSED")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description}: FAILED")
        print(f"Error: {e}")
        if e.stdout:
            print(f"Stdout: {e.stdout}")
        if e.stderr:
            print(f"Stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print(f"❌ {description}: COMMAND NOT FOUND")
        print(f"Command: {' '.join(cmd)}")
        return False


def main():
    """Run all health checks."""
    print("="*60)
    print("MULTI-FORMAT PARSER - HEALTH CHECK")
    print("="*60)

    checks = [
        (["python", "--version"], "Python Version"),
        (["python", "-m", "pytest", "--version"], "Pytest Installation"),
        (["python", "-c", "import lxml; print(f'lxml {lxml.__version__}')"], "LXML Installation"),
    ]

    # Optional checks
    optional_checks = [
        (["ruff", "--version"], "Ruff Linter (optional)"),
        (["black", "--version"], "Black Formatter (optional)"),
        (["mypy", "--version"], "Mypy Type Checker (optional)"),
    ]

    results = []

    # Required checks
    print("\n" + "="*60)
    print("REQUIRED DEPENDENCIES")
    print("="*60)

    for cmd, desc in checks:
        results.append(run_command(cmd, desc))

    # Optional checks
    print("\n" + "="*60)
    print("OPTIONAL DEVELOPMENT TOOLS")
    print("="*60)

    for cmd, desc in optional_checks:
        run_command(cmd, desc)  # Don't fail on these

    # Run tests if available
    tests_dir = Path("tests")
    if tests_dir.exists():
        print("\n" + "="*60)
        print("RUNNING TESTS")
        print("="*60)
        test_result = run_command(
            ["python", "-m", "pytest", "tests/", "-v", "--tb=short"],
            "Test Suite Execution"
        )
        results.append(test_result)
    else:
        print("\n⚠️  Tests directory not found. Skipping tests.")

    # Summary
    print("\n" + "="*60)
    print("HEALTH CHECK SUMMARY")
    print("="*60)

    passed = sum(results)
    total = len(results)

    if passed == total:
        print(f"✅ ALL CHECKS PASSED ({passed}/{total})")
        print("\n🎉 Your parser installation is healthy and ready to use!")
        print("\nNext steps:")
        print("  1. Review example configs: config_examples/")
        print("  2. Try dry-run: python parser.py --config config.json --out ./output --dry-run file.xml")
        print("  3. Parse real files: python parser.py --config config.json --out ./output file.xml")
        return 0
    else:
        print(f"❌ SOME CHECKS FAILED ({passed}/{total} passed)")
        print("\n⚠️  Please fix the failed checks before using the parser.")
        print("\nTroubleshooting:")
        print("  - Install dependencies: pip install -r requirements.txt")
        print("  - Install dev dependencies: pip install -e '.[dev]'")
        print("  - Check Python version: python --version (need 3.8+)")
        return 1


if __name__ == "__main__":
    sys.exit(main())
