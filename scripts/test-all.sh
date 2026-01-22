#!/bin/bash
# Comprehensive test script for pgmq-relay

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to run command with status
run_test() {
    local test_name="$1"
    local command="$2"
    
    print_status "Running $test_name..."
    
    if eval "$command"; then
        print_success "$test_name passed"
        return 0
    else
        print_error "$test_name failed"
        return 1
    fi
}

# Main test execution
main() {
    print_status "Starting comprehensive test suite for pgmq-relay"
    
    local failed_tests=0
    
    # Format check
    if ! run_test "Code formatting check" "cargo fmt --check"; then
        print_warning "Code needs formatting. Run 'cargo fmt' to fix."
        ((failed_tests++))
    fi
    
    # Clippy linting
    if ! run_test "Clippy linting" "cargo clippy -- -D warnings"; then
        ((failed_tests++))
    fi
    
    # Compilation check
    if ! run_test "Compilation check" "cargo check"; then
        ((failed_tests++))
    fi
    
    # Unit tests
    if ! run_test "Unit tests" "cargo test --lib"; then
        ((failed_tests++))
    fi
    
    # Integration tests
    if ! run_test "Integration tests" "cargo test --test integration_tests"; then
        ((failed_tests++))
    fi
    
    # Transformer tests
    if ! run_test "Transformer tests" "cargo test --test transformer_tests"; then
        ((failed_tests++))
    fi
    
    # Circuit breaker tests
    if ! run_test "Circuit breaker tests" "cargo test circuit_breaker::tests"; then
        ((failed_tests++))
    fi
    
    # Security audit (non-blocking)
    if ! run_test "Security audit" "cargo audit"; then
        print_warning "Security audit found issues - review recommended"
    fi
    
    # Build release version
    if ! run_test "Release build" "cargo build --release"; then
        ((failed_tests++))
    fi
    
    # Summary
    echo ""
    if [ $failed_tests -eq 0 ]; then
        print_success "All tests passed! 🎉"
        echo ""
        print_status "Ready for deployment:"
        echo "  - Code is properly formatted"
        echo "  - No linting issues"
        echo "  - All tests pass"
        echo "  - Release build successful"
        exit 0
    else
        print_error "$failed_tests test(s) failed"
        echo ""
        print_status "Please fix the issues above before deployment"
        exit 1
    fi
}

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ] || ! grep -q "pgmq-relay" Cargo.toml; then
    print_error "This script must be run from the pgmq-relay project root directory"
    exit 1
fi

# Install required tools if missing
if ! command -v cargo-audit &> /dev/null; then
    print_status "Installing cargo-audit..."
    cargo install cargo-audit
fi

# Run main function
main "$@"