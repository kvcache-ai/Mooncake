#!/bin/bash

# =============================================================================
# code_format.sh - Format C/C++ code changed in current PR
#
# This script formats all C/C++ files that have been modified in the current
# branch compared to the base branch (default: origin/main).
#
# Usage:
#   ./scripts/code_format.sh [OPTIONS]
#
# Options:
#   -a, --all             Format all C/C++ files in the project
#   -b, --base <branch>   Base branch to compare against (default: origin/main)
#   -c, --check           Check mode: only report files that need formatting
#   -h, --help            Show this help message
#
# Examples:
#   ./scripts/code_format.sh                    # Format all changed files
#   ./scripts/code_format.sh --all              # Format all C/C++ files
#   ./scripts/code_format.sh -b origin/dev      # Compare against origin/dev
#   ./scripts/code_format.sh --check            # Check without modifying files
# =============================================================================

set -e

# Default values
BASE_BRANCH="origin/main"
CHECK_MODE=false
ALL_MODE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# File extensions to format
FILE_EXTENSIONS="\.(h|hpp|cpp|cu|cuh|c|cc|cxx)$"

# Directories to exclude (add more patterns as needed)
EXCLUDE_DIRS=(
    "cachelib_memory_allocator"
    "thirdparty"
)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print usage information
usage() {
    sed -n '/^# ====/,/^# ====/p' "$0" | grep -v "^# ====" | sed 's/^# //'
    exit 0
}

# Print colored message
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Find clang-format binary (prefer version 20, fall back to any version)
find_clang_format() {
    if command -v clang-format-20 &> /dev/null; then
        echo "clang-format-20"
    elif command -v clang-format &> /dev/null; then
        # Print warning to stderr so it doesn't interfere with the return value
        >&2 print_warn "clang-format-20 not found. Using $(clang-format --version | head -1)"
        >&2 print_warn "Note: Results may differ from CI which uses clang-format-20"
        >&2 echo ""
        echo "clang-format"
    else
        print_error "clang-format not found. This project requires clang-format (preferably version 20)."
        echo ""
        print_info "Installation instructions for Ubuntu:"
        echo "  wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key | sudo tee /etc/apt/trusted.gpg.d/apt.llvm.org.asc > /dev/null"
        echo "  echo \"deb http://apt.llvm.org/\$(lsb_release -cs)/ llvm-toolchain-\$(lsb_release -cs)-20 main\" | sudo tee /etc/apt/sources.list.d/llvm-20.list"
        echo "  sudo apt-get update && sudo apt-get install -y clang-format-20"
        echo ""
        print_info "Or install default version:"
        echo "  sudo apt-get install -y clang-format"
        exit 1
    fi
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -a|--all)
                ALL_MODE=true
                shift
                ;;
            -b|--base)
                BASE_BRANCH="$2"
                shift 2
                ;;
            -c|--check)
                CHECK_MODE=true
                shift
                ;;
            -h|--help)
                usage
                ;;
            *)
                print_error "Unknown option: $1"
                usage
                ;;
        esac
    done
}

# Get list of all C/C++ files in the project
get_all_files() {
    cd "${PROJECT_ROOT}"
    
    # Find all files, respecting .gitignore, then filter by FILE_EXTENSIONS
    local all_files
    all_files=$(git ls-files --cached --others --exclude-standard 2>/dev/null || \
                find . -type f | sed 's|^\./||')
    
    # Filter by file extensions
    local result
    result=$(echo "${all_files}" | grep -E "${FILE_EXTENSIONS}" || true)
    
    # Exclude specified directories
    for pattern in "${EXCLUDE_DIRS[@]}"; do
        result=$(echo "${result}" | grep -v "${pattern}" || true)
    done
    
    echo "${result}"
}

# Get list of changed C/C++ files
get_changed_files() {
    cd "${PROJECT_ROOT}"
    
    # Get changed files compared to base branch
    local changed_files
    if git rev-parse --verify "${BASE_BRANCH}" &> /dev/null; then
        changed_files=$(git diff --name-only "${BASE_BRANCH}"...HEAD 2>/dev/null || \
                       git diff --name-only "${BASE_BRANCH}" HEAD 2>/dev/null || true)
    else
        print_warn "Base branch '${BASE_BRANCH}' not found, trying upstream tracking branch"
        # Try upstream tracking branch, then fall back to comparing all commits on current branch
        changed_files=$(git diff --name-only "@{upstream}"...HEAD 2>/dev/null || \
                       git log --name-only --pretty=format: HEAD 2>/dev/null | sort -u || true)
    fi
    
    # Filter C/C++ files and exclude specified directories
    local result
    result=$(echo "${changed_files}" | grep -E "${FILE_EXTENSIONS}" || true)
    
    for pattern in "${EXCLUDE_DIRS[@]}"; do
        result=$(echo "${result}" | grep -v "${pattern}" || true)
    done
    
    echo "${result}"
}

# Format files
format_files() {
    local files="$1"
    local clang_format="$2"
    local formatted_count=0
    local failed_count=0
    
    if [[ -z "${files}" ]]; then
        if ${ALL_MODE}; then
            print_info "No C/C++ files found in the project."
        else
            print_info "No C/C++ files changed in this PR."
        fi
        return 0
    fi
    
    print_info "Using $(${clang_format} --version)"
    if ${ALL_MODE}; then
        print_info "Formatting all C/C++ files in the project"
    else
        print_info "Comparing against: ${BASE_BRANCH}"
    fi
    echo ""
    
    while IFS= read -r file; do
        if [[ -f "${PROJECT_ROOT}/${file}" ]]; then
            if ${CHECK_MODE}; then
                # Check mode: verify if file needs formatting
                if ! ${clang_format} -style=file --dry-run -Werror "${PROJECT_ROOT}/${file}" 2>&1; then
                    print_warn "Needs formatting: ${file}"
                    failed_count=$((failed_count + 1))
                else
                    print_info "OK: ${file}"
                fi
            else
                # Format mode: apply formatting
                if ${clang_format} -style=file -i "${PROJECT_ROOT}/${file}"; then
                    # Check if file actually changed
                    if git diff --quiet "${PROJECT_ROOT}/${file}" 2>/dev/null; then
                        print_info "Already formatted: ${file}"
                    else
                        print_info "Formatted: ${file}"
                        formatted_count=$((formatted_count + 1))
                    fi
                else
                    print_error "Failed to format: ${file}"
                    failed_count=$((failed_count + 1))
                fi
            fi
        else
            print_warn "File not found (deleted?): ${file}"
        fi
    done <<< "${files}"
    
    echo ""
    if ${CHECK_MODE}; then
        if [[ ${failed_count} -gt 0 ]]; then
            print_error "${failed_count} file(s) need formatting."
            print_info "Run './scripts/code_format.sh' to format them."
            return 1
        else
            print_info "All files are properly formatted."
        fi
    else
        print_info "Formatted ${formatted_count} file(s)."
        if [[ ${failed_count} -gt 0 ]]; then
            print_error "Failed to format ${failed_count} file(s)."
            return 1
        fi
    fi
    
    return 0
}

# Main function
main() {
    parse_args "$@"
    
    print_info "Mooncake Code Format Script"
    echo ""
    
    # Check for .clang-format file
    if [[ ! -f "${PROJECT_ROOT}/.clang-format" ]]; then
        print_error ".clang-format not found in project root"
        exit 1
    fi
    
    # Find clang-format
    local clang_format
    clang_format=$(find_clang_format)
    
    # Get files to format
    local files
    if ${ALL_MODE}; then
        files=$(get_all_files)
    else
        files=$(get_changed_files)
    fi
    
    # Format files
    format_files "${files}" "${clang_format}"
}

main "$@"
