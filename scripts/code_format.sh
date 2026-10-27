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
#       --staged          Format only added/modified lines staged for commit
#       --changed-lines   Check lines changed relative to base (requires --check)
#   -h, --help            Show this help message
#
# Examples:
#   ./scripts/code_format.sh                    # Format all changed files
#   ./scripts/code_format.sh --all              # Format all C/C++ files
#   ./scripts/code_format.sh -b origin/dev      # Compare against origin/dev
#   ./scripts/code_format.sh --check            # Check without modifying files
#   ./scripts/code_format.sh --staged file.cpp  # Format staged lines (pre-commit)
#   ./scripts/code_format.sh --changed-lines --check  # Check PR-changed lines
# =============================================================================

set -e

# Default values
BASE_BRANCH="origin/main"
CHECK_MODE=false
ALL_MODE=false
STAGED_MODE=false
CHANGED_LINES_MODE=false
INPUT_FILES=()
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# File extensions to format
FILE_EXTENSIONS="\.(h|hpp|cpp|cu|cuh|c|cc|cxx)$"
GIT_CLANG_FORMAT_EXTENSIONS="h,hpp,cpp,cu,cuh,c,cc,cxx"

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

# Find clang-format binary (require version 20)
find_clang_format() {
    # Prefer clang-format-20, but also accept generic clang-format if it is version 20
    local candidates=("clang-format-20" "clang-format")
    for candidate in "${candidates[@]}"; do
        if command -v "${candidate}" &> /dev/null; then
            local version_out
            version_out=$("${candidate}" --version 2>/dev/null)
            if [[ "${version_out}" =~ version[[:space:]]+([0-9]+) ]] && [[ "${BASH_REMATCH[1]}" -eq 20 ]]; then
                echo "${candidate}"
                return 0
            fi
        fi
    done

    {
        print_error "clang-format version 20 not found."
        if command -v clang-format &> /dev/null; then
            local current
            current=$(clang-format --version 2>/dev/null | head -1)
            print_warn "Found: ${current}"
        fi
        echo ""
        print_info "Installation instructions for Ubuntu:"
        echo "  wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key | sudo tee /etc/apt/trusted.gpg.d/apt.llvm.org.asc > /dev/null"
        echo "  echo \"deb http://apt.llvm.org/\$(lsb_release -cs)/ llvm-toolchain-\$(lsb_release -cs)-20 main\" | sudo tee /etc/apt/sources.list.d/llvm-20.list"
        echo "  sudo apt-get update && sudo apt-get install -y clang-format-20"
        echo ""
        print_info "Or use the LLVM installation script:"
        echo "  wget https://apt.llvm.org/llvm.sh && chmod +x llvm.sh && sudo ./llvm.sh 20"
        echo "  sudo apt-get install -y clang-format-20"
    } >&2
    return 1
}

# Find the LLVM git integration helper. The versioned binary is preferred, but
# an unversioned helper is safe because clang-format 20 is passed via --binary.
find_git_clang_format() {
    local candidates=("git-clang-format-20" "git-clang-format")
    local candidate
    for candidate in "${candidates[@]}"; do
        if command -v "${candidate}" &> /dev/null; then
            echo "${candidate}"
            return 0
        fi
    done

    {
        print_error "A compatible git-clang-format helper was not found."
        print_info "Install the clang-format 20 package, which provides git-clang-format-20."
        echo "  sudo apt-get install -y clang-format-20"
    } >&2
    return 1
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
            --staged)
                STAGED_MODE=true
                shift
                ;;
            --changed-lines)
                CHANGED_LINES_MODE=true
                shift
                ;;
            -h|--help)
                usage
                ;;
            --)
                shift
                INPUT_FILES+=("$@")
                break
                ;;
            -*)
                print_error "Unknown option: $1"
                usage
                ;;
            *)
                INPUT_FILES+=("$1")
                shift
                ;;
        esac
    done

    if ${ALL_MODE} && ${STAGED_MODE}; then
        print_error "--all and --staged cannot be used together."
        exit 1
    fi
    if ${ALL_MODE} && ${CHANGED_LINES_MODE}; then
        print_error "--all and --changed-lines cannot be used together."
        exit 1
    fi
    if ${STAGED_MODE} && ${CHANGED_LINES_MODE}; then
        print_error "--staged and --changed-lines cannot be used together."
        exit 1
    fi
    if ${CHANGED_LINES_MODE} && ! ${CHECK_MODE}; then
        print_error "--changed-lines requires --check."
        exit 1
    fi
}

# Delegate changed-line selection and index handling to LLVM's git integration.
# A strict guard is retained because git-clang-format only checks for unstaged
# changes when formatting would actually rewrite a file.
format_selected_lines() {
    local clang_format="$1"
    local git_clang_format="$2"
    shift 2

    # With no explicit filenames, Git pathspecs select supported C/C++ files.
    # Exclude pathspecs keep vendored sources out in both automatic and explicit
    # modes without discovering and filtering files in this script.
    if [[ $# -eq 0 ]]; then
        set -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.cu' '*.cuh' '*.h' '*.hpp'
    fi
    local paths=(
        "$@"
        ':(exclude,glob)**/cachelib_memory_allocator/**'
        ':(exclude,glob)**/thirdparty/**'
    )

    print_info "Using $(${clang_format} --version)"
    if ${STAGED_MODE}; then
        print_info "Formatting only staged C/C++ line ranges"
    else
        print_info "Formatting only C/C++ line ranges changed against ${BASE_BRANCH}"
    fi
    echo ""

    if ${STAGED_MODE}; then
        local git_status
        if ! git_status=$(git -C "${PROJECT_ROOT}" status --porcelain=v1 \
            --untracked-files=no -- "${paths[@]}"); then
            print_error "Could not inspect staged and unstaged changes."
            return 1
        fi
        # Both porcelain status columns are populated when the same file has
        # changes in the index and the working tree.
        if printf '%s\n' "${git_status}" | grep -q '^[^ ][^ ]'; then
            print_error "Cannot process staged lines because a staged file also has unstaged changes."
            print_info "Stage or stash the unstaged changes, then retry."
            return 1
        fi
    else
        if ! git -C "${PROJECT_ROOT}" diff --quiet --no-ext-diff HEAD -- \
            "${paths[@]}"; then
            print_error "Cannot process committed lines because a selected file has uncommitted changes."
            print_info "Commit or stash the local changes, then retry."
            return 1
        fi
    fi

    local command=(
        "${git_clang_format}"
        --binary "${clang_format}"
        --style file
        --extensions "${GIT_CLANG_FORMAT_EXTENSIONS}"
    )
    if ${CHECK_MODE}; then
        command+=(--diff)
    fi
    if ${STAGED_MODE}; then
        command+=(--staged)
    else
        # LLVM 20 implements --diff_from_common_commit with a triple-dot
        # revision passed to diff-tree, which can produce an empty patch.
        # Resolve the merge base first and pass two concrete commits instead.
        local base_commit
        if ! base_commit=$(git -C "${PROJECT_ROOT}" merge-base \
            "${BASE_BRANCH}" HEAD); then
            print_error "Could not determine a merge base for '${BASE_BRANCH}' and HEAD."
            return 1
        fi
        command+=("${base_commit}" HEAD)
    fi
    command+=(-- "${paths[@]}")

    local status=0
    (
        cd "${PROJECT_ROOT}"
        "${command[@]}"
    ) || status=$?

    # git-clang-format returns 1 after successfully applying a rewrite. Treat
    # that as success in apply mode; in check mode it means a diff was found.
    if ! ${CHECK_MODE} && [[ ${status} -eq 1 ]]; then
        return 0
    fi
    return "${status}"
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
    if ! clang_format="$(find_clang_format)"; then
        exit 1
    fi

    if ${STAGED_MODE} || ${CHANGED_LINES_MODE}; then
        if ${CHANGED_LINES_MODE} &&
            ! git -C "${PROJECT_ROOT}" rev-parse --verify "${BASE_BRANCH}" &> /dev/null; then
            print_error "Base branch '${BASE_BRANCH}' not found."
            exit 1
        fi

        local git_clang_format
        if ! git_clang_format="$(find_git_clang_format)"; then
            exit 1
        fi
        format_selected_lines "${clang_format}" "${git_clang_format}" \
            "${INPUT_FILES[@]}"
        return
    fi

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
