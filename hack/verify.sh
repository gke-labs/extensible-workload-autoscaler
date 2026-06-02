#!/bin/bash

echo "=== Verifying Code ==="

echo "[1/4] Checking Formatting..."
# Check if gofmt would make any changes.
# List files that need formatting. If output is not empty, fail.
unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
    echo "Error: The following files are not formatted:"
    echo "$unformatted"
    echo "Run 'go fmt ./...' to fix."
    exit 1
fi

echo "[2/4] Running Vet..."
if ! go vet ./...; then
    echo "Error: 'go vet' failed. Please fix the compiler/vet issues listed above."
    exit 1
fi

echo "[3/4] Building..."
if ! go build ./...; then
    echo "Error: Build failed. Please fix the compilation errors listed above."
    exit 1
fi

echo "[4/4] Running Tests..."
if ! go test ./...; then
    echo "Error: Unit tests failed. Please fix the failing tests listed above."
    exit 1
fi

echo "[5/5] Checking Code Generation..."
# Capture stderr to a temp file in case codegen fails, but keep stdout quiet
TMP_ERR=$(mktemp)
if ! ./hack/update-codegen.sh > /dev/null 2> "$TMP_ERR"; then
    echo "Error: Code generation failed!"
    cat "$TMP_ERR"
    rm -f "$TMP_ERR"
    exit 1
fi
rm -f "$TMP_ERR"

# Check for changes in generated paths
GEN_PATHS="pkg/client deploy/crd pkg/apis/xas/v1/deepcopy_generated.go api/proto/v1"
if ! git diff --quiet -- $GEN_PATHS; then
    echo "Error: Generated code is out of date."
    echo "The following files have changed:"
    git diff --name-only -- $GEN_PATHS
    echo "Run 'make codegen' and commit the changes."
    exit 1
fi

echo "=== All Checks Passed ==="
