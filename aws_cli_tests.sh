#!/bin/bash

# This script tests Alarik using AWS CLI.

export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_DEFAULT_REGION="us-east-1"

ENDPOINT="http://localhost:8080"

aws_s3() {
    aws --endpoint-url "$ENDPOINT" --region us-east-1 "$@"
}

# Create buckets without versioning (default is disabled)
echo "Creating buckets without versioning..."
aws_s3 s3 mb s3://bucket-no-ver-1
aws_s3 s3 mb s3://bucket-no-ver-2

# Create buckets with versioning
echo "Creating buckets with versioning..."
aws_s3 s3 mb s3://bucket-ver-1
aws_s3 s3 mb s3://bucket-ver-2
aws_s3 s3api put-bucket-versioning --bucket bucket-ver-1 --versioning-configuration Status=Enabled
aws_s3 s3api put-bucket-versioning --bucket bucket-ver-2 --versioning-configuration Status=Enabled

# Upload test data to buckets without versioning
echo "Uploading test data to non-versioned buckets..."
echo "Initial content v1" | aws_s3 s3 cp - s3://bucket-no-ver-1/test.txt
echo "Initial content v1" | aws_s3 s3 cp - s3://bucket-no-ver-2/test.txt

# Overwrite with new content
echo "Updated content v2" | aws_s3 s3 cp - s3://bucket-no-ver-1/test.txt
echo "Updated content v2" | aws_s3 s3 cp - s3://bucket-no-ver-2/test.txt

# Verify non-versioned buckets: should only have v2, no versions
echo "Verifying non-versioned buckets..."
for bucket in bucket-no-ver-1 bucket-no-ver-2; do
    content=$(aws_s3 s3 cp s3://$bucket/test.txt -)
    if [ "$content" == "Updated content v2" ]; then
        echo "PASS: $bucket has expected content 'v2' (overwritten)."
    else
        echo "FAIL: $bucket has unexpected content: $content"
    fi

    # Check list-object-versions (should show only one version or none explicitly)
    versions=$(aws_s3 s3api list-object-versions --bucket $bucket --prefix test.txt | jq '.Versions | length')
    if [ "$versions" -le 1 ]; then  # Non-versioned may show current as one "version"
        echo "PASS: $bucket has no versioning (1 or fewer versions)."
    else
        echo "FAIL: $bucket unexpectedly has multiple versions: $versions"
    fi
done

# Upload test data to versioned buckets, with metadata
echo "Uploading test data to versioned buckets..."
echo "Initial content v1" | aws_s3 s3 cp - s3://bucket-ver-1/test.txt --metadata "key1=value1"
echo "Initial content v1" | aws_s3 s3 cp - s3://bucket-ver-2/test.txt --metadata "key1=value1"

# Overwrite with new content and different metadata
echo "Updated content v2" | aws_s3 s3 cp - s3://bucket-ver-1/test.txt --metadata "key2=value2"
echo "Updated content v2" | aws_s3 s3 cp - s3://bucket-ver-2/test.txt --metadata "key2=value2"

# Verify versioned buckets: should have v2 current, two versions, check metadata
echo "Verifying versioned buckets..."
for bucket in bucket-ver-1 bucket-ver-2; do
    # Check current content
    content=$(aws_s3 s3 cp s3://$bucket/test.txt -)
    if [ "$content" == "Updated content v2" ]; then
        echo "PASS: $bucket current content is 'v2'."
    else
        echo "FAIL: $bucket current content: $content"
    fi

    # Check metadata of current object
    metadata=$(aws_s3 s3api head-object --bucket $bucket --key test.txt | jq '.Metadata.key2')
    if [ "$metadata" == '"value2"' ]; then
        echo "PASS: $bucket current metadata is correct."
    else
        echo "FAIL: $bucket current metadata: $metadata"
    fi

    # List versions and get the previous version ID
    versions_output=$(aws_s3 s3api list-object-versions --bucket $bucket --prefix test.txt)
    version_count=$(echo "$versions_output" | jq '.Versions | length')
    if [ "$version_count" -eq 2 ]; then
        echo "PASS: $bucket has 2 versions as expected."
    else
        echo "FAIL: $bucket has $version_count versions."
    fi

    # Get the older version ID (assuming the first in list is latest, second is older)
    older_version_id=$(echo "$versions_output" | jq -r '.Versions[1].VersionId')

    # Download older version and check content
    older_content=$(aws_s3 s3api get-object --bucket $bucket --key test.txt --version-id "$older_version_id" /dev/stdout 2>/dev/null | head -n 1)
    if [ "$older_content" == "Initial content v1" ]; then
        echo "PASS: $bucket older version content is 'v1'."
    else
        echo "FAIL: $bucket older version content: $older_content"
    fi

    # Check metadata of older version
    older_metadata=$(aws_s3 s3api head-object --bucket $bucket --key test.txt --version-id "$older_version_id" | jq '.Metadata.key1')
    if [ "$older_metadata" == '"value1"' ]; then
        echo "PASS: $bucket older metadata is correct."
    else
        echo "FAIL: $bucket older metadata: $older_metadata"
    fi
done

echo ""
echo "=== Multipart Upload Tests ==="

# Create a bucket for multipart tests
echo "Creating bucket for multipart tests..."
aws_s3 s3 mb s3://multipart-test-bucket

# Create a test file larger than 5MB to trigger multipart upload
# AWS CLI uses multipart for files > 8MB by default, but we can force it with smaller threshold
echo "Creating test file for multipart upload..."
TEST_FILE=$(mktemp)
dd if=/dev/urandom of="$TEST_FILE" bs=1M count=10 2>/dev/null
ORIGINAL_MD5=$(md5 -q "$TEST_FILE" 2>/dev/null || md5sum "$TEST_FILE" | cut -d' ' -f1)

# Upload using multipart (AWS CLI will automatically use multipart for large files)
echo "Uploading file using multipart upload..."
aws_s3 s3 cp "$TEST_FILE" s3://multipart-test-bucket/large-file.bin --expected-size 10485760

# Verify the upload
echo "Verifying multipart upload..."
DOWNLOADED_FILE=$(mktemp)
aws_s3 s3 cp s3://multipart-test-bucket/large-file.bin "$DOWNLOADED_FILE"
DOWNLOADED_MD5=$(md5 -q "$DOWNLOADED_FILE" 2>/dev/null || md5sum "$DOWNLOADED_FILE" | cut -d' ' -f1)

if [ "$ORIGINAL_MD5" == "$DOWNLOADED_MD5" ]; then
    echo "PASS: Multipart upload - file integrity verified (MD5 match)."
else
    echo "FAIL: Multipart upload - file integrity check failed."
    echo "  Original MD5:   $ORIGINAL_MD5"
    echo "  Downloaded MD5: $DOWNLOADED_MD5"
fi

# Test multipart upload with s3api (manual control)
echo ""
echo "Testing manual multipart upload with s3api..."

# Create multipart upload
echo "Creating multipart upload..."
CREATE_RESPONSE=$(aws_s3 s3api create-multipart-upload --bucket multipart-test-bucket --key manual-multipart.txt --content-type "text/plain")
UPLOAD_ID=$(echo "$CREATE_RESPONSE" | jq -r '.UploadId')

if [ -n "$UPLOAD_ID" ] && [ "$UPLOAD_ID" != "null" ]; then
    echo "PASS: CreateMultipartUpload returned UploadId: $UPLOAD_ID"
else
    echo "FAIL: CreateMultipartUpload did not return UploadId"
    echo "  Response: $CREATE_RESPONSE"
fi

# Create part files
PART1_FILE=$(mktemp)
PART2_FILE=$(mktemp)
echo "This is part 1 of the multipart upload. " > "$PART1_FILE"
echo "This is part 2 of the multipart upload." > "$PART2_FILE"

# Upload parts
echo "Uploading parts..."
PART1_RESPONSE=$(aws_s3 s3api upload-part --bucket multipart-test-bucket --key manual-multipart.txt --part-number 1 --upload-id "$UPLOAD_ID" --body "$PART1_FILE")
ETAG1=$(echo "$PART1_RESPONSE" | jq -r '.ETag')

PART2_RESPONSE=$(aws_s3 s3api upload-part --bucket multipart-test-bucket --key manual-multipart.txt --part-number 2 --upload-id "$UPLOAD_ID" --body "$PART2_FILE")
ETAG2=$(echo "$PART2_RESPONSE" | jq -r '.ETag')

if [ -n "$ETAG1" ] && [ "$ETAG1" != "null" ]; then
    echo "PASS: UploadPart 1 returned ETag: $ETAG1"
else
    echo "FAIL: UploadPart 1 did not return ETag"
fi

if [ -n "$ETAG2" ] && [ "$ETAG2" != "null" ]; then
    echo "PASS: UploadPart 2 returned ETag: $ETAG2"
else
    echo "FAIL: UploadPart 2 did not return ETag"
fi

# List parts
echo "Listing parts..."
LIST_PARTS_RESPONSE=$(aws_s3 s3api list-parts --bucket multipart-test-bucket --key manual-multipart.txt --upload-id "$UPLOAD_ID")
PARTS_COUNT=$(echo "$LIST_PARTS_RESPONSE" | jq '.Parts | length')

if [ "$PARTS_COUNT" -eq 2 ]; then
    echo "PASS: ListParts shows 2 parts."
else
    echo "FAIL: ListParts shows $PARTS_COUNT parts (expected 2)."
fi

# Complete multipart upload
echo "Completing multipart upload..."
COMPLETE_RESPONSE=$(aws_s3 s3api complete-multipart-upload \
    --bucket multipart-test-bucket \
    --key manual-multipart.txt \
    --upload-id "$UPLOAD_ID" \
    --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$ETAG1},{\"PartNumber\":2,\"ETag\":$ETAG2}]}")

FINAL_ETAG=$(echo "$COMPLETE_RESPONSE" | jq -r '.ETag')

if [ -n "$FINAL_ETAG" ] && [ "$FINAL_ETAG" != "null" ]; then
    echo "PASS: CompleteMultipartUpload returned ETag: $FINAL_ETAG"
else
    echo "FAIL: CompleteMultipartUpload did not return ETag"
    echo "  Response: $COMPLETE_RESPONSE"
fi

# Verify the completed object
echo "Verifying completed multipart object..."
CONTENT=$(aws_s3 s3 cp s3://multipart-test-bucket/manual-multipart.txt -)
# Part 1 ends with " \n" (space + newline), Part 2 ends with "\n" (just newline)
# echo adds newline, so: "text1. \n" + "text2.\n" = expected content
# AWS CLI s3 cp to stdout preserves the content exactly

# Use process substitution to get exact expected content (same as what was uploaded)
EXPECTED_PART1="This is part 1 of the multipart upload. "
EXPECTED_PART2="This is part 2 of the multipart upload."
# Combine with newlines (echo adds \n to each)
EXPECTED=$(printf "%s\n%s\n" "$EXPECTED_PART1" "$EXPECTED_PART2")

if [ "$CONTENT" == "$EXPECTED" ]; then
    echo "PASS: Completed multipart object has correct content."
else
    echo "FAIL: Completed multipart object has unexpected content."
    echo "  Content length: ${#CONTENT}"
    echo "  Expected length: ${#EXPECTED}"
    # Debug: show hex for comparison if lengths differ
    if [ "${#CONTENT}" != "${#EXPECTED}" ]; then
        echo "  Expected hex: $(echo -n "$EXPECTED" | xxd -p | head -c 200)"
        echo "  Got hex: $(echo -n "$CONTENT" | xxd -p | head -c 200)"
    fi
fi

# Test ListMultipartUploads
echo ""
echo "Testing ListMultipartUploads..."

# Create a new upload but don't complete it
CREATE_RESPONSE2=$(aws_s3 s3api create-multipart-upload --bucket multipart-test-bucket --key incomplete-upload.txt)
UPLOAD_ID2=$(echo "$CREATE_RESPONSE2" | jq -r '.UploadId')

# List in-progress uploads
LIST_UPLOADS_RESPONSE=$(aws_s3 s3api list-multipart-uploads --bucket multipart-test-bucket)
UPLOADS_COUNT=$(echo "$LIST_UPLOADS_RESPONSE" | jq '.Uploads | length')

if [ "$UPLOADS_COUNT" -ge 1 ]; then
    echo "PASS: ListMultipartUploads shows $UPLOADS_COUNT in-progress upload(s)."
else
    echo "FAIL: ListMultipartUploads shows no uploads."
fi

# Test AbortMultipartUpload
echo "Testing AbortMultipartUpload..."
aws_s3 s3api abort-multipart-upload --bucket multipart-test-bucket --key incomplete-upload.txt --upload-id "$UPLOAD_ID2"

# Verify upload was aborted
LIST_UPLOADS_AFTER=$(aws_s3 s3api list-multipart-uploads --bucket multipart-test-bucket)
UPLOADS_AFTER=$(echo "$LIST_UPLOADS_AFTER" | jq '.Uploads | length // 0')

if [ "$UPLOADS_AFTER" -eq 0 ]; then
    echo "PASS: AbortMultipartUpload - upload was aborted."
else
    echo "FAIL: AbortMultipartUpload - upload still exists."
fi

# Cleanup temp files
rm -f "$TEST_FILE" "$DOWNLOADED_FILE" "$PART1_FILE" "$PART2_FILE"

echo ""
echo "=== Multipart Upload Tests Complete ==="
echo ""

echo "Test complete."