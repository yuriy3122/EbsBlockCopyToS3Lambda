# EbsBlockCopyToS3Lambda

*AWS Lambda function to copy EBS volume snapshot block data into S3-compatible storage, supporting optional encryption.*

---

## Table of Contents

* [Overview](#overview)
* [Features](#features)
* [Use Cases](#use-cases)
* [Architecture & Design](#architecture--design)
* [Configuration & Environment Variables](#configuration--environment-variables)
* [Invocation / Usage](#invocation--usage)
* [Permissions & IAM Role](#permissions--iam-role)
---

## Overview

This project implements an AWS Lambda function (written in C#) that:

1. Reads raw block data from an EBS snapshot (or a volume)
2. Streams the binary block data
3. Writes it to an S3 (or S3-compatible) bucket
4. Optionally encrypts the data in transit or at rest

This enables efficient transfer of entire block volumes to object storage, useful for backup, archival, or migration workflows.

---

## Features

* Efficient streaming of raw block data
* Supports large volumes / snapshots
* Plug-in support for encryption
* Works with S3-compatible endpoints
* Configurable via environment variables
* Minimal dependencies, designed to be lightweight

---

## Use Cases

* Backing up EBS volumes/snapshots as objects
* Restoring block volumes from object storage
* Migrating volume data to S3-backed storage tiers
* Use in DR (Disaster Recovery) pipelines
* Storing block devices snapshots off-site

---

## Architecture & Design

Simplified flow:

1. Lambda is triggered manually or via event
2. Lambda receives input (snapshot ID, volume ID, block range, etc.)
3. Using AWS SDK / EBS API, read blocks from snapshot
4. Stream block chunks (e.g. in fixed buffer sizes)
5. If encryption is enabled, encrypt chunk
6. Upload chunk(s) to S3 (multipart or chunked upload)
7. Finalize upload, handle errors / retries

---

## Prerequisites

* AWS account with permissions to access EBS snapshots, volumes, and S3
* .NET / C# development environment (matching your target Lambda runtime)
* The AWS SDK for .NET
* (Optional) KMS key if you plan to use server-side encryption
* (Optional) S3 endpoint or S3-compatible storage credentials

---

## Configuration & Environment Variables

| Variable Name      | Description                     | Example                     |
| ------------------ | ------------------------------- | --------------------------- |
| `TARGET_S3_BUCKET` | Destination bucket              | `my-ebs-backups`            |
| `S3_ENDPOINT`      | Custom S3 endpoint URL          | `https://my-s3.example.com` |
| `AWS_REGION`       | AWS region                      | `eu-west-1`                 |
| `ENCRYPTION`       | Enable encryption flag          | `true`                      |
| `KMS_KEY_ID`       | KMS key ARN / ID for encryption | `arn:aws:kms:...`           |
| `BLOCK_CHUNK_SIZE` | Size of block chunks (bytes)    | `5242880`                   |
| `MAX_RETRIES`      | Max retry count                 | `3`                         |

---

## Invocation / Usage

Invoke via AWS CLI, SDK, or API Gateway.

### Example Payload

```json
{
  "SnapshotId": "snap-0123456789abcdef0",
  "VolumeId": "vol-0abcdef1234567890",
  "StartBlock": 0,
  "BlockCount": 1000000,
  "DestinationKeyPrefix": "backups/volume1/",
  "UseEncryption": true
}
```

The function returns status and metadata (bytes uploaded, errors, etc.).

---

## Permissions & IAM Role

**EBS / EC2 Snapshot / Volume:**

* `ec2:DescribeSnapshots`
* `ec2:ReadSnapshotBlocks`
* `ec2:ListTagsForResource`

**S3:**

* `s3:PutObject`
* `s3:AbortMultipartUpload`
* `s3:ListMultipartUploadParts`

**KMS (if used):**

* `kms:Encrypt`
* `kms:GenerateDataKey`
* `kms:Decrypt`

---

## Limitations & Considerations

* Lambda timeout limit (15 min) — large volumes may exceed
* May need checkpoint/resume for big data
* Network throughput may limit speed
* Ensure multipart upload supported by destination
* Encryption adds compute overhead
* Split large workloads into smaller batches
