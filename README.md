# s3-migrate

(warning unmaintained - use the rustfs cli instead https://github.com/rustfs/cli)

A command-line tool for migrating data to and from S3-compatible storage.

## Features

- Download all buckets or specific buckets to local storage
- Upload files and directories to S3
- Multipart upload support for files over 5GB
- Concurrent transfers with configurable parallelism
- Progress tracking with real-time byte counters
- Support for custom S3-compatible endpoints (MinIO, DigitalOcean Spaces, etc.)

## Installation

```bash
cargo build --release
```

The binary will be available at `./target/release/s3-migrate`.

## Usage

```
s3-migrate [OPTIONS] <COMMAND>

Commands:
  download      Download buckets and objects to local directory
  upload        Upload local files and directories to S3
  list-buckets  List all buckets
  list-objects  List objects in a bucket

Options:
  -e, --endpoint <ENDPOINT>        S3 endpoint URL
  -r, --region <REGION>            AWS region [default: us-east-1]
      --access-key <ACCESS_KEY>    AWS access key ID
      --secret-key <SECRET_KEY>    AWS secret access key
  -c, --concurrency <CONCURRENCY>  Concurrent operations [default: 10]
  -v, --verbose                    Verbose output
  -h, --help                       Print help
```

## Examples

### Download all buckets

```bash
s3-migrate download -o ./backup
```

### Download a specific bucket

```bash
s3-migrate download -b my-bucket -o ./backup
```

### Download with prefix filter

```bash
s3-migrate download -b my-bucket -p "logs/2024/" -o ./backup
```

### Upload a directory

```bash
s3-migrate upload -i ./data -b my-bucket
```

### Upload with prefix

```bash
s3-migrate upload -i ./data -b my-bucket -p "backups/2024"
```

### Using a custom endpoint

```bash
s3-migrate -e https://s3.example.com download -b my-bucket -o ./backup
```

### Using environment variables

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export S3_ENDPOINT=https://s3.example.com
export AWS_REGION=us-west-2

s3-migrate download -o ./backup
```

## Configuration

Credentials can be provided via:

1. Command-line arguments (`--access-key`, `--secret-key`)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. AWS credentials file (~/.aws/credentials)
