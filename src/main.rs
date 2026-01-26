use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::{Client, config::Region, primitives::ByteStream};
use clap::{Parser, Subcommand};
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use walkdir::WalkDir;

// Multipart upload thresholds
const MULTIPART_THRESHOLD: u64 = 5 * 1024 * 1024 * 1024; // 5GB - use multipart for files larger than this
const MULTIPART_PART_SIZE: u64 = 16 * 1024 * 1024; // 16MiB per part

#[derive(Parser)]
#[command(name = "s3-migrate")]
#[command(about = "S3 migration tool for downloading and uploading files", long_about = None)]
struct Cli {
    /// S3 endpoint URL (e.g., https://s3.amazonaws.com or custom endpoint)
    #[arg(short, long, env = "S3_ENDPOINT")]
    endpoint: Option<String>,

    /// AWS region
    #[arg(short, long, default_value = "us-east-1", env = "AWS_REGION")]
    region: String,

    /// AWS access key ID
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    access_key: Option<String>,

    /// AWS secret access key
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    secret_key: Option<String>,

    /// Number of concurrent operations
    #[arg(short, long, default_value = "10")]
    concurrency: usize,

    /// Verbose output - show each file being processed
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Download all buckets and their contents to a local directory
    Download {
        /// Local directory to download files to
        #[arg(short, long, default_value = ".")]
        output: PathBuf,

        /// Specific bucket to download (downloads all if not specified)
        #[arg(short, long)]
        bucket: Option<String>,

        /// Prefix filter for objects
        #[arg(short, long)]
        prefix: Option<String>,
    },
    /// Upload local files and directories to S3
    Upload {
        /// Local directory or file to upload
        #[arg(short, long)]
        input: PathBuf,

        /// Target S3 bucket name
        #[arg(short, long)]
        bucket: String,

        /// Prefix to add to uploaded objects
        #[arg(short, long, default_value = "")]
        prefix: String,
    },
    /// List all buckets
    ListBuckets,
    /// List objects in a bucket
    ListObjects {
        /// Bucket name
        #[arg(short, long)]
        bucket: String,

        /// Prefix filter
        #[arg(short, long)]
        prefix: Option<String>,
    },
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let verbose = cli.verbose;

    if verbose {
        eprintln!("[verbose] Starting s3-migrate");
        eprintln!("[verbose] Region: {}", cli.region);
        if let Some(ref endpoint) = cli.endpoint {
            eprintln!("[verbose] Endpoint: {}", endpoint);
        }
        eprintln!("[verbose] Concurrency: {}", cli.concurrency);
    }

    let client = create_s3_client(&cli).await?;

    match cli.command {
        Commands::Download {
            output,
            bucket,
            prefix,
        } => {
            download(
                &client,
                &output,
                bucket.as_deref(),
                prefix.as_deref(),
                cli.concurrency,
                verbose,
            )
            .await?;
        }
        Commands::Upload {
            input,
            bucket,
            prefix,
        } => {
            upload(&client, &input, &bucket, &prefix, cli.concurrency, verbose).await?;
        }
        Commands::ListBuckets => {
            list_buckets(&client).await?;
        }
        Commands::ListObjects { bucket, prefix } => {
            list_objects(&client, &bucket, prefix.as_deref()).await?;
        }
    }

    Ok(())
}

async fn create_s3_client(cli: &Cli) -> Result<Client> {
    let mut config_loader =
        aws_config::defaults(BehaviorVersion::latest()).region(Region::new(cli.region.clone()));

    if let (Some(access_key), Some(secret_key)) = (&cli.access_key, &cli.secret_key) {
        config_loader = config_loader.credentials_provider(aws_sdk_s3::config::Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "cli-credentials",
        ));
    }

    let sdk_config = config_loader.load().await;

    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

    if let Some(endpoint) = &cli.endpoint {
        s3_config_builder = s3_config_builder
            .endpoint_url(endpoint)
            .force_path_style(true);
    }

    let client = Client::from_conf(s3_config_builder.build());
    Ok(client)
}

async fn list_buckets(client: &Client) -> Result<()> {
    let resp = client.list_buckets().send().await?;

    println!("Buckets:");
    for bucket in resp.buckets() {
        println!("  {}", bucket.name().unwrap_or("unknown"));
    }

    Ok(())
}

async fn list_objects(client: &Client, bucket: &str, prefix: Option<&str>) -> Result<()> {
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client.list_objects_v2().bucket(bucket);

        if let Some(p) = prefix {
            req = req.prefix(p);
        }

        if let Some(token) = &continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        for obj in resp.contents() {
            let key = obj.key().unwrap_or("unknown");
            let size = obj.size().unwrap_or(0);
            println!("  {} ({})", key, format_bytes(size as u64));
        }

        if resp.is_truncated() == Some(true) {
            continuation_token = resp.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    Ok(())
}

struct ObjectInfo {
    key: String,
    size: u64,
}

#[derive(Debug)]
struct UploadError {
    file_path: PathBuf,
    key: String,
    reason: String,
    details: Option<String>,
}

impl std::fmt::Display for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.file_path.display(), self.reason)
    }
}

impl std::error::Error for UploadError {}

fn format_s3_error<E: std::fmt::Debug>(err: &E) -> String {
    let debug_str = format!("{:?}", err);
    // Try to extract meaningful message from the debug output
    if debug_str.contains("AccessDenied") {
        "Access Denied - check your credentials and bucket permissions".to_string()
    } else if debug_str.contains("NoSuchBucket") {
        "Bucket does not exist".to_string()
    } else if debug_str.contains("InvalidAccessKeyId") {
        "Invalid access key ID".to_string()
    } else if debug_str.contains("SignatureDoesNotMatch") {
        "Invalid secret key (signature mismatch)".to_string()
    } else if debug_str.contains("RequestTimeTooSkewed") {
        "System clock is incorrect (time skew)".to_string()
    } else if debug_str.contains("SlowDown") {
        "Rate limited - too many requests".to_string()
    } else if debug_str.contains("ServiceUnavailable") {
        "S3 service unavailable - try again later".to_string()
    } else if debug_str.contains("InternalError") {
        "S3 internal error - try again later".to_string()
    } else if debug_str.contains("NoSuchKey") {
        "Object does not exist".to_string()
    } else if debug_str.contains("EntityTooLarge") {
        "File too large for single upload (max 5GB)".to_string()
    } else if debug_str.contains("InvalidBucketName") {
        "Invalid bucket name".to_string()
    } else if debug_str.contains("connection") || debug_str.contains("Connection") {
        format!(
            "Connection error - check endpoint URL and network: {}",
            debug_str
        )
    } else if debug_str.contains("timeout") || debug_str.contains("Timeout") {
        "Request timed out".to_string()
    } else if debug_str.contains("dns")
        || debug_str.contains("DNS")
        || debug_str.contains("resolve")
    {
        "DNS resolution failed - check endpoint URL".to_string()
    } else {
        debug_str
    }
}

async fn download(
    client: &Client,
    output_dir: &Path,
    bucket_filter: Option<&str>,
    prefix_filter: Option<&str>,
    concurrency: usize,
    verbose: bool,
) -> Result<()> {
    fs::create_dir_all(output_dir).await?;

    let buckets: Vec<String> = if let Some(bucket_name) = bucket_filter {
        vec![bucket_name.to_string()]
    } else {
        let resp = client.list_buckets().send().await?;
        resp.buckets()
            .iter()
            .filter_map(|b| b.name().map(|s| s.to_string()))
            .collect()
    };

    println!("Found {} bucket(s) to download", buckets.len());

    for bucket in buckets {
        println!("\nDownloading bucket: {}", bucket);
        let bucket_dir = output_dir.join(&bucket);
        fs::create_dir_all(&bucket_dir).await?;

        if verbose {
            eprintln!("[verbose] Listing objects in bucket '{}'...", bucket);
        }

        let objects = list_all_objects_with_size(client, &bucket, prefix_filter).await?;

        if objects.is_empty() {
            println!("  No objects found");
            continue;
        }

        let total_size: u64 = objects.iter().map(|o| o.size).sum();
        println!(
            "  {} objects, total size: {}",
            objects.len(),
            format_bytes(total_size)
        );

        let pb = ProgressBar::new(objects.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")?
                .progress_chars("#>-"),
        );

        let bytes_downloaded = Arc::new(AtomicU64::new(0));
        let error_count = Arc::new(AtomicU64::new(0));

        let results: Vec<Result<()>> = stream::iter(objects)
            .map(|obj| {
                let client = client.clone();
                let bucket = bucket.clone();
                let bucket_dir = bucket_dir.clone();
                let pb = pb.clone();
                let bytes_downloaded = bytes_downloaded.clone();
                let error_count = error_count.clone();
                async move {
                    let result =
                        download_object_streaming(&client, &bucket, &obj.key, &bucket_dir, verbose)
                            .await;
                    match &result {
                        Ok(_) => {
                            bytes_downloaded.fetch_add(obj.size, Ordering::Relaxed);
                        }
                        Err(e) => {
                            error_count.fetch_add(1, Ordering::Relaxed);
                            if verbose {
                                eprintln!("[verbose] Error downloading {}: {}", obj.key, e);
                            }
                        }
                    }
                    pb.inc(1);
                    let downloaded = bytes_downloaded.load(Ordering::Relaxed);
                    pb.set_message(format!("Downloaded: {}", format_bytes(downloaded)));
                    result
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        pb.finish_with_message(format!(
            "Done - Downloaded: {}",
            format_bytes(bytes_downloaded.load(Ordering::Relaxed))
        ));

        let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
        if !errors.is_empty() {
            eprintln!("  {} errors occurred during download:", errors.len());
            for err in errors.iter().take(10) {
                eprintln!("    - {}", err);
            }
            if errors.len() > 10 {
                eprintln!("    ... and {} more errors", errors.len() - 10);
            }
        }
    }

    println!("\nDownload complete!");
    Ok(())
}

async fn list_all_objects_with_size(
    client: &Client,
    bucket: &str,
    prefix: Option<&str>,
) -> Result<Vec<ObjectInfo>> {
    let mut objects = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client.list_objects_v2().bucket(bucket);

        if let Some(p) = prefix {
            req = req.prefix(p);
        }

        if let Some(token) = &continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                if !key.ends_with('/') {
                    objects.push(ObjectInfo {
                        key: key.to_string(),
                        size: obj.size().unwrap_or(0) as u64,
                    });
                }
            }
        }

        if resp.is_truncated() == Some(true) {
            continuation_token = resp.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    Ok(objects)
}

async fn download_object_streaming(
    client: &Client,
    bucket: &str,
    key: &str,
    output_dir: &Path,
    verbose: bool,
) -> Result<()> {
    let file_path = output_dir.join(key);

    if verbose {
        eprintln!(
            "[verbose] Downloading: {}/{} -> {}",
            bucket,
            key,
            file_path.display()
        );
    }

    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("Failed to download {}/{}", bucket, key))?;

    let mut file = fs::File::create(&file_path)
        .await
        .with_context(|| format!("Failed to create file: {}", file_path.display()))?;

    // Stream the body to file in chunks instead of loading entire file into memory
    let mut stream = resp.body.into_async_read();
    let mut buffer = vec![0u8; 8 * 1024 * 1024]; // 8MB buffer

    loop {
        let bytes_read = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer)
            .await
            .with_context(|| format!("Failed to read stream for {}/{}", bucket, key))?;

        if bytes_read == 0 {
            break;
        }

        file.write_all(&buffer[..bytes_read])
            .await
            .with_context(|| format!("Failed to write to file: {}", file_path.display()))?;
    }

    file.flush().await?;

    if verbose {
        eprintln!("[verbose] Completed: {}", key);
    }

    Ok(())
}

async fn upload(
    client: &Client,
    input_path: &Path,
    bucket: &str,
    prefix: &str,
    concurrency: usize,
    verbose: bool,
) -> Result<()> {
    if !input_path.exists() {
        anyhow::bail!("Input path does not exist: {:?}", input_path);
    }

    if verbose {
        eprintln!("[verbose] Scanning input path: {}", input_path.display());
    }

    // Verify bucket exists and we have access
    if verbose {
        eprintln!(
            "[verbose] Checking bucket '{}' exists and is accessible...",
            bucket
        );
    }
    match client.head_bucket().bucket(bucket).send().await {
        Ok(_) => {
            if verbose {
                eprintln!("[verbose] Bucket '{}' is accessible", bucket);
            }
        }
        Err(e) => {
            let err = e.into_service_error();
            anyhow::bail!(
                "Cannot access bucket '{}': {} - Check that the bucket exists and you have permission to write to it",
                bucket,
                format_s3_error(&err)
            );
        }
    }

    let files: Vec<PathBuf> = if input_path.is_file() {
        vec![input_path.to_path_buf()]
    } else {
        WalkDir::new(input_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .map(|e| e.path().to_path_buf())
            .collect()
    };

    if files.is_empty() {
        println!("No files found to upload");
        return Ok(());
    }

    // Calculate total size and check file accessibility
    let mut total_size: u64 = 0;
    let mut inaccessible_files: Vec<(PathBuf, String)> = Vec::new();

    for file in &files {
        match std::fs::metadata(file) {
            Ok(metadata) => {
                total_size += metadata.len();
                // Check if file is readable
                if let Err(e) = std::fs::File::open(file) {
                    inaccessible_files.push((file.clone(), format!("Cannot open: {}", e)));
                }
            }
            Err(e) => {
                inaccessible_files.push((file.clone(), format!("Cannot stat: {}", e)));
            }
        }
    }

    if !inaccessible_files.is_empty() {
        eprintln!(
            "Warning: {} file(s) cannot be read:",
            inaccessible_files.len()
        );
        for (path, reason) in inaccessible_files.iter().take(5) {
            eprintln!("  - {}: {}", path.display(), reason);
        }
        if inaccessible_files.len() > 5 {
            eprintln!("  ... and {} more", inaccessible_files.len() - 5);
        }
    }

    println!(
        "Uploading {} file(s) ({}) to bucket '{}'",
        files.len(),
        format_bytes(total_size),
        bucket
    );

    let pb = ProgressBar::new(files.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")?
            .progress_chars("#>-"),
    );

    let base_path = if input_path.is_file() {
        input_path.parent().unwrap_or(Path::new("."))
    } else {
        input_path
    };

    let bytes_uploaded = Arc::new(AtomicU64::new(0));

    let results: Vec<Result<(), UploadError>> = stream::iter(files)
        .map(|file_path| {
            let client = client.clone();
            let bucket = bucket.to_string();
            let prefix = prefix.to_string();
            let base_path = base_path.to_path_buf();
            let pb = pb.clone();
            let bytes_uploaded = bytes_uploaded.clone();
            async move {
                let file_size = std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0);
                let result =
                    upload_file(&client, &file_path, &bucket, &prefix, &base_path, verbose).await;
                match &result {
                    Ok(_) => {
                        bytes_uploaded.fetch_add(file_size, Ordering::Relaxed);
                    }
                    Err(_) => {}
                }
                pb.inc(1);
                let uploaded = bytes_uploaded.load(Ordering::Relaxed);
                pb.set_message(format!("Uploaded: {}", format_bytes(uploaded)));
                result
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    pb.finish_with_message(format!(
        "Done - Uploaded: {}",
        format_bytes(bytes_uploaded.load(Ordering::Relaxed))
    ));

    let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
    if !errors.is_empty() {
        eprintln!("\n{} errors occurred during upload:", errors.len());
        for err in errors.iter().take(10) {
            eprintln!("  - File: {}", err.file_path.display());
            eprintln!("    Key:  {}", err.key);
            eprintln!("    Error: {}", err.reason);
            if verbose {
                if let Some(ref details) = err.details {
                    eprintln!("    Details: {}", details);
                }
            }
        }
        if errors.len() > 10 {
            eprintln!("  ... and {} more errors", errors.len() - 10);
        }
    }

    println!("\nUpload complete!");
    Ok(())
}

async fn upload_file(
    client: &Client,
    file_path: &Path,
    bucket: &str,
    prefix: &str,
    base_path: &Path,
    verbose: bool,
) -> Result<(), UploadError> {
    let relative_path = file_path.strip_prefix(base_path).unwrap_or(file_path);

    let key = if prefix.is_empty() {
        relative_path.to_string_lossy().to_string()
    } else {
        format!(
            "{}/{}",
            prefix.trim_end_matches('/'),
            relative_path.to_string_lossy()
        )
    };

    let key = key.replace('\\', "/");

    if verbose {
        eprintln!(
            "[verbose] Uploading: {} -> {}/{}",
            file_path.display(),
            bucket,
            key
        );
    }

    // Check file size
    let file_size = match std::fs::metadata(file_path) {
        Ok(m) => m.len(),
        Err(e) => {
            return Err(UploadError {
                file_path: file_path.to_path_buf(),
                key: key.clone(),
                reason: format!("Cannot read file metadata: {}", e),
                details: Some(format!("OS error: {:?}", e.kind())),
            });
        }
    };

    if verbose {
        eprintln!(
            "[verbose] File size: {} ({})",
            file_size,
            format_bytes(file_size)
        );
    }

    // Use multipart upload for large files
    if file_size > MULTIPART_THRESHOLD {
        if verbose {
            eprintln!(
                "[verbose] Using multipart upload for large file ({})",
                format_bytes(file_size)
            );
        }
        return upload_multipart(client, file_path, bucket, &key, file_size, verbose).await;
    }

    // Single part upload for smaller files
    let body = match ByteStream::from_path(file_path).await {
        Ok(b) => b,
        Err(e) => {
            return Err(UploadError {
                file_path: file_path.to_path_buf(),
                key: key.clone(),
                reason: format!("Failed to open file for reading: {}", e),
                details: Some("Check file permissions and that the file still exists".to_string()),
            });
        }
    };

    match client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(body)
        .send()
        .await
    {
        Ok(_) => {
            if verbose {
                eprintln!("[verbose] Completed: {}", key);
            }
            Ok(())
        }
        Err(e) => {
            let service_err = e.into_service_error();
            let reason = format_s3_error(&service_err);
            Err(UploadError {
                file_path: file_path.to_path_buf(),
                key,
                reason,
                details: Some(format!("{:?}", service_err)),
            })
        }
    }
}

async fn upload_multipart(
    client: &Client,
    file_path: &Path,
    bucket: &str,
    key: &str,
    file_size: u64,
    verbose: bool,
) -> Result<(), UploadError> {
    // Create multipart upload
    let create_response = match client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let service_err = e.into_service_error();
            return Err(UploadError {
                file_path: file_path.to_path_buf(),
                key: key.to_string(),
                reason: format!(
                    "Failed to create multipart upload: {}",
                    format_s3_error(&service_err)
                ),
                details: Some(format!("{:?}", service_err)),
            });
        }
    };

    let upload_id = create_response.upload_id().ok_or_else(|| UploadError {
        file_path: file_path.to_path_buf(),
        key: key.to_string(),
        reason: "No upload ID returned from create_multipart_upload".to_string(),
        details: None,
    })?;

    if verbose {
        eprintln!("[verbose] Created multipart upload with ID: {}", upload_id);
    }

    // Calculate parts
    let num_parts = (file_size + MULTIPART_PART_SIZE - 1) / MULTIPART_PART_SIZE;
    if verbose {
        eprintln!(
            "[verbose] Uploading in {} parts of {} each",
            num_parts,
            format_bytes(MULTIPART_PART_SIZE)
        );
    }

    // Upload parts
    let completed_parts: Arc<Mutex<Vec<CompletedPart>>> = Arc::new(Mutex::new(Vec::new()));
    let mut part_uploads = Vec::new();

    for part_number in 1..=num_parts {
        let start = (part_number - 1) * MULTIPART_PART_SIZE;
        let end = std::cmp::min(start + MULTIPART_PART_SIZE, file_size);
        let part_size = end - start;

        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        let file_path = file_path.to_path_buf();
        let completed_parts = completed_parts.clone();

        part_uploads.push(async move {
            upload_part(
                &client,
                &file_path,
                &bucket,
                &key,
                &upload_id,
                part_number as i32,
                start,
                part_size,
                completed_parts,
                verbose,
            )
            .await
        });
    }

    // Execute part uploads with limited concurrency (4 concurrent parts)
    let results: Vec<Result<(), UploadError>> = stream::iter(part_uploads)
        .buffer_unordered(4)
        .collect()
        .await;

    // Check for errors
    let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
    if !errors.is_empty() {
        // Abort the multipart upload
        if verbose {
            eprintln!("[verbose] Aborting multipart upload due to errors");
        }
        let _ = client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;

        return Err(UploadError {
            file_path: file_path.to_path_buf(),
            key: key.to_string(),
            reason: format!("Multipart upload failed: {} part(s) failed", errors.len()),
            details: Some(
                errors
                    .iter()
                    .map(|e| e.reason.clone())
                    .collect::<Vec<_>>()
                    .join("; "),
            ),
        });
    }

    // Complete the multipart upload
    let mut parts = completed_parts.lock().await;
    parts.sort_by_key(|p| p.part_number());

    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts.clone()))
        .build();

    match client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
    {
        Ok(_) => {
            if verbose {
                eprintln!("[verbose] Completed multipart upload: {}", key);
            }
            Ok(())
        }
        Err(e) => {
            let service_err = e.into_service_error();
            Err(UploadError {
                file_path: file_path.to_path_buf(),
                key: key.to_string(),
                reason: format!(
                    "Failed to complete multipart upload: {}",
                    format_s3_error(&service_err)
                ),
                details: Some(format!("{:?}", service_err)),
            })
        }
    }
}

async fn upload_part(
    client: &Client,
    file_path: &Path,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    start: u64,
    part_size: u64,
    completed_parts: Arc<Mutex<Vec<CompletedPart>>>,
    verbose: bool,
) -> Result<(), UploadError> {
    if verbose {
        eprintln!(
            "[verbose] Uploading part {} (offset: {}, size: {})",
            part_number,
            format_bytes(start),
            format_bytes(part_size)
        );
    }

    // Read the part from file
    let mut file = match fs::File::open(file_path).await {
        Ok(f) => f,
        Err(e) => {
            return Err(UploadError {
                file_path: file_path.to_path_buf(),
                key: key.to_string(),
                reason: format!("Failed to open file for part {}: {}", part_number, e),
                details: None,
            });
        }
    };

    // Seek to the start position
    if let Err(e) = tokio::io::AsyncSeekExt::seek(&mut file, std::io::SeekFrom::Start(start)).await
    {
        return Err(UploadError {
            file_path: file_path.to_path_buf(),
            key: key.to_string(),
            reason: format!("Failed to seek in file for part {}: {}", part_number, e),
            details: None,
        });
    }

    // Read the part data
    let mut buffer = vec![0u8; part_size as usize];
    if let Err(e) = file.read_exact(&mut buffer).await {
        return Err(UploadError {
            file_path: file_path.to_path_buf(),
            key: key.to_string(),
            reason: format!("Failed to read file for part {}: {}", part_number, e),
            details: None,
        });
    }

    let body = ByteStream::from(buffer);

    // Upload the part
    let upload_result = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .body(body)
        .send()
        .await;

    match upload_result {
        Ok(response) => {
            let etag = response.e_tag().unwrap_or_default().to_string();
            if verbose {
                eprintln!("[verbose] Part {} completed, ETag: {}", part_number, etag);
            }

            let completed_part = CompletedPart::builder()
                .part_number(part_number)
                .e_tag(etag)
                .build();

            completed_parts.lock().await.push(completed_part);
            Ok(())
        }
        Err(e) => {
            let service_err = e.into_service_error();
            Err(UploadError {
                file_path: file_path.to_path_buf(),
                key: key.to_string(),
                reason: format!(
                    "Failed to upload part {}: {}",
                    part_number,
                    format_s3_error(&service_err)
                ),
                details: Some(format!("{:?}", service_err)),
            })
        }
    }
}
