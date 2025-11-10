use anyhow::{bail, Context, Result};
#[cfg(feature = "oss")]
use {
    aws_config::meta::region::RegionProviderChain, aws_config::BehaviorVersion,
    aws_sdk_s3::config::Region, aws_sdk_s3::primitives::ByteStream,
};
use chrono::Utc;
use chrono_tz::Asia::Tokyo;
use clap::Parser;
use futures::StreamExt;
use image::{
    codecs::{avif::AvifEncoder, jpeg::JpegEncoder, webp::WebPEncoder},
    imageops, GenericImageView, ImageFormat, RgbaImage,
};
use mozjpeg::{ColorSpace, Compress};
use reqwest::header::{self, HeaderMap};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256, Sha512};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::sync::Mutex;
use tokio_stream;

// --- 命令行和配置结构 ---
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(short, long, value_name = "FILE", default_value = "config.toml")]
    config: PathBuf,
    #[clap(short, long)]
    daemon: bool,
    #[clap(long, value_name = "FILE_PATH")]
    log: Option<Option<PathBuf>>,
}

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum ImageFormatType {
    Webp,
    Jpeg,
    Mozjpeg,
    Avif,
}

#[derive(Deserialize, Debug, Clone, Copy)]
struct ImageOutputConfig {
    format: ImageFormatType,
    quality: Option<u8>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
struct Config {
    download_concurrent_limit: usize,
    task_concurrent_limit: usize,
    subscriptions: Vec<String>,
    cookie: Option<String>,
    #[serde(default)]
    daemon: Option<DaemonConfig>,
    output: StorageConfig,
    image_output: Option<ImageOutputConfig>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
struct DaemonConfig {
    interval_seconds: u64,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StorageConfig {
    Local {
        path: String,
    },
    #[cfg(feature = "oss")]
    S3 {
        endpoint: String,
        bucket: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
        base_path: Option<String>,
    },
}

// --- 对象存储客户端 ---
enum StorageClient {
    Local {
        root: PathBuf,
    },
    #[cfg(feature = "oss")]
    S3 {
        client: aws_sdk_s3::Client,
        bucket: String,
        base_path: String,
    },
}

impl StorageClient {
    async fn new(config: &StorageConfig) -> Result<Self> {
        match config {
            StorageConfig::Local { path } => Ok(StorageClient::Local {
                root: PathBuf::from(path),
            }),
            #[cfg(feature = "oss")]
            StorageConfig::S3 {
                endpoint,
                bucket,
                region,
                access_key_id,
                secret_access_key,
                base_path,
            } => {
                let region_provider = RegionProviderChain::first_try(Region::new(region.clone()));
                let creds = aws_sdk_s3::config::Credentials::new(
                    access_key_id,
                    secret_access_key,
                    None,
                    None,
                    "Static",
                );
                let sdk_config = aws_config::defaults(BehaviorVersion::latest())
                    .region(region_provider)
                    .credentials_provider(creds)
                    .endpoint_url(endpoint)
                    .load()
                    .await;
                let client = aws_sdk_s3::Client::new(&sdk_config);
                println!("[信息] S3/OSS 客户端初始化成功，目标存储桶: {}", bucket);
                Ok(StorageClient::S3 {
                    client,
                    bucket: bucket.clone(),
                    base_path: base_path.clone().unwrap_or_default(),
                })
            }
        }
    }
    async fn save(&self, key: &Path, data: Vec<u8>) -> Result<()> {
        match self {
            StorageClient::Local { root } => {
                let full_path = root.join(key);
                if let Some(parent) = full_path.parent() {
                    fs::create_dir_all(parent).await?;
                }
                fs::write(&full_path, data).await?;
                Ok(())
            }
            #[cfg(feature = "oss")]
            StorageClient::S3 {
                client,
                bucket,
                base_path,
            } => {
                let full_key = Path::new(base_path).join(key);
                let stream = ByteStream::from(data);
                client
                    .put_object()
                    .bucket(bucket)
                    .key(full_key.to_string_lossy())
                    .body(stream)
                    .send()
                    .await?;
                Ok(())
            }
        }
    }
    async fn read_to_vec(&self, key: &Path) -> Result<Option<Vec<u8>>> {
        match self {
            StorageClient::Local { root } => {
                let full_path = root.join(key);
                if !fs::try_exists(&full_path).await? {
                    return Ok(None);
                }
                Ok(Some(fs::read(&full_path).await?))
            }
            #[cfg(feature = "oss")]
            StorageClient::S3 {
                client,
                bucket,
                base_path,
            } => {
                let full_key = Path::new(base_path).join(key);
                match client
                    .get_object()
                    .bucket(bucket)
                    .key(full_key.to_string_lossy())
                    .send()
                    .await
                {
                    Ok(resp) => Ok(Some(resp.body.collect().await?.into_bytes().to_vec())),
                    Err(sdk_err) => match sdk_err.into_service_error() {
                        e if e.is_no_such_key() => Ok(None),
                        e => Err(e.into()),
                    },
                }
            }
        }
    }
}

// --- Ciao Shogakukan API 数据结构 ---
#[derive(Deserialize, Debug)]
struct SearchResponse {
    title_list: Vec<SeriesInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SeriesInfo {
    title_id: u32,
    title_name: String,
    author_text: String,
    introduction_text: String,
    thumbnail_rect_image_url: String,
    episode_id_list: Vec<u32>,
    genre_id_list: Vec<u32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SeriesMetadata {
    title_id: u32,
    title_name: String,
    author: String,
    description: String,
    cover_url: String,
    genres: Vec<String>,
    tags: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct EpisodeListResponse {
    episode_list: Vec<EpisodeInfo>,
}

#[derive(Deserialize, Debug, Clone)]
struct EpisodeInfo {
    episode_id: u32,
    episode_name: String,
    point: u32,
}

#[derive(Deserialize, Debug)]
struct EpisodeViewerResponse {
    page_list: Vec<String>,
    scramble_seed: u32,
    // 更新: 添加 scramble_ver 字段
    scramble_ver: Option<u32>,
}

#[derive(Deserialize, Debug)]
struct GenreListResponse {
    genre_list: Vec<GenreInfo>,
}
#[derive(Deserialize, Debug)]
struct GenreInfo {
    genre_name: String,
}

#[derive(Deserialize, Debug)]
struct TagListResponse {
    tag_info_list: Vec<TagInfo>,
}
#[derive(Deserialize, Debug)]
struct TagInfo {
    tag_name: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct DownloadLog {
    series_id: u32,
    series_title: String,
    chapters: HashMap<String, ChapterLog>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ChapterLog {
    title: String,
    completed: bool,
}

// --- 图像处理和辅助函数 ---
struct Xorshift {
    state: u32,
}
impl Iterator for Xorshift {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 17;
        self.state ^= self.state << 5;
        Some(self.state)
    }
}

fn seeded_shuffle(indices: &mut Vec<usize>, seed: u32) {
    let mut generator = Xorshift { state: seed };
    let mut pairs: Vec<(u32, usize)> = indices
        .iter()
        .map(|&val| (generator.next().unwrap(), val))
        .collect();
    pairs.sort_by_key(|k| k.0);
    *indices = pairs.into_iter().map(|(_, val)| val).collect();
}

// 更新: 这是图像还原逻辑的核心调度器
// 它接收加扰版本号 `scramble_ver`，并根据其值选择调用 V1 或 V2 的实现
fn restore_image(scrambled_data: &[u8], seed: u32, ver: u32) -> Result<RgbaImage> {
    let scrambled_img =
        image::load_from_memory_with_format(scrambled_data, ImageFormat::Jpeg)?.to_rgba8();

    match ver {
        2 => restore_image_v2(&scrambled_img, seed),
        _ => restore_image_v1(&scrambled_img, seed),
    }
}

// 这是原始的、用于 scramble_ver: 1 的图像还原逻辑
// 计算方式为：有效尺寸 = (总尺寸 / 8) * 8，图块尺寸 = 有效尺寸 / 4
fn restore_image_v1(scrambled_img: &RgbaImage, seed: u32) -> Result<RgbaImage> {
    let (width, height) = scrambled_img.dimensions();

    const GRID_DIMENSION: u32 = 4;
    const ALIGNMENT_VALUE: u32 = 8;

    let effective_width = (width / ALIGNMENT_VALUE) * ALIGNMENT_VALUE;
    let effective_height = (height / ALIGNMENT_VALUE) * ALIGNMENT_VALUE;

    if effective_width < GRID_DIMENSION || effective_height < GRID_DIMENSION {
        bail!("V1: 计算出的有效尺寸过小，无法还原图片。");
    }

    let tile_width = effective_width / GRID_DIMENSION;
    let tile_height = effective_height / GRID_DIMENSION;

    // 先覆盖基础图像，再根据种子重排图块
    let mut restored_image = RgbaImage::new(width, height);
    imageops::overlay(&mut restored_image, scrambled_img, 0, 0);

    let num_cells = (GRID_DIMENSION * GRID_DIMENSION) as usize;
    let mut indices: Vec<usize> = (0..num_cells).collect();
    seeded_shuffle(&mut indices, seed);

    for (dest_index, &source_index) in indices.iter().enumerate() {
        let source_col = source_index as u32 % GRID_DIMENSION;
        let source_row = source_index as u32 / GRID_DIMENSION;
        let dest_col = dest_index as u32 % GRID_DIMENSION;
        let dest_row = dest_index as u32 / GRID_DIMENSION;

        let src_x = source_col * tile_width;
        let src_y = source_row * tile_height;
        let dest_x = dest_col * tile_width;
        let dest_y = dest_row * tile_height;

        let block_view = scrambled_img.view(src_x, src_y, tile_width, tile_height);
        imageops::replace(
            &mut restored_image,
            &block_view.to_image(),
            dest_x.into(),
            dest_y.into(),
        );
    }

    Ok(restored_image)
}

// 更新: 这是新增的、用于 scramble_ver: 2 的图像还原逻辑
// 计算方式为：图块尺寸 = ((总尺寸 / 8) / 4) * 8
fn restore_image_v2(scrambled_img: &RgbaImage, seed: u32) -> Result<RgbaImage> {
    let (width, height) = scrambled_img.dimensions();

    const GRID_DIMENSION: u32 = 4;
    const ALIGNMENT_VALUE: u32 = 8;

    if width < GRID_DIMENSION * ALIGNMENT_VALUE || height < GRID_DIMENSION * ALIGNMENT_VALUE {
        bail!("V2: 图像尺寸过小，无法进行还原。");
    }

    let temp_width = width / ALIGNMENT_VALUE;
    let temp_height = height / ALIGNMENT_VALUE;

    let tile_width = (temp_width / GRID_DIMENSION) * ALIGNMENT_VALUE;
    let tile_height = (temp_height / GRID_DIMENSION) * ALIGNMENT_VALUE;

    if tile_width == 0 || tile_height == 0 {
        bail!("V2: 计算出的图块尺寸为零，无法还原。");
    }

    let mut restored_image = RgbaImage::new(width, height);
    imageops::overlay(&mut restored_image, scrambled_img, 0, 0);

    let num_cells = (GRID_DIMENSION * GRID_DIMENSION) as usize;
    let mut indices: Vec<usize> = (0..num_cells).collect();
    seeded_shuffle(&mut indices, seed);

    for (dest_index, &source_index) in indices.iter().enumerate() {
        let source_col = source_index as u32 % GRID_DIMENSION;
        let source_row = source_index as u32 / GRID_DIMENSION;
        let dest_col = dest_index as u32 % GRID_DIMENSION;
        let dest_row = dest_index as u32 / GRID_DIMENSION;

        let src_x = source_col * tile_width;
        let src_y = source_row * tile_height;
        let dest_x = dest_col * tile_width;
        let dest_y = dest_row * tile_height;

        let block_view = scrambled_img.view(src_x, src_y, tile_width, tile_height);
        imageops::replace(
            &mut restored_image,
            &block_view.to_image(),
            dest_x.into(),
            dest_y.into(),
        );
    }

    Ok(restored_image)
}

fn encode_image(
    img: &RgbaImage,
    config: Option<&ImageOutputConfig>,
) -> Result<(Vec<u8>, &'static str)> {
    let (format, quality_opt) =
        config.map_or((ImageFormatType::Webp, None), |c| (c.format, c.quality));
    match format {
        ImageFormatType::Webp => {
            let mut buffer = std::io::Cursor::new(Vec::new());
            img.write_with_encoder(WebPEncoder::new_lossless(&mut buffer))?;
            Ok((buffer.into_inner(), "webp"))
        }
        ImageFormatType::Jpeg => {
            let mut buffer = std::io::Cursor::new(Vec::new());
            let quality = quality_opt.unwrap_or(90).max(0).min(100);
            // 在编码为 JPEG 前，必须将 RGBA 图像转换为 RGB 图像以移除 Alpha 通道
            let rgb_img = image::DynamicImage::ImageRgba8(img.clone()).into_rgb8();
            rgb_img.write_with_encoder(JpegEncoder::new_with_quality(&mut buffer, quality))?;
            Ok((buffer.into_inner(), "jpg"))
        }
        ImageFormatType::Mozjpeg => {
            let quality = quality_opt.unwrap_or(90).max(0).min(100) as f32;
            let (width, height) = img.dimensions();
            let rgb_pixels: Vec<u8> = img.pixels().flat_map(|p| [p[0], p[1], p[2]]).collect();
            let mut comp = Compress::new(ColorSpace::JCS_RGB);
            comp.set_size(width as usize, height as usize);
            comp.set_quality(quality);
            let mut comp_started = comp.start_compress(Vec::new())?;
            comp_started.write_scanlines(&rgb_pixels)?;
            let jpeg_data = comp_started.finish()?;
            Ok((jpeg_data, "jpg"))
        }
        ImageFormatType::Avif => {
            let mut buffer = std::io::Cursor::new(Vec::new());
            let quality = quality_opt.unwrap_or(80).max(0).min(100);
            let speed = 7;
            img.write_with_encoder(AvifEncoder::new_with_speed_quality(
                &mut buffer,
                speed,
                100 - quality,
            ))?;
            Ok((buffer.into_inner(), "avif"))
        }
    }
}

fn sanitize_filename(name: &str) -> String {
    name.replace(&['/', '\\', ':', '*', '?', '"', '<', '>', '|'][..], "_")
}

// --- API 请求逻辑 ---
fn generate_bambi_hash(params: &HashMap<String, String>) -> String {
    let mut keys: Vec<&String> = params.keys().collect();
    keys.sort();
    let processed_parts: Vec<String> = keys
        .iter()
        .map(|&key| {
            let value = params.get(key).unwrap();
            let hash_k = format!("{:x}", Sha256::digest(key.as_bytes()));
            let hash_v = format!("{:x}", Sha512::digest(value.as_bytes()));
            format!("{}_{}", hash_k, hash_v)
        })
        .collect();
    let combined = processed_parts.join(",");
    let final_hash_1 = format!("{:x}", Sha256::digest(combined.as_bytes()));
    format!("{:x}", Sha512::digest(final_hash_1.as_bytes()))
}

async fn download_episode(
    storage: &StorageClient,
    http_client: &reqwest::Client,
    semaphore: Arc<tokio::sync::Semaphore>,
    episode_id: u32,
    episode_title: &str,
    base_key: &Path,
    image_output_config: Option<ImageOutputConfig>,
) -> Result<bool> {
    let mut params = HashMap::new();
    params.insert("version".to_string(), "6.0.0".to_string());
    params.insert("platform".to_string(), "3".to_string());
    params.insert("episode_id".to_string(), episode_id.to_string());
    let bambi_hash = generate_bambi_hash(&params);
    let viewer_url = "https://api.ciao.shogakukan.co.jp/web/episode/viewer";

    let episode_data: EpisodeViewerResponse = http_client
        .get(viewer_url)
        .query(&params)
        .header("x-bambi-hash", bambi_hash)
        .header("x-bambi-is-crawler", "false")
        .send()
        .await?
        .json()
        .await?;

    let pages_to_download = episode_data.page_list;
    let expected_page_count = pages_to_download.len();
    println!("[处理] '{}' 共有 {} 页。", episode_title, expected_page_count);
    if expected_page_count == 0 {
        return Ok(true);
    }

    // 如果 API 未返回 scramble_ver，则默认为 1，确保向后兼容
    let scramble_ver = episode_data.scramble_ver.unwrap_or(1);

    let successful_saves = Arc::new(Mutex::new(0));
    let errors = Arc::new(Mutex::new(Vec::new()));

    let page_stream = tokio_stream::iter(pages_to_download.into_iter().enumerate());
    page_stream
        .for_each_concurrent(None, |(i, image_url)| {
            let http_client = http_client.clone();
            let base_key = base_key.to_path_buf();
            let successful_saves = Arc::clone(&successful_saves);
            let errors = Arc::clone(&errors);
            let semaphore = Arc::clone(&semaphore);
            let storage = &*storage;
            let scramble_seed = episode_data.scramble_seed;
            let scramble_ver = scramble_ver;

            async move {
                let task = async {
                    let page_num = i + 1;
                    let _permit = semaphore.acquire().await.unwrap();
                    let scrambled_bytes = http_client.get(&image_url).send().await?.bytes().await?;

                    let (img_bytes, extension) =
                        tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, &'static str)> {
                            // 将版本号传递给还原函数，由它来决定使用哪种算法
                            let final_image = restore_image(&scrambled_bytes, scramble_seed, scramble_ver)?;
                            encode_image(&final_image, image_output_config.as_ref())
                        })
                        .await??;

                    let file_key = base_key.join(format!("{:03}.{}", page_num, extension));
                    storage.save(&file_key, img_bytes).await?;

                    let mut count = successful_saves.lock().await;
                    *count += 1;
                    Ok::<(), anyhow::Error>(())
                };
                if let Err(e) = task.await {
                    let mut errors_guard = errors.lock().await;
                    errors_guard.push(e);
                }
            }
        })
        .await;

    let final_count = *successful_saves.lock().await;
    let final_errors = errors.lock().await;
    if !final_errors.is_empty() {
        eprintln!(
            "[验证失败] '{}' 下载出现错误 (成功 {} / 预期 {})。",
            episode_title, final_count, expected_page_count
        );
        final_errors
            .iter()
            .for_each(|e| eprintln!("  - 失败原因: {}", e));
        return Ok(false);
    }
    if final_count != expected_page_count {
        eprintln!(
            "[验证失败] '{}' 下载未完成 (成功 {} / 预期 {})。",
            episode_title, final_count, expected_page_count
        );
        return Ok(false);
    }
    println!("[验证成功] '{}' 所有页码已下载。", episode_title);
    Ok(true)
}

// --- 主任务循环 ---
async fn run_tasks(
    config: &Config,
    http_client: &reqwest::Client,
    storage: &Arc<StorageClient>,
) -> Result<()> {
    println!("--- 开始执行 Ciao Shogakukan 下载任务 ---");
    // 使用 Tokio 提供的 Semaphore::MAX_PERMITS 来安全地表示“无限制”并发，避免 panic
    let download_semaphore = Arc::new(tokio::sync::Semaphore::new(
        if config.download_concurrent_limit == 0 {
            tokio::sync::Semaphore::MAX_PERMITS
        } else {
            config.download_concurrent_limit
        },
    ));

    for keyword in &config.subscriptions {
        println!(
            "\n========================================\n处理订阅关键词: '{}'\n========================================",
            keyword
        );

        let search_url = "https://api.ciao.shogakukan.co.jp/search/title";
        let mut search_params = HashMap::new();
        search_params.insert("keyword".to_string(), keyword.clone());
        search_params.insert("limit".to_string(), "99999".to_string());
        search_params.insert("version".to_string(), "6.0.0".to_string());
        search_params.insert("platform".to_string(), "3".to_string());
        let search_bambi_hash = generate_bambi_hash(&search_params);

        let search_response: SearchResponse = http_client
            .get(search_url)
            .query(&search_params)
            .header("x-bambi-hash", search_bambi_hash)
            .header("x-bambi-is-crawler", "false")
            .send()
            .await
            .context("搜索请求失败")?
            .json()
            .await
            .context("解析搜索响应JSON失败")?;

        if search_response.title_list.is_empty() {
            println!("[警告] 关键词 '{}' 没有找到任何结果。", keyword);
            continue;
        }

        for series_info in search_response.title_list {
            // 保持漫画文件夹命名不变，只使用清理后的标题
            let manga_key = PathBuf::from(sanitize_filename(&series_info.title_name));

            println!(
                ">>> 处理系列: '{}' (ID: {})",
                series_info.title_name, series_info.title_id
            );

            // 在处理任何章节之前，先读取一次日志文件，用于后续的有状态冲突检测
            let log_key = manga_key.join("download_log.json");
            let log_data = storage.read_to_vec(&log_key).await?;
            let log_instance: DownloadLog = if let Some(data) = log_data {
                serde_json::from_slice(&data).unwrap_or_default()
            } else {
                DownloadLog {
                    series_id: series_info.title_id,
                    series_title: series_info.title_name.clone(),
                    chapters: HashMap::new(),
                }
            };

            let episode_ids_str = series_info
                .episode_id_list
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            let episode_list_url = "https://api.ciao.shogakukan.co.jp/episode/list";

            let mut episode_params = HashMap::new();
            episode_params.insert("version".to_string(), "6.0.0".to_string());
            episode_params.insert("platform".to_string(), "3".to_string());
            episode_params.insert("episode_id_list".to_string(), episode_ids_str);
            let episode_bambi_hash = generate_bambi_hash(&episode_params);

            let episode_list_response: EpisodeListResponse = http_client
                .post(episode_list_url)
                .form(&episode_params)
                .header("x-bambi-hash", episode_bambi_hash)
                .header("x-bambi-is-crawler", "false")
                .send()
                .await
                .context("获取章节列表请求失败")?
                .json()
                .await
                .context("解析章节列表JSON失败")?;

            let free_episodes: Vec<EpisodeInfo> = episode_list_response
                .episode_list
                .into_iter()
                .filter(|ep| ep.point == 0)
                .collect();

            if free_episodes.is_empty() {
                println!(
                    "[信息] 系列 '{}' (ID: {}) 未找到可下载的免费章节。",
                    series_info.title_name, series_info.title_id
                );
                continue;
            }
            println!("[信息] 找到 {} 个免费章节。", free_episodes.len());

            // 更新: 实现有状态的冲突检测，通过结合当前API返回的章节和日志中记录的历史章节，全面地识别重名情况
            let mut conflicting_names: HashSet<String> = HashSet::new();

            // 1. 检测当前API返回的列表中是否存在重名
            let mut current_name_counts = HashMap::new();
            for episode in &free_episodes {
                *current_name_counts.entry(sanitize_filename(&episode.episode_name)).or_insert(0) += 1;
            }
            for (name, count) in current_name_counts {
                if count > 1 {
                    conflicting_names.insert(name);
                }
            }

            // 2. 检测新章节（未记录在日志中）是否与历史章节重名
            let historical_titles: HashSet<String> = log_instance
                .chapters
                .values()
                .map(|log| sanitize_filename(&log.title))
                .collect();

            for episode in &free_episodes {
                let episode_id_str = episode.episode_id.to_string();
                // 如果一个章节的ID不在我们的下载记录中，但它的标题存在于历史标题集合里，
                // 那么它就是一个与历史章节重名的新章节，需要标记
                if !log_instance.chapters.contains_key(&episode_id_str) {
                    let sanitized_name = sanitize_filename(&episode.episode_name);
                    if historical_titles.contains(&sanitized_name) {
                        conflicting_names.insert(sanitized_name);
                    }
                }
            }
            
            // 将最终结果转换为 Arc<HashSet> 以便在异步任务中共享
            let conflicting_names_arc = Arc::new(conflicting_names);

            if !conflicting_names_arc.is_empty() {
                println!("[注意] 检测到以下章节存在重名(包括历史记录)，将为其文件夹附加ID: {:?}", conflicting_names_arc);
            }

            let info_key = manga_key.join("info.json");
            if storage.read_to_vec(&info_key).await?.is_none() {
                println!("[信息] 正在获取系列 '{}' 的元数据 (类型与标签)...", series_info.title_name);

                let genres_future = get_genres(http_client, &series_info.genre_id_list);
                let tags_future = get_tags(http_client, series_info.title_id);
                let (genres_result, tags_result) = tokio::join!(genres_future, tags_future);

                let genres = genres_result.unwrap_or_else(|e| {
                    eprintln!("[警告] 获取类型信息失败: {}。将使用空列表。", e);
                    Vec::new()
                });
                let tags = tags_result.unwrap_or_else(|e| {
                    eprintln!("[警告] 获取标签信息失败: {}。将使用空列表。", e);
                    Vec::new()
                });

                let metadata = SeriesMetadata {
                    title_id: series_info.title_id,
                    title_name: series_info.title_name.clone(),
                    author: series_info.author_text.clone(),
                    description: series_info.introduction_text.clone(),
                    cover_url: series_info.thumbnail_rect_image_url.clone(),
                    genres,
                    tags,
                };
                storage
                    .save(
                        &info_key,
                        serde_json::to_string_pretty(&metadata)?.into_bytes(),
                    )
                    .await?;
            }

            if !series_info.thumbnail_rect_image_url.is_empty() {
                let cover_url = &series_info.thumbnail_rect_image_url;
                let cover_path = Url::parse(cover_url)?.path().to_string();
                let cover_ext = Path::new(&cover_path)
                    .extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or("jpg");
                let cover_key = manga_key.join(format!("cover.{}", cover_ext));

                if storage.read_to_vec(&cover_key).await?.is_none() {
                    println!("[下载] 封面 (ID: {})...", series_info.title_id);
                    if let Ok(res) = http_client.get(cover_url).send().await {
                        storage
                            .save(&cover_key, res.bytes().await?.to_vec())
                            .await?;
                    }
                }
            }

            // 将从文件中读取的 log_instance 移动到 Arc<Mutex> 中，用于后续的并发读写
            let log = Arc::new(Mutex::new(log_instance));

            let chapter_tasks: Vec<_> = free_episodes
                .into_iter()
                .map(|episode| {
                    let storage = Arc::clone(&storage);
                    let http_client = http_client.clone();
                    let manga_key = manga_key.clone();
                    let log = Arc::clone(&log);
                    let download_semaphore = Arc::clone(&download_semaphore);
                    let image_output_config = config.image_output;
                    let conflicting_names = Arc::clone(&conflicting_names_arc);

                    async move {
                        let episode_id_str = episode.episode_id.to_string();
                        // 检查日志，如果已完成则跳过
                        if let Some(chapter_log) = log.lock().await.chapters.get(&episode_id_str) {
                            if chapter_log.completed {
                                println!("[跳过] '{}'", episode.episode_name);
                                return;
                            }
                        }

                        println!("[任务] 开始处理 '{}'", episode.episode_name);

                        // 更新: 动态生成章节文件夹名称
                        // 如果章节名在冲突集合中，则附加ID，否则只使用章节名
                        let sanitized_name = sanitize_filename(&episode.episode_name);
                        let chapter_folder_name = if conflicting_names.contains(&sanitized_name) {
                            format!("{} [{}]", sanitized_name, episode.episode_id)
                        } else {
                            sanitized_name
                        };
                        let episode_base_key = manga_key.join(chapter_folder_name);

                        match download_episode(
                            &storage,
                            &http_client,
                            Arc::clone(&download_semaphore),
                            episode.episode_id,
                            &episode.episode_name,
                            &episode_base_key,
                            image_output_config,
                        )
                        .await
                        {
                            Ok(true) => {
                                // 如果下载成功，则在内存中的日志对象里记录该章节
                                let mut lg = log.lock().await;
                                lg.chapters.insert(
                                    episode_id_str,
                                    ChapterLog {
                                        title: episode.episode_name.clone(),
                                        completed: true,
                                    },
                                );
                            }
                            Err(e) => {
                                eprintln!("[错误] 处理 '{}' 失败: {}", episode.episode_name, e)
                            }
                            _ => {}
                        }
                    }
                })
                .collect();

            let task_limit = if config.task_concurrent_limit == 0 {
                None
            } else {
                Some(config.task_concurrent_limit)
            };
            tokio_stream::iter(chapter_tasks)
                .for_each_concurrent(task_limit, |task| task)
                .await;

            // 在处理完一个系列的所有章节后，将内存中完整的日志对象写回文件
            println!(
                "[日志] 正在将下载记录写回存储 (ID: {})...",
                series_info.title_id
            );
            storage
                .save(
                    &log_key,
                    serde_json::to_string_pretty(&*log.lock().await)?.into_bytes(),
                )
                .await?;
        }
    }
    println!("\n--- 所有任务执行完毕！ ---");
    Ok(())
}

async fn get_genres(client: &reqwest::Client, genre_ids: &[u32]) -> Result<Vec<String>> {
    if genre_ids.is_empty() {
        return Ok(Vec::new());
    }
    let ids_str = genre_ids.iter().map(ToString::to_string).collect::<Vec<_>>().join(",");
    let url = "https://api.ciao.shogakukan.co.jp/genre/list";

    let mut params = HashMap::new();
    params.insert("version".to_string(), "6.0.0".to_string());
    params.insert("platform".to_string(), "3".to_string());
    params.insert("genre_id_list".to_string(), ids_str);
    let bambi_hash = generate_bambi_hash(&params);

    let response: GenreListResponse = client
        .get(url)
        .query(&params)
        .header("x-bambi-hash", bambi_hash)
        .header("x-bambi-is-crawler", "false")
        .send().await?.json().await?;

    Ok(response.genre_list.into_iter().map(|g| g.genre_name).collect())
}

async fn get_tags(client: &reqwest::Client, title_id: u32) -> Result<Vec<String>> {
    let url = "https://api.ciao.shogakukan.co.jp/tag/list";

    let mut params = HashMap::new();
    params.insert("version".to_string(), "6.0.0".to_string());
    params.insert("platform".to_string(), "3".to_string());
    params.insert("title_id".to_string(), title_id.to_string());
    let bambi_hash = generate_bambi_hash(&params);

    let response: TagListResponse = client
        .get(url)
        .query(&params)
        .header("x-bambi-hash", bambi_hash)
        .header("x-bambi-is-crawler", "false")
        .send().await?.json().await?;

    Ok(response.tag_info_list.into_iter().map(|t| t.tag_name).collect())
}

// --- 守护进程和主函数入口 ---
#[cfg(unix)]
async fn daemon_main_loop(config_path: PathBuf) -> Result<()> {
    let config_str = fs::read_to_string(&config_path).await?;
    let config: Config = toml::from_str(&config_str)?;
    let interval = config
        .daemon
        .as_ref()
        .context("配置文件中缺少 [daemon] 部分")?
        .interval_seconds;
    let storage = Arc::new(StorageClient::new(&config.output).await?);
    let http_client = build_http_client(&config.cookie)?;
    loop {
        println!(
            "\n--- [{}] 开始新一轮检查 ---",
            Utc::now().with_timezone(&Tokyo).to_rfc2822()
        );
        if let Err(e) = run_tasks(&config, &http_client, &storage).await {
            eprintln!("[错误] 守护进程任务执行失败: {}", e);
        }
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

fn build_http_client(cookie: &Option<String>) -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::USER_AGENT,
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0"
            .parse()?,
    );
    headers.insert(
        header::REFERER,
        "https://ciao.shogakukan.co.jp/".parse()?,
    );
    headers.insert(header::ORIGIN, "https://ciao.shogakukan.co.jp/".parse()?);
    if let Some(c) = cookie {
        headers.insert(header::COOKIE, c.parse()?);
    }
    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(60))
        .build()?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_str = fs::read_to_string(&cli.config).await?;
    let config: Config = toml::from_str(&config_str)?;

    if cli.daemon {
        #[cfg(unix)]
        {
            if config.daemon.is_none() {
                bail!("已通过命令行启用守护进程模式，但配置文件 '{}' 中缺少 [daemon] 配置节。", cli.config.to_string_lossy());
            }
            let default_log_path = std::env::temp_dir().join("ciao_dl.log");
            let log_file = match cli.log {
                Some(Some(path)) => std::fs::File::create(path)?,
                Some(None) => std::fs::File::create(default_log_path)?,
                None => std::fs::File::create("/dev/null")?,
            };
            let stderr = log_file.try_clone()?;
            let daemonize = daemonize::Daemonize::new()
                .pid_file("/tmp/.ciao_dl.pid")
                .working_directory("/")
                .stdout(log_file)
                .stderr(stderr);
            match daemonize.start() {
                Ok(_) => {
                    println!("已进入守护进程模式。");
                    if let Err(e) = daemon_main_loop(cli.config).await {
                        eprintln!("守护进程主循环异常退出: {}", e);
                    }
                }
                Err(e) => eprintln!("守护进程启动失败: {}", e),
            }
        }
        #[cfg(not(unix))]
        {
            bail!("守护进程模式 (-d, --daemon) 仅在 Unix-like 系统上受支持。");
        }
    } else {
        let storage = Arc::new(StorageClient::new(&config.output).await?);
        let http_client = build_http_client(&config.cookie)?;
        run_tasks(&config, &http_client, &storage).await?;
    }
    Ok(())
}