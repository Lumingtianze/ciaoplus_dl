# CiaoPlus Downloader (ciaoplus_dl)

一个为 `ciao.shogakukan.co.jp` 网站设计的命令行漫画下载工具。它能自动处理复杂的 API 签名和图片反爬机制，支持多种现代图像格式输出、并发下载以及多种存储后端。

## ✨ 功能特性

-   **智能处理反爬**:
    -   **API 签名**: 自动处理并生成 API 请求所需的、基于多重哈希的 `x-bambi-hash` 认证头。
    -   **图片还原**: 自动处理基于种子（seed）的 4x4 网格加扰反爬机制，将碎片化的图片完美还原成原始图像。
-   **高级图像处理**:
    -   支持将下载的图片实时转换为多种现代格式，包括 **WebP (无损)**, **JPEG**, **AVIF**。
    -   集成 **MozJPEG** 编码器，可在输出 JPEG 时获得更高的压缩率和质量。
    -   可自定义输出图像的质量参数，实现体积与质量的平衡。
-   **高性能下载**:
    -   基于 `tokio` 异步运行时，支持高度并发的章节处理和图片下载。
    -   可自由配置任务和下载的并发数量，充分利用网络和 CPU 资源。
-   **灵活的存储选项**:
    -   **本地存储**: 将漫画直接保存到本地文件系统。
    -   **对象存储 (S3/OSS)**: （需启用 `oss` feature）支持将漫画直接上传到任何兼容 S3 API 的对象存储服务，如 AWS S3, MinIO, 阿里云 OSS 等。
-   **断点续传与元数据**:
    -   为每部漫画维护一个 `download_log.json` 下载记录，自动跳过已完成的章节。
    -   自动抓取并保存漫画封面及包含作者、描述、类型、标签等信息的 `info.json` 元数据文件。
    -   通过在文件夹名称中附加系列 ID 来确保唯一性。
-   **守护进程模式**: （仅限 Unix-like 系统）支持以 `-d` 或 `--daemon` 参数在后台持续运行，按配置的间隔时间自动检查和下载更新。

## 🚀 快速开始

### 1. 安装

**从源码构建 (需要 Rust 环境):**

```bash
# 安装基础版本 (支持 WebP, JPEG, AVIF)
./cargo install --path .

# 如果你需要 S3/OSS 支持，请启用 'oss' feature
./cargo install --path . --features oss
```

### 2. 创建配置文件

将目录下的 `config.toml.sample` 重命名为 `config.toml`，并修改其中的订阅列表。

### 3. 运行

**单次运行:**

```bash
./ciao
```

**使用守护进程模式 (后台持续运行):**

```bash
# 启动守护进程，默认日志输出到 /dev/null
./ciao --daemon

# 将日志输出到默认文件 /tmp/ciao_dl.log
./ciao -d --log

# 指定日志文件路径
./ciao -d --log=/path/to/your/log/file.log
```

## ⚙️ 命令行参数

```
./ciao [OPTIONS]

OPTIONS:
    -c, --config <FILE>    指定配置文件的路径 [默认: config.toml]
    -d, --daemon           以守护进程模式在后台运行 (仅限 Unix-like 系统)
        --log [FILE_PATH]  (与 --daemon 配合使用) 启用日志记录。
                           如果未提供路径，则默认为系统临时目录。
    -h, --help             打印帮助信息
    -V, --version          打印版本信息
```

## 🛠️ 从源码构建

1.  克隆本仓库。
2.  安装 [Rust](https://www.rust-lang.org/tools/install) 和 `clang` (用于 `mozjpeg` 依赖)。
3.  执行 `cargo build --release` (基础版) 或 `cargo build --release --features oss` (S3/OSS 版)。
4.  可执行文件位于 `target/release/ciao`。