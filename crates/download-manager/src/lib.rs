mod downloader;
mod error;
mod manager;
mod models;
mod store;
mod validate;
mod validator;

pub use error::{Error, Result};
pub use manager::{DownloadManager, DownloadManagerConfig, OnChanged};
pub use models::{CreateOptions, DownloadActionResponse, DownloadItem, DownloadStatus};
pub use store::STORE_FILE_NAME;
pub use validate::path as validate_path;
pub use validate::store_dir as validate_store_dir;
pub use validate::user_agent as validate_user_agent;
