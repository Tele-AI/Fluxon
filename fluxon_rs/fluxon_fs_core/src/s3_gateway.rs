use std::fmt::Write as _;

// English note:
// - This module defines the stable v1 contract between:
//   - fluxon_fs agents (filesystem readers/writers)
//   - fluxon_fs_s3_gateway (S3 + UI gateway running inside the fs master HTTP server)
// - We keep it in fluxon_fs_core to avoid drift between crates.

pub const FS_S3_STAGE_OBJECT_TO_KV_RPC_PATH: &str = "/fluxon_fs_s3/v1/stage_object_to_kv";

// English note:
// - This RPC stages a single fixed-size piece of a file into KV.
// - It is intentionally defined at the FS contract layer (not KV), because only FS agents know how
//   to map (export, relpath) to a safe local filesystem path.
// - Naming uses "file part" to keep it reusable for other future FS gateways/modules.
pub const FS_S3_LOAD_PART_FILE_TO_KV_RPC_PATH: &str = "/fluxon_fs_s3/v1/load_part_file_to_kv";
pub const FS_S3_LOAD_PART_FILE_RANGE_TO_KV_RPC_PATH: &str =
    "/fluxon_fs_s3/v1/load_part_file_range_to_kv";

// Internal multipart staging layout kept inside the export directory.
pub const FS_S3_MULTIPART_DIR_PREFIX: &str = ".fluxon_fs_s3_multipart";
pub const FS_S3_INTERNAL_MULTIPART_PAYLOAD_KEY: &str = "fs_s3_internal_multipart";

// S3 gateway KV layout:
//   <export.cache_kv_key_prefix>/fs_s3/v2/<export>/<relpath>/manifest/<sig>
//   <export.cache_kv_key_prefix>/fs_s3/v2/<export>/<relpath>/piece/<sig>/<piece_index>
pub const FS_S3_KV_LAYOUT_VERSION: &str = "v2";

// Piece size for the KV-backed object cache.
//
// Causal chain:
// - The gateway uses FluxonFS export RPC `read_chunk` to fetch missing bytes.
// - FluxonFS export RPC `read_chunk` is explicitly bounded (currently 1MiB).
// - Therefore the S3 gateway cache piece size must match that bound to avoid fragmentation.
// - We keep this as a single shared constant to avoid drift across agent/gateway.
pub const FS_S3_OBJECT_PIECE_BYTES: usize = 1024 * 1024;

pub fn is_internal_multipart_relpath(relpath: &str) -> bool {
    let trimmed = relpath.trim_start_matches('/');
    trimmed == FS_S3_MULTIPART_DIR_PREFIX
        || trimmed.starts_with(&format!("{}/", FS_S3_MULTIPART_DIR_PREFIX))
}

pub fn object_sig_string(size: i64, mtime_ns: i64) -> String {
    // Keep it ASCII and KV-key friendly.
    format!("s{}_m{}", size, mtime_ns)
}

pub fn kv_object_prefix(
    export_cache_kv_key_prefix: &str,
    export_name: &str,
    relpath: &str,
) -> String {
    let base = export_cache_kv_key_prefix.trim_end_matches('/');
    format!(
        "{}/fs_s3/{}/{}/{}",
        base, FS_S3_KV_LAYOUT_VERSION, export_name, relpath
    )
}

pub fn kv_manifest_key(
    export_cache_kv_key_prefix: &str,
    export_name: &str,
    relpath: &str,
    sig: &str,
) -> String {
    format!(
        "{}/manifest/{}",
        kv_object_prefix(export_cache_kv_key_prefix, export_name, relpath),
        sig
    )
}

pub fn kv_piece_key(
    export_cache_kv_key_prefix: &str,
    export_name: &str,
    relpath: &str,
    sig: &str,
    piece_index: i64,
) -> String {
    let mut s = String::new();
    // Avoid `format!` allocations in hot loops.
    let _ = write!(
        &mut s,
        "{}/piece/{}/{}",
        kv_object_prefix(export_cache_kv_key_prefix, export_name, relpath),
        sig,
        piece_index
    );
    s
}
