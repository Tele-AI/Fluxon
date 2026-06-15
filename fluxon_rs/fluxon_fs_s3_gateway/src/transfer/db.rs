#[cfg(test)]
use fluxon_fs_core::config::FluxonFsTransferManifestEntryWire;
use fluxon_fs_core::config::FluxonFsTransferManifestWire;
use fluxon_fs_core::path::safe_relpath;

pub(crate) fn normalize_transfer_root_relpath(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "." || trimmed == "/" {
        return Ok(".".to_string());
    }
    safe_relpath(trimmed)
        .map(|v| {
            let s = v.to_string();
            if s.is_empty() { ".".to_string() } else { s }
        })
        .map_err(|e| format!("invalid transfer root relpath: input={} err={}", raw, e))
}

#[cfg(test)]
pub(crate) fn encode_transfer_manifest_blob(
    entries: Vec<FluxonFsTransferManifestEntryWire>,
) -> Result<Vec<u8>, String> {
    FluxonFsTransferManifestWire::new(entries, Vec::new())
        .encode_to_blob()
        .map_err(|e| format!("encode transfer manifest failed: {}", e))
}

#[cfg(test)]
pub(crate) fn encode_transfer_manifest_blob_with_empty_dirs(
    entries: Vec<FluxonFsTransferManifestEntryWire>,
    empty_dir_relpaths: Vec<String>,
) -> Result<Vec<u8>, String> {
    FluxonFsTransferManifestWire::new(entries, empty_dir_relpaths)
        .encode_to_blob()
        .map_err(|e| format!("encode transfer manifest failed: {}", e))
}

pub(crate) fn decode_transfer_manifest_blob(
    blob: &[u8],
) -> Result<FluxonFsTransferManifestWire, String> {
    FluxonFsTransferManifestWire::decode_from_blob(blob)
        .map_err(|e| format!("decode transfer manifest failed: {}", e))
}
