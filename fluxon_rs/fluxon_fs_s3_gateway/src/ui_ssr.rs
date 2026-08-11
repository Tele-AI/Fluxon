// English note:
// - This file is now the SSR UI aggregator.
// - Static assets, UI-local types, and handlers live in dedicated ui_ssr_* files.

include!("ui_ssr_assets.rs");

fn ui_basic_auth_required() -> Response {
    let mut resp = text_response(StatusCode::UNAUTHORIZED, "basic auth required".to_string());
    resp.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"fluxon_fs_s3\""),
    );
    resp
}

fn ui_basic_auth_account(headers: &HeaderMap, st: &GatewayState) -> Option<AuthAccount> {
    let Some(v) = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) else {
        return None;
    };
    let Some(rest) = v.strip_prefix("Basic ") else {
        return None;
    };
    let raw = match base64::engine::general_purpose::STANDARD.decode(rest.trim()) {
        Ok(v) => v,
        Err(_) => return None,
    };
    let s = String::from_utf8_lossy(&raw);
    let Some((username, password)) = s.split_once(':') else {
        return None;
    };
    let account = find_account_by_username(st, username)?;
    if account.password != password {
        return None;
    }
    Some(account)
}

include!("ui_ssr_types.rs");

#[derive(Debug, Clone)]
struct UiIdentity {
    viewer: AuthAccount,
    actor: AuthAccount,
    as_user: Option<String>,
}

impl UiIdentity {
    fn viewer_username(&self) -> &str {
        self.viewer.username.as_str()
    }

    fn actor_username(&self) -> &str {
        self.actor.username.as_str()
    }

    fn is_impersonating(&self) -> bool {
        self.as_user.is_some() && self.viewer.username != self.actor.username
    }
}

fn ui_manager_account_required_text(username: &str, action: &str) -> String {
    format!(
        "account {} cannot {}; this action requires a manager account (can_manage_users=true)",
        username, action
    )
}

fn ui_manager_account_forbidden(username: &str, action: &str) -> Response {
    ui_forbidden_response(ui_manager_account_required_text(username, action))
}

fn ui_normalize_as_user(as_user: Option<String>) -> Option<String> {
    let s = as_user?;
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    Some(t.to_string())
}

fn ui_require_identity(
    headers: &HeaderMap,
    st: &GatewayState,
    as_user: Option<String>,
) -> Result<UiIdentity, Response> {
    ui_require_identity_impl(headers, st, as_user, false)
}

fn ui_require_identity_for_credentials_change(
    headers: &HeaderMap,
    st: &GatewayState,
    as_user: Option<String>,
) -> Result<UiIdentity, Response> {
    ui_require_identity_impl(headers, st, as_user, true)
}

fn ui_require_identity_impl(
    headers: &HeaderMap,
    st: &GatewayState,
    as_user: Option<String>,
    allow_pending_credentials_change: bool,
) -> Result<UiIdentity, Response> {
    let viewer = match ui_basic_auth_account(headers, st) {
        Some(v) => v,
        None => return Err(ui_basic_auth_required()),
    };
    if !allow_pending_credentials_change
        && st.account_requires_credentials_change(viewer.username.as_str())
    {
        let mut resp = Response::new(boxed(Body::empty()));
        *resp.status_mut() = StatusCode::SEE_OTHER;
        let location = format!(
            "{}/ui/account/password/",
            st.external_base_path.trim_end_matches('/')
        );
        resp.headers_mut().insert(
            header::LOCATION,
            HeaderValue::from_str(location.as_str()).unwrap(),
        );
        return Err(resp);
    }

    let as_user = ui_normalize_as_user(as_user);
    let Some(as_username) = as_user.clone() else {
        return Ok(UiIdentity {
            viewer: viewer.clone(),
            actor: viewer,
            as_user: None,
        });
    };

    if !account_can_manage_permissions(&viewer) {
        return Err(ui_manager_account_forbidden(
            viewer.username.as_str(),
            "use view-as",
        ));
    }

    let Some(actor) = find_account_by_username(st, &as_username) else {
        return Err(UiHandlerError::BadRequest(format!("unknown view-as user: {}", as_username)).into_text_response());
    };

    Ok(UiIdentity {
        viewer,
        actor,
        as_user: Some(as_username),
    })
}

fn ui_validate_prefix(prefix: String) -> Result<String, UiHandlerError> {
    if prefix.starts_with('/') {
        return Err(UiHandlerError::BadRequest("prefix must not start with '/'".to_string()));
    }
    if !prefix.is_empty() && !prefix.ends_with('/') {
        return Err(UiHandlerError::BadRequest(
            "prefix must end with '/' (directory prefix)".to_string(),
        ));
    }
    Ok(prefix)
}

fn ui_validate_object_key(raw: &str, label: &str) -> Result<String, UiHandlerError> {
    let key = safe_relpath(raw)
        .map_err(|e| UiHandlerError::BadRequest(format!("invalid {}: {}", label, e)))?;
    verify_user_object_key(&key).map_err(|e| UiHandlerError::BadRequest(e.to_string()))?;
    Ok(key)
}

fn ui_validate_folder_name(raw: &str) -> Result<String, UiHandlerError> {
    let name = raw.trim().to_string();
    if name.is_empty() {
        return Err(UiHandlerError::BadRequest(
            "folder name must be non-empty".to_string(),
        ));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(UiHandlerError::BadRequest(
            "folder name must not contain '/'".to_string(),
        ));
    }
    Ok(name)
}

fn ui_validate_upload_file_name(raw: &str) -> Result<String, UiHandlerError> {
    let name = raw.trim();
    if name.is_empty() {
        return Err(UiHandlerError::BadRequest("file name must be non-empty".to_string()));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(UiHandlerError::BadRequest(
            "file name must not contain path separators".to_string(),
        ));
    }
    Ok(name.to_string())
}

fn ui_join_prefix_name(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{}{}", prefix, name)
    }
}

#[derive(Debug, Clone)]
struct UiTransferPrescanSourceCandidate {
    export_name: String,
    src_root_relpath: String,
    remote_root_dir_abs: String,
}

fn ui_transfer_root_relpath_to_prefix(root_relpath: &str) -> String {
    let trimmed = root_relpath.trim();
    if trimmed.is_empty() || trimmed == "." {
        return String::new();
    }
    format!("{}/", trimmed.trim_matches('/'))
}

fn ui_normalize_transfer_root_relpath(raw: &str) -> Result<String, UiHandlerError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "." || trimmed == "/" {
        return Ok(".".to_string());
    }
    safe_relpath(trimmed)
        .map(|value| {
            if value.is_empty() {
                ".".to_string()
            } else {
                value
            }
        })
        .map_err(|e| UiHandlerError::BadRequest(format!("invalid transfer root relpath: {}", e)))
}

fn ui_transfer_root_relpath_from_prefix(prefix: &str) -> Result<String, UiHandlerError> {
    let validated = ui_validate_prefix(prefix.to_string())?;
    ui_normalize_transfer_root_relpath(validated.trim_end_matches('/'))
}

fn ui_account_has_any_bucket_action(
    account: &AuthAccount,
    bucket: &str,
    action: FluxonFsS3PermissionAction,
) -> bool {
    if account.can_manage_users {
        return true;
    }
    account.permissions.iter().any(|rule| {
        permission_bucket_matches(&rule.bucket, bucket)
            && rule
                .actions
                .iter()
                .any(|allowed| permission_action_matches(*allowed, action))
    })
}

fn ui_transfer_target_export_names(st: &GatewayState, account: &AuthAccount) -> Vec<String> {
    let mut names = st
        .fs_cache
        .exports
        .keys()
        .filter(|name| {
            ui_account_has_any_bucket_action(account, name.as_str(), FluxonFsS3PermissionAction::PutObject)
        })
        .cloned()
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn ui_transfer_root_relpath_under_export(
    export_root_dir_abs: &str,
    root_dir_abs: &str,
) -> Result<Option<String>, UiHandlerError> {
    let export_root = safe_abs_dirpath(export_root_dir_abs)
        .map_err(|e| UiHandlerError::BadGateway(format!("invalid export root path: {}", e)))?;
    let root_abs = safe_abs_dirpath(root_dir_abs)
        .map_err(|e| UiHandlerError::BadRequest(format!("invalid prescan source root path: {}", e)))?;
    if export_root == "/" {
        let rel = root_abs.trim_start_matches('/');
        if rel.is_empty() {
            return Ok(Some(".".to_string()));
        }
        return safe_relpath(rel)
            .map(Some)
            .map_err(|e| UiHandlerError::BadRequest(format!("invalid prescan source relpath: {}", e)));
    }
    if root_abs == export_root {
        return Ok(Some(".".to_string()));
    }
    let prefix = format!("{}/", export_root.trim_end_matches('/'));
    let Some(rest) = root_abs.strip_prefix(prefix.as_str()) else {
        return Ok(None);
    };
    safe_relpath(rest)
        .map(Some)
        .map_err(|e| UiHandlerError::BadRequest(format!("invalid prescan source relpath: {}", e)))
}

fn ui_decode_local_transfer_prescan_spec(
    job: &FsTransferJobRecord,
) -> Result<Option<fluxon_fs_core::config::FluxonFsLocalTransferCheckJobSpecWire>, UiHandlerError> {
    if job.src_export != fluxon_fs_core::config::FLUXON_FS_LOCAL_TRANSFER_CHECK_SRC_EXPORT
        || job.dst_export != fluxon_fs_core::config::FLUXON_FS_LOCAL_TRANSFER_CHECK_DST_EXPORT
    {
        return Ok(None);
    }
    let mut spec: fluxon_fs_core::config::FluxonFsLocalTransferCheckJobSpecWire =
        serde_json::from_slice(job.job_spec_blob.as_slice()).map_err(|e| {
            UiHandlerError::BadGateway(format!(
                "decode local transfer prescan spec failed: job_id={} err={}",
                job.job_id, e
            ))
        })?;
    spec.skip_entries = fluxon_fs_core::config::normalize_transfer_skip_entries(spec.skip_entries)
        .map_err(|e| {
            UiHandlerError::BadGateway(format!(
                "normalize local transfer prescan skip_entries failed: job_id={} err={}",
                job.job_id, e
            ))
        })?;
    Ok(Some(spec))
}

fn ui_transfer_prescan_source_candidates(
    st: &GatewayState,
    account: &AuthAccount,
    spec: &fluxon_fs_core::config::FluxonFsLocalTransferCheckJobSpecWire,
) -> Result<Vec<UiTransferPrescanSourceCandidate>, UiHandlerError> {
    let mut out = Vec::new();
    let effective_exports = st
        .load_effective_fs_exports()
        .map_err(|e| UiHandlerError::BadGateway(format!("load effective exports failed: {}", e)))?;
    for (export_name, export) in &effective_exports {
        let Some(src_root_relpath) = ui_transfer_root_relpath_under_export(
            export.remote_root_dir_abs.as_str(),
            spec.src_root_dir_abs.as_str(),
        )? else {
            continue;
        };
        let src_prefix = ui_transfer_root_relpath_to_prefix(src_root_relpath.as_str());
        if !account_has_bucket_action(
            account,
            export_name.as_str(),
            src_prefix.as_str(),
            FluxonFsS3PermissionAction::ListBucket,
        ) {
            continue;
        }
        if !account_has_bucket_action(
            account,
            export_name.as_str(),
            src_prefix.as_str(),
            FluxonFsS3PermissionAction::GetObject,
        ) {
            continue;
        }
        out.push(UiTransferPrescanSourceCandidate {
            export_name: export_name.clone(),
            src_root_relpath,
            remote_root_dir_abs: export.remote_root_dir_abs.clone(),
        });
    }
    out.sort_by(|a, b| {
        a.src_root_relpath
            .split('/')
            .count()
            .cmp(&b.src_root_relpath.split('/').count())
            .then(a.export_name.cmp(&b.export_name))
    });
    Ok(out)
}

fn ui_validate_transfer_job_binding(
    st: &GatewayState,
    account: &AuthAccount,
    src_export_raw: String,
    src_root_relpath_raw: String,
    dst_export_raw: String,
    dst_root_relpath_raw: String,
) -> Result<(String, String, String, String), UiHandlerError> {
    let src_export = src_export_raw.trim().to_string();
    if src_export.is_empty() {
        return Err(UiHandlerError::BadRequest("src_export must be non-empty".to_string()));
    }
    let dst_export = dst_export_raw.trim().to_string();
    if dst_export.is_empty() {
        return Err(UiHandlerError::BadRequest("dst_export must be non-empty".to_string()));
    }
    ui_require_bucket(st, src_export.as_str())?;
    ui_require_bucket(st, dst_export.as_str())?;
    let src_root_relpath = ui_normalize_transfer_root_relpath(src_root_relpath_raw.as_str())?;
    let dst_root_relpath = ui_normalize_transfer_root_relpath(dst_root_relpath_raw.as_str())?;
    if src_export == dst_export && src_root_relpath == dst_root_relpath {
        return Err(UiHandlerError::Conflict(
            "source and destination are identical".to_string(),
        ));
    }
    let src_prefix = ui_transfer_root_relpath_to_prefix(src_root_relpath.as_str());
    if !account_has_bucket_action(
        account,
        src_export.as_str(),
        src_prefix.as_str(),
        FluxonFsS3PermissionAction::ListBucket,
    ) {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} lacks s3:ListBucket on s3://{}/{}",
            account.username,
            src_export,
            src_prefix
        )));
    }
    if !account_has_bucket_action(
        account,
        src_export.as_str(),
        src_prefix.as_str(),
        FluxonFsS3PermissionAction::GetObject,
    ) {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} lacks s3:GetObject on s3://{}/{}",
            account.username,
            src_export,
            src_prefix
        )));
    }
    let dst_prefix = ui_transfer_root_relpath_to_prefix(dst_root_relpath.as_str());
    if !account_has_bucket_action(
        account,
        dst_export.as_str(),
        dst_prefix.as_str(),
        FluxonFsS3PermissionAction::PutObject,
    ) {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} lacks s3:PutObject on s3://{}/{}",
            account.username,
            dst_export,
            dst_prefix
        )));
    }
    Ok((src_export, src_root_relpath, dst_export, dst_root_relpath))
}

fn ui_bucket_provider_items_from_runtime_exports(
    bucket: &str,
    configured_remote_root_dir_abs: &str,
    runtime_exports: &[FsExportRegistryRecord],
) -> Vec<UiBrowseProviderItem> {
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    let mut items: Vec<UiBrowseProviderItem> = Vec::new();
    for record in runtime_exports {
        if record.export_name != bucket {
            continue;
        }
        let key = (
            record.agent_instance_key.clone(),
            record.remote_root_dir_abs.clone(),
        );
        if !seen.insert(key.clone()) {
            continue;
        }
        items.push(UiBrowseProviderItem {
            agent_instance_key: key.0,
            remote_root_dir_abs: key.1,
        });
    }
    items.sort_by(|a, b| {
        (a.agent_instance_key.clone(), a.remote_root_dir_abs.clone())
            .cmp(&(b.agent_instance_key.clone(), b.remote_root_dir_abs.clone()))
    });
    if items.is_empty() {
        items.push(UiBrowseProviderItem {
            agent_instance_key: "configured".to_string(),
            remote_root_dir_abs: configured_remote_root_dir_abs.to_string(),
        });
    }
    items
}

fn ui_bucket_provider_items(
    st: &GatewayState,
    bucket: &str,
    configured_remote_root_dir_abs: &str,
) -> Result<Vec<UiBrowseProviderItem>, UiHandlerError> {
    let runtime_exports = st
        .list_fs_export_registry_records()
        .map_err(|e| UiHandlerError::BadGateway(format!("load runtime export providers failed: {}", e)))?;
    Ok(ui_bucket_provider_items_from_runtime_exports(
        bucket,
        configured_remote_root_dir_abs,
        &runtime_exports,
    ))
}

fn ui_provider_display_text(provider_items: &[UiBrowseProviderItem]) -> String {
    provider_items
        .iter()
        .map(|item| format!("{}: {}", item.agent_instance_key, item.remote_root_dir_abs))
        .collect::<Vec<_>>()
        .join(" | ")
}

fn ui_require_bucket(st: &GatewayState, bucket: &str) -> Result<(), UiHandlerError> {
    st.ensure_effective_fs_export(bucket)
        .map_err(|e| UiHandlerError::BadGateway(format!("load effective export failed: {}", e)))?
        .map(|_| ())
        .ok_or_else(|| UiHandlerError::NotFound(format!("no such bucket: {}", bucket)))
}

fn ui_require_bucket_export(
    st: &GatewayState,
    bucket: &str,
) -> Result<FluxonFsExport, UiHandlerError> {
    st.ensure_effective_fs_export(bucket)
        .map_err(|e| UiHandlerError::BadGateway(format!("load effective export failed: {}", e)))?
        .ok_or_else(|| UiHandlerError::NotFound(format!("no such bucket: {}", bucket)))
}

fn ui_handler_error_from_s3_error(op: &str, err: S3Error) -> UiHandlerError {
    match err {
        S3Error::AccessDenied { detail } => UiHandlerError::Forbidden(format!("{} denied: {}", op, detail)),
        S3Error::InvalidRequest { detail } | S3Error::InvalidRange { detail } => {
            UiHandlerError::BadRequest(format!("{} failed: {}", op, detail))
        }
        S3Error::NoSuchBucket { bucket } => UiHandlerError::NotFound(format!("{} failed: no such bucket: {}", op, bucket)),
        S3Error::NoSuchKey { bucket, key } => {
            UiHandlerError::NotFound(format!("{} failed: no such key: s3://{}/{}", op, bucket, key))
        }
        S3Error::NoSuchUpload { upload_id } => {
            UiHandlerError::NotFound(format!("{} failed: no such upload: {}", op, upload_id))
        }
        S3Error::Internal { detail } => UiHandlerError::BadGateway(format!("{} failed: internal error: {}", op, detail)),
    }
}

#[derive(Clone)]
struct UiCopyOrMovePlan {
    kind: UiTransferTaskKind,
    name: String,
    source: UiTransferTaskEndpoint,
    target: UiTransferTaskEndpoint,
    request_identity: FluxonFsRequestIdentity,
    src_bucket_arc: Arc<str>,
    dst_bucket_arc: Arc<str>,
    src_arc: Arc<str>,
    dst_arc: Arc<str>,
    src_size: i64,
    src_mtime_ns: i64,
    remove_source: bool,
}

fn ui_parent_prefix_for_object_key(key: &str) -> String {
    let Some((parent, _)) = key.rsplit_once('/') else {
        return "".to_string();
    };
    format!("{}/", parent)
}

fn ui_transfer_kind_title(kind: UiTransferTaskKind) -> &'static str {
    match kind {
        UiTransferTaskKind::Copy => "Copy",
        UiTransferTaskKind::Move => "Move",
    }
}

fn ui_transfer_kind_present_participle(kind: UiTransferTaskKind) -> &'static str {
    match kind {
        UiTransferTaskKind::Copy => "Copying",
        UiTransferTaskKind::Move => "Moving",
    }
}

fn ui_transfer_task_detail(done_bytes: i64, total_bytes: i64) -> String {
    format!("{} / {}", fmt_bytes(done_bytes), fmt_bytes(total_bytes))
}

#[derive(Debug)]
enum UiTransferExecError {
    Handler(UiHandlerError),
    Cancelled,
}

impl From<UiHandlerError> for UiTransferExecError {
    fn from(value: UiHandlerError) -> Self {
        Self::Handler(value)
    }
}

async fn ui_wait_for_transfer_runnable(
    task: Option<&UiTransferTaskHandle>,
    done_bytes: i64,
    total_bytes: i64,
) -> Result<(), UiTransferExecError> {
    let Some(task) = task else {
        return Ok(());
    };
    loop {
        let notified = task.wait_token();
        match task.gate(done_bytes, total_bytes) {
            UiTransferTaskGate::Run => return Ok(()),
            UiTransferTaskGate::Cancel => return Err(UiTransferExecError::Cancelled),
            UiTransferTaskGate::Wait => notified.await,
        }
    }
}

async fn ui_cleanup_cancelled_transfer(st: &GatewayState, plan: &UiCopyOrMovePlan) -> Result<(), UiHandlerError> {
    let dst_stat = st
        .backend
        .stat(
            plan.request_identity.clone(),
            plan.dst_bucket_arc.clone(),
            plan.dst_arc.clone(),
        )
        .await
        .map_err(|e| ui_handler_error_from_s3_error("stat cancelled destination", e))?;
    if dst_stat.exists {
        st.backend
            .unlink(
                plan.request_identity.clone(),
                plan.dst_bucket_arc.clone(),
                plan.dst_arc.clone(),
            )
            .await
            .map_err(|e| ui_handler_error_from_s3_error("cleanup cancelled destination", e))?;
    }
    Ok(())
}

async fn ui_prepare_copy_or_move_object(
    st: &GatewayState,
    account: &AuthAccount,
    src_bucket: &str,
    src_key: String,
    dst_bucket: String,
    dst_prefix: String,
    remove_source: bool,
) -> Result<UiCopyOrMovePlan, UiHandlerError> {
    let request_identity = request_identity_from_account(account);
    ui_require_bucket(st, src_bucket)?;
    ui_require_bucket(st, &dst_bucket)?;
    let src_key = ui_validate_object_key(&src_key, "source key")?;
    let dst_prefix = ui_validate_prefix(dst_prefix)?;
    let name = src_key
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| UiHandlerError::BadRequest("source key must name an object".to_string()))?
        .to_string();
    let dst_key = ui_validate_object_key(&ui_join_prefix_name(&dst_prefix, &name), "destination key")?;
    if src_bucket == dst_bucket && src_key == dst_key {
        return Err(UiHandlerError::Conflict(
            "source and destination are identical".to_string(),
        ));
    }
    if !account_has_object_action(account, src_bucket, &src_key, FluxonFsS3PermissionAction::GetObject) {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} lacks s3:GetObject on s3://{}/{}",
            account.username, src_bucket, src_key
        )));
    }
    if !account_has_object_action(account, &dst_bucket, &dst_key, FluxonFsS3PermissionAction::PutObject) {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} lacks s3:PutObject on s3://{}/{}",
            account.username, dst_bucket, dst_key
        )));
    }
    if remove_source
        && !account_has_object_action(account, src_bucket, &src_key, FluxonFsS3PermissionAction::DeleteObject)
    {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} lacks s3:DeleteObject on s3://{}/{}",
            account.username, src_bucket, src_key
        )));
    }

    let src_bucket_arc: Arc<str> = src_bucket.to_string().into();
    let dst_bucket_arc: Arc<str> = dst_bucket.clone().into();
    let src_arc: Arc<str> = src_key.clone().into();
    let dst_arc: Arc<str> = dst_key.clone().into();
    let src_stat = st
        .backend
        .stat(
            request_identity.clone(),
            src_bucket_arc.clone(),
            src_arc.clone(),
        )
        .await
        .map_err(|e| ui_handler_error_from_s3_error("stat source", e))?;
    if !src_stat.exists || !src_stat.is_file {
        return Err(UiHandlerError::NotFound(format!(
            "no such object: s3://{}/{}",
            src_bucket, src_key
        )));
    }
    let dst_stat = st
        .backend
        .stat(
            request_identity.clone(),
            dst_bucket_arc.clone(),
            dst_arc.clone(),
        )
        .await
        .map_err(|e| ui_handler_error_from_s3_error("stat destination", e))?;
    if dst_stat.exists {
        return Err(UiHandlerError::Conflict(
            "destination object already exists".to_string(),
        ));
    }

    ensure_parent_dirs(st, request_identity.clone(), dst_bucket_arc.clone(), &dst_key)
        .await
        .map_err(|e| ui_handler_error_from_s3_error("mkdir destination", e))?;

    Ok(UiCopyOrMovePlan {
        kind: if remove_source {
            UiTransferTaskKind::Move
        } else {
            UiTransferTaskKind::Copy
        },
        name,
        source: UiTransferTaskEndpoint {
            bucket: src_bucket.to_string(),
            key: src_key.clone(),
            prefix: ui_parent_prefix_for_object_key(&src_key),
        },
        target: UiTransferTaskEndpoint {
            bucket: dst_bucket.clone(),
            key: dst_key.clone(),
            prefix: dst_prefix.clone(),
        },
        request_identity,
        src_bucket_arc,
        dst_bucket_arc,
        src_arc,
        dst_arc,
        src_size: src_stat.size,
        src_mtime_ns: src_stat.mtime_ns,
        remove_source,
    })
}

async fn ui_execute_copy_or_move_plan(
    st: &GatewayState,
    plan: &UiCopyOrMovePlan,
    task: Option<&UiTransferTaskHandle>,
) -> Result<UiKeyResultBody, UiTransferExecError> {
    if plan.remove_source && plan.source.bucket == plan.target.bucket {
        st.backend
            .rename(
                plan.request_identity.clone(),
                plan.src_bucket_arc.clone(),
                plan.src_arc.clone(),
                plan.dst_arc.clone(),
            )
            .await
            .map_err(|e| ui_handler_error_from_s3_error("rename", e))?;
        if let Some(task) = task {
            task.set_running(
                plan.src_size,
                format!("{} completed", ui_transfer_kind_title(plan.kind)),
                ui_transfer_task_detail(plan.src_size, plan.src_size),
            );
        }
        return Ok(UiKeyResultBody {
            ok: true,
            bucket: plan.target.bucket.clone(),
            key: plan.target.key.clone(),
            prefix: plan.target.prefix.clone(),
        });
    }

    let mut offset: i64 = 0;
    while offset < plan.src_size {
        ui_wait_for_transfer_runnable(task, offset, plan.src_size).await?;
        let remain = plan.src_size - offset;
        let len = remain.min(FS_RPC_CHUNK_BYTES as i64);
        let chunk = st
            .backend
            .read_chunk_cached(
                plan.request_identity.clone(),
                plan.src_bucket_arc.clone(),
                plan.src_arc.clone(),
                offset,
                len,
                plan.src_size,
                plan.src_mtime_ns,
            )
            .await
            .map_err(|e| ui_handler_error_from_s3_error("read source", e))?;
        if chunk.is_empty() {
            return Err(UiHandlerError::BadGateway(
                "source read returned empty chunk before EOF".to_string(),
            )
            .into());
        }
        ui_wait_for_transfer_runnable(task, offset, plan.src_size).await?;
        st.backend
            .write_chunk(
                plan.request_identity.clone(),
                plan.dst_bucket_arc.clone(),
                plan.dst_arc.clone(),
                offset,
                chunk.clone(),
            )
            .await
            .map_err(|e| ui_handler_error_from_s3_error("write destination", e))?;
        offset += chunk.len() as i64;
        ui_wait_for_transfer_runnable(task, offset, plan.src_size).await?;
        if let Some(task) = task {
            task.set_running(
                offset,
                format!("{} data", ui_transfer_kind_present_participle(plan.kind)),
                ui_transfer_task_detail(offset, plan.src_size),
            );
        }
    }
    ui_wait_for_transfer_runnable(task, offset, plan.src_size).await?;
    st.backend
        .truncate(
            plan.request_identity.clone(),
            plan.dst_bucket_arc.clone(),
            plan.dst_arc.clone(),
            plan.src_size,
        )
        .await
        .map_err(|e| ui_handler_error_from_s3_error("truncate destination", e))?;
    ui_wait_for_transfer_runnable(task, offset, plan.src_size).await?;
    if plan.remove_source {
        st.backend
            .unlink(
                plan.request_identity.clone(),
                plan.src_bucket_arc.clone(),
                plan.src_arc.clone(),
            )
            .await
            .map_err(|e| ui_handler_error_from_s3_error("unlink source", e))?;
    }

    Ok(UiKeyResultBody {
        ok: true,
        bucket: plan.target.bucket.clone(),
        key: plan.target.key.clone(),
        prefix: plan.target.prefix.clone(),
    })
}

async fn ui_start_copy_or_move_task(
    st: &Arc<GatewayState>,
    account: &AuthAccount,
    src_bucket: &str,
    src_key: String,
    dst_bucket: String,
    dst_prefix: String,
    remove_source: bool,
) -> Result<UiTransferTaskSnapshot, UiHandlerError> {
    let plan = ui_prepare_copy_or_move_object(st, account, src_bucket, src_key, dst_bucket, dst_prefix, remove_source).await?;
    let kind_title = ui_transfer_kind_title(plan.kind);
    let task = st.create_ui_transfer_task(
        account.username.clone(),
        plan.kind,
        plan.name.clone(),
        plan.src_size,
        format!("Preparing {}", kind_title.to_lowercase()),
        ui_transfer_task_detail(0, plan.src_size),
        plan.source.clone(),
        plan.target.clone(),
    );

    if plan.remove_source && plan.source.bucket == plan.target.bucket {
        match ui_execute_copy_or_move_plan(st, &plan, Some(&task)).await {
            Ok(result) => {
                task.set_done(
                    plan.src_size,
                    format!("{} completed", kind_title),
                    result.key.clone(),
                );
                return Ok(task.snapshot());
            }
            Err(UiTransferExecError::Handler(err)) => {
                task.set_error(0, format!("{} failed", kind_title), err.to_string());
                return Err(err);
            }
            Err(UiTransferExecError::Cancelled) => {
                task.set_cancelled(0, format!("{} cancelled", kind_title), ui_transfer_task_detail(0, plan.src_size));
                return Err(UiHandlerError::Conflict(format!("{} cancelled", kind_title.to_lowercase())));
            }
        }
    }

    let st_cloned = st.clone();
    let task_cloned = task.clone();
    tokio::spawn(async move {
        match ui_execute_copy_or_move_plan(&st_cloned, &plan, Some(&task_cloned)).await {
            Ok(result) => {
                task_cloned.set_done(
                    plan.src_size,
                    format!("{} completed", kind_title),
                    result.key.clone(),
                );
            }
            Err(UiTransferExecError::Cancelled) => {
                let done_bytes = task_cloned.snapshot().done_bytes;
                match ui_cleanup_cancelled_transfer(&st_cloned, &plan).await {
                    Ok(()) => {
                        task_cloned.set_cancelled(
                            done_bytes,
                            format!("{} cancelled", kind_title),
                            ui_transfer_task_detail(done_bytes, plan.src_size),
                        );
                    }
                    Err(err) => {
                        task_cloned.set_error(
                            done_bytes,
                            format!("{} cancel failed", kind_title),
                            err.to_string(),
                        );
                    }
                }
            }
            Err(UiTransferExecError::Handler(err)) => {
                let done_bytes = task_cloned.snapshot().done_bytes;
                task_cloned.set_error(done_bytes, format!("{} failed", kind_title), err.to_string());
            }
        }
    });

    Ok(task.snapshot())
}

async fn ui_load_listing(
    st: &GatewayState,
    account: &AuthAccount,
    bucket: &str,
    prefix: String,
) -> Result<UiBrowsePayload, UiHandlerError> {
    let request_identity = request_identity_from_account(account);
    let export = ui_require_bucket_export(st, bucket)?;
    if !account_can_browse_ui_prefix(account, bucket, &prefix) {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} cannot browse bucket {} prefix {}",
            account.username, bucket, prefix
        )));
    }
    let dir_rel = normalize_dir_rel_from_prefix(&prefix);
    let entries = st
        .backend
        .list_dir(request_identity, bucket.to_string().into(), dir_rel.into())
        .await
        .map_err(|e| ui_handler_error_from_s3_error("list_dir", e))?;

    let mut dirs: Vec<UiDirItem> = Vec::new();
    let mut files: Vec<UiFileItem> = Vec::new();
    for entry in entries {
        if entry.name == MULTIPART_DIR_PREFIX {
            continue;
        }
        if entry.is_dir {
            let dir_prefix = format!("{}{}/", prefix, entry.name);
            if account_can_browse_ui_prefix(account, bucket, &dir_prefix) {
                dirs.push(UiDirItem {
                    name: entry.name,
                    mtime_ns: entry.mtime_ns,
                });
            }
            continue;
        }
        if entry.is_file {
            let key = ui_join_prefix_name(&prefix, &entry.name);
            if account_can_browse_ui_file(account, bucket, &key) {
                files.push(UiFileItem {
                    name: entry.name,
                    key,
                    size: entry.size,
                    mtime_ns: entry.mtime_ns,
                });
            }
        }
    }
    dirs.sort_by(|a, b| a.name.cmp(&b.name));
    files.sort_by(|a, b| a.name.cmp(&b.name));

    let provider_items = ui_bucket_provider_items(st, bucket, &export.remote_root_dir_abs)?;
    Ok(UiBrowsePayload {
        bucket: bucket.to_string(),
        mount_path: export.remote_root_dir_abs.clone(),
        provider_items,
        prefix: prefix.clone(),
        parent_prefix: ui_parent_prefix(&prefix),
        dirs,
        files,
    })
}

async fn ui_multipart_create_impl(
    st: &GatewayState,
    account: &AuthAccount,
    bucket: &str,
    prefix: String,
    name: String,
) -> Result<UiMultipartCreateBody, UiHandlerError> {
    let request_identity = request_identity_from_account(account);
    ui_require_bucket(st, bucket)?;
    let prefix = ui_validate_prefix(prefix)?;
    let name = ui_validate_upload_file_name(&name)?;
    let key = ui_validate_object_key(&ui_join_prefix_name(&prefix, &name), "key")?;
    if !account_has_object_action(account, bucket, &key, FluxonFsS3PermissionAction::PutObject) {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} lacks s3:PutObject on s3://{}/{}",
            account.username, bucket, key
        )));
    }
    let stat = st
        .backend
        .stat(
            request_identity.clone(),
            bucket.to_string().into(),
            key.clone().into(),
        )
        .await
        .map_err(|e| ui_handler_error_from_s3_error("stat", e))?;
    if stat.exists {
        return Err(UiHandlerError::Conflict("object already exists".to_string()));
    }
    let upload_id = multipart_create_upload_id(st, request_identity, bucket.to_string().into(), &key)
        .await
        .map_err(|e| ui_handler_error_from_s3_error("multipart create", e))?;
    Ok(UiMultipartCreateBody {
        ok: true,
        key,
        prefix,
        upload_id,
    })
}

async fn ui_delete_object_impl(
    st: &GatewayState,
    account: &AuthAccount,
    bucket: &str,
    key: String,
) -> Result<(), UiHandlerError> {
    let request_identity = request_identity_from_account(account);
    ui_require_bucket(st, bucket)?;
    let key = ui_validate_object_key(&key, "key")?;
    if !account_has_object_action(account, bucket, &key, FluxonFsS3PermissionAction::DeleteObject) {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} lacks s3:DeleteObject on s3://{}/{}",
            account.username, bucket, key
        )));
    }
    let stat = st
        .backend
        .stat(
            request_identity.clone(),
            bucket.to_string().into(),
            key.clone().into(),
        )
        .await
        .map_err(|e| ui_handler_error_from_s3_error("stat", e))?;
    if stat.exists {
        st.backend
            .unlink(request_identity, bucket.to_string().into(), key.into())
            .await
            .map_err(|e| ui_handler_error_from_s3_error("unlink", e))?;
    }
    Ok(())
}

async fn ui_delete_folder_impl(
    st: &GatewayState,
    account: &AuthAccount,
    bucket: &str,
    prefix: String,
) -> Result<(), UiHandlerError> {
    let request_identity = request_identity_from_account(account);
    ui_require_bucket(st, bucket)?;
    let prefix = ui_validate_prefix(prefix)?;
    let dir_rel = normalize_dir_rel_from_prefix(&prefix);
    if dir_rel == "." {
        return Err(UiHandlerError::BadRequest(
            "deleting bucket export root is not allowed".to_string(),
        ));
    }
    if !account_can_browse_ui_prefix(account, bucket, &prefix) {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} cannot browse bucket {} prefix {}",
            account.username, bucket, prefix
        )));
    }
    if !account_has_object_action(account, bucket, &dir_rel, FluxonFsS3PermissionAction::DeleteObject) {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} lacks s3:DeleteObject on s3://{}/{}",
            account.username, bucket, dir_rel
        )));
    }
    let dir_arc: Arc<str> = Arc::from(dir_rel.as_str());
    let stat = st
        .backend
        .stat(
            request_identity.clone(),
            bucket.to_string().into(),
            dir_arc.clone(),
        )
        .await
        .map_err(|e| ui_handler_error_from_s3_error("stat", e))?;
    if !stat.exists {
        return Ok(());
    }
    let entries = st
        .backend
        .list_dir(
            request_identity.clone(),
            bucket.to_string().into(),
            dir_arc.clone(),
        )
        .await
        .map_err(|e| ui_handler_error_from_s3_error("list_dir", e))?;
    for entry in entries {
        let child_key = if dir_rel == "." {
            entry.name.clone()
        } else {
            format!("{}/{}", dir_rel, entry.name)
        };
        if !account_has_object_action(
            account,
            bucket,
            &child_key,
            FluxonFsS3PermissionAction::DeleteObject,
        ) {
            return Err(UiHandlerError::Forbidden(format!(
                "account {} lacks s3:DeleteObject on s3://{}/{}",
                account.username, bucket, child_key
            )));
        }
        if entry.is_file {
            st.backend
                .unlink(
                    request_identity.clone(),
                    bucket.to_string().into(),
                    Arc::from(child_key.as_str()),
                )
                .await
                .map_err(|e| ui_handler_error_from_s3_error("unlink", e))?;
            continue;
        }
        if entry.is_dir {
            let child_prefix = format!("{}/", child_key);
            Box::pin(ui_delete_folder_impl(
                st,
                account,
                bucket,
                child_prefix,
            ))
            .await?;
        }
    }
    st.backend
        .rmdir(request_identity, bucket.to_string().into(), dir_arc)
        .await
        .map_err(|e| ui_handler_error_from_s3_error("rmdir", e))?;
    Ok(())
}

async fn ui_upload_object_impl(
    st: &GatewayState,
    account: &AuthAccount,
    bucket: &str,
    multipart: &mut Multipart,
) -> Result<UiKeyResultBody, UiHandlerError> {
    let request_identity = request_identity_from_account(account);
    ui_require_bucket(st, bucket)?;
    let bucket_arc: Arc<str> = bucket.to_string().into();
    let mut prefix: Option<String> = None;

    loop {
        let next = multipart
            .next_field()
            .await
            .map_err(|e| UiHandlerError::BadRequest(format!("read multipart field failed: {}", e)))?;
        let Some(mut field) = next else {
            break;
        };
        let name = field.name().unwrap_or("").to_string();
        if name == "prefix" {
            let value = field
                .text()
                .await
                .map_err(|e| UiHandlerError::BadRequest(format!("read prefix failed: {}", e)))?;
            prefix = Some(ui_validate_prefix(value)?);
            continue;
        }
        if name != "file" {
            continue;
        }
        let Some(prefix_value) = prefix.clone() else {
            return Err(UiHandlerError::BadRequest(
                "missing prefix (must appear before file)".to_string(),
            ));
        };
        let Some(file_name) = field.file_name().map(|v| v.to_string()) else {
            return Err(UiHandlerError::BadRequest("missing file name".to_string()));
        };

        let key = ui_validate_object_key(&ui_join_prefix_name(&prefix_value, &file_name), "key")?;
        if !account_has_object_action(account, bucket, &key, FluxonFsS3PermissionAction::PutObject) {
            return Err(UiHandlerError::Forbidden(format!(
                "account {} lacks s3:PutObject on s3://{}/{}",
                account.username, bucket, key
            )));
        }
        let key_arc: Arc<str> = key.clone().into();
        ensure_parent_dirs(st, request_identity.clone(), bucket_arc.clone(), &key)
            .await
            .map_err(|e| ui_handler_error_from_s3_error("mkdir", e))?;

        let dst_stat = st
            .backend
            .stat(
                request_identity.clone(),
                bucket_arc.clone(),
                key_arc.clone(),
            )
            .await
            .map_err(|e| ui_handler_error_from_s3_error("stat", e))?;
        if dst_stat.exists {
            return Err(UiHandlerError::Conflict("object already exists".to_string()));
        }

        let mut off: i64 = 0;
        loop {
            let chunk = field
                .chunk()
                .await
                .map_err(|e| UiHandlerError::BadRequest(format!("read file chunk failed: {}", e)))?;
            let Some(bytes) = chunk else {
                break;
            };
            if bytes.is_empty() {
                continue;
            }
            for part in bytes.chunks(FS_RPC_CHUNK_BYTES) {
                st.backend
                    .write_chunk(
                        request_identity.clone(),
                        bucket_arc.clone(),
                        key_arc.clone(),
                        off,
                        part.to_vec(),
                    )
                    .await
                    .map_err(|e| ui_handler_error_from_s3_error("write", e))?;
                off += part.len() as i64;
            }
        }
        st.backend
            .truncate(request_identity, bucket_arc, key_arc, off)
            .await
            .map_err(|e| ui_handler_error_from_s3_error("truncate", e))?;
        return Ok(UiKeyResultBody {
            ok: true,
            bucket: bucket.to_string(),
            key,
            prefix: prefix_value,
        });
    }

    Err(UiHandlerError::BadRequest("missing file".to_string()))
}

async fn ui_mkdir_impl(
    st: &GatewayState,
    account: &AuthAccount,
    bucket: &str,
    prefix: String,
    name: String,
) -> Result<UiKeyResultBody, UiHandlerError> {
    let request_identity = request_identity_from_account(account);
    ui_require_bucket(st, bucket)?;
    let prefix = ui_validate_prefix(prefix)?;
    let name = ui_validate_folder_name(&name)?;
    let key = ui_validate_object_key(&ui_join_prefix_name(&prefix, &name), "folder path")?;
    if !account_has_object_action(account, bucket, &key, FluxonFsS3PermissionAction::PutObject) {
        return Err(UiHandlerError::Forbidden(format!(
            "account {} lacks s3:PutObject on s3://{}/{}",
            account.username, bucket, key
        )));
    }
    let stat = st
        .backend
        .stat(
            request_identity.clone(),
            bucket.to_string().into(),
            key.clone().into(),
        )
        .await
        .map_err(|e| ui_handler_error_from_s3_error("stat", e))?;
    if stat.exists {
        return Err(UiHandlerError::Conflict("folder already exists".to_string()));
    }
    ensure_parent_dirs(st, request_identity.clone(), bucket.to_string().into(), &key)
        .await
        .map_err(|e| ui_handler_error_from_s3_error("mkdir parent", e))?;
    st.backend
        .mkdir(
            request_identity,
            bucket.to_string().into(),
            key.clone().into(),
            0o755,
        )
        .await
        .map_err(|e| ui_handler_error_from_s3_error("mkdir", e))?;
    Ok(UiKeyResultBody {
        ok: true,
        bucket: bucket.to_string(),
        key,
        prefix,
    })
}

#[cfg(test)]
async fn ui_copy_or_move_object_impl(
    st: &GatewayState,
    account: &AuthAccount,
    src_bucket: &str,
    src_key: String,
    dst_bucket: String,
    dst_prefix: String,
    remove_source: bool,
) -> Result<UiKeyResultBody, UiHandlerError> {
    let plan =
        ui_prepare_copy_or_move_object(st, account, src_bucket, src_key, dst_bucket, dst_prefix, remove_source).await?;
    match ui_execute_copy_or_move_plan(st, &plan, None).await {
        Ok(result) => Ok(result),
        Err(UiTransferExecError::Handler(err)) => Err(err),
        Err(UiTransferExecError::Cancelled) => {
            ui_cleanup_cancelled_transfer(st, &plan).await?;
            Err(UiHandlerError::Conflict(format!(
                "{} cancelled",
                ui_transfer_kind_title(plan.kind).to_lowercase()
            )))
        }
    }
}

include!("ui_ssr_render_handlers.rs");
