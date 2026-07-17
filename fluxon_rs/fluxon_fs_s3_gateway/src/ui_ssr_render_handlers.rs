// English note:
// - This file holds SSR page rendering helpers and HTTP handlers extracted from ui_ssr.rs.
// - Shared validation and storage helpers stay in ui_ssr.rs so handler flow remains thin here.

use askama::Template;
use fluxon_fs_core::config::FluxonFsTransferJobState;

#[derive(Clone)]
struct UiCrumbView {
    href: String,
    has_href: bool,
    label: String,
    current: bool,
}

#[derive(Clone)]
struct UiCheckboxUserView {
    username: String,
    is_manager: bool,
    checked: bool,
}

#[derive(Clone)]
struct UiActionButtonView {
    is_link: bool,
    href: String,
    id: String,
    class_name: String,
    button_type: String,
    label: String,
}

#[derive(Clone)]
struct UiSelectOptionView {
    value: String,
    label: String,
}

fn render_template<T: Template>(template: &T) -> String {
    template.render().unwrap()
}

#[derive(Template)]
#[template(path = "ui/page.html")]
struct UiPageTemplate {
    ui_page_title: String,
    title_suffix: String,
    ui_css: String,
    ui_js: String,
    home_href: String,
    buckets_href: String,
    transfers_href: String,
    buckets_active_class: String,
    transfers_active_class: String,
    crumbs_html: String,
    page_title: String,
    show_subtitle: bool,
    subtitle_html: String,
    show_top_buttons: bool,
    top_buttons_html: String,
    user_actions_html: String,
    main_html: String,
}

#[derive(Template)]
#[template(path = "ui/crumbs.html")]
struct UiCrumbsTemplate {
    crumbs: Vec<UiCrumbView>,
}

#[derive(Template)]
#[template(path = "ui/provider_display.html")]
struct UiProviderDisplayTemplate {
    display: String,
}

#[derive(Template)]
#[template(path = "ui/user_actions.html")]
struct UiUserActionsTemplate {
    viewer_username: String,
    show_as_user: bool,
    actor_username: String,
    exit_view_as_href: String,
    account_password_href: String,
    show_manage: bool,
    admin_manage_href: String,
}

#[derive(Template)]
#[template(path = "ui/scope_user_checkboxes.html")]
struct UiScopeUserCheckboxesTemplate {
    show_empty_hint: bool,
    field_name: String,
    users: Vec<UiCheckboxUserView>,
}

#[derive(Template)]
#[template(path = "ui/action_buttons.html")]
struct UiActionButtonsTemplate {
    buttons: Vec<UiActionButtonView>,
}

#[derive(Clone)]
struct UiAdminHomeCardView {
    href: String,
    icon_style: String,
    icon_html: String,
    title: String,
    desc: String,
}

#[derive(Clone)]
struct UiBucketRowView {
    filter_text: String,
    name: String,
    provider_display_html: String,
    nodes_count: String,
    bucket_href: String,
}

#[derive(Template)]
#[template(path = "ui/pathbar.html")]
struct UiPathbarTemplate {
    show_subject: bool,
    subject_label: String,
    subject_username: String,
    show_as: bool,
    actor_username: String,
    show_cluster: bool,
    cluster_name: String,
    show_access_db: bool,
    access_db_path: String,
}

#[derive(Template)]
#[template(path = "ui/transfers_main.html")]
struct UiTransfersMainTemplate;

#[derive(Template)]
#[template(path = "ui/admin_home_main.html")]
struct UiAdminHomeMainTemplate {
    cards: Vec<UiAdminHomeCardView>,
}

#[derive(Template)]
#[template(path = "ui/account_password_main.html")]
struct UiAccountPasswordMainTemplate {
    show_error: bool,
    error_msg: String,
}

#[derive(Template)]
#[template(path = "ui/buckets_index_main.html")]
struct UiBucketsIndexMainTemplate {
    rows: Vec<UiBucketRowView>,
}

#[derive(Template)]
#[template(path = "ui/bucket_browse_subtitle.html")]
struct UiBucketBrowseSubtitleTemplate {
    bucket: String,
    provider_display_html: String,
}

#[derive(Template)]
#[template(path = "ui/bucket_browse_main.html")]
struct UiBucketBrowseMainTemplate {
    bootstrap_json: String,
    prefix: String,
    prefix_display: String,
}

#[derive(Clone)]
struct UiScopeAccessCardView {
    filter_text: String,
    export_name: String,
    prefix_display: String,
    prefix_value: String,
    mode_title: String,
    mode_value: String,
    delete_action: String,
    update_users_action: String,
    bound_users_html: String,
}

#[derive(Template)]
#[template(path = "ui/scope_access_main.html")]
struct UiScopeAccessMainTemplate {
    warnings: Vec<String>,
    success_messages: Vec<String>,
    show_content: bool,
    create_action: String,
    bucket_options: Vec<UiSelectOptionView>,
    mode_options: Vec<UiSelectOptionView>,
    create_bound_users_html: String,
    scope_count_label: String,
    show_empty_scopes: bool,
    scopes: Vec<UiScopeAccessCardView>,
}

#[derive(Clone)]
struct UiAdminUserCardView {
    filter_text: String,
    username: String,
    scope_count_label: String,
    is_manager: bool,
    view_as_href: String,
    delete_action: String,
    update_access_action: String,
    reset_action: String,
}

#[derive(Template)]
#[template(path = "ui/admin_users_main.html")]
struct UiAdminUsersMainTemplate {
    warnings: Vec<String>,
    show_content: bool,
    create_action: String,
    scope_access_href: String,
    user_count_label: String,
    show_empty_users: bool,
    users: Vec<UiAdminUserCardView>,
}

#[derive(Clone)]
struct UiFsMasterRuntimeAgentView {
    agent_instance_key: String,
    runtime_exports_count: usize,
    show_empty_runtime_export_names: bool,
    runtime_export_names_display: String,
}

#[derive(Template)]
#[template(path = "ui/fs_master_runtime_section.html")]
struct UiFsMasterRuntimeSectionTemplate {
    show_empty_agents: bool,
    agents: Vec<UiFsMasterRuntimeAgentView>,
}

#[derive(Clone)]
struct UiFsMasterMemberView {
    kind: String,
    member_id: String,
    owner_id: String,
    hostname: String,
    addr: String,
    pid: String,
    cmd: String,
}

#[derive(Template)]
#[template(path = "ui/fs_master_membership_section.html")]
struct UiFsMasterMembershipSectionTemplate {
    show_empty_members: bool,
    members: Vec<UiFsMasterMemberView>,
}

#[derive(Clone)]
struct UiFsMasterManagedExportView {
    export_name: String,
    remote_root_dir_abs: String,
    remove_action: String,
    agent_instance_key: String,
    remove_icon_html: String,
}

#[derive(Clone)]
struct UiFsMasterExportManagerAgentView {
    agent_instance_key: String,
    managed_exports_count: usize,
    show_empty_managed_exports: bool,
    managed_exports: Vec<UiFsMasterManagedExportView>,
}

#[derive(Template)]
#[template(path = "ui/fs_master_export_manager_section.html")]
struct UiFsMasterExportManagerSectionTemplate {
    show_empty_agents: bool,
    agents: Vec<UiFsMasterExportManagerAgentView>,
    add_action: String,
    browse_href: String,
}

#[derive(Clone)]
struct UiFsMasterMountOverviewRowView {
    remote_root_dir_abs: String,
    mounts_count: usize,
    externals_count: usize,
    providers_count: usize,
    owners_mapped_text: String,
}

#[derive(Clone)]
struct UiFsMasterMountRowView {
    external_instance_key: String,
    local_mount_dirs: Vec<String>,
}

#[derive(Clone)]
struct UiFsMasterProviderRowView {
    config_key: String,
    nodes: String,
    mapped_lines: Vec<String>,
}

#[derive(Clone)]
struct UiFsMasterMountDetailView {
    remote_root_dir_abs: String,
    mounts_count: usize,
    providers_count: usize,
    show_empty_mounts: bool,
    mounts: Vec<UiFsMasterMountRowView>,
    show_empty_providers: bool,
    providers: Vec<UiFsMasterProviderRowView>,
}

#[derive(Template)]
#[template(path = "ui/fs_master_mount_graph_section.html")]
struct UiFsMasterMountGraphSectionTemplate {
    show_empty_overview: bool,
    overview_rows: Vec<UiFsMasterMountOverviewRowView>,
    details: Vec<UiFsMasterMountDetailView>,
}

#[derive(Template)]
#[template(path = "ui/fs_master_main.html")]
struct UiFsMasterMainTemplate {
    warnings: Vec<String>,
    member_count: usize,
    agent_count: usize,
    controller_count: usize,
    runtime_exports_count: usize,
    managed_exports_count: usize,
    runtime_section_html: String,
    membership_section_html: String,
    export_manager_section_html: String,
    mount_graph_section_html: String,
}

fn ui_page_html(
    title_suffix: &str,
    home_href: &str,
    buckets_href: &str,
    transfers_href: &str,
    active_nav: &str,
    crumbs_html: &str,
    page_title: &str,
    page_subtitle_html: Option<&str>,
    top_buttons_html: Option<&str>,
    user_actions_html: &str,
    main_html: &str,
) -> String {
    let ui_js = UI_JS
        .replace("__UI_MULTIPART_UPLOAD_PART_BYTES__", &UI_MULTIPART_UPLOAD_PART_BYTES.to_string())
        .replace("__UI_MULTIPART_UPLOAD_MAX_INFLIGHT__", &UI_MULTIPART_UPLOAD_MAX_INFLIGHT.to_string())
        .replace(
            "__DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY__",
            &DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY.to_string(),
        );
    render_template(&UiPageTemplate {
        ui_page_title: UI_PAGE_TITLE.to_string(),
        title_suffix: title_suffix.to_string(),
        ui_css: UI_CSS.to_string(),
        ui_js,
        home_href: home_href.to_string(),
        buckets_href: buckets_href.to_string(),
        transfers_href: transfers_href.to_string(),
        buckets_active_class: if active_nav == "buckets" {
            " active".to_string()
        } else {
            String::new()
        },
        transfers_active_class: if active_nav == "transfers" {
            " active".to_string()
        } else {
            String::new()
        },
        crumbs_html: crumbs_html.to_string(),
        page_title: page_title.to_string(),
        show_subtitle: page_subtitle_html.is_some(),
        subtitle_html: page_subtitle_html.unwrap_or("").to_string(),
        show_top_buttons: top_buttons_html.is_some(),
        top_buttons_html: top_buttons_html.unwrap_or("").to_string(),
        user_actions_html: user_actions_html.to_string(),
        main_html: main_html.to_string(),
    })
}

fn ui_prefix_from_query(q: Option<String>) -> Result<String, Response> {
    // Root semantics:
    // - prefix is omitted => root
    // - prefix is non-empty => must be a directory prefix ending with '/'
    ui_validate_prefix(q.unwrap_or_else(|| "".to_string())).map_err(UiHandlerError::into_text_response)
}

fn ui_parent_prefix(prefix: &str) -> Option<String> {
    let p = prefix.trim_start_matches('/').trim_end_matches('/');
    if p.is_empty() {
        return None;
    }
    let mut parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
    parts.pop();
    if parts.is_empty() {
        Some("".to_string())
    } else {
        Some(format!("{}/", parts.join("/")))
    }
}

fn ui_prefix_crumbs_html(bucket: &str, prefix: &str, as_user: Option<&str>) -> String {
    let mut crumbs = vec![
        UiCrumbView {
            href: ui_href_with_as("../", as_user),
            has_href: true,
            label: "Object Storage".to_string(),
            current: false,
        },
        UiCrumbView {
            href: ui_href_with_as("../", as_user),
            has_href: true,
            label: "Buckets".to_string(),
            current: false,
        },
        UiCrumbView {
            href: ui_href_with_as("./", as_user),
            has_href: true,
            label: bucket.to_string(),
            current: true,
        },
    ];
    if prefix.is_empty() {
        return render_template(&UiCrumbsTemplate { crumbs });
    }
    let p = prefix.trim_end_matches('/');
    let parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
    let mut cur = String::new();
    for seg in parts {
        if cur.is_empty() {
            cur = format!("{}/", seg);
        } else {
            cur = format!("{}{}/", cur, seg);
        }
        crumbs.push(UiCrumbView {
            href: ui_href_with_as(&format!("./?prefix={}", urlencoding::encode(&cur)), as_user),
            has_href: true,
            label: seg.to_string(),
            current: false,
        });
    }
    render_template(&UiCrumbsTemplate { crumbs })
}

fn ui_manage_subpage_crumbs_html(current_label: &str, as_user: Option<&str>) -> String {
    render_template(&UiCrumbsTemplate {
        crumbs: vec![
            UiCrumbView {
                href: ui_href_with_as("../../", as_user),
                has_href: true,
                label: "Object Storage".to_string(),
                current: false,
            },
            UiCrumbView {
                href: ui_href_with_as("../", as_user),
                has_href: true,
                label: "Manage".to_string(),
                current: false,
            },
            UiCrumbView {
                href: String::new(),
                has_href: false,
                label: current_label.to_string(),
                current: true,
            },
        ],
    })
}

fn ui_manage_pathbar_html(
    st: &GatewayState,
    identity: &UiIdentity,
    subject_label: &str,
    show_as: bool,
    show_cluster: bool,
) -> String {
    render_template(&UiPathbarTemplate {
        show_subject: true,
        subject_label: subject_label.to_string(),
        subject_username: identity.viewer_username().to_string(),
        show_as: show_as && identity.is_impersonating(),
        actor_username: identity.actor_username().to_string(),
        show_cluster,
        cluster_name: st.cluster_name.as_str().to_string(),
        show_access_db: true,
        access_db_path: st.access_db_path().to_string(),
    })
}

fn fmt_bytes(n: i64) -> String {
    if n < 0 {
        return "-".to_string();
    }
    let n = n as f64;
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut v = n;
    let mut idx = 0usize;
    while v >= 1024.0 && idx + 1 < units.len() {
        v /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{} {}", v as i64, units[idx])
    } else {
        format!("{:.1} {}", v, units[idx])
    }
}

fn ui_provider_display_scroll_html(provider_items: &[UiBrowseProviderItem]) -> String {
    render_template(&UiProviderDisplayTemplate {
        display: ui_provider_display_text(provider_items),
    })
}

async fn ui_redirect_to_ui_slash() -> Response {
    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    // Causal chain:
    // - The UI is nested under a dynamic base path (direct: /fs_s3/..., proxy: /r/fs_s3/<cluster>/...).
    // - Relative links only work reliably when the current URL ends with a trailing slash.
    // - Redirect to the canonical "/ui/" *relative to the current base*.
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_static("./ui/"));
    resp
}

async fn ui_bucket_redirect_to_slash(Path(bucket): Path<String>) -> Response {
    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    resp.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!("./{}/", urlencoding::encode(&bucket))).unwrap(),
    );
    resp
}

async fn ui_admin_permissions_redirect_to_slash() -> Response {
    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_static("./permissions/"));
    resp
}

async fn ui_admin_redirect_to_slash() -> Response {
    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_static("./admin/"));
    resp
}

async fn ui_admin_users_redirect_to_slash() -> Response {
    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_static("./users/"));
    resp
}

async fn ui_account_password_redirect_to_slash() -> Response {
    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_static("./password/"));
    resp
}

async fn ui_transfers_redirect_to_slash() -> Response {
    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_static("./transfers/"));
    resp
}

async fn ui_transfers_page(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let crumbs = render_template(&UiCrumbsTemplate {
        crumbs: vec![
            UiCrumbView {
                href: "../".to_string(),
                has_href: true,
                label: "Object Storage".to_string(),
                current: false,
            },
            UiCrumbView {
                href: String::new(),
                has_href: false,
                label: "Transfers".to_string(),
                current: true,
            },
        ],
    });

    let home_href = ui_href_with_as("../", identity.as_user.as_deref());
    let buckets_href = ui_href_with_as("../", identity.as_user.as_deref());
    let transfers_href = ui_href_with_as("./", identity.as_user.as_deref());
    let account_password_href = ui_href_with_as("../account/password/", identity.as_user.as_deref());
    let admin_manage_href = ui_href_with_as("../admin/", identity.as_user.as_deref());
    let user_actions_html = ui_user_actions_html(&identity, &account_password_href, &admin_manage_href, "../");
    let top_buttons_html = render_template(&UiActionButtonsTemplate {
        buttons: vec![UiActionButtonView {
            is_link: false,
            href: String::new(),
            id: "transfers_clear_btn".to_string(),
            class_name: "btn".to_string(),
            button_type: "button".to_string(),
            label: "Clear Completed".to_string(),
        }],
    });

    let html = ui_page_html(
        "Transfers",
        &home_href,
        &buckets_href,
        &transfers_href,
        "transfers",
        &crumbs,
        "Transfers",
        None,
        Some(&top_buttons_html),
        &user_actions_html,
        &render_template(&UiTransfersMainTemplate),
    );
    Html(html).into_response()
}

#[derive(serde::Deserialize)]
struct UiAsQuery {
    #[serde(rename = "as")]
    as_user: Option<String>,
}

#[derive(serde::Deserialize)]
struct UiTransferFileIssueQuery {
    #[serde(rename = "as")]
    as_user: Option<String>,
    batch_id: String,
    relpath: String,
}

fn ui_append_query_param(url: &str, key: &str, value: &str) -> String {
    let sep = if url.contains('?') { "&" } else { "?" };
    format!("{}{}{}={}", url, sep, key, urlencoding::encode(value))
}

fn ui_href_with_as(href: &str, as_user: Option<&str>) -> String {
    let Some(u) = as_user else {
        return href.to_string();
    };
    ui_append_query_param(href, "as", u)
}

fn ui_as_user_from_query_string(query: Option<&str>) -> Option<String> {
    let q = query?;
    for part in q.split('&') {
        let Some((k, v)) = part.split_once('=') else {
            continue;
        };
        if k != "as" {
            continue;
        }
        return urlencoding::decode(v).ok().map(|x| x.into_owned());
    }
    None
}

fn ui_user_actions_html(
    identity: &UiIdentity,
    account_password_href: &str,
    admin_manage_href: &str,
    exit_view_as_href: &str,
) -> String {
    render_template(&UiUserActionsTemplate {
        viewer_username: identity.viewer_username().to_string(),
        show_as_user: identity.is_impersonating(),
        actor_username: identity.actor_username().to_string(),
        exit_view_as_href: exit_view_as_href.to_string(),
        account_password_href: account_password_href.to_string(),
        show_manage: account_can_manage_permissions(&identity.viewer),
        admin_manage_href: admin_manage_href.to_string(),
    })
}

struct UiAdminScopeUsersForm {
    bucket: String,
    prefix: String,
    mode: String,
    usernames: Option<Vec<String>>,
}

fn ui_set_single_form_field(
    slot: &mut Option<String>,
    field_name: &'static str,
    value: String,
) -> Result<(), UiHandlerError> {
    if slot.is_some() {
        return Err(UiHandlerError::BadRequest(format!(
            "duplicate form field: {}",
            field_name
        )));
    }
    *slot = Some(value);
    Ok(())
}

fn ui_parse_scope_users_form(raw_form: &[u8]) -> Result<UiAdminScopeUsersForm, UiHandlerError> {
    let mut bucket: Option<String> = None;
    let mut prefix: Option<String> = None;
    let mut mode: Option<String> = None;
    let mut usernames: Vec<String> = Vec::new();

    for (key, value) in url::form_urlencoded::parse(raw_form) {
        match key.as_ref() {
            "bucket" => ui_set_single_form_field(&mut bucket, "bucket", value.into_owned())?,
            "prefix" => ui_set_single_form_field(&mut prefix, "prefix", value.into_owned())?,
            "mode" => ui_set_single_form_field(&mut mode, "mode", value.into_owned())?,
            "usernames" => usernames.push(value.into_owned()),
            _ => {}
        }
    }

    Ok(UiAdminScopeUsersForm {
        bucket: bucket.ok_or_else(|| {
            UiHandlerError::BadRequest("missing form field: bucket".to_string())
        })?,
        prefix: prefix.ok_or_else(|| {
            UiHandlerError::BadRequest("missing form field: prefix".to_string())
        })?,
        mode: mode.ok_or_else(|| {
            UiHandlerError::BadRequest("missing form field: mode".to_string())
        })?,
        usernames: if usernames.is_empty() {
            None
        } else {
            Some(usernames)
        },
    })
}

#[derive(serde::Deserialize)]
struct UiAdminScopeDeleteForm {
    bucket: String,
    prefix: String,
    mode: String,
}

fn ui_validate_scope_bucket(st: &GatewayState, value: &str) -> Result<String, UiHandlerError> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Err(UiHandlerError::BadRequest(
            "scope_access bucket must be non-empty".to_string(),
        ));
    }
    if trimmed != value {
        return Err(UiHandlerError::BadRequest(
            "scope_access bucket must not have leading/trailing whitespace".to_string(),
        ));
    }
    if st
        .ensure_effective_fs_export(&trimmed)
        .map_err(|err| UiHandlerError::BadGateway(format!("load effective export failed: {}", err)))?
        .is_none()
    {
        return Err(UiHandlerError::BadRequest(format!(
            "scope_access bucket not found: {}",
            trimmed
        )));
    }
    Ok(trimmed)
}

fn ui_validate_scope_mode(value: &str) -> Result<ScopeAccessMode, UiHandlerError> {
    ScopeAccessMode::from_form_value(value).ok_or_else(|| {
        UiHandlerError::BadRequest(format!("scope_access mode invalid: {}", value))
    })
}

fn ui_validate_scope_usernames(
    raw: Option<Vec<String>>,
    users: &[AccessUser],
) -> Result<Vec<String>, UiHandlerError> {
    let known_users: BTreeSet<&str> = users.iter().map(|user| user.username.as_str()).collect();
    let Some(values) = raw else {
        return Err(UiHandlerError::BadRequest(
            "scope_access must bind at least one user".to_string(),
        ));
    };
    let mut out: Vec<String> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for value in values {
        let trimmed = value.trim().to_string();
        if trimmed.is_empty() {
            return Err(UiHandlerError::BadRequest(
                "scope_access usernames must be non-empty".to_string(),
            ));
        }
        if trimmed != value {
            return Err(UiHandlerError::BadRequest(format!(
                "scope_access username must not have leading/trailing whitespace: {}",
                value
            )));
        }
        if !known_users.contains(trimmed.as_str()) {
            return Err(UiHandlerError::BadRequest(format!(
                "scope_access references unknown user: {}",
                trimmed
            )));
        }
        if !seen.insert(trimmed.clone()) {
            return Err(UiHandlerError::BadRequest(format!(
                "scope_access duplicates user: {}",
                trimmed
            )));
        }
        out.push(trimmed);
    }
    if out.is_empty() {
        return Err(UiHandlerError::BadRequest(
            "scope_access must bind at least one user".to_string(),
        ));
    }
    out.sort();
    Ok(out)
}

fn ui_scope_user_checkboxes_html(
    users: &[AccessUser],
    selected_usernames: &BTreeSet<String>,
    field_name: &str,
) -> String {
    let checkbox_users: Vec<UiCheckboxUserView> = users
        .iter()
        .map(|user| UiCheckboxUserView {
            username: user.username.clone(),
            is_manager: user.can_manage_users,
            checked: selected_usernames.contains(&user.username),
        })
        .collect();
    render_template(&UiScopeUserCheckboxesTemplate {
        show_empty_hint: checkbox_users.is_empty(),
        field_name: field_name.to_string(),
        users: checkbox_users,
    })
}

fn ui_scope_access_page_response(
    st: &GatewayState,
    identity: &UiIdentity,
    error_msg: Option<&str>,
    success_msg: Option<&str>,
    status: StatusCode,
) -> Response {
    let mut resp = Html(ui_render_scope_access_page(st, identity, error_msg, success_msg)).into_response();
    *resp.status_mut() = status;
    resp
}

fn ui_render_scope_access_page(
    st: &GatewayState,
    identity: &UiIdentity,
    error_msg: Option<&str>,
    success_msg: Option<&str>,
) -> String {
    let crumbs = ui_manage_subpage_crumbs_html("Scope Access", identity.as_user.as_deref());
    let subtitle = ui_manage_pathbar_html(st, identity, "editor", false, false);
    let mut warnings: Vec<String> = error_msg.into_iter().map(|msg| msg.to_string()).collect();
    let success_messages: Vec<String> = success_msg.into_iter().map(|msg| msg.to_string()).collect();
    let create_action = ui_href_with_as("./create", identity.as_user.as_deref());
    let top_buttons_html = render_template(&UiActionButtonsTemplate {
        buttons: vec![UiActionButtonView {
            is_link: true,
            href: ui_href_with_as("../users/", identity.as_user.as_deref()),
            id: String::new(),
            class_name: "btn".to_string(),
            button_type: String::new(),
            label: "Open Users".to_string(),
        }],
    });
    let bucket_options = match st.load_effective_fs_exports() {
        Ok(exports) => exports
            .keys()
            .map(|bucket| UiSelectOptionView {
                value: bucket.clone(),
                label: bucket.clone(),
            })
            .collect(),
        Err(err) => {
            warnings.push(format!("load effective exports failed: {}", err));
            Vec::new()
        }
    };

    let main = match access_model_from_s3_permission_list(&st.permission_list.read()) {
        Ok(model) => {
            let mode_options: Vec<UiSelectOptionView> = [ScopeAccessMode::Read, ScopeAccessMode::ReadWrite]
                .into_iter()
                .map(|mode| UiSelectOptionView {
                    value: mode.form_value().to_string(),
                    label: mode.title().to_string(),
                })
                .collect();
            let scopes: Vec<UiScopeAccessCardView> = model
                .scope_access
                .iter()
                .map(|scope| {
                    let selected_usernames: BTreeSet<String> = scope.usernames.iter().cloned().collect();
                    UiScopeAccessCardView {
                        filter_text: format!(
                            "{} {} {} {}",
                            scope.export_name,
                            scope.prefix,
                            scope.mode.form_value(),
                            scope.usernames.join(" ")
                        ),
                        export_name: scope.export_name.clone(),
                        prefix_display: if scope.prefix.is_empty() {
                            "/".to_string()
                        } else {
                            scope.prefix.clone()
                        },
                        prefix_value: scope.prefix.clone(),
                        mode_title: scope.mode.title().to_string(),
                        mode_value: scope.mode.form_value().to_string(),
                        delete_action: ui_href_with_as("./delete", identity.as_user.as_deref()),
                        update_users_action: ui_href_with_as("./update_users", identity.as_user.as_deref()),
                        bound_users_html: ui_scope_user_checkboxes_html(
                            &model.users,
                            &selected_usernames,
                            "usernames",
                        ),
                    }
                })
                .collect();
            render_template(&UiScopeAccessMainTemplate {
                warnings,
                success_messages,
                show_content: true,
                create_action,
                bucket_options,
                mode_options,
                create_bound_users_html: ui_scope_user_checkboxes_html(
                    &model.users,
                    &BTreeSet::new(),
                    "usernames",
                ),
                scope_count_label: if model.scope_access.len() == 1 {
                    "1 scope".to_string()
                } else {
                    format!("{} scopes", model.scope_access.len())
                },
                show_empty_scopes: scopes.is_empty(),
                scopes,
            })
        }
        Err(err) => {
            warnings.push(err);
            render_template(&UiScopeAccessMainTemplate {
                warnings,
                success_messages,
                show_content: false,
                create_action,
                bucket_options: Vec::new(),
                mode_options: Vec::new(),
                create_bound_users_html: String::new(),
                scope_count_label: String::new(),
                show_empty_scopes: true,
                scopes: Vec::new(),
            })
        }
    };

    let home_href = ui_href_with_as("../../", identity.as_user.as_deref());
    let buckets_href = ui_href_with_as("../../", identity.as_user.as_deref());
    let transfers_href = ui_href_with_as("../../transfers/", identity.as_user.as_deref());
    let account_password_href = ui_href_with_as("../../account/password/", identity.as_user.as_deref());
    let admin_manage_href = ui_href_with_as("../", identity.as_user.as_deref());
    let user_actions_html = ui_user_actions_html(identity, &account_password_href, &admin_manage_href, "./");
    ui_page_html(
        "Scope Access",
        &home_href,
        &buckets_href,
        &transfers_href,
        "buckets",
        &crumbs,
        "Scope Access",
        Some(&subtitle),
        Some(&top_buttons_html),
        &user_actions_html,
        &main,
    )
}

async fn ui_admin_permissions_page(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "manage scope_access");
    }
    Html(ui_render_scope_access_page(&st, &identity, None, None)).into_response()
}

async fn ui_admin_scope_create(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    axum::extract::RawForm(form): axum::extract::RawForm,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "manage scope_access");
    }
    let f = match ui_parse_scope_users_form(form.as_ref()) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(
                &st,
                &identity,
                Some(&err.to_string()),
                None,
                StatusCode::BAD_REQUEST,
            )
        }
    };

    let mut model = match access_model_from_s3_permission_list(&st.permission_list.read()) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };

    let bucket = match ui_validate_scope_bucket(&st, &f.bucket) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(&st, &identity, Some(&err.to_string()), None, StatusCode::BAD_REQUEST)
        }
    };
    let prefix = match ui_validate_prefix(f.prefix) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(&st, &identity, Some(&err.to_string()), None, StatusCode::BAD_REQUEST)
        }
    };
    let mode = match ui_validate_scope_mode(&f.mode) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(&st, &identity, Some(&err.to_string()), None, StatusCode::BAD_REQUEST)
        }
    };
    let usernames = match ui_validate_scope_usernames(f.usernames, &model.users) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(&st, &identity, Some(&err.to_string()), None, StatusCode::BAD_REQUEST)
        }
    };

    if model
        .scope_access
        .iter()
        .any(|scope| scope.export_name == bucket && scope.prefix == prefix && scope.mode == mode)
    {
        return ui_scope_access_page_response(
            &st,
            &identity,
            Some("scope_access already exists for the same export_name + prefix + mode"),
            None,
            StatusCode::CONFLICT,
        );
    }

    model.scope_access.push(ScopeAccess {
        export_name: bucket,
        prefix,
        mode,
        usernames,
    });
    model.scope_access.sort_by(|a, b| {
        a.export_name
            .cmp(&b.export_name)
            .then(a.prefix.cmp(&b.prefix))
            .then(a.mode.cmp(&b.mode))
    });

    let permission_list = match s3_permission_list_from_access_model(&model) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    if let Err(err) = persist_permission_list(&st, &permission_list) {
        return ui_scope_access_page_response(&st, &identity, Some(&err), None, StatusCode::BAD_GATEWAY);
    }

    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::SEE_OTHER;
    let mut href = "./".to_string();
    if let Some(as_user) = identity.as_user.as_deref() {
        href = ui_href_with_as(&href, Some(as_user));
    }
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&href).unwrap());
    resp
}

async fn ui_admin_scope_update_users(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    axum::extract::RawForm(form): axum::extract::RawForm,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "manage scope_access");
    }
    let f = match ui_parse_scope_users_form(form.as_ref()) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(
                &st,
                &identity,
                Some(&err.to_string()),
                None,
                StatusCode::BAD_REQUEST,
            )
        }
    };

    let mut model = match access_model_from_s3_permission_list(&st.permission_list.read()) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    let bucket = match ui_validate_scope_bucket(&st, &f.bucket) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(&st, &identity, Some(&err.to_string()), None, StatusCode::BAD_REQUEST)
        }
    };
    let prefix = match ui_validate_prefix(f.prefix) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(&st, &identity, Some(&err.to_string()), None, StatusCode::BAD_REQUEST)
        }
    };
    let mode = match ui_validate_scope_mode(&f.mode) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(&st, &identity, Some(&err.to_string()), None, StatusCode::BAD_REQUEST)
        }
    };
    let usernames = match ui_validate_scope_usernames(f.usernames, &model.users) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(&st, &identity, Some(&err.to_string()), None, StatusCode::BAD_REQUEST)
        }
    };

    let Some(scope) = model
        .scope_access
        .iter_mut()
        .find(|scope| scope.export_name == bucket && scope.prefix == prefix && scope.mode == mode)
    else {
        return ui_scope_access_page_response(
            &st,
            &identity,
            Some("scope_access not found"),
            None,
            StatusCode::NOT_FOUND,
        );
    };
    scope.usernames = usernames;

    let permission_list = match s3_permission_list_from_access_model(&model) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    if let Err(err) = persist_permission_list(&st, &permission_list) {
        return ui_scope_access_page_response(&st, &identity, Some(&err), None, StatusCode::BAD_GATEWAY);
    }

    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::SEE_OTHER;
    let mut href = "./".to_string();
    if let Some(as_user) = identity.as_user.as_deref() {
        href = ui_href_with_as(&href, Some(as_user));
    }
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&href).unwrap());
    resp
}

async fn ui_admin_scope_delete(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiAdminScopeDeleteForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "manage scope_access");
    }

    let mut model = match access_model_from_s3_permission_list(&st.permission_list.read()) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    let bucket = match ui_validate_scope_bucket(&st, &f.bucket) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(&st, &identity, Some(&err.to_string()), None, StatusCode::BAD_REQUEST)
        }
    };
    let prefix = match ui_validate_prefix(f.prefix) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(&st, &identity, Some(&err.to_string()), None, StatusCode::BAD_REQUEST)
        }
    };
    let mode = match ui_validate_scope_mode(&f.mode) {
        Ok(value) => value,
        Err(err) => {
            return ui_scope_access_page_response(&st, &identity, Some(&err.to_string()), None, StatusCode::BAD_REQUEST)
        }
    };

    let before = model.scope_access.len();
    model
        .scope_access
        .retain(|scope| !(scope.export_name == bucket && scope.prefix == prefix && scope.mode == mode));
    if model.scope_access.len() == before {
        return ui_scope_access_page_response(
            &st,
            &identity,
            Some("scope_access not found"),
            None,
            StatusCode::NOT_FOUND,
        );
    }

    let permission_list = match s3_permission_list_from_access_model(&model) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    if let Err(err) = persist_permission_list(&st, &permission_list) {
        return ui_scope_access_page_response(&st, &identity, Some(&err), None, StatusCode::BAD_GATEWAY);
    }

    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::SEE_OTHER;
    let mut href = "./".to_string();
    if let Some(as_user) = identity.as_user.as_deref() {
        href = ui_href_with_as(&href, Some(as_user));
    }
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&href).unwrap());
    resp
}

fn ui_render_admin_home_page(st: &GatewayState, identity: &UiIdentity) -> String {
    let crumbs = render_template(&UiCrumbsTemplate {
        crumbs: vec![
            UiCrumbView {
                href: ui_href_with_as("../", identity.as_user.as_deref()),
                has_href: true,
                label: "Object Storage".to_string(),
                current: false,
            },
            UiCrumbView {
                href: String::new(),
                has_href: false,
                label: "Manage".to_string(),
                current: true,
            },
        ],
    });

    let subtitle = render_template(&UiPathbarTemplate {
        show_subject: true,
        subject_label: "viewer".to_string(),
        subject_username: identity.viewer_username().to_string(),
        show_as: identity.is_impersonating(),
        actor_username: identity.actor_username().to_string(),
        show_cluster: false,
        cluster_name: String::new(),
        show_access_db: true,
        access_db_path: st.access_db_path().to_string(),
    });
    let main = render_template(&UiAdminHomeMainTemplate {
        cards: vec![
            UiAdminHomeCardView {
                href: ui_href_with_as("./users/", identity.as_user.as_deref()),
                icon_style: "background:rgba(2,132,199,.1);color:var(--brand)".to_string(),
                icon_html: "&#128100;".to_string(),
                title: "Users".to_string(),
                desc: "Create, delete, reset password, view-as".to_string(),
            },
            UiAdminHomeCardView {
                href: ui_href_with_as("./permissions/", identity.as_user.as_deref()),
                icon_style: "background:rgba(234,179,8,.1);color:#b45309".to_string(),
                icon_html: "&#128274;".to_string(),
                title: "Scope Access".to_string(),
                desc: "Bind read / read_write scopes to one or more users".to_string(),
            },
            UiAdminHomeCardView {
                href: ui_href_with_as("./fs_master/", identity.as_user.as_deref()),
                icon_style: "background:rgba(15,23,42,.08);color:#0f172a".to_string(),
                icon_html: "&#128451;".to_string(),
                title: "FS Master".to_string(),
                desc: "Inspect members, mounts, and effective exports for online fs agents".to_string(),
            },
        ],
    });

    let home_href = ui_href_with_as("../", identity.as_user.as_deref());
    let buckets_href = ui_href_with_as("../", identity.as_user.as_deref());
    let transfers_href = ui_href_with_as("../transfers/", identity.as_user.as_deref());
    let account_password_href = ui_href_with_as("../account/password/", identity.as_user.as_deref());
    let admin_manage_href = ui_href_with_as("./", identity.as_user.as_deref());
    let user_actions_html = ui_user_actions_html(identity, &account_password_href, &admin_manage_href, "./");

    ui_page_html(
        "Manage",
        &home_href,
        &buckets_href,
        &transfers_href,
        "buckets",
        &crumbs,
        "Manage",
        Some(&subtitle),
        None,
        &user_actions_html,
        &main,
    )
}

async fn ui_admin_home_page(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "access admin manage page");
    }
    Html(ui_render_admin_home_page(&st, &identity)).into_response()
}

#[derive(serde::Deserialize)]
struct UiFsMasterExportAddForm {
    agent_instance_key: String,
    export_name: String,
    remote_root_dir_abs: String,
}

#[derive(serde::Deserialize)]
struct UiFsMasterExportRemoveForm {
    agent_instance_key: String,
    export_name: String,
}

#[derive(serde::Deserialize)]
struct UiFsMasterBrowseQuery {
    agent_instance_key: String,
    dir_abs: Option<String>,
    #[serde(rename = "as")]
    as_user: Option<String>,
}

fn ui_fs_master_member_addr_text(member: &FsMasterMemberRecord) -> String {
    let mut addr_text = member.addresses.join(",");
    if let Some(port) = member.port {
        if !addr_text.is_empty() {
            addr_text = format!("{}:{}", addr_text, port);
        } else {
            addr_text = port.to_string();
        }
    }
    addr_text
}

fn ui_fs_master_remove_icon_html() -> String {
    r#"<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16"></path><path d="M9 7V4h6v3"></path><path d="M7 7l1 12h8l1-12"></path><path d="M10 11v6"></path><path d="M14 11v6"></path></svg>"#
        .to_string()
}

fn ui_fs_master_validate_agent_export_fields<'a>(
    agent_instance_key_raw: &'a str,
    export_name_raw: &'a str,
) -> Result<(&'a str, &'a str), &'static str> {
    let agent_instance_key = agent_instance_key_raw.trim();
    if agent_instance_key.is_empty() {
        return Err("agent_instance_key must be non-empty");
    }
    let export_name = export_name_raw.trim();
    if export_name.is_empty() {
        return Err("export_name must be non-empty");
    }
    Ok((agent_instance_key, export_name))
}

fn ui_fs_master_normalize_dir_abs(dir_abs_raw: &str) -> Result<String, UiHandlerError> {
    safe_abs_dirpath(dir_abs_raw)
        .map_err(|e| UiHandlerError::BadRequest(format!("invalid dir_abs: {}", e)))
}

fn ui_fs_master_parent_dir_abs(dir_abs: &str) -> Option<String> {
    if dir_abs == "/" {
        return None;
    }
    let mut parts: Vec<&str> = dir_abs
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if parts.is_empty() {
        return None;
    }
    parts.pop();
    if parts.is_empty() {
        return Some("/".to_string());
    }
    Some(format!("/{}", parts.join("/")))
}

fn ui_fs_master_join_dir_abs(dir_abs: &str, name: &str) -> String {
    if dir_abs == "/" {
        return format!("/{}", name);
    }
    format!("{}/{}", dir_abs, name)
}

fn ui_fs_master_export_manager_agents(
    snapshot: &FsMasterAdminSnapshot,
    identity: &UiIdentity,
) -> Vec<UiFsMasterExportManagerAgentView> {
    let remove_action = ui_href_with_as("./exports/remove", identity.as_user.as_deref());
    let remove_icon_html = ui_fs_master_remove_icon_html();
    snapshot
        .managed_agent_exports
        .iter()
        .map(|agent| {
            let managed_exports: Vec<UiFsMasterManagedExportView> = agent
                .managed_exports
                .iter()
                .map(|record| UiFsMasterManagedExportView {
                    export_name: record.export_name.clone(),
                    remote_root_dir_abs: record.remote_root_dir_abs.clone(),
                    remove_action: remove_action.clone(),
                    agent_instance_key: agent.agent_instance_key.clone(),
                    remove_icon_html: remove_icon_html.clone(),
                })
                .collect();
            UiFsMasterExportManagerAgentView {
                agent_instance_key: agent.agent_instance_key.clone(),
                managed_exports_count: managed_exports.len(),
                show_empty_managed_exports: managed_exports.is_empty(),
                managed_exports,
            }
        })
        .collect()
}

fn ui_fs_master_redirect_to_current_dir(identity: &UiIdentity) -> Response {
    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::SEE_OTHER;
    let mut href = "./".to_string();
    if let Some(as_user) = identity.as_user.as_deref() {
        href = ui_href_with_as(&href, Some(as_user));
    }
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&href).unwrap());
    resp
}

async fn ui_fs_master_page_response(
    st: &GatewayState,
    identity: &UiIdentity,
    error_msg: Option<&str>,
    status: StatusCode,
) -> Response {
    let snapshot = match st.snapshot_fs_master_admin().await {
        Ok(value) => value,
        Err(err) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("load fs master admin snapshot failed: {}", err),
            )
        }
    };
    let html = match ui_render_fs_master_page(st, identity, &snapshot, error_msg) {
        Ok(value) => value,
        Err(err) => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("render fs master admin page failed: {}", err),
            )
        }
    };
    let mut resp = Html(html).into_response();
    *resp.status_mut() = status;
    resp
}

fn ui_render_fs_master_page(
    st: &GatewayState,
    identity: &UiIdentity,
    snapshot: &FsMasterAdminSnapshot,
    error_msg: Option<&str>,
) -> Result<String, String> {
    let crumbs = ui_manage_subpage_crumbs_html("FS Master", identity.as_user.as_deref());

    let agent_count = snapshot
        .members
        .iter()
        .filter(|member| member.kind == FsMasterMemberKind::Agent)
        .count();
    let controller_count = snapshot
        .members
        .iter()
        .filter(|member| member.kind == FsMasterMemberKind::Controller)
        .count();
    let subtitle = ui_manage_pathbar_html(st, identity, "viewer", true, true);

    let mut members_by_id_owner: BTreeMap<String, String> = BTreeMap::new();
    for member in &snapshot.members {
        if !member.member_id.trim().is_empty() {
            members_by_id_owner.insert(member.member_id.clone(), member.owner_id.clone());
        }
    }

    let mut exports_by_remote: BTreeMap<String, Vec<(String, FluxonFsExport)>> = BTreeMap::new();
    for (export_name, export) in st.load_effective_fs_exports()? {
        exports_by_remote
            .entry(export.remote_root_dir_abs.clone())
            .or_default()
            .push((export_name, export));
    }

    let mut mounts_by_remote: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
    for mount in &snapshot.mounts {
        if mount.remote_root_dir_abs.trim().is_empty() || mount.local_mount_dir_abs.trim().is_empty() {
            continue;
        }
        mounts_by_remote
            .entry(mount.remote_root_dir_abs.clone())
            .or_default()
            .entry(mount.external_instance_key.clone())
            .or_default()
            .push(mount.local_mount_dir_abs.clone());
    }

    let mut remote_roots: BTreeSet<String> = BTreeSet::new();
    for remote_root in exports_by_remote.keys() {
        remote_roots.insert(remote_root.clone());
    }
    for remote_root in mounts_by_remote.keys() {
        remote_roots.insert(remote_root.clone());
    }
    let total_runtime_exports: usize = snapshot
        .runtime_agent_exports
        .iter()
        .map(|agent| agent.runtime_exports.len())
        .sum();
    let runtime_agents: Vec<UiFsMasterRuntimeAgentView> = snapshot
        .runtime_agent_exports
        .iter()
        .map(|agent| {
            let runtime_export_names: Vec<String> =
                agent.runtime_exports.iter().map(|record| record.export_name.clone()).collect();
            UiFsMasterRuntimeAgentView {
                agent_instance_key: agent.agent_instance_key.clone(),
                runtime_exports_count: runtime_export_names.len(),
                show_empty_runtime_export_names: runtime_export_names.is_empty(),
                runtime_export_names_display: runtime_export_names.join(", "),
            }
        })
        .collect();

    let member_rows: Vec<UiFsMasterMemberView> = snapshot
        .members
        .iter()
        .map(|member| UiFsMasterMemberView {
            kind: member.kind.as_str().to_string(),
            member_id: member.member_id.clone(),
            owner_id: member.owner_id.clone(),
            hostname: member.hostname.clone(),
            addr: ui_fs_master_member_addr_text(member),
            pid: member.pid.clone(),
            cmd: member.cmd.clone(),
        })
        .collect();

    let export_manager_agents = ui_fs_master_export_manager_agents(snapshot, identity);
    let total_managed_exports: usize = export_manager_agents
        .iter()
        .map(|agent| agent.managed_exports_count)
        .sum();

    let mut overview_rows: Vec<UiFsMasterMountOverviewRowView> = Vec::new();
    let mut details: Vec<UiFsMasterMountDetailView> = Vec::new();
    for remote_root in &remote_roots {
        let export_items = exports_by_remote.get(remote_root).cloned().unwrap_or_default();
        let mount_map = mounts_by_remote.get(remote_root).cloned().unwrap_or_default();
        let mounts_count: usize = mount_map
            .values()
            .map(|paths| paths.iter().collect::<BTreeSet<_>>().len())
            .sum();
        let externals_count = mount_map.len();
        let providers_count = export_items.len();
        let mut owners_total: BTreeSet<String> = BTreeSet::new();
        let mut owners_mapped_count = 0usize;
        for (_export_name, export) in &export_items {
            for node_id in &export.nodes {
                if owners_total.insert(node_id.clone()) && members_by_id_owner.contains_key(node_id) {
                    owners_mapped_count += 1;
                }
            }
        }
        overview_rows.push(UiFsMasterMountOverviewRowView {
            remote_root_dir_abs: remote_root.clone(),
            mounts_count,
            externals_count,
            providers_count,
            owners_mapped_text: format!("{}/{}", owners_mapped_count, owners_total.len()),
        });

        let mut mounts: Vec<UiFsMasterMountRowView> = Vec::new();
        let mut external_keys: Vec<String> = mount_map.keys().cloned().collect();
        external_keys.sort();
        for external_instance_key in external_keys {
            let mut locals: BTreeSet<String> = BTreeSet::new();
            if let Some(paths) = mount_map.get(&external_instance_key) {
                for path in paths {
                    locals.insert(path.clone());
                }
            }
            mounts.push(UiFsMasterMountRowView {
                external_instance_key,
                local_mount_dirs: locals.into_iter().collect(),
            });
        }

        let providers: Vec<UiFsMasterProviderRowView> = export_items
            .iter()
            .map(|(export_name, export)| {
                let mapped_lines: Vec<String> = export
                    .nodes
                    .iter()
                    .map(|node_id| {
                        if let Some(owner_id) = members_by_id_owner.get(node_id) {
                            format!("{}: owner={}", node_id, owner_id)
                        } else {
                            format!("{}: (offline)", node_id)
                        }
                    })
                    .collect();
                UiFsMasterProviderRowView {
                    config_key: export_name.clone(),
                    nodes: export.nodes.join(","),
                    mapped_lines,
                }
            })
            .collect();

        details.push(UiFsMasterMountDetailView {
            remote_root_dir_abs: remote_root.clone(),
            mounts_count,
            providers_count,
            show_empty_mounts: mounts.is_empty(),
            mounts,
            show_empty_providers: providers.is_empty(),
            providers,
        });
    }

    let main = render_template(&UiFsMasterMainTemplate {
        warnings: error_msg.into_iter().map(|msg| msg.to_string()).collect(),
        member_count: snapshot.members.len(),
        agent_count,
        controller_count,
        runtime_exports_count: total_runtime_exports,
        managed_exports_count: total_managed_exports,
        runtime_section_html: render_template(&UiFsMasterRuntimeSectionTemplate {
            show_empty_agents: runtime_agents.is_empty(),
            agents: runtime_agents,
        }),
        membership_section_html: render_template(&UiFsMasterMembershipSectionTemplate {
            show_empty_members: member_rows.is_empty(),
            members: member_rows,
        }),
        export_manager_section_html: render_template(&UiFsMasterExportManagerSectionTemplate {
            show_empty_agents: export_manager_agents.is_empty(),
            agents: export_manager_agents,
            add_action: ui_href_with_as("./exports/add", identity.as_user.as_deref()),
            browse_href: "./browse".to_string(),
        }),
        mount_graph_section_html: render_template(&UiFsMasterMountGraphSectionTemplate {
            show_empty_overview: overview_rows.is_empty(),
            overview_rows,
            details,
        }),
    });

    let home_href = ui_href_with_as("../../", identity.as_user.as_deref());
    let buckets_href = ui_href_with_as("../../", identity.as_user.as_deref());
    let transfers_href = ui_href_with_as("../../transfers/", identity.as_user.as_deref());
    let account_password_href = ui_href_with_as("../../account/password/", identity.as_user.as_deref());
    let admin_manage_href = ui_href_with_as("../", identity.as_user.as_deref());
    let user_actions_html = ui_user_actions_html(identity, &account_password_href, &admin_manage_href, "./");

    Ok(ui_page_html(
        "FS Master",
        &home_href,
        &buckets_href,
        &transfers_href,
        "buckets",
        &crumbs,
        "FS Master",
        Some(&subtitle),
        None,
        &user_actions_html,
        &main,
    ))
}

async fn ui_admin_fs_master_redirect_to_slash() -> Response {
    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_static("./fs_master/"));
    resp
}

async fn ui_admin_fs_master_page(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "access fs master admin");
    }
    ui_fs_master_page_response(&st, &identity, None, StatusCode::OK).await
}

async fn ui_admin_fs_master_agent_browse(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiFsMasterBrowseQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return UiHandlerError::Forbidden(ui_manager_account_required_text(
            identity.viewer.username.as_str(),
            "browse fs agent directories",
        ))
        .into_json_response();
    }
    let agent_instance_key = q.agent_instance_key.trim();
    if agent_instance_key.is_empty() {
        return UiHandlerError::BadRequest("agent_instance_key must be non-empty".to_string())
            .into_json_response();
    }
    let dir_abs = match ui_fs_master_normalize_dir_abs(q.dir_abs.as_deref().unwrap_or("/")) {
        Ok(value) => value,
        Err(err) => return err.into_json_response(),
    };
    let entries = match st.list_fs_master_agent_dir(agent_instance_key, dir_abs.as_str()).await {
        Ok(value) => value,
        Err(err) => {
            return UiHandlerError::BadGateway(format!(
                "list fs master agent dir failed: agent_instance_key={} dir_abs={} err={}",
                agent_instance_key, dir_abs, err
            ))
            .into_json_response()
        }
    };
    let body = UiFsMasterBrowseBody {
        ok: true,
        agent_instance_key: agent_instance_key.to_string(),
        dir_abs: dir_abs.clone(),
        parent_dir_abs: ui_fs_master_parent_dir_abs(dir_abs.as_str()),
        entries: entries
            .into_iter()
            .map(|entry| UiFsMasterBrowseEntry {
                path_abs: ui_fs_master_join_dir_abs(dir_abs.as_str(), entry.name.as_str()),
                name: entry.name,
                is_dir: entry.is_dir,
                is_file: entry.is_file,
            })
            .collect(),
    };
    json_response(StatusCode::OK, &body)
}

async fn ui_admin_fs_master_export_add(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(form): Form<UiFsMasterExportAddForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "manage fs exports");
    }
    let (agent_instance_key, export_name) = match ui_fs_master_validate_agent_export_fields(
        form.agent_instance_key.as_str(),
        form.export_name.as_str(),
    ) {
        Ok(value) => value,
        Err(msg) => {
            return ui_fs_master_page_response(&st, &identity, Some(msg), StatusCode::BAD_REQUEST)
                .await;
        }
    };
    let remote_root_dir_abs = match ui_fs_master_normalize_dir_abs(form.remote_root_dir_abs.as_str()) {
        Ok(value) => value,
        Err(err) => {
            let err_msg = err.to_string();
            return ui_fs_master_page_response(
                &st,
                &identity,
                Some(err_msg.as_str()),
                StatusCode::BAD_REQUEST,
            )
            .await;
        }
    };
    if let Err(err) = st.add_fs_master_export(agent_instance_key, export_name, remote_root_dir_abs.as_str()) {
        return ui_fs_master_page_response(
            &st,
            &identity,
            Some(&format!(
                "add fs master export failed: agent_instance_key={} export_name={} remote_root_dir_abs={} err={}",
                agent_instance_key, export_name, remote_root_dir_abs, err
            )),
            StatusCode::BAD_GATEWAY,
        )
        .await;
    }
    ui_fs_master_redirect_to_current_dir(&identity)
}

async fn ui_admin_fs_master_export_remove(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(form): Form<UiFsMasterExportRemoveForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "manage fs exports");
    }
    let (agent_instance_key, export_name) = match ui_fs_master_validate_agent_export_fields(
        form.agent_instance_key.as_str(),
        form.export_name.as_str(),
    ) {
        Ok(value) => value,
        Err(msg) => {
            return ui_fs_master_page_response(&st, &identity, Some(msg), StatusCode::BAD_REQUEST)
                .await;
        }
    };
    if let Err(err) = st.remove_fs_master_export(agent_instance_key, export_name) {
        return ui_fs_master_page_response(
            &st,
            &identity,
            Some(&format!(
                "remove fs master export failed: agent_instance_key={} export_name={} err={}",
                agent_instance_key, export_name, err
            )),
            StatusCode::BAD_GATEWAY,
        )
        .await;
    }
    ui_fs_master_redirect_to_current_dir(&identity)
}

#[derive(serde::Deserialize)]
struct UiAccountPasswordForm {
    current_password: String,
    new_password: String,
    confirm_new_password: String,
}

fn ui_render_account_password_page(
    st: &GatewayState,
    identity: &UiIdentity,
    error_msg: Option<&str>,
) -> String {
    let crumbs = render_template(&UiCrumbsTemplate {
        crumbs: vec![
            UiCrumbView {
                href: ui_href_with_as("../../", identity.as_user.as_deref()),
                has_href: true,
                label: "Object Storage".to_string(),
                current: false,
            },
            UiCrumbView {
                href: String::new(),
                has_href: false,
                label: "Change Password".to_string(),
                current: true,
            },
        ],
    });
    let subtitle = render_template(&UiPathbarTemplate {
        show_subject: true,
        subject_label: "viewer".to_string(),
        subject_username: identity.viewer_username().to_string(),
        show_as: identity.is_impersonating(),
        actor_username: identity.actor_username().to_string(),
        show_cluster: false,
        cluster_name: String::new(),
        show_access_db: true,
        access_db_path: st.access_db_path().to_string(),
    });
    let main = render_template(&UiAccountPasswordMainTemplate {
        show_error: error_msg.is_some(),
        error_msg: error_msg.unwrap_or("").to_string(),
    });

    let home_href = ui_href_with_as("../../", identity.as_user.as_deref());
    let buckets_href = ui_href_with_as("../../", identity.as_user.as_deref());
    let transfers_href = ui_href_with_as("../../transfers/", identity.as_user.as_deref());
    let account_password_href = ui_href_with_as("./", identity.as_user.as_deref());
    let admin_manage_href = ui_href_with_as("../../admin/", identity.as_user.as_deref());
    let user_actions_html = ui_user_actions_html(identity, &account_password_href, &admin_manage_href, "./");

    ui_page_html(
        "Change Password",
        &home_href,
        &buckets_href,
        &transfers_href,
        "buckets",
        &crumbs,
        "Change Password",
        Some(&subtitle),
        None,
        &user_actions_html,
        &main,
    )
}

async fn ui_account_password_page(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    Html(ui_render_account_password_page(&st, &identity, None)).into_response()
}

async fn ui_account_password_save(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiAccountPasswordForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user.clone()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    if identity.viewer.password != f.current_password {
        let html = ui_render_account_password_page(&st, &identity, Some("current password mismatch"));
        let mut resp = Html(html).into_response();
        *resp.status_mut() = StatusCode::FORBIDDEN;
        return resp;
    }

    let new_password = f.new_password.trim().to_string();
    if new_password.is_empty() {
        let html = ui_render_account_password_page(&st, &identity, Some("new_password must be non-empty"));
        let mut resp = Html(html).into_response();
        *resp.status_mut() = StatusCode::BAD_REQUEST;
        return resp;
    }
    if f.new_password != new_password {
        let html = ui_render_account_password_page(
            &st,
            &identity,
            Some("new_password must not have leading/trailing whitespace"),
        );
        let mut resp = Html(html).into_response();
        *resp.status_mut() = StatusCode::BAD_REQUEST;
        return resp;
    }
    if f.confirm_new_password != new_password {
        let html = ui_render_account_password_page(&st, &identity, Some("confirm_new_password mismatch"));
        let mut resp = Html(html).into_response();
        *resp.status_mut() = StatusCode::BAD_REQUEST;
        return resp;
    }

    let mut permission_list = st.permission_list.read().clone();
    let Some(item) = permission_list.iter_mut().find(|v| v.username == identity.viewer.username) else {
        return text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "internal error: viewer account not found in access state: {}",
                identity.viewer.username
            ),
        );
    };
    item.password = new_password;
    if let Err(err) = persist_permission_list(&st, &permission_list) {
        return text_response(StatusCode::BAD_GATEWAY, err);
    }

    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::SEE_OTHER;
    let mut href = "./".to_string();
    if let Some(as_user) = identity.as_user.as_deref() {
        href = ui_href_with_as(&href, Some(as_user));
    }
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&href).unwrap());
    resp
}

#[derive(serde::Deserialize)]
struct UiAdminUserCreateForm {
    username: String,
    password: String,
    confirm_password: String,
    can_manage_users: Option<String>,
}

#[derive(serde::Deserialize)]
struct UiAdminUserResetPasswordForm {
    username: String,
    new_password: String,
    confirm_new_password: String,
}

#[derive(serde::Deserialize)]
struct UiAdminUserDeleteForm {
    username: String,
}

#[derive(serde::Deserialize)]
struct UiAdminUserAccessForm {
    username: String,
    can_manage_users: Option<String>,
}

fn ui_render_admin_users_page(
    st: &GatewayState,
    identity: &UiIdentity,
    error_msg: Option<&str>,
) -> String {
    let crumbs = ui_manage_subpage_crumbs_html("Users", identity.as_user.as_deref());
    let subtitle = ui_manage_pathbar_html(st, identity, "editor", true, false);

    let create_action = ui_href_with_as("./create", identity.as_user.as_deref());
    let update_access_action = ui_href_with_as("./access", identity.as_user.as_deref());
    let reset_action = ui_href_with_as("./reset_password", identity.as_user.as_deref());
    let delete_action = ui_href_with_as("./delete", identity.as_user.as_deref());
    let scope_access_href = ui_href_with_as("../permissions/", identity.as_user.as_deref());
    let mut warnings: Vec<String> = error_msg.into_iter().map(|msg| msg.to_string()).collect();

    let model = match access_model_from_s3_permission_list(&st.permission_list.read()) {
        Ok(value) => value,
        Err(err) => {
            warnings.push(err);

            let home_href = ui_href_with_as("../../", identity.as_user.as_deref());
            let buckets_href = ui_href_with_as("../../", identity.as_user.as_deref());
            let transfers_href = ui_href_with_as("../../transfers/", identity.as_user.as_deref());
            let account_password_href = ui_href_with_as("../../account/password/", identity.as_user.as_deref());
            let admin_manage_href = ui_href_with_as("../", identity.as_user.as_deref());
            let user_actions_html = ui_user_actions_html(identity, &account_password_href, &admin_manage_href, "./");
            let top_buttons_html = render_template(&UiActionButtonsTemplate {
                buttons: vec![UiActionButtonView {
                    is_link: true,
                    href: scope_access_href.clone(),
                    id: String::new(),
                    class_name: "btn".to_string(),
                    button_type: String::new(),
                    label: "Open Scope Access".to_string(),
                }],
            });
            return ui_page_html(
                "Users",
                &home_href,
                &buckets_href,
                &transfers_href,
                "buckets",
                &crumbs,
                "Users",
                Some(&subtitle),
                Some(&top_buttons_html),
                &user_actions_html,
                &render_template(&UiAdminUsersMainTemplate {
                    warnings,
                    show_content: false,
                    create_action,
                    scope_access_href,
                    user_count_label: String::new(),
                    show_empty_users: true,
                    users: Vec::new(),
                }),
            );
        }
    };

    let mut scope_counts: BTreeMap<String, usize> = BTreeMap::new();
    for scope in &model.scope_access {
        for username in &scope.usernames {
            *scope_counts.entry(username.clone()).or_insert(0) += 1;
        }
    }
    let view_as_base = "../../".to_string();
    let users: Vec<UiAdminUserCardView> = model
        .users
        .iter()
        .map(|user| {
            let scope_count = *scope_counts.get(&user.username).unwrap_or(&0usize);
            UiAdminUserCardView {
                filter_text: user.username.clone(),
                username: user.username.clone(),
                scope_count_label: if scope_count == 1 {
                    "1 scope".to_string()
                } else {
                    format!("{} scopes", scope_count)
                },
                is_manager: user.can_manage_users,
                view_as_href: ui_append_query_param(&view_as_base, "as", &user.username),
                delete_action: delete_action.clone(),
                update_access_action: update_access_action.clone(),
                reset_action: reset_action.clone(),
            }
        })
        .collect();

    let main = render_template(&UiAdminUsersMainTemplate {
        warnings,
        show_content: true,
        create_action,
        scope_access_href: scope_access_href.clone(),
        user_count_label: if model.users.len() == 1 {
            "1 user".to_string()
        } else {
            format!("{} users", model.users.len())
        },
        show_empty_users: users.is_empty(),
        users,
    });

    let home_href = ui_href_with_as("../../", identity.as_user.as_deref());
    let buckets_href = ui_href_with_as("../../", identity.as_user.as_deref());
    let transfers_href = ui_href_with_as("../../transfers/", identity.as_user.as_deref());
    let account_password_href = ui_href_with_as("../../account/password/", identity.as_user.as_deref());
    let admin_manage_href = ui_href_with_as("../", identity.as_user.as_deref());
    let user_actions_html = ui_user_actions_html(identity, &account_password_href, &admin_manage_href, "./");
    let top_buttons_html = render_template(&UiActionButtonsTemplate {
        buttons: vec![UiActionButtonView {
            is_link: true,
            href: scope_access_href,
            id: String::new(),
            class_name: "btn".to_string(),
            button_type: String::new(),
            label: "Open Scope Access".to_string(),
        }],
    });

    ui_page_html(
        "Users",
        &home_href,
        &buckets_href,
        &transfers_href,
        "buckets",
        &crumbs,
        "Users",
        Some(&subtitle),
        Some(&top_buttons_html),
        &user_actions_html,
        &main,
    )
}

async fn ui_admin_users_page(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "manage users");
    }
    Html(ui_render_admin_users_page(&st, &identity, None)).into_response()
}

fn ui_validate_password_no_whitespace(value: &str, label: &str) -> Result<String, UiHandlerError> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Err(UiHandlerError::BadRequest(format!("{} must be non-empty", label)));
    }
    if trimmed != value {
        return Err(UiHandlerError::BadRequest(format!(
            "{} must not have leading/trailing whitespace",
            label
        )));
    }
    Ok(trimmed)
}

fn ui_validate_username_for_basic_auth(value: &str) -> Result<String, UiHandlerError> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Err(UiHandlerError::BadRequest("username must be non-empty".to_string()));
    }
    if trimmed != value {
        return Err(UiHandlerError::BadRequest(
            "username must not have leading/trailing whitespace".to_string(),
        ));
    }
    if trimmed.contains(':') {
        return Err(UiHandlerError::BadRequest(
            "username must not contain ':' because Basic auth uses username:password".to_string(),
        ));
    }
    Ok(trimmed)
}

async fn ui_admin_users_create(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiAdminUserCreateForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user.clone()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "manage users");
    }

    let username = match ui_validate_username_for_basic_auth(&f.username) {
        Ok(v) => v,
        Err(e) => return e.into_text_response(),
    };
    let password = match ui_validate_password_no_whitespace(&f.password, "password") {
        Ok(v) => v,
        Err(e) => return e.into_text_response(),
    };
    if f.confirm_password != password {
        return UiHandlerError::BadRequest("confirm_password mismatch".to_string()).into_text_response();
    }

    let mut permission_list = st.permission_list.read().clone();
    if permission_list.iter().any(|v| v.username == username) {
        return UiHandlerError::Conflict(format!("username already exists: {}", username)).into_text_response();
    }
    let mut permissions = Vec::new();
    if f.can_manage_users.is_some() {
        permissions.push(scope_access_manage_rule());
    }
    permission_list.push(FluxonFsS3PermissionAccount {
        username,
        password,
        permissions,
    });
    if let Err(err) = persist_permission_list(&st, &permission_list) {
        return text_response(StatusCode::BAD_GATEWAY, err);
    }

    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::SEE_OTHER;
    let mut href = "./".to_string();
    if let Some(as_user) = identity.as_user.as_deref() {
        href = ui_href_with_as(&href, Some(as_user));
    }
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&href).unwrap());
    resp
}

async fn ui_admin_users_update_access(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiAdminUserAccessForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user.clone()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "manage users");
    }

    let username = match ui_validate_username_for_basic_auth(&f.username) {
        Ok(v) => v,
        Err(e) => return e.into_text_response(),
    };

    let mut permission_list = st.permission_list.read().clone();
    let Some(item) = permission_list.iter_mut().find(|v| v.username == username) else {
        return UiHandlerError::NotFound(format!("no such user: {}", username)).into_text_response();
    };
    item.permissions.retain(|rule| !permission_rule_is_manage(rule));
    if f.can_manage_users.is_some() {
        item.permissions.push(scope_access_manage_rule());
    }
    item.permissions.sort_by(|a, b| {
        a.bucket
            .cmp(&b.bucket)
            .then(a.prefix.cmp(&b.prefix))
            .then(a.actions.len().cmp(&b.actions.len()))
    });
    if let Err(err) = persist_permission_list(&st, &permission_list) {
        return text_response(StatusCode::BAD_GATEWAY, err);
    }

    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::SEE_OTHER;
    let mut href = "./".to_string();
    if let Some(as_user) = identity.as_user.as_deref() {
        href = ui_href_with_as(&href, Some(as_user));
    }
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&href).unwrap());
    resp
}

async fn ui_admin_users_reset_password(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiAdminUserResetPasswordForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user.clone()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "manage users");
    }

    let username = match ui_validate_username_for_basic_auth(&f.username) {
        Ok(v) => v,
        Err(e) => return e.into_text_response(),
    };
    let new_password = match ui_validate_password_no_whitespace(&f.new_password, "new_password") {
        Ok(v) => v,
        Err(e) => return e.into_text_response(),
    };
    if f.confirm_new_password != new_password {
        return UiHandlerError::BadRequest("confirm_new_password mismatch".to_string()).into_text_response();
    }

    let mut permission_list = st.permission_list.read().clone();
    let Some(item) = permission_list.iter_mut().find(|v| v.username == username) else {
        return UiHandlerError::NotFound(format!("no such user: {}", username)).into_text_response();
    };
    item.password = new_password;
    if let Err(err) = persist_permission_list(&st, &permission_list) {
        return text_response(StatusCode::BAD_GATEWAY, err);
    }

    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::SEE_OTHER;
    let mut href = "./".to_string();
    if let Some(as_user) = identity.as_user.as_deref() {
        href = ui_href_with_as(&href, Some(as_user));
    }
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&href).unwrap());
    resp
}

async fn ui_admin_users_delete(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiAdminUserDeleteForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user.clone()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !account_can_manage_permissions(&identity.viewer) {
        return ui_manager_account_forbidden(identity.viewer.username.as_str(), "manage users");
    }

    let username = match ui_validate_username_for_basic_auth(&f.username) {
        Ok(v) => v,
        Err(e) => return e.into_text_response(),
    };
    let mut permission_list = st.permission_list.read().clone();
    let before = permission_list.len();
    permission_list.retain(|v| v.username != username);
    if permission_list.len() == before {
        return UiHandlerError::NotFound(format!("no such user: {}", username)).into_text_response();
    }
    if let Err(err) = persist_permission_list(&st, &permission_list) {
        return text_response(StatusCode::BAD_GATEWAY, err);
    }

    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::SEE_OTHER;
    let mut href = "./".to_string();
    if let Some(as_user) = identity.as_user.as_deref() {
        href = ui_href_with_as(&href, Some(as_user));
    }
    resp.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&href).unwrap());
    resp
}

async fn ui_index(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let crumbs = render_template(&UiCrumbsTemplate {
        crumbs: vec![
            UiCrumbView {
                href: "./".to_string(),
                has_href: true,
                label: "Object Storage".to_string(),
                current: false,
            },
            UiCrumbView {
                href: String::new(),
                has_href: false,
                label: "Buckets".to_string(),
                current: true,
            },
        ],
    });
    let runtime_exports = match st.list_fs_export_registry_records() {
        Ok(value) => value,
        Err(err) => {
            return UiHandlerError::BadGateway(format!("load bucket providers failed: {}", err)).into_text_response();
        }
    };
    let effective_exports = match st.load_effective_fs_exports() {
        Ok(value) => value,
        Err(err) => {
            return UiHandlerError::BadGateway(format!("load effective exports failed: {}", err)).into_text_response();
        }
    };

    let mut rows: Vec<UiBucketRowView> = Vec::new();
    for (name, exp) in effective_exports.iter() {
        if !account_has_ui_bucket_browse_access(&identity.actor, name) {
            continue;
        }
        let provider_items = ui_bucket_provider_items_from_runtime_exports(
            name,
            &exp.remote_root_dir_abs,
            &runtime_exports,
        );
        let provider_display = ui_provider_display_text(&provider_items);
        let bucket_href = ui_href_with_as(&format!("./{}/", urlencoding::encode(name)), identity.as_user.as_deref());
        rows.push(UiBucketRowView {
            filter_text: format!("{} {}", name, provider_display),
            name: name.clone(),
            provider_display_html: ui_provider_display_scroll_html(&provider_items),
            nodes_count: exp.nodes.len().to_string(),
            bucket_href,
        });
    }
    let main = render_template(&UiBucketsIndexMainTemplate { rows });
    let subtitle = render_template(&UiPathbarTemplate {
        show_subject: false,
        subject_label: String::new(),
        subject_username: String::new(),
        show_as: false,
        actor_username: String::new(),
        show_cluster: true,
        cluster_name: st.cluster_name.clone(),
        show_access_db: false,
        access_db_path: String::new(),
    });
    let home_href = ui_href_with_as("./", identity.as_user.as_deref());
    let buckets_href = ui_href_with_as("./", identity.as_user.as_deref());
    let transfers_href = ui_href_with_as("./transfers/", identity.as_user.as_deref());
    let account_password_href = ui_href_with_as("./account/password/", identity.as_user.as_deref());
    let admin_manage_href = ui_href_with_as("./admin/", identity.as_user.as_deref());
    let user_actions_html = ui_user_actions_html(&identity, &account_password_href, &admin_manage_href, "./");

    let html = ui_page_html(
        "Buckets",
        &home_href,
        &buckets_href,
        &transfers_href,
        "buckets",
        &crumbs,
        "Buckets",
        Some(&subtitle),
        None,
        &user_actions_html,
        &main,
    );
    Html(html).into_response()
}

#[derive(serde::Deserialize)]
struct UiBrowseQuery {
    prefix: Option<String>,
    #[serde(rename = "as")]
    as_user: Option<String>,
}

#[derive(serde::Deserialize)]
struct UiApiDeleteForm {
    key: String,
}

#[derive(serde::Deserialize)]
struct UiApiDeleteFolderForm {
    prefix: String,
}

#[derive(serde::Deserialize)]
struct UiApiTransferForm {
    src_key: String,
    dst_bucket: String,
    dst_prefix: String,
}

#[derive(serde::Deserialize)]
struct UiApiTransferJobCreateForm {
    src_export: String,
    src_root_relpath: String,
    dst_export: String,
    dst_root_relpath: String,
    desired_scan_concurrency: i64,
    desired_worker_count: i64,
    batch_ready_bytes: i64,
    skip_entries_json: Option<String>,
}

#[derive(serde::Deserialize)]
struct UiApiTransferJobWorkersForm {
    desired_scan_concurrency: i64,
    desired_worker_count: i64,
}

#[derive(serde::Deserialize)]
struct UiApiTransferPrescanImportForm {
    src_export: String,
    dst_export: String,
    dst_prefix: String,
    desired_scan_concurrency: i64,
    desired_worker_count: i64,
}

fn ui_transfer_job_spec_blob_from_form(
    form: &UiApiTransferJobCreateForm,
) -> Result<Vec<u8>, UiHandlerError> {
    let Some(raw) = form
        .skip_entries_json
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(Vec::new());
    };
    let entries: Vec<fluxon_fs_core::config::FluxonFsTransferSkipEntryWire> =
        serde_json::from_str(raw).map_err(|e| {
            UiHandlerError::BadRequest(format!("skip_entries_json invalid JSON: {}", e))
        })?;
    let skip_entries = fluxon_fs_core::config::normalize_transfer_skip_entries(entries)
        .map_err(UiHandlerError::BadRequest)?;
    let spec = fluxon_fs_core::config::FluxonFsTransferJobSpecWire { skip_entries };
    fluxon_fs_core::config::encode_transfer_job_spec(&spec).map_err(UiHandlerError::BadRequest)
}

#[derive(serde::Deserialize)]
struct UiApiMultipartCreateForm {
    prefix: String,
    name: String,
}

async fn ui_bucket_api_ls(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    axum::extract::Query(q): axum::extract::Query<UiBrowseQuery>,
    headers: HeaderMap,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user.clone()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let prefix = match ui_validate_prefix(q.prefix.unwrap_or_else(|| "".to_string())) {
        Ok(value) => value,
        Err(err) => return err.into_json_response(),
    };
    match ui_load_listing(&st, &identity.actor, &bucket, prefix).await {
        Ok(payload) => json_response(StatusCode::OK, &payload),
        Err(err) => err.into_json_response(),
    }
}

async fn ui_bucket_api_multipart_create(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiApiMultipartCreateForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match ui_multipart_create_impl(&st, &identity.actor, &bucket, f.prefix, f.name).await {
        Ok(result) => json_response(StatusCode::OK, &result),
        Err(err) => err.into_json_response(),
    }
}

async fn ui_bucket_api_multipart_part(
    State(st): State<Arc<GatewayState>>,
    Path((bucket, upload_id, part_number)): Path<(String, String, i64)>,
    req: Request<Body>,
) -> Response {
    let headers = req.headers().clone();
    let as_user = ui_as_user_from_query_string(req.uri().query());
    let body = req.into_body();
    let identity = match ui_require_identity(&headers, &st, as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let request_identity = request_identity_from_account(&identity.actor);
    let meta = match multipart_load_meta(
        &st,
        request_identity.clone(),
        bucket.clone().into(),
        &upload_id,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_REQUEST, format!("{}", e)),
    };
    if !account_has_object_action(&identity.actor, &bucket, &meta.key, FluxonFsS3PermissionAction::PutObject) {
        return ui_forbidden_response(format!(
            "account {} lacks s3:PutObject on s3://{}/{}",
            identity.actor.username, bucket, meta.key
        ));
    }
    match multipart_upload_part(
        &st,
        request_identity,
        bucket.clone().into(),
        &meta.key,
        &upload_id,
        part_number,
        body,
    )
    .await
    {
        Ok(resp) => {
            let etag = resp
                .headers()
                .get(header::ETAG)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            json_response(
                StatusCode::OK,
                &UiMultipartPartBody {
                    ok: true,
                    etag,
                },
            )
        }
        Err(e) => text_response(StatusCode::BAD_REQUEST, format!("{}", e)),
    }
}

async fn ui_bucket_api_multipart_complete(
    State(st): State<Arc<GatewayState>>,
    Path((bucket, upload_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Response {
    let headers = req.headers().clone();
    let as_user = ui_as_user_from_query_string(req.uri().query());
    let body = req.into_body();
    let identity = match ui_require_identity(&headers, &st, as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let request_identity = request_identity_from_account(&identity.actor);
    let meta = match multipart_load_meta(
        &st,
        request_identity.clone(),
        bucket.clone().into(),
        &upload_id,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_REQUEST, format!("{}", e)),
    };
    if !account_has_object_action(&identity.actor, &bucket, &meta.key, FluxonFsS3PermissionAction::PutObject) {
        return ui_forbidden_response(format!(
            "account {} lacks s3:PutObject on s3://{}/{}",
            identity.actor.username, bucket, meta.key
        ));
    }
    match multipart_complete(&st, request_identity, bucket.into(), &meta.key, &upload_id, body).await {
        Ok(_) => json_response(StatusCode::OK, &UiApiOkBody { ok: true }),
        Err(e) => text_response(StatusCode::BAD_REQUEST, format!("{}", e)),
    }
}

async fn ui_bucket_api_multipart_abort(
    State(st): State<Arc<GatewayState>>,
    Path((bucket, upload_id)): Path<(String, String)>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let request_identity = request_identity_from_account(&identity.actor);
    let meta = match multipart_load_meta(
        &st,
        request_identity.clone(),
        bucket.clone().into(),
        &upload_id,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_REQUEST, format!("{}", e)),
    };
    if !account_has_object_action(
        &identity.actor,
        &bucket,
        &meta.key,
        FluxonFsS3PermissionAction::AbortMultipartUpload,
    ) {
        return ui_forbidden_response(format!(
            "account {} lacks s3:AbortMultipartUpload on s3://{}/{}",
            identity.actor.username, bucket, meta.key
        ));
    }
    match multipart_abort(&st, request_identity, bucket.into(), &meta.key, &upload_id).await {
        Ok(_) => json_response(StatusCode::OK, &UiApiOkBody { ok: true }),
        Err(e) => text_response(StatusCode::BAD_REQUEST, format!("{}", e)),
    }
}

async fn ui_bucket_api_delete(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiApiDeleteForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match ui_delete_object_impl(&st, &identity.actor, &bucket, f.key).await {
        Ok(()) => json_response(StatusCode::OK, &UiApiOkBody { ok: true }),
        Err(err) => err.into_json_response(),
    }
}

async fn ui_bucket_api_delete_folder(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiApiDeleteFolderForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match ui_delete_folder_impl(&st, &identity.actor, &bucket, f.prefix).await {
        Ok(()) => json_response(StatusCode::OK, &UiApiOkBody { ok: true }),
        Err(err) => err.into_json_response(),
    }
}

async fn ui_bucket_api_upload(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    mut multipart: Multipart,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match ui_upload_object_impl(&st, &identity.actor, &bucket, &mut multipart).await {
        Ok(result) => json_response(StatusCode::OK, &result),
        Err(err) => err.into_json_response(),
    }
}

async fn ui_bucket_api_mkdir(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiMkdirForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match ui_mkdir_impl(&st, &identity.actor, &bucket, f.prefix, f.name).await {
        Ok(result) => json_response(StatusCode::OK, &result),
        Err(err) => err.into_json_response(),
    }
}

async fn ui_transfer_task_status(
    State(st): State<Arc<GatewayState>>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let Some(task) = st.ui_transfer_task_for_owner(&task_id, &identity.actor.username) else {
        return UiHandlerError::NotFound(format!("no such transfer task: {}", task_id)).into_json_response();
    };
    json_response(StatusCode::OK, &task.snapshot())
}

async fn ui_transfer_task_list(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    json_response(
        StatusCode::OK,
        &UiTransferTaskListBody {
            ok: true,
            tasks: st.list_ui_transfer_tasks_for_owner(&identity.actor.username),
        },
    )
}

async fn ui_transfer_task_control(
    State(st): State<Arc<GatewayState>>,
    Path((task_id, action)): Path<(String, String)>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let Some(task) = st.ui_transfer_task_for_owner(&task_id, &identity.actor.username) else {
        return UiHandlerError::NotFound(format!("no such transfer task: {}", task_id)).into_json_response();
    };
    let snapshot = match action.as_str() {
        "pause" => task.request_pause(),
        "resume" => task.request_resume(),
        "cancel" => task.request_cancel(),
        _ => {
            return UiHandlerError::BadRequest(format!("unknown transfer action: {}", action)).into_json_response();
        }
    };
    json_response(StatusCode::OK, &snapshot)
}

async fn ui_transfer_prescan_list(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let target_exports = ui_transfer_target_export_names(&st, &identity.actor);
    match st.list_transfer_job_snapshots() {
        Ok(items) => {
            let mut rows = Vec::new();
            for snapshot in items {
                let spec = match ui_decode_local_transfer_prescan_spec(&snapshot.job) {
                    Ok(v) => v,
                    Err(err) => return err.into_json_response(),
                };
                let Some(spec) = spec else {
                    continue;
                };
                let source_candidates =
                    match ui_transfer_prescan_source_candidates(&st, &identity.actor, &spec) {
                        Ok(v) => v,
                        Err(err) => return err.into_json_response(),
                    };
                let live_scan = snapshot.live_detail.as_ref().map(|detail| &detail.scan);
                rows.push(serde_json::json!({
                    "job_id": snapshot.job.job_id,
                    "scan_epoch": snapshot.scan_epoch,
                    "scan_finished": snapshot.scan_finished,
                    "job_state": snapshot.job.state.as_db_str(),
                    "open_batches": snapshot.open_batches,
                    "batch_ready_bytes": snapshot.job.batch_ready_bytes,
                    "src_root_dir_abs": spec.src_root_dir_abs,
                    "skip_entries_count": spec.skip_entries.len(),
                    "created_at_unix_ms": snapshot.job.created_at_unix_ms,
                    "updated_at_unix_ms": snapshot.job.updated_at_unix_ms,
                    "scan": {
                        "discovered_batch_count": live_scan.map(|v| v.discovered_batch_count).unwrap_or(0),
                        "discovered_file_count": live_scan.map(|v| v.discovered_file_count).unwrap_or(0),
                        "discovered_bytes": live_scan.map(|v| v.discovered_bytes).unwrap_or(0),
                        "queued_scan_unit_count": live_scan.map(|v| v.queued_scan_unit_count).unwrap_or(0),
                        "inflight_scan_unit_count": live_scan.map(|v| v.inflight_scan_unit_count).unwrap_or(0),
                        "completed_scan_unit_count": live_scan.map(|v| v.completed_scan_unit_count).unwrap_or(0),
                    },
                    "source_export_candidates": source_candidates.into_iter().map(|candidate| {
                        serde_json::json!({
                            "export_name": candidate.export_name,
                            "src_root_relpath": candidate.src_root_relpath,
                            "remote_root_dir_abs": candidate.remote_root_dir_abs,
                        })
                    }).collect::<Vec<_>>(),
                }));
            }
            json_response(StatusCode::OK, &serde_json::json!({
                "ok": true,
                "target_exports": target_exports,
                "items": rows,
            }))
        }
        Err(detail) => UiHandlerError::BadGateway(detail).into_json_response(),
    }
}

async fn ui_transfer_job_list(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let _identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match st.list_transfer_job_summaries() {
        Ok(items) => {
            let items = items
                .into_iter()
                .map(|summary| {
                    let live_detail = summary.live_detail.as_ref().map(|detail| {
                        serde_json::json!({
                            "scan": {
                                "queued_scan_unit_count": detail.scan.queued_scan_unit_count,
                                "inflight_scan_unit_count": detail.scan.inflight_scan_unit_count,
                                "completed_scan_unit_count": detail.scan.completed_scan_unit_count,
                                "discovered_batch_count": detail.scan.discovered_batch_count,
                                "discovered_file_count": detail.scan.discovered_file_count,
                                "discovered_bytes": detail.scan.discovered_bytes,
                                "scan_rate_files_per_sec": detail.scan.scan_rate_files_per_sec,
                                "scan_rate_bytes_per_sec": detail.scan.scan_rate_bytes_per_sec,
                                "last_scan_result_unix_ms": detail.scan.last_scan_result_unix_ms,
                            },
                            "workers": {
                                "launching_worker_count": detail.workers.launching_worker_count,
                                "running_worker_count": detail.workers.running_worker_count,
                                "stopped_worker_count": detail.workers.stopped_worker_count,
                                "finished_worker_count": detail.workers.finished_worker_count,
                                "writing_batch_count": detail.workers.writing_batch_count,
                                "aggregate_visible_file_count": detail.workers.aggregate_visible_file_count,
                                "aggregate_visible_bytes": detail.workers.aggregate_visible_bytes,
                                "aggregate_live_bandwidth_bytes_per_sec": detail.workers.aggregate_live_bandwidth_bytes_per_sec,
                                "aggregate_total_written_bytes": detail.workers.aggregate_total_written_bytes,
                            },
                            "recent_failures": detail.recent_failures.iter().map(|failure| {
                                serde_json::json!({
                                    "failure_index": failure.failure_index,
                                    "unix_ms": failure.unix_ms,
                                    "scope": failure.scope.as_db_str(),
                                    "message": failure.message,
                                })
                            }).collect::<Vec<_>>(),
                            "recent_failure_count": detail.recent_failures.len(),
                        })
                    });
                    serde_json::json!({
                        "scan_epoch": summary.scan_epoch,
                        "scan_finished": summary.scan_finished,
                        "open_batches": summary.open_batches,
                        "pending_batches": summary.pending_batches,
                        "done_batches": summary.done_batches,
                        "failed_file_count": summary.failed_file_count,
                        "job": {
                            "job_id": summary.job.job_id,
                            "src_export": summary.job.src_export,
                            "src_root_relpath": summary.job.src_root_relpath,
                            "dst_export": summary.job.dst_export,
                            "dst_root_relpath": summary.job.dst_root_relpath,
                            "desired_scan_concurrency": summary.job.desired_scan_concurrency,
                            "desired_worker_count": summary.job.desired_worker_count,
                            "batch_ready_bytes": summary.job.batch_ready_bytes,
                            "scan_epoch": summary.job.scan_epoch,
                            "scan_finished": summary.job.scan_finished,
                            "state": summary.job.state.as_db_str(),
                            "last_error": summary.job.last_error,
                            "created_at_unix_ms": summary.job.created_at_unix_ms,
                            "updated_at_unix_ms": summary.job.updated_at_unix_ms,
                        },
                        "live_detail": live_detail,
                    })
                })
                .collect::<Vec<_>>();
            json_response(StatusCode::OK, &serde_json::json!({
            "ok": true,
            "items": items,
        }))
        }
        Err(detail) => UiHandlerError::BadGateway(detail).into_json_response(),
    }
}

async fn ui_transfer_job_detail(
    State(st): State<Arc<GatewayState>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let _identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let snapshot = match st.transfer_job_snapshot(job_id.as_str()) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return UiHandlerError::NotFound(format!("no such transfer job: {}", job_id))
                .into_json_response();
        }
        Err(detail) => return UiHandlerError::BadGateway(detail).into_json_response(),
    };
    let pending_batches = snapshot
        .job
        .ready_batch_count
        .max(0)
        .saturating_add(snapshot.job.expired_batch_count.max(0));
    let done_batches = snapshot.done_batches.max(0);
    let failed_files = snapshot
        .failed_files
        .into_iter()
        .map(|issue| {
            serde_json::json!({
                "job_id": issue.job_id,
                "batch_id": issue.batch_id,
                "relpath": issue.relpath,
                "reason_kind": issue.reason_kind.as_db_str(),
                "created_at_unix_ms": issue.created_at_unix_ms,
                "updated_at_unix_ms": issue.updated_at_unix_ms,
            })
        })
        .collect::<Vec<_>>();
    let running_batches = snapshot
        .running_batches
        .into_iter()
        .map(|batch| {
            serde_json::json!({
                "job_id": batch.job_id,
                "batch_id": batch.batch_id,
                "root_relpath": batch.root_relpath,
                "batch_kind": batch.batch_kind.as_db_str(),
                "state": batch.state.as_db_str(),
                "owner_worker_id": batch.owner_worker_id,
                "owner_worker_task_id": batch.owner_worker_task_id,
                "lease_expire_unix_ms": batch.lease_expire_unix_ms,
                "generation": batch.generation,
            })
        })
        .collect::<Vec<_>>();
    let worker_attempts = snapshot
        .worker_attempts
        .into_iter()
        .map(|attempt| {
            serde_json::json!({
                "job_id": attempt.job_id,
                "batch_id": attempt.batch_id,
                "worker_id": attempt.worker_id,
                "worker_task_id": attempt.worker_task_id,
                "dst_exporter_id": attempt.dst_exporter_id,
                "state": attempt.state.as_db_str(),
                "launch_attempt_count": attempt.launch_attempt_count,
                "visible_file_count": attempt.visible_file_count,
                "visible_bytes": attempt.visible_bytes,
                "last_error": attempt.last_error,
                "stop_reason": attempt.stop_reason.map(|v| match v {
                    fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Superseded => "superseded".to_string(),
                    fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Cancelled => "cancelled".to_string(),
                }),
                "created_at_unix_ms": attempt.created_at_unix_ms,
                "updated_at_unix_ms": attempt.updated_at_unix_ms,
            })
        })
        .collect::<Vec<_>>();
    let live_detail = snapshot.live_detail.as_ref().map(|detail| {
        serde_json::json!({
            "scan": {
                "queued_scan_unit_count": detail.scan.queued_scan_unit_count,
                "inflight_scan_unit_count": detail.scan.inflight_scan_unit_count,
                "completed_scan_unit_count": detail.scan.completed_scan_unit_count,
                "discovered_batch_count": detail.scan.discovered_batch_count,
                "discovered_file_count": detail.scan.discovered_file_count,
                "discovered_bytes": detail.scan.discovered_bytes,
                "scan_rate_files_per_sec": detail.scan.scan_rate_files_per_sec,
                "scan_rate_bytes_per_sec": detail.scan.scan_rate_bytes_per_sec,
                "last_scan_result_unix_ms": detail.scan.last_scan_result_unix_ms,
            },
            "workers": {
                "launching_worker_count": detail.workers.launching_worker_count,
                "running_worker_count": detail.workers.running_worker_count,
                "stopped_worker_count": detail.workers.stopped_worker_count,
                "finished_worker_count": detail.workers.finished_worker_count,
                "writing_batch_count": detail.workers.writing_batch_count,
                "aggregate_visible_file_count": detail.workers.aggregate_visible_file_count,
                "aggregate_visible_bytes": detail.workers.aggregate_visible_bytes,
                "aggregate_live_bandwidth_bytes_per_sec": detail.workers.aggregate_live_bandwidth_bytes_per_sec,
                "aggregate_total_written_bytes": detail.workers.aggregate_total_written_bytes,
            },
            "recent_failures": detail.recent_failures.iter().map(|failure| {
                serde_json::json!({
                    "failure_index": failure.failure_index,
                    "unix_ms": failure.unix_ms,
                    "scope": failure.scope.as_db_str(),
                    "message": failure.message,
                })
            }).collect::<Vec<_>>(),
            "active_workers": detail.active_workers.iter().map(|worker| {
                serde_json::json!({
                    "worker_id": worker.worker_id,
                    "worker_task_id": worker.worker_task_id,
                    "batch_id": worker.batch_id,
                    "state": worker.state.as_db_str(),
                    "launch_attempt_count": worker.launch_attempt_count,
                    "visible_file_count": worker.visible_file_count,
                    "visible_bytes": worker.visible_bytes,
                    "lease_expire_unix_ms": worker.lease_expire_unix_ms,
                    "last_heartbeat_unix_ms": worker.last_heartbeat_unix_ms,
                    "current_bandwidth_bytes_per_sec": worker.current_bandwidth_bytes_per_sec,
                    "total_written_bytes": worker.total_written_bytes,
                    "desired_file_lanes": worker.desired_file_lanes,
                    "last_error": worker.last_error,
                    "stop_reason": worker.stop_reason.map(|v| match v {
                        fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Superseded => "superseded".to_string(),
                        fluxon_fs_core::config::FluxonFsTransferWorkerStopReasonWire::Cancelled => "cancelled".to_string(),
                    }),
                })
            }).collect::<Vec<_>>(),
        })
    });
    json_response(StatusCode::OK, &serde_json::json!({
        "ok": true,
        "item": {
            "scan_epoch": snapshot.scan_epoch,
            "scan_finished": snapshot.scan_finished,
            "open_batches": snapshot.open_batches,
            "pending_batches": pending_batches,
            "done_batches": done_batches,
            "failed_file_count": snapshot.failed_file_count,
            "job": {
                "job_id": snapshot.job.job_id,
                "src_export": snapshot.job.src_export,
                "src_root_relpath": snapshot.job.src_root_relpath,
                "dst_export": snapshot.job.dst_export,
                "dst_root_relpath": snapshot.job.dst_root_relpath,
                "desired_scan_concurrency": snapshot.job.desired_scan_concurrency,
                "desired_worker_count": snapshot.job.desired_worker_count,
                "batch_ready_bytes": snapshot.job.batch_ready_bytes,
                "scan_epoch": snapshot.job.scan_epoch,
                "scan_finished": snapshot.job.scan_finished,
                "state": snapshot.job.state.as_db_str(),
                "last_error": snapshot.job.last_error,
                "created_at_unix_ms": snapshot.job.created_at_unix_ms,
                "updated_at_unix_ms": snapshot.job.updated_at_unix_ms,
            },
            "running_batches": running_batches,
            "worker_attempts": worker_attempts,
            "failed_files": failed_files,
            "live_detail": live_detail,
        }
    }))
}

#[derive(Debug, serde::Deserialize)]
struct UiTransferHistoryQuery {
    #[serde(rename = "as")]
    as_user: Option<String>,
    start_unix_ms: Option<i64>,
    end_unix_ms: Option<i64>,
}

async fn ui_transfer_job_history(
    State(st): State<Arc<GatewayState>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiTransferHistoryQuery>,
) -> Response {
    let _identity = match ui_require_identity(&headers, &st, q.as_user.clone()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let snapshot = match st.transfer_job_snapshot(job_id.as_str()) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return UiHandlerError::NotFound(format!("no such transfer job: {}", job_id))
                .into_json_response();
        }
        Err(detail) => return UiHandlerError::BadGateway(detail).into_json_response(),
    };
    let start_unix_ms = q
        .start_unix_ms
        .unwrap_or(snapshot.job.created_at_unix_ms)
        .max(0);
    let default_end_unix_ms = if snapshot.job.state == FluxonFsTransferJobState::Running {
        Utc::now().timestamp_millis()
    } else {
        snapshot.job.updated_at_unix_ms.max(snapshot.job.created_at_unix_ms)
    };
    let end_unix_ms = q.end_unix_ms.unwrap_or(default_end_unix_ms).max(start_unix_ms);
    let history = match st
        .transfer_job_history_snapshot(job_id.as_str(), start_unix_ms, end_unix_ms)
        .await
    {
        Ok(v) => v,
        Err(detail) => return UiHandlerError::BadGateway(detail).into_json_response(),
    };
    json_response(StatusCode::OK, &serde_json::json!({
        "ok": true,
        "job_id": job_id,
        "history": {
            "start_unix_ms": history.start_unix_ms,
            "end_unix_ms": history.end_unix_ms,
            "points": history.points.iter().map(|point| {
                serde_json::json!({
                    "unix_ms": point.unix_ms,
                    "bandwidth_bytes_per_sec": point.bandwidth_bytes_per_sec,
                    "running_worker_count": point.running_worker_count,
                    "writing_batch_count": point.writing_batch_count,
                    "total_written_bytes": point.total_written_bytes,
                })
            }).collect::<Vec<_>>(),
        }
    }))
}

async fn ui_transfer_job_failure_detail(
    State(st): State<Arc<GatewayState>>,
    Path((job_id, failure_index)): Path<(String, i64)>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let _identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match st.transfer_job_recent_failure_detail(job_id.as_str(), failure_index) {
        Ok(Some(failure)) => json_response(StatusCode::OK, &serde_json::json!({
            "ok": true,
            "failure": {
                "failure_index": failure.failure_index,
                "unix_ms": failure.unix_ms,
                "scope": failure.scope.as_db_str(),
                "message": failure.message,
            }
        })),
        Ok(None) => UiHandlerError::NotFound(format!(
            "no such transfer failure: job_id={} failure_index={}",
            job_id, failure_index
        ))
        .into_json_response(),
        Err(detail) => UiHandlerError::BadGateway(detail).into_json_response(),
    }
}

async fn ui_transfer_job_file_issue_detail(
    State(st): State<Arc<GatewayState>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiTransferFileIssueQuery>,
) -> Response {
    let _identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match st.transfer_job_file_issue_detail(job_id.as_str(), q.batch_id.as_str(), q.relpath.as_str()) {
        Ok(Some(issue)) => json_response(StatusCode::OK, &serde_json::json!({
            "ok": true,
            "file_issue": {
                "job_id": issue.job_id,
                "batch_id": issue.batch_id,
                "relpath": issue.relpath,
                "reason_kind": issue.reason_kind.as_db_str(),
                "reason_detail": issue.reason_detail,
                "created_at_unix_ms": issue.created_at_unix_ms,
                "updated_at_unix_ms": issue.updated_at_unix_ms,
            }
        })),
        Ok(None) => UiHandlerError::NotFound(format!(
            "no such transfer file issue: job_id={} batch_id={} relpath={}",
            job_id, q.batch_id, q.relpath
        ))
        .into_json_response(),
        Err(detail) => UiHandlerError::BadGateway(detail).into_json_response(),
    }
}

async fn ui_transfer_prescan_import(
    State(st): State<Arc<GatewayState>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiApiTransferPrescanImportForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if f.desired_scan_concurrency <= 0 {
        return UiHandlerError::BadRequest("desired_scan_concurrency must be > 0".to_string())
            .into_json_response();
    }
    if f.desired_worker_count < 0 {
        return UiHandlerError::BadRequest("desired_worker_count must be >= 0".to_string())
            .into_json_response();
    }
    let snapshot = match st.transfer_job_snapshot(job_id.as_str()) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return UiHandlerError::NotFound(format!("no such transfer prescan job: {}", job_id))
                .into_json_response();
        }
        Err(detail) => return UiHandlerError::BadGateway(detail).into_json_response(),
    };
    let spec = match ui_decode_local_transfer_prescan_spec(&snapshot.job) {
        Ok(v) => v,
        Err(err) => return err.into_json_response(),
    };
    let Some(spec) = spec else {
        return UiHandlerError::BadRequest(format!(
            "transfer job is not a local prescan job: {}",
            job_id
        ))
        .into_json_response();
    };
    let candidates = match ui_transfer_prescan_source_candidates(&st, &identity.actor, &spec) {
        Ok(v) => v,
        Err(err) => return err.into_json_response(),
    };
    let Some(selected_source) = candidates
        .into_iter()
        .find(|candidate| candidate.export_name == f.src_export.trim())
    else {
        return UiHandlerError::BadRequest(format!(
            "selected src_export is not available for this prescan job: {}",
            f.src_export
        ))
        .into_json_response();
    };
    let dst_root_relpath = match ui_transfer_root_relpath_from_prefix(f.dst_prefix.as_str()) {
        Ok(v) => v,
        Err(err) => return err.into_json_response(),
    };
    let (src_export, src_root_relpath, dst_export, dst_root_relpath) =
        match ui_validate_transfer_job_binding(
            &st,
            &identity.actor,
            selected_source.export_name,
            selected_source.src_root_relpath,
            f.dst_export,
            dst_root_relpath,
        ) {
            Ok(v) => v,
            Err(err) => return err.into_json_response(),
        };
    match st.import_transfer_prescan_job(
        job_id.as_str(),
        src_export.as_str(),
        src_root_relpath.as_str(),
        dst_export.as_str(),
        dst_root_relpath.as_str(),
        f.desired_scan_concurrency,
        f.desired_worker_count,
    ) {
        Ok(job) => json_response(StatusCode::OK, &serde_json::json!({
            "ok": true,
            "job": job,
        })),
        Err(detail) => UiHandlerError::BadRequest(detail).into_json_response(),
    }
}

async fn ui_transfer_job_create(
    State(st): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiApiTransferJobCreateForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if f.desired_scan_concurrency <= 0 {
        return UiHandlerError::BadRequest("desired_scan_concurrency must be > 0".to_string())
            .into_json_response();
    }
    if f.desired_worker_count < 0 {
        return UiHandlerError::BadRequest("desired_worker_count must be >= 0".to_string())
            .into_json_response();
    }
    if f.batch_ready_bytes <= 0 {
        return UiHandlerError::BadRequest("batch_ready_bytes must be > 0".to_string())
            .into_json_response();
    }
    let job_spec_blob = match ui_transfer_job_spec_blob_from_form(&f) {
        Ok(v) => v,
        Err(err) => return err.into_json_response(),
    };
    let (src_export, src_root_relpath, dst_export, dst_root_relpath) =
        match ui_validate_transfer_job_binding(
            &st,
            &identity.actor,
            f.src_export,
            f.src_root_relpath,
            f.dst_export,
            f.dst_root_relpath,
        ) {
            Ok(v) => v,
            Err(err) => return err.into_json_response(),
        };
    match st.create_transfer_job(FsTransferCreateJobArg {
        src_export,
        src_root_relpath,
        dst_export,
        dst_root_relpath,
        desired_scan_concurrency: f.desired_scan_concurrency,
        desired_worker_count: f.desired_worker_count,
        batch_ready_bytes: f.batch_ready_bytes,
        job_spec_blob,
    }) {
        Ok(job) => json_response(StatusCode::OK, &serde_json::json!({
            "ok": true,
            "job": job,
        })),
        Err(detail) => UiHandlerError::BadRequest(detail).into_json_response(),
    }
}

async fn ui_transfer_job_update_workers(
    State(st): State<Arc<GatewayState>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiApiTransferJobWorkersForm>,
) -> Response {
    let _identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if f.desired_scan_concurrency <= 0 {
        return UiHandlerError::BadRequest("desired_scan_concurrency must be > 0".to_string())
            .into_json_response();
    }
    if f.desired_worker_count < 0 {
        return UiHandlerError::BadRequest("desired_worker_count must be >= 0".to_string())
            .into_json_response();
    }
    match st.update_transfer_job_desired_concurrency(
        job_id.as_str(),
        f.desired_scan_concurrency,
        f.desired_worker_count,
    ) {
        Ok(()) => json_response(StatusCode::OK, &serde_json::json!({ "ok": true })),
        Err(detail) => UiHandlerError::BadRequest(detail).into_json_response(),
    }
}

async fn ui_transfer_job_cancel(
    State(st): State<Arc<GatewayState>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
) -> Response {
    let _identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match st.cancel_transfer_job(job_id.as_str()) {
        Ok(()) => json_response(StatusCode::OK, &serde_json::json!({ "ok": true })),
        Err(detail) => UiHandlerError::BadRequest(detail).into_json_response(),
    }
}

async fn ui_bucket_api_copy(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiApiTransferForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match ui_start_copy_or_move_task(&st, &identity.actor, &bucket, f.src_key, f.dst_bucket, f.dst_prefix, false).await {
        Ok(result) => json_response(StatusCode::OK, &result),
        Err(err) => err.into_json_response(),
    }
}

async fn ui_bucket_api_move(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiApiTransferForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match ui_start_copy_or_move_task(&st, &identity.actor, &bucket, f.src_key, f.dst_bucket, f.dst_prefix, true).await {
        Ok(result) => json_response(StatusCode::OK, &result),
        Err(err) => err.into_json_response(),
    }
}

async fn ui_bucket_browse(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    axum::extract::Query(q): axum::extract::Query<UiBrowseQuery>,
    headers: HeaderMap,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user.clone()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let prefix = match ui_validate_prefix(q.prefix.unwrap_or_else(|| "".to_string())) {
        Ok(value) => value,
        Err(err) => return err.into_text_response(),
    };
    let listing = match ui_load_listing(&st, &identity.actor, &bucket, prefix).await {
        Ok(value) => value,
        Err(err) => return err.into_text_response(),
    };
    let available_buckets = match list_permitted_buckets(&st, &identity.actor) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };

    let crumbs = ui_prefix_crumbs_html(&bucket, "", identity.as_user.as_deref());
    let bootstrap = UiWorkspaceBootstrap {
        initial_tab: listing.clone(),
        available_buckets,
        transfer_enabled: st.transfer_feature_enabled(),
    };
    let buttons = render_template(&UiActionButtonsTemplate {
        buttons: vec![
            UiActionButtonView {
                is_link: false,
                href: String::new(),
                id: "new_page_btn".to_string(),
                class_name: "btn".to_string(),
                button_type: "button".to_string(),
                label: "New Page".to_string(),
            },
            UiActionButtonView {
                is_link: false,
                href: String::new(),
                id: "split_pane_btn".to_string(),
                class_name: "btn".to_string(),
                button_type: "button".to_string(),
                label: "Split Right".to_string(),
            },
            UiActionButtonView {
                is_link: false,
                href: String::new(),
                id: "open_bucket_btn".to_string(),
                class_name: "btn".to_string(),
                button_type: "button".to_string(),
                label: "Open Bucket".to_string(),
            },
            UiActionButtonView {
                is_link: false,
                href: String::new(),
                id: "mkdir_btn".to_string(),
                class_name: "btn".to_string(),
                button_type: "button".to_string(),
                label: "New Folder".to_string(),
            },
            UiActionButtonView {
                is_link: false,
                href: String::new(),
                id: "upload_btn".to_string(),
                class_name: "btn primary".to_string(),
                button_type: "button".to_string(),
                label: "Upload Object".to_string(),
            },
        ],
    });
    let subtitle = render_template(&UiBucketBrowseSubtitleTemplate {
        bucket: listing.bucket.clone(),
        provider_display_html: ui_provider_display_scroll_html(&listing.provider_items),
    });
    let main = render_template(&UiBucketBrowseMainTemplate {
        bootstrap_json: ui_json_for_script(&bootstrap),
        prefix: listing.prefix.clone(),
        prefix_display: if listing.prefix.is_empty() {
            "/".to_string()
        } else {
            listing.prefix.clone()
        },
    });

    let exit_view_as_href = format!("./?prefix={}", urlencoding::encode(&listing.prefix));
    let home_href = ui_href_with_as("../", identity.as_user.as_deref());
    let buckets_href = ui_href_with_as("../", identity.as_user.as_deref());
    let transfers_href = ui_href_with_as("../transfers/", identity.as_user.as_deref());
    let account_password_href = ui_href_with_as("../account/password/", identity.as_user.as_deref());
    let admin_manage_href = ui_href_with_as("../admin/", identity.as_user.as_deref());
    let user_actions_html = ui_user_actions_html(
        &identity,
        &account_password_href,
        &admin_manage_href,
        &exit_view_as_href,
    );
    let html = ui_page_html(
        &format!("Bucket {}", bucket),
        &home_href,
        &buckets_href,
        &transfers_href,
        "buckets",
        &crumbs,
        "Objects",
        Some(&subtitle),
        Some(&buttons),
        &user_actions_html,
        &main,
    );
    Html(html).into_response()
}

#[derive(serde::Deserialize)]
struct UiDeleteForm {
    key: String,
    prefix: String,
}

async fn ui_bucket_delete(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiDeleteForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user.clone()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(err) = ui_delete_object_impl(&st, &identity.actor, &bucket, f.key).await {
        return err.into_text_response();
    }
    let prefix = match ui_prefix_from_query(Some(f.prefix)) {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let mut resp = Response::new(boxed(Body::empty()));
    *resp.status_mut() = StatusCode::SEE_OTHER;
    let mut href = format!("./?prefix={}", urlencoding::encode(&prefix));
    if let Some(as_user) = identity.as_user.as_deref() {
        href = ui_href_with_as(&href, Some(as_user));
    }
    resp.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&href).unwrap(),
    );
    resp
}

async fn ui_bucket_upload(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    mut multipart: Multipart,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user.clone()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match ui_upload_object_impl(&st, &identity.actor, &bucket, &mut multipart).await {
        Ok(result) => {
            let mut resp = Response::new(boxed(Body::empty()));
            *resp.status_mut() = StatusCode::SEE_OTHER;
            let mut href = format!("./?prefix={}", urlencoding::encode(&result.prefix));
            if let Some(as_user) = identity.as_user.as_deref() {
                href = ui_href_with_as(&href, Some(as_user));
            }
            resp.headers_mut().insert(
                header::LOCATION,
                HeaderValue::from_str(&href).unwrap(),
            );
            resp
        }
        Err(err) => err.into_text_response(),
    }
}

#[derive(serde::Deserialize)]
struct UiMkdirForm {
    prefix: String,
    name: String,
}

async fn ui_bucket_mkdir(
    State(st): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    Form(f): Form<UiMkdirForm>,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user.clone()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match ui_mkdir_impl(&st, &identity.actor, &bucket, f.prefix, f.name).await {
        Ok(result) => {
            let mut resp = Response::new(boxed(Body::empty()));
            *resp.status_mut() = StatusCode::SEE_OTHER;
            let mut href = format!("./?prefix={}", urlencoding::encode(&result.prefix));
            if let Some(as_user) = identity.as_user.as_deref() {
                href = ui_href_with_as(&href, Some(as_user));
            }
            resp.headers_mut().insert(
                header::LOCATION,
                HeaderValue::from_str(&href).unwrap(),
            );
            resp
        }
        Err(err) => err.into_text_response(),
    }
}

async fn ui_bucket_get_object(
    State(st): State<Arc<GatewayState>>,
    Path((bucket, key)): Path<(String, String)>,
    axum::extract::Query(q): axum::extract::Query<UiAsQuery>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let identity = match ui_require_identity(&headers, &st, q.as_user) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(err) = ui_require_bucket(&st, &bucket) {
        return err.into_text_response();
    }
    let rel = match safe_relpath(&key) {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_REQUEST, format!("invalid key: {}", e)),
    };
    if let Err(e) = verify_user_object_key(&rel) {
        return text_response(StatusCode::BAD_REQUEST, format!("{}", e));
    }
    if !account_has_object_action(&identity.actor, &bucket, &rel, FluxonFsS3PermissionAction::GetObject) {
        return ui_forbidden_response(format!(
            "account {} lacks s3:GetObject on s3://{}/{}",
            identity.actor.username, bucket, rel
        ));
    }
    let request_identity = request_identity_from_account(&identity.actor);
    let stat = match st
        .backend
        .stat(
            request_identity.clone(),
            bucket.clone().into(),
            rel.clone().into(),
        )
        .await
    {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_GATEWAY, format!("stat failed: {}", e)),
    };
    if !stat.exists || !stat.is_file {
        return text_response(StatusCode::NOT_FOUND, "not found".to_string());
    }
    let (range_start, range_end_inclusive) = match parse_range_header(&headers, stat.size) {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::RANGE_NOT_SATISFIABLE, format!("{}", e)),
    };
    if range_start.is_some() {
        if let Some(v) = headers.get(header::IF_RANGE) {
            let if_range = match v.to_str() {
                Ok(s) => s,
                Err(_) => {
                    return text_response(
                        StatusCode::PRECONDITION_FAILED,
                        "invalid If-Range header".to_string(),
                    );
                }
            };
            if !if_range_allows_range(if_range, stat.size, stat.mtime_ns) {
                return text_response(
                    StatusCode::PRECONDITION_FAILED,
                    "object changed; range resume rejected".to_string(),
                );
            }
        }
    }
    if method == Method::HEAD {
        let mut resp = resp_empty(if range_start.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        });
        apply_object_headers(resp.headers_mut(), stat.size, stat.mtime_ns, range_start, range_end_inclusive);
        resp.headers_mut().insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(&format!("attachment; filename=\"{}\"", rel.rsplit('/').next().unwrap_or("download"))).unwrap(),
        );
        return resp;
    }
    let body = match get_object_stream(
        st.clone(),
        request_identity,
        bucket.clone().into(),
        rel.clone().into(),
        stat.size,
        stat.mtime_ns,
        range_start,
        range_end_inclusive,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => return text_response(StatusCode::BAD_GATEWAY, format!("get failed: {}", e)),
    };
    let mut resp: Response = Response::new(boxed(body));
    *resp.status_mut() = if range_start.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    apply_object_headers(resp.headers_mut(), stat.size, stat.mtime_ns, range_start, range_end_inclusive);
    resp.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{}\"", rel.rsplit('/').next().unwrap_or("download"))).unwrap(),
    );
    resp
}
