// English note:
// - This file holds embedded SSR UI assets extracted from ui_ssr.rs.
// - Keep static payloads out of the aggregator so server-side behavior stays easier to scan.

const UI_PAGE_TITLE: &str = "Fluxon Object Storage Console";
const UI_MULTIPART_UPLOAD_PART_BYTES: usize = 16 * 1024 * 1024;
const UI_MULTIPART_UPLOAD_MAX_INFLIGHT: usize = 4;

// NOTE: UI must be fully self-contained (no external CDN / assets).
const UI_CSS: &str = r#"
:root{
  --bg:#f8fafc;
  --panel:#ffffff;
  --muted:#64748b;
  --text:#0f172a;
  --border:#e5e7eb;
  --brand:#0284c7;
  --brand2:#0ea5e9;
  --sidebar:#0f172a;
  --sidebarText:#e2e8f0;
  --danger:#dc2626;
}
*{box-sizing:border-box;}
html,body{height:100%;}
body{
  margin:0;
  font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;
  color:var(--text);
  background:var(--bg);
}
a{color:inherit;text-decoration:none;}
.layout{display:flex;height:100vh;overflow:hidden;}
.sidebar{width:260px;flex:0 0 auto;background:var(--sidebar);color:var(--sidebarText);display:flex;flex-direction:column;box-shadow:0 8px 24px rgba(15,23,42,.25);z-index:10;}
.side_head{height:56px;display:flex;align-items:center;padding:0 18px;border-bottom:1px solid rgba(226,232,240,.08);}
.brand{display:flex;gap:10px;align-items:center;font-weight:700;letter-spacing:.2px;}
.brand_mark{width:28px;height:28px;border-radius:8px;background:linear-gradient(135deg,var(--brand2),var(--brand));display:inline-block;}
.brand_name{font-size:18px;}
.nav{padding:14px 10px;display:flex;flex-direction:column;gap:6px;}
.nav_item{display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:10px;color:rgba(226,232,240,.88);}
.nav_item:hover{background:rgba(226,232,240,.06);}
.nav_item.active{background:rgba(2,132,199,.9);color:#fff;box-shadow:0 10px 22px rgba(2,132,199,.25);}
.main{flex:1 1 auto;min-width:0;display:flex;flex-direction:column;overflow:hidden;}
.topbar{height:56px;background:var(--panel);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 18px;z-index:5;}
.crumbs{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--muted);min-width:0;}
.crumbs a{color:var(--muted);}
.crumbs a:hover{color:var(--text);}
.crumb_sep{color:#cbd5e1;}
.crumb_cur{color:var(--text);font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.top_actions{display:flex;align-items:center;gap:12px;}
.search{position:relative;}
.search input{
  width:280px;max-width:45vw;
  padding:8px 10px 8px 30px;
  border:1px solid var(--border);
  border-radius:10px;
  font-size:13px;
  outline:none;
  background:#fff;
}
.search input:focus{border-color:rgba(2,132,199,.55);box-shadow:0 0 0 4px rgba(2,132,199,.14);}
.search_icon{position:absolute;left:10px;top:50%;transform:translateY(-50%);color:#94a3b8;font-size:13px;}
.userpill{font-size:12px;color:var(--muted);border:1px solid var(--border);background:#fff;border-radius:999px;padding:6px 10px;}
.content{flex:1 1 auto;overflow:auto;padding:18px;}
.card{background:var(--panel);border:1px solid var(--border);border-radius:14px;box-shadow:0 1px 2px rgba(15,23,42,.06);overflow:hidden;}
.card > .warn{margin:16px 16px 0;}
.card > .warn:last-child{margin-bottom:16px;}
.card > .workspace_notice.open{margin:16px 16px 0;}
.card > .row{margin:16px;}
.card_head{display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid var(--border);}
.title{font-size:18px;font-weight:700;margin:0;}
.subtitle{font-size:12px;color:var(--muted);margin-top:4px;line-height:1.35;}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;padding:9px 12px;border-radius:10px;border:1px solid var(--border);background:#fff;color:var(--text);font-weight:600;font-size:13px;cursor:pointer;}
.btn:hover{background:#f8fafc;}
.btn.primary{border-color:rgba(2,132,199,.25);background:rgba(2,132,199,.92);color:#fff;}
.btn.primary:hover{background:rgba(2,132,199,1);}
.btn.danger{border-color:rgba(220,38,38,.25);background:rgba(220,38,38,.08);color:var(--danger);}
.btn.danger:hover{background:rgba(220,38,38,.12);}
.btn:disabled{opacity:.55;cursor:not-allowed;}
table{width:100%;border-collapse:separate;border-spacing:0;}
thead th{
  text-align:left;
  font-size:11px;
  letter-spacing:.08em;
  text-transform:uppercase;
  color:var(--muted);
  background:#f1f5f9;
  padding:12px 14px;
  border-bottom:1px solid var(--border);
}
tbody td{padding:12px 14px;border-bottom:1px solid var(--border);vertical-align:middle;}
tbody tr:hover{background:#f8fafc;}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;}
.mono_scroll_x{
  display:inline-block;
  max-width:min(44vw,560px);
  overflow-x:auto;
  overflow-y:hidden;
  white-space:nowrap;
  vertical-align:bottom;
  scrollbar-width:thin;
}
.mono_scroll_x::-webkit-scrollbar{height:6px;}
.mono_scroll_x::-webkit-scrollbar-thumb{background:#cbd5e1;border-radius:999px;}
.pill .mono_scroll_x{max-width:min(64vw,760px);}
.muted{color:var(--muted);}
.row_actions{display:flex;justify-content:flex-end;gap:10px;align-items:center;flex-wrap:wrap;}
.link{color:rgba(2,132,199,1);}
.link:hover{text-decoration:underline;}
.row{display:flex;flex-direction:column;gap:12px;}
.warn{
  display:flex;align-items:flex-start;gap:10px;
  padding:14px 16px;
  border:1px solid rgba(220,38,38,.18);
  border-radius:14px;
  background:linear-gradient(180deg,rgba(254,242,242,.98),rgba(254,242,242,.88));
  color:#b91c1c;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.72);
}
.pathbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;}
.pill{display:inline-flex;gap:6px;align-items:center;border:1px solid var(--border);background:#fff;border-radius:999px;padding:6px 10px;font-size:12px;color:var(--muted);}
.pill strong{color:var(--text);}
.modal_backdrop{
  position:fixed;inset:0;background:rgba(15,23,42,.45);
  display:none;align-items:center;justify-content:center;padding:20px;z-index:50;
}
.modal_backdrop.open{display:flex;}
.modal{
  width:560px;max-width:95vw;background:#fff;border:1px solid var(--border);
  border-radius:14px;box-shadow:0 24px 64px rgba(15,23,42,.32);
  overflow:hidden;
}
.modal_head{display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid var(--border);}
.modal_body{padding:16px;}
.field{display:flex;flex-direction:column;gap:6px;margin:12px 0;}
.field label{font-size:12px;color:var(--muted);}
.field input[type=text], .field input[type=password], .field input[type=file], .field textarea, .field select{
  width:100%;
  border:1px solid var(--border);
  border-radius:10px;
  padding:10px 12px;
  font-size:13px;
  outline:none;
  background:#fff;
  color:var(--text);
}
.field input[type=text]:focus, .field input[type=password]:focus, .field input[type=file]:focus, .field textarea:focus, .field select:focus{
  border-color:rgba(2,132,199,.55);box-shadow:0 0 0 4px rgba(2,132,199,.14);
}
.field textarea{resize:vertical;line-height:1.5;}
.code_area{
  min-height:160px;
  font:12px ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;
}
.code_area.tall{min-height:520px;}
.modal_foot{display:flex;gap:10px;justify-content:flex-end;padding:12px 16px;border-top:1px solid var(--border);background:#f8fafc;}
.hint{font-size:12px;color:var(--muted);line-height:1.45;}
.account_credentials_body{
  display:flex;
  flex-direction:column;
  gap:18px;
  padding:20px;
}
.account_credentials_intro{font-size:13px;line-height:1.55;}
.account_credentials_form{
  display:flex;
  flex-direction:column;
  gap:14px;
  width:100%;
  max-width:560px;
}
.account_credentials_form .field{margin:0;}
.account_credentials_actions{display:flex;align-items:center;gap:10px;padding-top:2px;}
.workspace{display:flex;flex-direction:column;gap:14px;padding:16px;min-height:100%;}
.workspace_notice{display:none;border:1px solid var(--border);border-radius:12px;background:#f8fafc;color:var(--muted);padding:12px 14px;font-size:13px;}
.workspace_notice.open{display:block;}
.workspace_notice.info{border-color:rgba(2,132,199,.18);background:rgba(2,132,199,.06);color:#075985;}
.workspace_notice.error{border-color:rgba(220,38,38,.22);background:rgba(220,38,38,.06);color:#b91c1c;}
.pane_strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:14px;align-items:stretch;}
.pane_shell{display:flex;flex-direction:column;min-width:0;border:1px solid var(--border);border-radius:16px;background:#fff;box-shadow:0 8px 24px rgba(15,23,42,.06);overflow:hidden;}
.pane_shell.active{border-color:rgba(2,132,199,.45);box-shadow:0 16px 32px rgba(2,132,199,.12);}
.pane_shell.drop_target{border-color:rgba(2,132,199,.92);box-shadow:0 0 0 3px rgba(2,132,199,.12);}
.pane_head{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:12px 14px;border-bottom:1px solid var(--border);background:#fff;}
.pane_title{display:flex;align-items:center;gap:10px;min-width:0;}
.pane_title_text{font-size:13px;font-weight:700;color:var(--text);}
.pane_title_meta{font-size:12px;color:var(--muted);}
.pane_tools{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
.pane_tool_btn{padding:7px 10px;font-size:12px;border-radius:9px;}
.pane_tabbar{display:flex;gap:10px;align-items:stretch;flex-wrap:wrap;padding:12px;border-bottom:1px solid var(--border);background:linear-gradient(180deg,#ffffff 0%,#f8fafc 100%);}
.page_tab{display:flex;align-items:stretch;min-width:0;border:1px solid var(--border);border-radius:12px;background:#fff;box-shadow:0 1px 2px rgba(15,23,42,.04);overflow:hidden;}
.page_tab.active{border-color:rgba(2,132,199,.45);box-shadow:0 12px 24px rgba(2,132,199,.12);}
.page_tab.drop_target{border-color:rgba(2,132,199,.92);box-shadow:0 0 0 3px rgba(2,132,199,.12);}
.page_tab_main{border:0;background:transparent;padding:10px 14px;min-width:180px;max-width:320px;text-align:left;font-weight:700;font-size:13px;color:var(--text);cursor:pointer;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.page_tab_main:hover{background:#f8fafc;}
.page_tab_close{border:0;border-left:1px solid var(--border);background:transparent;padding:0 12px;font-size:16px;line-height:1;color:var(--muted);cursor:pointer;}
.page_tab_close:hover{background:#f8fafc;color:var(--text);}
.page_panel{display:flex;flex-direction:column;flex:1;min-height:0;border:0;border-radius:0;background:#fff;overflow:hidden;}
.page_panel_head{padding:14px 16px;border-bottom:1px solid var(--border);background:transparent;}
.page_meta{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;}
.page_actions{display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
.clipboard_pill.copy{border-color:rgba(2,132,199,.2);background:rgba(2,132,199,.08);}
.clipboard_pill.cut{border-color:rgba(234,88,12,.22);background:rgba(234,88,12,.08);color:#9a3412;}
.pill_button{border:0;background:transparent;color:inherit;font-weight:700;cursor:pointer;padding:0;}
.page_hint{font-size:12px;color:var(--muted);}
.page_surface{position:relative;flex:1;min-height:140px;}
.pane_empty{padding:24px 16px;color:var(--muted);font-size:13px;}
.table_drop_target{width:100%;table-layout:fixed;}
.table_drop_target th:nth-child(2){width:92px;}
.table_drop_target th:nth-child(3){width:84px;}
.table_drop_target th:nth-child(4){width:168px;}
.table_drop_target th:nth-child(5){width:112px;}
.table_drop_target tbody tr.drop_target td{background:rgba(2,132,199,.08);}
.empty_state{padding:36px 16px;text-align:center;color:var(--muted);font-size:13px;}
.progress_meta{display:none;border:1px solid rgba(2,132,199,.16);background:rgba(2,132,199,.05);border-radius:10px;padding:10px 12px;font-size:12px;color:#075985;line-height:1.45;}
.progress_meta.open{display:block;}
.progress_meta.error{border-color:rgba(220,38,38,.22);background:rgba(220,38,38,.06);color:#b91c1c;}
.progress_shell{
  display:none;
  border:1px solid rgba(2,132,199,.16);
  background:rgba(2,132,199,.05);
  border-radius:12px;
  padding:12px;
}
.progress_shell.open{display:block;}
.progress_shell.error{border-color:rgba(220,38,38,.22);background:rgba(220,38,38,.06);}
.progress_shell.done{border-color:rgba(22,163,74,.18);background:rgba(22,163,74,.06);}
.progress_head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;}
.progress_label{font-size:13px;font-weight:700;color:var(--text);line-height:1.35;word-break:break-word;}
.progress_stage{font-size:12px;color:var(--muted);white-space:nowrap;}
.progress_bar{margin-top:10px;height:10px;border-radius:999px;background:#dbeafe;overflow:hidden;}
.progress_fill{
  display:block;
  height:100%;
  width:0%;
  background:linear-gradient(90deg,var(--brand2),var(--brand));
  transition:width .18s ease;
}
.progress_shell.error .progress_bar{background:#fee2e2;}
.progress_shell.error .progress_fill{background:linear-gradient(90deg,#f87171,#dc2626);}
.progress_shell.done .progress_bar{background:#dcfce7;}
.progress_shell.done .progress_fill{background:linear-gradient(90deg,#4ade80,#16a34a);}
.progress_detail{margin-top:8px;font-size:12px;color:var(--muted);line-height:1.45;word-break:break-word;}
.progress_actions{margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;}
.progress_btn{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  gap:6px;
  padding:7px 10px;
  border-radius:9px;
  border:1px solid var(--border);
  background:#fff;
  color:var(--text);
  font-size:12px;
  font-weight:600;
  cursor:pointer;
}
.progress_btn:hover{background:#f8fafc;}
.progress_btn.danger{border-color:rgba(220,38,38,.25);background:rgba(220,38,38,.08);color:var(--danger);}
.progress_btn.danger:hover{background:rgba(220,38,38,.12);}
.context_menu{position:fixed;display:none;min-width:180px;background:#fff;border:1px solid var(--border);border-radius:12px;box-shadow:0 24px 64px rgba(15,23,42,.22);padding:6px;z-index:80;}
.context_menu.open{display:block;}
.context_item{display:block;width:100%;border:0;background:transparent;border-radius:8px;padding:9px 10px;text-align:left;font-size:13px;color:var(--text);cursor:pointer;}
.context_item:hover{background:#f8fafc;}
.context_item.danger{color:var(--danger);}
.admin_grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:14px;padding:16px;}
.admin_card{display:flex;align-items:center;gap:16px;padding:18px 20px;border:1px solid var(--border);border-radius:14px;background:#fff;cursor:pointer;transition:border-color .15s,box-shadow .15s;}
.admin_card:hover{border-color:rgba(2,132,199,.4);box-shadow:0 8px 24px rgba(2,132,199,.1);}
.admin_card_icon{width:44px;height:44px;border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:22px;flex-shrink:0;}
.admin_card_body{flex:1;min-width:0;}
.admin_card_title{font-size:15px;font-weight:700;color:var(--text);}
.admin_card_desc{font-size:13px;color:var(--muted);margin-top:2px;}
.admin_card_arrow{font-size:22px;color:#cbd5e1;flex-shrink:0;transition:transform .15s;}
.admin_card:hover .admin_card_arrow{transform:translateX(3px);color:var(--brand);}
.users_page{
  display:flex;
  flex-direction:column;
  gap:18px;
  padding:18px;
  background:
    radial-gradient(circle at top right, rgba(14,165,233,.08), transparent 32%),
    linear-gradient(180deg, rgba(248,250,252,.92), #fff 22%);
}
.users_layout{
  display:grid;
  grid-template-columns:minmax(340px,420px) minmax(0,1fr);
  gap:18px;
  align-items:start;
}
.users_panel{
  border:1px solid var(--border);
  border-radius:18px;
  background:linear-gradient(180deg,#fff,#f8fafc);
  box-shadow:0 10px 30px rgba(15,23,42,.06);
  overflow:hidden;
}
.users_panel_head{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:16px;
  padding:18px 20px;
  border-bottom:1px solid rgba(229,231,235,.9);
  background:linear-gradient(180deg,rgba(255,255,255,.98),rgba(248,250,252,.95));
}
.users_panel_eyebrow{
  font-size:11px;
  font-weight:700;
  letter-spacing:.1em;
  text-transform:uppercase;
  color:var(--brand);
}
.users_panel_title{margin:4px 0 0;font-size:17px;font-weight:700;color:var(--text);}
.users_panel_desc{margin-top:6px;font-size:13px;color:var(--muted);line-height:1.55;max-width:52ch;}
.users_panel_body{padding:20px;}
.users_stat{
  display:inline-flex;
  align-items:center;
  gap:6px;
  border:1px solid rgba(2,132,199,.18);
  border-radius:999px;
  background:rgba(2,132,199,.08);
  color:#075985;
  padding:8px 12px;
  font-size:12px;
  font-weight:700;
  white-space:nowrap;
}
.users_form{display:flex;flex-direction:column;gap:16px;}
.users_form_grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;}
.users_form_grid .field{margin:0;}
.users_form_grid .field.full{grid-column:1 / -1;}
.users_hint{font-size:12px;color:var(--muted);line-height:1.6;}
.users_form_actions{display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
.fs_master_layout{grid-template-columns:repeat(2,minmax(0,1fr));align-items:start;}
.fs_master_layout .users_panel_body{min-width:0;overflow-x:auto;}
.fs_master_summary_strip{display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
.fs_master_panel_half{min-width:0;}
.fs_master_panel_full{grid-column:1 / -1;}
.fs_master_agent_grid{display:grid;grid-template-columns:minmax(0,1fr);gap:14px;}
.fs_master_agent_card{
  display:flex;
  flex-direction:column;
  gap:14px;
  min-width:0;
  padding:16px;
  border:1px solid var(--border);
  border-radius:16px;
  background:linear-gradient(180deg,#ffffff 0%,#f8fafc 100%);
  box-shadow:0 6px 18px rgba(15,23,42,.04);
}
.fs_master_agent_card_head{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
}
.fs_master_agent_name{
  font-size:15px;
  font-weight:700;
  color:var(--text);
  word-break:break-word;
}
.fs_master_agent_table_shell{
  min-width:0;
  overflow-x:auto;
  overflow-y:hidden;
}
.fs_master_agent_table{
  width:max-content;
  min-width:100%;
}
.fs_master_agent_table_value{
  display:block;
  white-space:nowrap;
}
.fs_master_icon_form{display:inline-flex;margin:0;}
.fs_master_export_modal{width:min(820px,96vw);}
.fs_master_browse_shell{
  display:flex;
  flex-direction:column;
  gap:12px;
  margin-top:16px;
  padding:16px;
  border:1px solid var(--border);
  border-radius:14px;
  background:#f8fafc;
}
.fs_master_browse_head{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
}
.fs_master_browse_actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
.fs_master_browse_path{
  padding:10px 12px;
  border:1px solid var(--border);
  border-radius:12px;
  background:#fff;
  overflow-x:auto;
  overflow-y:hidden;
  white-space:nowrap;
}
.fs_master_browse_status{min-height:18px;}
.fs_master_browse_status.error{color:var(--danger);}
.fs_master_browse_entries{
  display:flex;
  flex-direction:column;
  gap:8px;
  max-height:320px;
  overflow:auto;
  padding-right:4px;
}
.fs_master_browse_entry{
  display:flex;
  align-items:center;
  gap:12px;
  width:100%;
  padding:10px 12px;
  border:1px solid var(--border);
  border-radius:12px;
  background:#fff;
  color:var(--text);
  text-align:left;
}
button.fs_master_browse_entry{cursor:pointer;}
button.fs_master_browse_entry:hover{border-color:rgba(2,132,199,.28);background:#eff6ff;}
.fs_master_browse_entry.disabled{color:var(--muted);}
.fs_master_browse_entry_icon{font-size:16px;flex:0 0 auto;}
.fs_master_browse_entry_name{
  flex:1 1 auto;
  min-width:0;
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
}
.fs_master_browse_entry_meta{font-size:12px;color:var(--muted);flex:0 0 auto;}
.fs_master_browse_loading,.fs_master_browse_empty{padding:20px 12px;text-align:center;color:var(--muted);font-size:13px;}
.check_grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;}
.check_item{
  display:flex;
  align-items:center;
  gap:10px;
  min-height:44px;
  padding:10px 12px;
  border:1px solid var(--border);
  border-radius:12px;
  background:#fff;
  color:var(--text);
  cursor:pointer;
}
.check_item input[type=checkbox]{width:auto;margin:0;flex:0 0 auto;}
.check_item:hover{border-color:rgba(2,132,199,.28);background:#f8fafc;}
.check_meta{
  margin-left:auto;
  font-size:11px;
  font-weight:700;
  letter-spacing:.06em;
  text-transform:uppercase;
  color:var(--muted);
}
.users_list{display:flex;flex-direction:column;gap:12px;}
.user_card{
  display:flex;
  flex-direction:column;
  gap:16px;
  padding:16px;
  border:1px solid var(--border);
  border-radius:16px;
  background:#fff;
  box-shadow:0 4px 18px rgba(15,23,42,.04);
  transition:border-color .15s,box-shadow .15s,transform .15s;
}
.user_card:hover{
  border-color:rgba(2,132,199,.28);
  box-shadow:0 14px 30px rgba(2,132,199,.08);
  transform:translateY(-1px);
}
.user_card_head{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:16px;
  flex-wrap:wrap;
}
.user_card_identity{display:flex;flex-direction:column;gap:8px;min-width:0;}
.user_card_name{
  font-size:16px;
  font-weight:700;
  color:var(--text);
  word-break:break-word;
}
.user_badges{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
.rule_badge{
  display:inline-flex;
  align-items:center;
  gap:6px;
  border:1px solid rgba(2,132,199,.16);
  border-radius:999px;
  background:rgba(2,132,199,.08);
  color:#075985;
  padding:6px 10px;
  font-size:12px;
  font-weight:700;
}
.user_quick_actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
.user_action_form{display:flex;margin:0;}
.user_reset_shell{
  display:flex;
  flex-direction:column;
  gap:10px;
  padding-top:16px;
  border-top:1px dashed #cbd5e1;
}
.user_reset_label{
  font-size:11px;
  font-weight:700;
  letter-spacing:.08em;
  text-transform:uppercase;
  color:var(--muted);
}
.user_reset_form{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr)) auto;
  gap:10px;
  align-items:end;
}
.user_reset_form .field{margin:0;}
.user_reset_form .btn{min-height:40px;}
.nav_badge{display:none;min-width:18px;height:18px;border-radius:999px;background:rgba(234,88,12,.9);color:#fff;font-size:11px;font-weight:700;text-align:center;line-height:18px;padding:0 5px;margin-left:auto;}
.nav_badge.visible{display:inline-block;}
.transfer_toast{
  position:fixed;bottom:20px;right:20px;z-index:60;
  width:340px;max-width:calc(100vw - 40px);
  background:#fff;border:1px solid var(--border);border-radius:14px;
  box-shadow:0 16px 48px rgba(15,23,42,.18);
  display:none;overflow:hidden;
  transition:opacity .2s;
}
.transfer_toast.open{display:block;}
.transfer_toast_head{
  display:flex;align-items:center;justify-content:space-between;gap:10px;
  padding:12px 14px;border-bottom:1px solid var(--border);cursor:pointer;
}
.transfer_toast_head:hover{background:#f8fafc;}
.transfer_toast_title{font-size:13px;font-weight:700;color:var(--text);}
.transfer_toast_meta{font-size:12px;color:var(--muted);}
.transfer_toast_body{max-height:240px;overflow-y:auto;}
.transfer_toast_item{padding:10px 14px;border-bottom:1px solid var(--border);}
.transfer_toast_item:last-child{border-bottom:0;}
.transfer_toast_item_head{display:flex;align-items:center;justify-content:space-between;gap:8px;}
.transfer_toast_item_name{font-size:12px;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0;}
.transfer_toast_item_pct{font-size:11px;color:var(--muted);flex-shrink:0;}
.transfer_toast_item_bar{margin-top:6px;height:6px;border-radius:999px;background:#e2e8f0;overflow:hidden;}
.transfer_toast_item_fill{display:block;height:100%;background:linear-gradient(90deg,var(--brand2),var(--brand));transition:width .18s ease;}
.transfer_toast_item.error .transfer_toast_item_fill{background:linear-gradient(90deg,#f87171,#dc2626);}
.transfer_toast_item.error .transfer_toast_item_bar{background:#fee2e2;}
.transfer_toast_item.done .transfer_toast_item_fill{background:linear-gradient(90deg,#4ade80,#16a34a);}
.transfer_toast_item.done .transfer_toast_item_bar{background:#dcfce7;}
.transfer_toast_item.paused .transfer_toast_item_fill{background:linear-gradient(90deg,#fbbf24,#d97706);}
.transfer_toast_item.paused .transfer_toast_item_bar{background:#fef3c7;}
.transfer_toast_item_detail{font-size:11px;color:var(--muted);margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.transfers_page{padding:16px;display:flex;flex-direction:column;gap:12px;}
.transfer_section{
  border:1px solid var(--border);
  border-radius:14px;
  background:#fff;
  box-shadow:0 1px 2px rgba(15,23,42,.04);
  overflow:hidden;
}
.transfer_section_head{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:12px;
  padding:14px 16px;
  border-bottom:1px solid var(--border);
  background:linear-gradient(180deg,#fff,#f8fafc);
}
.transfer_section_title{font-size:14px;font-weight:700;color:var(--text);}
.transfer_section_hint{margin-top:4px;font-size:12px;color:var(--muted);line-height:1.45;}
.transfer_section_body{padding:12px;}
.transfer_jobs_layout{
  display:grid;
  grid-template-columns:minmax(340px,420px) minmax(0,1fr);
  gap:14px;
  padding:12px;
  align-items:start;
}
.transfer_jobs_list,.transfer_job_detail{
  min-width:0;
  min-height:220px;
}
.transfer_job_card{
  border:1px solid var(--border);
  border-radius:12px;
  background:#fff;
  padding:14px;
  display:flex;
  flex-direction:column;
  gap:10px;
  cursor:pointer;
  transition:border-color .15s,box-shadow .15s,transform .15s;
}
.transfer_job_card:hover{
  border-color:rgba(2,132,199,.32);
  box-shadow:0 14px 28px rgba(2,132,199,.08);
  transform:translateY(-1px);
}
.transfer_job_card.active{
  border-color:rgba(2,132,199,.58);
  box-shadow:0 16px 30px rgba(2,132,199,.12);
  background:linear-gradient(180deg,rgba(240,249,255,.75),#fff);
}
.transfer_job_card_head{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:10px;
}
.transfer_job_card_title{
  font-size:13px;
  font-weight:700;
  color:var(--text);
  line-height:1.4;
  word-break:break-word;
}
.transfer_job_card_meta{
  display:flex;
  flex-wrap:wrap;
  gap:8px;
}
.transfer_job_card_line{
  font-size:12px;
  color:var(--muted);
  line-height:1.5;
  word-break:break-word;
}
.transfer_job_pill{
  display:inline-flex;
  align-items:center;
  gap:6px;
  padding:5px 9px;
  border:1px solid var(--border);
  border-radius:999px;
  background:#f8fafc;
  color:var(--muted);
  font-size:11px;
  font-weight:700;
  letter-spacing:.02em;
}
.transfer_job_pill.running{border-color:rgba(2,132,199,.18);background:rgba(2,132,199,.08);color:#075985;}
.transfer_job_pill.done{border-color:rgba(22,163,74,.2);background:rgba(22,163,74,.08);color:#166534;}
.transfer_job_pill.cancelled{border-color:rgba(100,116,139,.22);background:rgba(148,163,184,.12);color:#475569;}
.transfer_job_pill.error{border-color:rgba(220,38,38,.2);background:rgba(220,38,38,.08);color:#b91c1c;}
.transfer_job_stats{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:10px;
}
.transfer_job_stat{
  border:1px solid var(--border);
  border-radius:10px;
  background:#f8fafc;
  padding:10px 12px;
}
.transfer_job_stat_label{
  font-size:11px;
  letter-spacing:.06em;
  text-transform:uppercase;
  color:var(--muted);
}
.transfer_job_stat_value{
  margin-top:4px;
  font-size:15px;
  font-weight:700;
  color:var(--text);
}
.transfer_job_detail_card{
  border:1px solid var(--border);
  border-radius:14px;
  background:linear-gradient(180deg,#fff,#f8fafc);
  box-shadow:0 10px 26px rgba(15,23,42,.05);
  padding:16px;
  display:flex;
  flex-direction:column;
  gap:16px;
}
.transfer_job_detail_head{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
}
.transfer_job_detail_title{
  font-size:16px;
  font-weight:700;
  color:var(--text);
  line-height:1.4;
}
.transfer_job_detail_subtitle{
  margin-top:6px;
  font-size:12px;
  color:var(--muted);
  line-height:1.5;
}
.transfer_job_detail_grid{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:12px;
}
.transfer_job_detail_stack{
  display:flex;
  flex-direction:column;
  gap:12px;
}
.transfer_job_detail_block{
  border:1px solid var(--border);
  border-radius:12px;
  background:#fff;
  padding:14px;
}
.transfer_job_detail_block_title{
  font-size:12px;
  font-weight:700;
  color:var(--text);
  margin-bottom:10px;
}
.transfer_job_detail_list{
  display:flex;
  flex-direction:column;
  gap:8px;
}
.transfer_job_tuning_form{
  display:flex;
  flex-direction:column;
  gap:10px;
}
.transfer_job_tuning_form .field{margin:0;}
.transfer_job_tuning_actions{
  display:flex;
  justify-content:flex-end;
}
.transfer_job_detail_item{
  font-size:12px;
  color:var(--muted);
  line-height:1.55;
  word-break:break-word;
}
.transfer_job_detail_section{
  padding:0;
  overflow:hidden;
}
.transfer_job_detail_section_head{
  width:100%;
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  border:0;
  background:#fff;
  padding:14px;
  cursor:pointer;
  text-align:left;
}
.transfer_job_detail_section_head:hover{
  background:#f8fafc;
}
.transfer_job_detail_section_title{
  font-size:12px;
  font-weight:700;
  color:var(--text);
}
.transfer_job_detail_section_meta{
  font-size:11px;
  color:var(--muted);
  flex:1;
  text-align:right;
  min-width:0;
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
}
.transfer_job_detail_section_chev{
  font-size:14px;
  color:var(--muted);
  flex-shrink:0;
}
.transfer_job_detail_section_body{
  padding:0 14px 14px;
}
.transfer_job_detail_lazy_state{
  padding-top:2px;
}
.transfer_job_history_stack{
  display:flex;
  flex-direction:column;
  gap:10px;
}
.transfer_job_history_overview{
  display:flex;
  flex-wrap:wrap;
  justify-content:space-between;
  gap:8px 12px;
}
.transfer_job_history_chart{
  border:1px solid var(--border);
  border-radius:10px;
  background:linear-gradient(180deg,#fff,#f8fafc);
  padding:10px 12px;
  position:relative;
  overflow:hidden;
}
.transfer_job_history_chart_head{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
}
.transfer_job_history_chart_title{
  font-size:12px;
  font-weight:700;
  color:var(--text);
}
.transfer_job_history_chart_meta{
  font-size:11px;
  color:var(--muted);
  text-align:right;
}
.transfer_job_history_svg{
  width:100%;
  height:72px;
  display:block;
  margin-top:8px;
}
.transfer_job_history_hover_band{
  fill:transparent;
  cursor:crosshair;
}
.transfer_job_history_focus_line{
  stroke:#94a3b8;
  stroke-width:1;
  stroke-dasharray:3 3;
  pointer-events:none;
}
.transfer_job_history_focus_line.hidden{
  opacity:0;
}
.transfer_job_history_focus_dot{
  stroke:#fff;
  stroke-width:1.5;
  pointer-events:none;
}
.transfer_job_history_focus_dot.hidden{
  opacity:0;
}
.transfer_job_history_tooltip{
  position:absolute;
  z-index:2;
  min-width:132px;
  max-width:220px;
  padding:8px 10px;
  border:1px solid rgba(148,163,184,.35);
  border-radius:10px;
  background:rgba(255,255,255,.96);
  box-shadow:0 10px 30px rgba(15,23,42,.10);
  color:var(--text);
  font-size:11px;
  line-height:1.45;
  pointer-events:none;
  opacity:0;
  transform:translate3d(0,0,0);
  transition:opacity .08s linear;
}
.transfer_job_history_tooltip.hidden{
  opacity:0;
}
.transfer_job_history_tooltip.visible{
  opacity:1;
}
.transfer_job_history_tooltip_metric{
  font-size:11px;
  font-weight:700;
  color:var(--text);
}
.transfer_job_history_tooltip_value{
  margin-top:2px;
  font-size:12px;
  font-weight:700;
  color:#0f172a;
}
.transfer_job_history_tooltip_time{
  margin-top:4px;
  color:var(--muted);
}
.transfer_job_history_axis{
  display:flex;
  justify-content:space-between;
  gap:12px;
  margin-top:6px;
  font-size:10px;
  color:var(--muted);
}
.transfer_job_failure_list,.transfer_job_worker_list{
  display:flex;
  flex-direction:column;
  gap:10px;
}
.transfer_job_failure_item,.transfer_job_worker_item{
  border:1px solid var(--border);
  border-radius:10px;
  background:#fff;
  padding:12px;
}
.transfer_job_failure_item{border-color:rgba(220,38,38,.16);background:rgba(254,242,242,.72);}
.transfer_job_failure_head,.transfer_job_worker_head{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:10px;
  flex-wrap:wrap;
}
.transfer_job_failure_scope,.transfer_job_worker_title{
  font-size:12px;
  font-weight:700;
  color:var(--text);
}
.transfer_job_failure_time,.transfer_job_worker_meta{
  font-size:11px;
  color:var(--muted);
}
.transfer_job_failure_message{
  margin-top:8px;
  font-size:12px;
  color:#991b1b;
  line-height:1.55;
  white-space:pre-wrap;
  word-break:break-word;
}
.transfer_job_worker_lines{
  margin-top:8px;
  display:flex;
  flex-direction:column;
  gap:6px;
}
.transfer_prescan_list{
  display:flex;
  flex-direction:column;
  gap:10px;
}
.transfer_prescan_card{
  border:1px solid var(--border);
  border-radius:12px;
  background:#fff;
  padding:14px;
  display:flex;
  flex-direction:column;
  gap:12px;
}
.transfer_prescan_head{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
}
.transfer_prescan_lines{
  display:flex;
  flex-direction:column;
  gap:6px;
}
.transfer_prescan_actions{
  display:flex;
  align-items:center;
  justify-content:flex-end;
  gap:10px;
}
.transfer_prescan_hint{
  font-size:12px;
  color:var(--muted);
  line-height:1.5;
}
.workspace_surface_hidden{display:none;}
.workspace_surface_visible{display:block;}
.transfer_row{
  display:flex;align-items:center;gap:16px;padding:14px 16px;
  border:1px solid var(--border);border-radius:12px;background:#fff;
}
.transfer_row.clickable{cursor:pointer;transition:border-color .15s,box-shadow .15s,transform .15s;}
.transfer_row.clickable:hover{border-color:rgba(2,132,199,.32);box-shadow:0 14px 28px rgba(2,132,199,.1);transform:translateY(-1px);}
.transfer_row.paused{border-color:rgba(234,179,8,.24);background:rgba(234,179,8,.05);}
.transfer_row.cancelled{border-color:rgba(100,116,139,.22);background:rgba(148,163,184,.06);}
.transfer_row.error{border-color:rgba(220,38,38,.22);}
.transfer_row.done{border-color:rgba(22,163,74,.18);}
.transfer_row_icon{width:36px;height:36px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0;background:rgba(2,132,199,.08);color:var(--brand);}
.transfer_row.paused .transfer_row_icon{background:rgba(234,179,8,.14);color:#a16207;}
.transfer_row.cancelled .transfer_row_icon{background:rgba(148,163,184,.16);color:#475569;}
.transfer_row.error .transfer_row_icon{background:rgba(220,38,38,.08);color:var(--danger);}
.transfer_row.done .transfer_row_icon{background:rgba(22,163,74,.08);color:#16a34a;}
.transfer_row_body{flex:1;min-width:0;}
.transfer_row_name{font-size:13px;font-weight:700;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.transfer_row_detail{font-size:12px;color:var(--muted);margin-top:2px;}
.transfer_row_bar{margin-top:8px;height:8px;border-radius:999px;background:#e2e8f0;overflow:hidden;}
.transfer_row.paused .transfer_row_bar{background:#fef3c7;}
.transfer_row.paused .transfer_row_fill{background:linear-gradient(90deg,#fbbf24,#d97706);}
.transfer_row_fill{display:block;height:100%;background:linear-gradient(90deg,var(--brand2),var(--brand));transition:width .18s ease;}
.transfer_row.cancelled .transfer_row_bar{background:#e2e8f0;}
.transfer_row.cancelled .transfer_row_fill{background:linear-gradient(90deg,#94a3b8,#64748b);}
.transfer_row.error .transfer_row_bar{background:#fee2e2;}
.transfer_row.error .transfer_row_fill{background:linear-gradient(90deg,#f87171,#dc2626);}
.transfer_row.done .transfer_row_bar{background:#dcfce7;}
.transfer_row.done .transfer_row_fill{background:linear-gradient(90deg,#4ade80,#16a34a);}
.transfer_row_controls{display:flex;align-items:center;gap:8px;flex-wrap:wrap;justify-content:flex-end;}
.transfer_row_controls .btn{padding:7px 10px;font-size:12px;border-radius:9px;}
.transfer_row_pct{font-size:13px;font-weight:700;color:var(--text);flex-shrink:0;width:52px;text-align:right;}
.object_name_cell{max-width:0;overflow:hidden;}
.object_name_link{display:block;min-width:0;max-width:100%;vertical-align:bottom;overflow:hidden;}
.object_name_scroll{
  display:block;
  width:100%;
  max-width:100%;
  overflow-x:auto;
  overflow-y:hidden;
  white-space:nowrap;
  vertical-align:bottom;
  scrollbar-width:thin;
}
.object_name_scroll::-webkit-scrollbar{height:6px;}
.object_name_scroll::-webkit-scrollbar-thumb{background:#cbd5e1;border-radius:999px;}
.action_icon_btn{width:34px;height:34px;padding:0;flex:0 0 auto;}
.action_icon_btn svg{
  width:16px;
  height:16px;
  display:block;
  stroke:currentColor;
  fill:none;
  stroke-width:1.8;
  stroke-linecap:round;
  stroke-linejoin:round;
}
.table_drop_target tbody tr.focus_target td{background:rgba(2,132,199,.14);}
@media (max-width:1100px){
  .users_layout:not(.fs_master_layout){grid-template-columns:1fr;}
  .fs_master_agent_grid{grid-template-columns:1fr;}
}
@media (max-width:900px){
  .pane_strip{grid-template-columns:1fr;}
  .page_tab_main{min-width:140px;max-width:220px;}
  .search input{width:180px;}
  .fs_master_layout{grid-template-columns:1fr;}
  .users_page{padding:14px;}
  .users_panel_head{flex-direction:column;align-items:flex-start;}
  .users_form_grid,.user_reset_form{grid-template-columns:1fr;}
  .transfer_jobs_layout,.transfer_job_detail_grid,.transfer_job_stats{grid-template-columns:1fr;}
}
"#;

const UI_JS: &str = r##"
(function(){
  var MULTIPART_UPLOAD_PART_BYTES=__UI_MULTIPART_UPLOAD_PART_BYTES__;
  var MULTIPART_UPLOAD_MAX_INFLIGHT=__UI_MULTIPART_UPLOAD_MAX_INFLIGHT__;
  if(window.indexedDB&&typeof window.indexedDB.deleteDatabase==='function'){
    var cleanupReq=window.indexedDB.deleteDatabase('fluxon_s3_ui_downloads');
    cleanupReq.onerror=function(){console.warn('cleanup legacy resumable download DB failed',cleanupReq.error);};
  }

  function qs(id){return document.getElementById(id);}
  function openModal(id){
    var el=qs(id);
    if(el){el.classList.add('open');}
  }
  function closeModal(id){
    var el=qs(id);
    if(el){el.classList.remove('open');}
  }
  function escapeHtml(value){
    return String(value)
      .replace(/&/g,'&amp;')
      .replace(/</g,'&lt;')
      .replace(/>/g,'&gt;')
      .replace(/\"/g,'&quot;')
      .replace(/'/g,'&#39;');
  }
  function xmlEscape(value){
    return String(value)
      .replace(/&/g,'&amp;')
      .replace(/</g,'&lt;')
      .replace(/>/g,'&gt;')
      .replace(/"/g,'&quot;')
      .replace(/'/g,'&apos;');
  }
  function prefixLabel(prefix){return prefix ? prefix : '/';}
  function attachStaticSearch(){
    var q=qs('search');
    if(!q){return;}
    var rows=Array.prototype.slice.call(document.querySelectorAll('[data-filter-row]'));
    var apply=function(){
      var s=(q.value||'').trim().toLowerCase();
      for(var i=0;i<rows.length;i++){
        var t=(rows[i].getAttribute('data-filter-text')||'').toLowerCase();
        rows[i].style.display=(!s||t.indexOf(s)>=0)?'':'';
      }
    };
    q.addEventListener('input', apply);
  }
  function initFsMasterAdminPage(){
    var modal=qs('fs_master_export_modal');
    var form=qs('fs_master_export_form');
    if(!modal||!form){return;}
    var browseHref=String(form.getAttribute('data-browse-href')||'').trim();
    var agentInput=qs('fs_master_export_agent_input');
    var pathInput=qs('fs_master_export_path_input');
    var agentView=qs('fs_master_export_agent_view');
    var pathView=qs('fs_master_export_path_view');
    var nameInput=qs('fs_master_export_name_input');
    var dirView=qs('fs_master_browse_dir_view');
    var statusEl=qs('fs_master_browse_status');
    var entriesEl=qs('fs_master_browse_entries');
    var rootBtn=qs('fs_master_browse_root_btn');
    var upBtn=qs('fs_master_browse_up_btn');
    if(
      !browseHref||!agentInput||!pathInput||!agentView||!pathView||!nameInput||
      !dirView||!statusEl||!entriesEl||!rootBtn||!upBtn
    ){
      return;
    }
    var browseState={agentInstanceKey:'',dirAbs:'/',parentDirAbs:null,loading:false};

    function setBrowseStatus(message,isError){
      statusEl.textContent=message||'';
      statusEl.className=isError?'hint fs_master_browse_status error':'hint fs_master_browse_status';
    }

    function syncBrowseSelection(){
      var dirAbs=browseState.dirAbs||'/';
      agentInput.value=browseState.agentInstanceKey||'';
      pathInput.value=dirAbs;
      agentView.textContent=browseState.agentInstanceKey||'';
      pathView.textContent=dirAbs;
      dirView.textContent=dirAbs;
      rootBtn.disabled=browseState.loading||dirAbs==='/';
      upBtn.disabled=browseState.loading||!browseState.parentDirAbs;
    }

    function renderBrowseEntries(entries){
      if(!Array.isArray(entries)||!entries.length){
        entriesEl.innerHTML='<div class="fs_master_browse_empty">No entries.</div>';
        return;
      }
      var html='';
      for(var i=0;i<entries.length;i++){
        var entry=entries[i]||{};
        var name=String(entry.name||'');
        var pathAbs=String(entry.path_abs||'');
        if(entry.is_dir){
          html+='<button class="fs_master_browse_entry" type="button" data-fs-master-dir-entry="'+escapeHtml(pathAbs)+'">';
          html+='<span class="fs_master_browse_entry_icon" aria-hidden="true">&#128193;</span>';
          html+='<span class="mono fs_master_browse_entry_name">'+escapeHtml(name)+'</span>';
          html+='<span class="fs_master_browse_entry_meta">directory</span>';
          html+='</button>';
          continue;
        }
        html+='<div class="fs_master_browse_entry disabled">';
        html+='<span class="fs_master_browse_entry_icon" aria-hidden="true">&#128196;</span>';
        html+='<span class="mono fs_master_browse_entry_name">'+escapeHtml(name)+'</span>';
        html+='<span class="fs_master_browse_entry_meta">file</span>';
        html+='</div>';
      }
      entriesEl.innerHTML=html;
    }

    async function loadBrowseDir(dirAbs){
      if(!browseState.agentInstanceKey){return;}
      browseState.loading=true;
      browseState.dirAbs=dirAbs||'/';
      browseState.parentDirAbs=null;
      syncBrowseSelection();
      setBrowseStatus('',false);
      entriesEl.innerHTML='<div class="fs_master_browse_loading">Loading...</div>';
      try{
        var payload=await apiRequestWithAs(
          browseHref+'?agent_instance_key='+encodeURIComponent(browseState.agentInstanceKey)+'&dir_abs='+encodeURIComponent(browseState.dirAbs),
          currentAsUser
        );
        browseState.dirAbs=String(payload&&payload.dir_abs||'/');
        browseState.parentDirAbs=typeof (payload&&payload.parent_dir_abs)==='string' ? String(payload.parent_dir_abs) : null;
        syncBrowseSelection();
        renderBrowseEntries(payload&&payload.entries);
      }catch(err){
        entriesEl.innerHTML='';
        setBrowseStatus(uiErrorMessage(err),true);
      }finally{
        browseState.loading=false;
        syncBrowseSelection();
      }
    }

    function openBrowseModal(agentInstanceKey){
      browseState.agentInstanceKey=String(agentInstanceKey||'').trim();
      browseState.dirAbs='/';
      browseState.parentDirAbs=null;
      browseState.loading=false;
      nameInput.value='';
      setBrowseStatus('',false);
      entriesEl.innerHTML='';
      syncBrowseSelection();
      openModal('fs_master_export_modal');
      window.setTimeout(function(){nameInput.focus();},0);
      loadBrowseDir('/');
    }

    entriesEl.addEventListener('click',function(ev){
      var target=ev.target;
      if(target&&typeof target.closest!=='function'){target=target.parentElement;}
      var btn=target&&target.closest('[data-fs-master-dir-entry]');
      if(!btn||browseState.loading){return;}
      loadBrowseDir(String(btn.getAttribute('data-fs-master-dir-entry')||'/'));
    });
    rootBtn.addEventListener('click',function(){
      if(browseState.loading||browseState.dirAbs==='/'){return;}
      loadBrowseDir('/');
    });
    upBtn.addEventListener('click',function(){
      if(browseState.loading||!browseState.parentDirAbs){return;}
      loadBrowseDir(browseState.parentDirAbs);
    });
    form.addEventListener('submit',function(ev){
      var agentInstanceKey=String(agentInput.value||'').trim();
      var exportName=String(nameInput.value||'').trim();
      var dirAbs=String(pathInput.value||'').trim();
      if(!agentInstanceKey){
        ev.preventDefault();
        setBrowseStatus('agent_instance_key must be non-empty',true);
        return;
      }
      if(!dirAbs||dirAbs.charAt(0)!=='/'){
        ev.preventDefault();
        setBrowseStatus('select an absolute directory first',true);
        return;
      }
      if(!exportName){
        ev.preventDefault();
        setBrowseStatus('export_name must be non-empty',true);
      }
    });
    var addBtns=document.querySelectorAll('[data-fs-master-add-export]');
    for(var i=0;i<addBtns.length;i++){
      addBtns[i].addEventListener('click',function(ev){
        openBrowseModal(ev.currentTarget.getAttribute('data-fs-master-add-export'));
      });
    }
  }
  window.__fluxonUiOpen=openModal;
  window.__fluxonUiClose=closeModal;

  // --- Transfer state shared across all pages ---
  var TRANSFER_STAGE_GLOBAL=Object.freeze({
    RUNNING:'running',
    PAUSED:'paused',
    DONE:'done',
    ERROR:'error',
    CANCELLED:'cancelled'
  });
  var DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY=Number(__DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY__)||10;
  var TRANSFER_JOB_HISTORY_AUTO_REFRESH_MS=5000;
  var TRANSFER_STORAGE_KEY_GLOBAL='fluxon.fs.s3.transfers.v1';
  var TRANSFER_BROADCAST_CH_GLOBAL='fluxon_fs_transfers';

  function loadTransferStateGlobal(){
    try{
      var raw=window.localStorage.getItem(TRANSFER_STORAGE_KEY_GLOBAL);
      if(raw){
        var parsed=JSON.parse(raw);
        if(parsed&&Array.isArray(parsed.items)){
          if(!Array.isArray(parsed.dismissedTaskIds)){parsed.dismissedTaskIds=[];}
          return parsed;
        }
      }
    }catch(_){}
    return {nextId:1,items:[],dismissedTaskIds:[]};
  }

  function countActiveGlobal(ts){
    var n=0;
    for(var i=0;i<ts.items.length;i++){
      if(isTransferActiveStage(ts.items[i].stage)){n++;}
    }
    return n;
  }

  function updateNavBadgeGlobal(){
    var badge=qs('nav_transfer_badge');
    if(!badge){return;}
    var ts=loadTransferStateGlobal();
    var active=countActiveGlobal(ts);
    if(active>0){
      badge.textContent=String(active);
      badge.className='nav_badge visible';
    }else{
      badge.textContent='';
      badge.className='nav_badge';
    }
  }

  // Listen for cross-tab broadcasts on all pages
  var globalBroadcast=null;
  try{globalBroadcast=new BroadcastChannel(TRANSFER_BROADCAST_CH_GLOBAL);}catch(_){}

  var currentAsUser=String(new URLSearchParams(window.location.search||'').get('as')||'').trim();
  var TRANSFER_KIND=Object.freeze({UPLOAD:'upload',COPY:'copy',MOVE:'move'});
  var TRANSFER_STAGE=TRANSFER_STAGE_GLOBAL;
  var MAX_TRANSFER_ITEMS=50;
  var WORKSPACE_QUERY_KEY='ws';
  var WORKSPACE_FOCUS_KEY='fluxon.fs.s3.workspace.focus.v1';
  var WORKSPACE_OPEN_MODE=Object.freeze({APPLY:'apply',NAVIGATE:'navigate'});
  var transferState=normalizeTransferStateForUiPage(loadTransferStateGlobal());
  updateNavBadgeGlobal();
  var bootEl=qs('ui_bootstrap');
  var transfersHost=qs('transfers_host');
  var transferPrescansHost=qs('transfer_prescans_host');
  var transferJobsHost=qs('transfer_jobs_host');
  var transferJobDetailHost=qs('transfer_job_detail_host');
  var transferFailureDetailMeta=qs('transfer_failure_detail_meta');
  var transferFailureDetailMessage=qs('transfer_failure_detail_message');
  var transferFileIssueDetailMeta=qs('transfer_file_issue_detail_meta');
  var transferFileIssueDetailMessage=qs('transfer_file_issue_detail_message');
  var navBucketsLink=qs('nav_buckets_link');
  var navTransfersLink=qs('nav_transfers_link');
  var pageCrumbs=qs('page_crumbs');
  var pageTitleEl=qs('page_title');
  var pageSubtitleEl=qs('page_subtitle');
  var pageRowActionsEl=qs('page_row_actions');
  var workspaceSurface=qs('workspace_surface');
  var workspaceTransfersSurface=qs('workspace_transfers_surface');
  var transferPageMode=Object.freeze({WORKSPACE:'workspace',TRANSFERS:'transfers'});
  var currentPageMode=transferPageMode.WORKSPACE;
  var workspaceChromeState={
    title:'',
    subtitleHtml:'',
    crumbsHtml:'',
    rowActionsHtml:'',
  };
  var transferJobState={
    items:[],
    detailByJobId:{},
    historyByJobId:{},
    selectedJobId:'',
    syncTimer:0,
    syncInFlight:false,
    detailInFlightJobId:'',
    sectionLoadByJobId:{},
    tuneDraftJobId:'',
    tuneScanConcurrencyText:'',
    tuneWorkerCountText:'',
    tuneSubmitInFlight:false,
    cancelSubmitInFlightJobId:'',
    failureModal:{
      jobId:'',
      failureIndex:'',
      loading:false,
      detail:null,
    },
    fileIssueModal:{
      jobId:'',
      batchId:'',
      relpath:'',
      loading:false,
      detail:null,
    },
  };
  var transferPrescanState={
    items:[],
    targetExports:[],
    syncTimer:0,
    syncInFlight:false,
    importItem:null,
  };

  function transferJobsApiPath(){
    return uiRootBase()+'api/transfer_jobs';
  }

  function transferPrescansApiPath(){
    return uiRootBase()+'api/transfer_prescans';
  }

  function transferPrescanImportApiPath(jobId){
    return uiRootBase()+'api/transfer_prescans/'+encodeURIComponent(String(jobId||''))+'/import';
  }

  function transferJobWorkersApiPath(jobId){
    return uiRootBase()+'api/transfer_job/'+encodeURIComponent(String(jobId||''))+'/workers';
  }

  function transferJobCancelApiPath(jobId){
    return uiRootBase()+'api/transfer_job/'+encodeURIComponent(String(jobId||''))+'/cancel';
  }

  function transferJobDetailApiPath(jobId){
    return uiRootBase()+'api/transfer_job/'+encodeURIComponent(String(jobId||''));
  }

  function transferJobHistoryApiPath(jobId){
    return uiRootBase()+'api/transfer_job/'+encodeURIComponent(String(jobId||''))+'/history';
  }

  function transferJobSectionKey(sectionName,jobId){
    return String(sectionName||'')+'::'+String(jobId||'');
  }

  function transferJobSectionState(jobId,sectionName){
    var key=transferJobSectionKey(sectionName,jobId);
    var state=transferJobState.sectionLoadByJobId[key];
    if(!state){
      state={open:false,loading:false,loaded:false,error:'',last_requested_unix_ms:0};
      transferJobState.sectionLoadByJobId[key]=state;
    }
    return state;
  }

  function setTransferJobSectionState(jobId,sectionName,patch){
    var key=transferJobSectionKey(sectionName,jobId);
    var next=Object.assign({},transferJobSectionState(jobId,sectionName),patch||{});
    transferJobState.sectionLoadByJobId[key]=next;
    return next;
  }

  function transferJobFailureDetailApiPath(jobId,failureIndex){
    return uiRootBase()+'api/transfer_job/'+encodeURIComponent(String(jobId||''))+'/failure/'+encodeURIComponent(String(failureIndex||''));
  }

  function transferJobFileIssueDetailApiPath(jobId,batchId,relpath){
    return uiRootBase()+'api/transfer_job/'+encodeURIComponent(String(jobId||''))+'/file_issue?batch_id='+
      encodeURIComponent(String(batchId||''))+'&relpath='+encodeURIComponent(String(relpath||''));
  }

  function normalizeTransferJobHistoryPayload(payload){
    var history=payload&&payload.history?payload.history:{};
    var points=Array.isArray(history.points)?history.points.map(function(item){
      return {
        unix_ms:Math.max(0,Number(item&&item.unix_ms)||0),
        bandwidth_bytes_per_sec:Math.max(0,Number(item&&item.bandwidth_bytes_per_sec)||0),
        running_worker_count:Math.max(0,Number(item&&item.running_worker_count)||0),
        writing_batch_count:Math.max(0,Number(item&&item.writing_batch_count)||0),
        total_written_bytes:Math.max(0,Number(item&&item.total_written_bytes)||0)
      };
    }).filter(function(item){
      return item.unix_ms>0;
    }):[];
    points.sort(function(a,b){return a.unix_ms-b.unix_ms;});
    return {
      start_unix_ms:Math.max(0,Number(history.start_unix_ms)||0),
      end_unix_ms:Math.max(0,Number(history.end_unix_ms)||0),
      points:points,
      loaded_at_unix_ms:Date.now()
    };
  }

  // --- Transfers page (no workspace bootstrap needed) ---
  if(transfersHost&&!bootEl){
    attachStaticSearch();
    renderTransfersPage();
    renderTransferPrescansPage();
    renderTransferJobsPage();
    updateNavBadgeGlobal();

    var clearBtn=qs('transfers_clear_btn');
    if(clearBtn){
      clearBtn.addEventListener('click',clearCompletedTransfers);
    }

    if(globalBroadcast){
      globalBroadcast.onmessage=function(ev){
        if(ev.data&&ev.data.type==='transfer_update'){
          transferState=normalizeTransferStateForUiPage(ev.data.state||{nextId:1,items:[],dismissedTaskIds:[]});
          renderTransfersPage();
          renderTransferJobsPage();
          updateNavBadgeGlobal();
          scheduleServerTransferSync();
        }
      };
    }
    startServerTransferSyncLoop();
    startTransferPrescanSyncLoop();
    startTransferJobSyncLoop();
    return;
  }

  if(!bootEl){
    attachStaticSearch();
    initFsMasterAdminPage();
    if(globalBroadcast){
      globalBroadcast.onmessage=function(ev){
        if(ev.data&&ev.data.type==='transfer_update'){
          transferState=normalizeTransferStateForUiPage(ev.data.state||{nextId:1,items:[],dismissedTaskIds:[]});
          updateNavBadgeGlobal();
          scheduleServerTransferSync();
        }
      };
    }
    startServerTransferSyncLoop();
    return;
  }

  var CLIPBOARD_MODE=Object.freeze({COPY:'copy',CUT:'cut'});
  var DRAG_KIND=Object.freeze({OBJECT:'object',FOLDER:'folder',TAB:'tab'});
  var pendingWorkspaceFocusSpecs=[];
  var state={nextPaneId:2,nextTabId:2,activePaneId:1,panes:[],tabs:[],clipboard:null,availableBuckets:[]};
  state.asUser=currentAsUser;
  if(globalBroadcast){
    globalBroadcast.onmessage=function(ev){
      if(ev.data&&ev.data.type==='transfer_update'){
        transferState=normalizeTransferStateForUiPage(ev.data.state||{nextId:1,items:[],dismissedTaskIds:[]});
        renderTransferToast();
        renderTransfersPage();
        updateNavBadge();
        scheduleServerTransferSync();
      }
    };
  }
  var searchInput=qs('search');
  var workspaceHost=qs('ui_workspace_host');
  var menu=qs('ui_context_menu');
  var notice=qs('workspace_notice');
  var openBucketForm=qs('open_bucket_form');
  var mkdirForm=qs('mkdir_form');
  var uploadForm=qs('upload_form');
  var folderTransferForm=qs('folder_transfer_form');
  var folderTransferSrcExportInput=qs('folder_transfer_src_export');
  var folderTransferSrcRootRelpathInput=qs('folder_transfer_src_root_relpath');
  var folderTransferDstExportInput=qs('folder_transfer_dst_export');
  var folderTransferDstRootRelpathInput=qs('folder_transfer_dst_root_relpath');
  var folderTransferSrcView=qs('folder_transfer_src_view');
  var folderTransferDstView=qs('folder_transfer_dst_view');
  var folderTransferScanConcurrencyInput=qs('folder_transfer_scan_concurrency');
  var folderTransferWorkerCountInput=qs('folder_transfer_worker_count');
  var folderTransferBatchReadyBytesInput=qs('folder_transfer_batch_ready_bytes');
  var transferUnavailableSrcView=qs('transfer_unavailable_src_view');
  var transferUnavailableDstView=qs('transfer_unavailable_dst_view');
  var transferUnavailableReasonView=qs('transfer_unavailable_reason');
  var transferPrescanImportForm=qs('transfer_prescan_import_form');
  var transferPrescanImportJobIdInput=qs('transfer_prescan_import_job_id');
  var transferPrescanImportSrcRootView=qs('transfer_prescan_import_src_root');
  var transferPrescanImportSrcExportSelect=qs('transfer_prescan_import_src_export');
  var transferPrescanImportBatchReadyView=qs('transfer_prescan_import_batch_ready');
  var transferPrescanImportDstExportSelect=qs('transfer_prescan_import_dst_export');
  var transferPrescanImportDstPrefixInput=qs('transfer_prescan_import_dst_prefix');
  var transferPrescanImportScanConcurrencyInput=qs('transfer_prescan_import_scan_concurrency');
  var transferPrescanImportWorkerCountInput=qs('transfer_prescan_import_worker_count');
  var openBucketSelect=qs('open_bucket_select');
  var openBucketPrefixInput=qs('open_bucket_prefix_input');
  var uploadProgressShell=qs('upload_progress_shell');
  var uploadProgressLabel=qs('upload_progress_label');
  var uploadProgressStage=qs('upload_progress_stage');
  var uploadProgressFill=qs('upload_progress_fill');
  var uploadStatus=qs('upload_status');

  function transferRenderHosts(){
    var hosts=[];
    if(transfersHost){hosts.push(transfersHost);}
    return hosts;
  }

  function transfersPageUrl(){
    var transfersUrl=uiRootBase()+'transfers/';
    if(currentAsUser){transfersUrl+='?as='+encodeURIComponent(currentAsUser);}
    return transfersUrl;
  }

  function workspacePageUrl(){
    if(!bootEl){return '';}
    return workspaceUrlForSnapshot(workspaceSnapshot());
  }

  function workspaceCrumbsHtmlForCurrentWorkspace(){
    var tab=activeTab();
    if(!tab||!tab.bucket){return '';}
    var html='<a href="'+escapeHtml(withAs(uiRootBase()))+'">Object Storage</a>';
    html+='<span class="crumb_sep">/</span>';
    html+='<a href="'+escapeHtml(withAs(uiRootBase()))+'"><span class="crumb_cur">Buckets</span></a>';
    return html;
  }

  function transfersCrumbsHtml(){
    var html='<a href="'+escapeHtml(withAs(uiRootBase()))+'">Object Storage</a>';
    html+='<span class="crumb_sep">/</span>';
    html+='<span class="crumb_cur">Transfers</span>';
    return html;
  }

  function setSurfaceVisibility(surface,visible){
    if(!surface){return;}
    surface.className=visible?'workspace_surface_visible':'workspace_surface_hidden';
  }

  function setNavActiveMode(mode){
    var bucketsClass='nav_item';
    var transfersClass='nav_item';
    if(mode===transferPageMode.TRANSFERS){
      transfersClass+=' active';
    }else{
      bucketsClass+=' active';
    }
    if(navBucketsLink){navBucketsLink.className=bucketsClass;}
    if(navTransfersLink){navTransfersLink.className=transfersClass;}
  }

  function setPageChrome(statePatch){
    if(pageTitleEl&&statePatch.title!==undefined){pageTitleEl.textContent=String(statePatch.title||'');}
    if(pageSubtitleEl&&statePatch.subtitleHtml!==undefined){
      pageSubtitleEl.innerHTML=String(statePatch.subtitleHtml||'');
      pageSubtitleEl.style.display=statePatch.subtitleHtml?'':'none';
    }
    if(pageCrumbs&&statePatch.crumbsHtml!==undefined){pageCrumbs.innerHTML=String(statePatch.crumbsHtml||'');}
    if(pageRowActionsEl&&statePatch.rowActionsHtml!==undefined){
      pageRowActionsEl.innerHTML=String(statePatch.rowActionsHtml||'');
      pageRowActionsEl.style.display=statePatch.rowActionsHtml?'':'none';
    }
  }

  function captureWorkspaceChromeState(){
    if(currentPageMode!==transferPageMode.WORKSPACE){return;}
    workspaceChromeState.title=pageTitleEl?pageTitleEl.textContent:'';
    workspaceChromeState.subtitleHtml=pageSubtitleEl?pageSubtitleEl.innerHTML:'';
    workspaceChromeState.crumbsHtml=pageCrumbs?pageCrumbs.innerHTML:'';
    workspaceChromeState.rowActionsHtml=pageRowActionsEl?pageRowActionsEl.innerHTML:'';
  }

  function transfersRowActionsHtml(){
    return '<button id="workspace_transfers_clear_btn" class="btn" type="button">Clear Completed</button>';
  }

  function attachTransfersSurfaceChromeEvents(){
    var clearBtn=qs('workspace_transfers_clear_btn');
    if(clearBtn){
      clearBtn.addEventListener('click',clearCompletedTransfers);
    }
  }

  function attachWorkspaceChromeEvents(){
    var newPageBtn=qs('new_page_btn');
    if(newPageBtn){
      newPageBtn.addEventListener('click',function(){
        var tab=activeTab();
        if(!tab){return;}
        openBucketPrefixInNewTab(tab.bucket,tab.prefix,state.activePaneId).catch(function(err){setNotice(err.message,true);});
      });
    }
    var splitPaneBtn=qs('split_pane_btn');
    if(splitPaneBtn){
      splitPaneBtn.addEventListener('click',function(){
        splitActivePane().catch(function(err){setNotice(err.message,true);});
      });
    }
    var openBucketBtn=qs('open_bucket_btn');
    if(openBucketBtn){
      openBucketBtn.addEventListener('click',function(){updateModalPrefixViews();openModal('open_bucket_modal');});
    }
    var mkdirBtn=qs('mkdir_btn');
    if(mkdirBtn){
      mkdirBtn.addEventListener('click',function(){updateModalPrefixViews();openModal('mkdir_modal');});
    }
    var uploadBtn=qs('upload_btn');
    if(uploadBtn){
      uploadBtn.addEventListener('click',function(){updateModalPrefixViews();openModal('upload_modal');});
    }
  }

  function applyPageMode(mode,historyMode){
    currentPageMode=mode===transferPageMode.TRANSFERS?transferPageMode.TRANSFERS:transferPageMode.WORKSPACE;
    if(bootEl){
      setSurfaceVisibility(workspaceSurface,currentPageMode===transferPageMode.WORKSPACE);
      setSurfaceVisibility(workspaceTransfersSurface,currentPageMode===transferPageMode.TRANSFERS);
      if(currentPageMode===transferPageMode.TRANSFERS){
        renderTransfersPage();
        renderTransferPrescansPage();
        renderTransferJobsPage();
        setPageChrome({
          title:'Transfers',
          subtitleHtml:'',
          crumbsHtml:transfersCrumbsHtml(),
          rowActionsHtml:transfersRowActionsHtml(),
        });
        attachTransfersSurfaceChromeEvents();
      }else{
        setPageChrome({
          title:workspaceChromeState.title,
          subtitleHtml:workspaceChromeState.subtitleHtml,
          crumbsHtml:workspaceChromeState.crumbsHtml||workspaceCrumbsHtmlForCurrentWorkspace(),
          rowActionsHtml:workspaceChromeState.rowActionsHtml,
        });
        attachWorkspaceChromeEvents();
      }
      setNavActiveMode(currentPageMode);
    }
    if(historyMode==='push'){
      if(currentPageMode===transferPageMode.TRANSFERS){
        window.history.pushState({pageMode:transferPageMode.TRANSFERS},'',transfersPageUrl());
      }else{
        var nextWorkspaceUrl=workspacePageUrl();
        if(nextWorkspaceUrl){
          window.history.pushState({pageMode:transferPageMode.WORKSPACE},'',nextWorkspaceUrl);
        }
      }
    }else if(historyMode==='replace'){
      if(currentPageMode===transferPageMode.TRANSFERS){
        window.history.replaceState({pageMode:transferPageMode.TRANSFERS},'',transfersPageUrl());
      }else{
        var replaceWorkspaceUrl=workspacePageUrl();
        if(replaceWorkspaceUrl){
          window.history.replaceState({pageMode:transferPageMode.WORKSPACE},'',replaceWorkspaceUrl);
        }
      }
    }
    renderTransferToast();
  }

  function openTransfersSurface(){
    if(bootEl){
      applyPageMode(transferPageMode.TRANSFERS,'push');
      return;
    }
    window.location.href=transfersPageUrl();
  }

  function closeTransfersSurface(){
    if(!bootEl){return false;}
    applyPageMode(transferPageMode.WORKSPACE,'push');
    return true;
  }

  function emptyTransferState(){
    return {nextId:1,items:[],dismissedTaskIds:[]};
  }

  function isTransferActiveStage(stage){
    return stage===TRANSFER_STAGE.RUNNING||stage===TRANSFER_STAGE.PAUSED;
  }

  function isTransferTerminalStage(stage){
    return stage===TRANSFER_STAGE.DONE||stage===TRANSFER_STAGE.ERROR||stage===TRANSFER_STAGE.CANCELLED;
  }

  function isServerBackedTransferItem(item){
    return !!(item&&item.taskId);
  }

  function hasActiveBrowserOwnedUploads(){
    for(var i=0;i<transferState.items.length;i++){
      var item=transferState.items[i];
      if(
        item&&
        item.kind===TRANSFER_KIND.UPLOAD&&
        !item.taskId&&
        isTransferActiveStage(item.stage)
      ){
        return true;
      }
    }
    return false;
  }

  if(navTransfersLink&&bootEl){
    navTransfersLink.addEventListener('click',function(ev){
      if(
        ev.defaultPrevented||
        ev.button!==0||
        ev.metaKey||
        ev.ctrlKey||
        ev.shiftKey||
        ev.altKey
      ){
        return;
      }
      ev.preventDefault();
      openTransfersSurface();
    });
  }

  window.addEventListener('beforeunload',function(ev){
    if(!hasActiveBrowserOwnedUploads()){return;}
    ev.preventDefault();
    ev.returnValue='';
  });

  function transferTaskStageMap(items){
    var out={};
    for(var i=0;i<items.length;i++){
      var item=items[i];
      if(!isServerBackedTransferItem(item)){continue;}
      out[String(item.taskId)]=String(item.stage||'');
    }
    return out;
  }

  function didAnyServerTransferReachTerminal(prevStageByTaskId){
    for(var i=0;i<transferState.items.length;i++){
      var item=transferState.items[i];
      if(!isServerBackedTransferItem(item)){continue;}
      var prevStage=prevStageByTaskId[String(item.taskId)];
      if(isTransferActiveStage(prevStage)&&isTransferTerminalStage(item.stage)){
        return true;
      }
    }
    return false;
  }

  function sortTransferItems(items){
    items.sort(function(a,b){
      var aStart=Number(a&&a.startedAt)||0;
      var bStart=Number(b&&b.startedAt)||0;
      if(bStart!==aStart){return bStart-aStart;}
      return (Number(b&&b.id)||0)-(Number(a&&a.id)||0);
    });
  }

  function refreshTransferTelemetry(item,now){
    var changed=false;
    item.lastProgressAt=Math.max(0,Number(item.lastProgressAt)||0);
    item.lastProgressBytes=Math.max(0,Number(item.lastProgressBytes)||0);
    item.recentBytesPerSec=Math.max(0,Number(item.recentBytesPerSec)||0);
    if(item.lastProgressBytes>item.doneBytes){
      item.lastProgressBytes=item.doneBytes;
      item.lastProgressAt=now;
      item.recentBytesPerSec=0;
      changed=true;
    }
    if(item.doneBytes>item.lastProgressBytes){
      var baseAt=item.lastProgressAt>0?item.lastProgressAt:(item.startedAt>0?item.startedAt:now);
      var deltaMs=Math.max(1,now-baseAt);
      var deltaBytes=item.doneBytes-item.lastProgressBytes;
      item.recentBytesPerSec=deltaBytes>0?(deltaBytes*1000/deltaMs):0;
      item.lastProgressBytes=item.doneBytes;
      item.lastProgressAt=now;
      changed=true;
    }else if(item.stage===TRANSFER_STAGE.PAUSED){
      if(item.recentBytesPerSec!==0){
        item.recentBytesPerSec=0;
        changed=true;
      }
    }else if(
      item.stage===TRANSFER_STAGE.RUNNING&&
      item.lastProgressAt>0&&
      now-item.lastProgressAt>=3000&&
      item.recentBytesPerSec!==0
    ){
      item.recentBytesPerSec=0;
      changed=true;
    }
    return changed;
  }

  function normalizeTransferStateForUiPage(ts){
    var raw=ts&&Array.isArray(ts.items)?ts:emptyTransferState();
    var changed=false;
    var dismissedTaskIds=[];
    var seenDismissed={};
    var rawDismissed=Array.isArray(raw.dismissedTaskIds)?raw.dismissedTaskIds:[];
    for(var i=0;i<rawDismissed.length;i++){
      var taskId=String(rawDismissed[i]||'').trim();
      if(!taskId||seenDismissed[taskId]){
        changed=true;
        continue;
      }
      seenDismissed[taskId]=true;
      dismissedTaskIds.push(taskId);
    }
    var items=[];
    var maxId=0;
    for(var j=0;j<raw.items.length;j++){
      var item=raw.items[j]||{};
      var stage=String(item.stage||TRANSFER_STAGE.ERROR);
      if(
        stage!==TRANSFER_STAGE.RUNNING&&
        stage!==TRANSFER_STAGE.PAUSED&&
        stage!==TRANSFER_STAGE.DONE&&
        stage!==TRANSFER_STAGE.ERROR&&
        stage!==TRANSFER_STAGE.CANCELLED
      ){
        stage=TRANSFER_STAGE.ERROR;
        changed=true;
      }
      var nextItem={
        id:Math.max(1,Number(item.id)||items.length+1),
        kind:String(item.kind||TRANSFER_KIND.UPLOAD),
        name:String(item.name||''),
        doneBytes:Math.max(0,Number(item.doneBytes)||0),
        totalBytes:Math.max(0,Number(item.totalBytes)||0),
        stage:stage,
        summary:String(item.summary||''),
        detail:String(item.detail||''),
        taskId:item.taskId?String(item.taskId):null,
        sourceBucket:item.sourceBucket?String(item.sourceBucket):null,
        sourceKey:item.sourceKey?String(item.sourceKey):null,
        sourcePrefix:item.sourcePrefix!==undefined&&item.sourcePrefix!==null?String(item.sourcePrefix):null,
        targetBucket:item.targetBucket?String(item.targetBucket):null,
        targetKey:item.targetKey?String(item.targetKey):null,
        targetPrefix:item.targetPrefix!==undefined&&item.targetPrefix!==null?String(item.targetPrefix):null,
        startedAt:Math.max(0,Number(item.startedAt)||Date.now()),
        canPause:!!item.canPause,
        canResume:!!item.canResume,
        canCancel:!!item.canCancel,
        lastProgressAt:Math.max(0,Number(item.lastProgressAt)||0),
        lastProgressBytes:Math.max(0,Number(item.lastProgressBytes)||0),
        recentBytesPerSec:Math.max(0,Number(item.recentBytesPerSec)||0),
      };
      if(nextItem.stage===TRANSFER_STAGE.RUNNING&&!nextItem.taskId){
        nextItem.stage=TRANSFER_STAGE.ERROR;
        nextItem.summary='Interrupted';
        nextItem.detail='Page was closed during browser transfer';
        nextItem.canPause=false;
        nextItem.canResume=false;
        nextItem.canCancel=false;
        changed=true;
      }
      if(nextItem.taskId&&dismissedTaskIds.indexOf(nextItem.taskId)>=0&&isTransferActiveStage(nextItem.stage)){
        dismissedTaskIds=dismissedTaskIds.filter(function(v){return v!==nextItem.taskId;});
        changed=true;
      }
      if(refreshTransferTelemetry(nextItem,Date.now())){
        changed=true;
      }
      if(nextItem.id>maxId){maxId=nextItem.id;}
      items.push(nextItem);
    }
    sortTransferItems(items);
    var out={
      nextId:Math.max(1,Math.max(Number(raw.nextId)||1,maxId+1)),
      items:items,
      dismissedTaskIds:dismissedTaskIds,
    };
    if(
      changed||
      !Array.isArray(raw.dismissedTaskIds)||
      (Number(raw.nextId)||1)!==out.nextId
    ){
      try{window.localStorage.setItem(TRANSFER_STORAGE_KEY_GLOBAL,JSON.stringify(out));}catch(_){}
    }
    return out;
  }

  function clearCompletedTransfers(){
    var nextItems=[];
    var nextDismissed=(transferState.dismissedTaskIds||[]).slice();
    for(var i=0;i<transferState.items.length;i++){
      var item=transferState.items[i];
      if(isTransferActiveStage(item.stage)){
        nextItems.push(item);
        if(item.taskId){
          nextDismissed=nextDismissed.filter(function(v){return v!==item.taskId;});
        }
        continue;
      }
      if(item.taskId&&nextDismissed.indexOf(item.taskId)<0){
        nextDismissed.push(item.taskId);
      }
    }
    transferState.items=nextItems;
    transferState.dismissedTaskIds=nextDismissed;
    persistTransferState();
    renderTransfersPage();
    if(bootEl){renderTransferToast();}
    updateNavBadgeGlobal();
  }

  function persistTransferState(){
    sortTransferItems(transferState.items);
    try{
      window.localStorage.setItem(TRANSFER_STORAGE_KEY_GLOBAL,JSON.stringify(transferState));
    }catch(_){}
    if(globalBroadcast){
      try{globalBroadcast.postMessage({type:'transfer_update',state:transferState});}catch(_){}
    }
  }

  function transferJobStateClass(job){
    var state=String(job&&job.job&&job.job.state||'').toLowerCase();
    if(state==='running'){return 'running';}
    if(state==='done'||state==='finished'||state==='completed'){return 'done';}
    if(state==='cancelled'){return 'cancelled';}
    if(state==='error'||state==='failed'||state==='stopped'){return 'error';}
    return '';
  }

  function transferJobIsRunning(item){
    return String(item&&item.job&&item.job.state||'').toLowerCase()==='running';
  }

  function formatBytesPerSec(value){
    var safe=Math.max(0,Number(value)||0);
    if(!safe){return '0 B/s';}
    return formatBytes(safe)+'/s';
  }

  function formatUnixMs(value){
    var ms=Math.max(0,Number(value)||0);
    if(!ms){return 'n/a';}
    try{
      return new Date(ms).toLocaleString();
    }catch(_){
      return String(ms);
    }
  }

  function transferJobDirectionText(job){
    if(!job||!job.job){return '';}
    return String(job.job.src_export||'')+':'+String(job.job.src_root_relpath||'.')+
      ' -> '+String(job.job.dst_export||'')+':'+String(job.job.dst_root_relpath||'.');
  }

  function normalizeTransferJobItems(items){
    var list=Array.isArray(items)?items.slice():[];
    list.sort(function(a,b){
      var aUpdated=Math.max(0,Number(a&&a.job&&a.job.updated_at_unix_ms)||0);
      var bUpdated=Math.max(0,Number(b&&b.job&&b.job.updated_at_unix_ms)||0);
      if(bUpdated!==aUpdated){return bUpdated-aUpdated;}
      var aJobId=String(a&&a.job&&a.job.job_id||'');
      var bJobId=String(b&&b.job&&b.job.job_id||'');
      return aJobId<bJobId?-1:(aJobId>bJobId?1:0);
    });
    return list;
  }

  function selectedTransferJobSummary(){
    var selected=String(transferJobState.selectedJobId||'');
    if(!selected){return null;}
    for(var i=0;i<transferJobState.items.length;i++){
      var item=transferJobState.items[i];
      if(String(item&&item.job&&item.job.job_id||'')===selected){return item;}
    }
    return null;
  }

  function ensureSelectedTransferJob(){
    if(!transferJobState.items.length){
      transferJobState.selectedJobId='';
      return;
    }
    var selected=String(transferJobState.selectedJobId||'');
    if(!selected){
      transferJobState.selectedJobId=String(transferJobState.items[0].job.job_id||'');
      return;
    }
    for(var i=0;i<transferJobState.items.length;i++){
      if(String(transferJobState.items[i].job.job_id||'')===selected){return;}
    }
    transferJobState.selectedJobId=String(transferJobState.items[0].job.job_id||'');
  }

  function resetTransferJobSectionState(jobId){
    var sections=['history','running_batches','active_workers','worker_attempts','file_issues','intermediate_failures'];
    for(var i=0;i<sections.length;i++){
      transferJobState.sectionLoadByJobId[transferJobSectionKey(sections[i],jobId)]={open:false,loading:false,loaded:false,error:'',last_requested_unix_ms:0};
    }
  }

  function selectedTransferJob(){
    var summary=selectedTransferJobSummary();
    if(!summary||!summary.job){return null;}
    var jobId=String(summary.job.job_id||'');
    var detail=transferJobState.detailByJobId[jobId];
    if(!detail){return summary;}
    return Object.assign({},summary,detail);
  }

  function transferJobItemById(jobId){
    var targetJobId=String(jobId||'');
    if(!targetJobId){return null;}
    for(var i=0;i<transferJobState.items.length;i++){
      var summary=transferJobState.items[i];
      if(String(summary&&summary.job&&summary.job.job_id||'')!==targetJobId){continue;}
      var detail=transferJobState.detailByJobId[targetJobId];
      if(!detail){return summary;}
      return Object.assign({},summary,detail);
    }
    var loaded=transferJobState.detailByJobId[targetJobId];
    if(loaded&&loaded.job){return loaded;}
    return null;
  }

  function transferJobTuneEnsureDraft(item){
    var jobId=String(item&&item.job&&item.job.job_id||'');
    if(!jobId){return;}
    if(String(transferJobState.tuneDraftJobId||'')===jobId){return;}
    transferJobState.tuneDraftJobId=jobId;
    transferJobState.tuneScanConcurrencyText=String(Math.max(1,Number(item.job.desired_scan_concurrency)||DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY));
    transferJobState.tuneWorkerCountText=String(Math.max(0,Number(item.job.desired_worker_count)||0));
  }

  function transferJobCardHtml(item){
    var live=item&&item.live_detail?item.live_detail:null;
    var scan=live&&live.scan?live.scan:null;
    var workers=live&&live.workers?live.workers:null;
    var activeClass=String(item&&item.job&&item.job.job_id||'')===String(transferJobState.selectedJobId||'')?' active':'';
    var cardClass='transfer_job_card'+activeClass;
    var pillClass='transfer_job_pill '+transferJobStateClass(item);
    var writingBatches=workers?Math.max(0,Number(workers.writing_batch_count)||0):0;
    return '<div class="'+cardClass+'" data-transfer-job-id="'+escapeHtml(item.job.job_id)+'">'+
      '<div class="transfer_job_card_head">'+
        '<div class="transfer_job_card_title">'+escapeHtml(transferJobDirectionText(item))+'</div>'+
        '<span class="'+pillClass+'">'+escapeHtml(String(item.job.state||'unknown'))+'</span>'+
      '</div>'+
      '<div class="transfer_job_card_meta">'+
        '<span class="transfer_job_pill">job '+escapeHtml(String(item.job.job_id||''))+'</span>'+
        '<span class="transfer_job_pill">open batches '+escapeHtml(String(Math.max(0,Number(item.open_batches)||0)))+'</span>'+
      '</div>'+
      '<div class="transfer_job_card_line">'+escapeHtml(joinCompact([
        'scan epoch '+String(Math.max(0,Number(item.scan_epoch)||0)),
        item.scan_finished?'scan finished':'scan running',
        'desired scans '+String(Math.max(1,Number(item.job.desired_scan_concurrency)||DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY)),
        'desired workers '+String(Math.max(0,Number(item.job.desired_worker_count)||0)),
        'pending batches '+String(Math.max(0,Number(item.pending_batches)||0)),
        'done batches '+String(Math.max(0,Number(item.done_batches)||0)),
        'failed files '+String(Math.max(0,Number(item.failed_file_count)||0))
      ]))+'</div>'+
      '<div class="transfer_job_stats">'+
        '<div class="transfer_job_stat"><div class="transfer_job_stat_label">Scanned Batches</div><div class="transfer_job_stat_value">'+escapeHtml(String(scan?Math.max(0,Number(scan.discovered_batch_count)||0):0))+'</div></div>'+
        '<div class="transfer_job_stat"><div class="transfer_job_stat_label">Scanned Files</div><div class="transfer_job_stat_value">'+escapeHtml(String(scan?Math.max(0,Number(scan.discovered_file_count)||0):0))+'</div></div>'+
        '<div class="transfer_job_stat"><div class="transfer_job_stat_label">Writing Batches</div><div class="transfer_job_stat_value">'+escapeHtml(String(writingBatches))+'</div></div>'+
        '<div class="transfer_job_stat"><div class="transfer_job_stat_label">Live Bandwidth</div><div class="transfer_job_stat_value">'+escapeHtml(workers?formatBytesPerSec(workers.aggregate_live_bandwidth_bytes_per_sec):'0 B/s')+'</div></div>'+
        '<div class="transfer_job_stat"><div class="transfer_job_stat_label">Total Written</div><div class="transfer_job_stat_value">'+escapeHtml(formatBytes(workers&&workers.aggregate_total_written_bytes))+'</div></div>'+
      '</div>'+
    '</div>';
  }

  function transferJobFailuresHtml(jobId,failures){
    var items=Array.isArray(failures)?failures:[];
    if(!items.length){
      return '<div class="empty_state">No intermediate failures recorded.</div>';
    }
    var html='<div class="transfer_job_failure_list">';
    for(var i=0;i<items.length;i++){
      var item=items[i]||{};
      html+='<div class="transfer_job_failure_item">'+
        '<div class="transfer_job_failure_head">'+
          '<div class="transfer_job_failure_scope">'+escapeHtml(String(item.scope||'unknown'))+'</div>'+
          '<div class="transfer_job_failure_time">'+escapeHtml(formatUnixMs(item.unix_ms))+'</div>'+
        '</div>'+
        '<div class="transfer_job_failure_message">'+escapeHtml(String(item.message||''))+'</div>'+
        '<div class="transfer_job_tuning_actions">'+
          '<button class="btn" type="button" data-transfer-failure-open="1" data-transfer-job-id="'+escapeHtml(String(jobId||''))+'" data-transfer-failure-index="'+escapeHtml(String(item.failure_index||''))+'">View Detail</button>'+
        '</div>'+
      '</div>';
    }
    html+='</div>';
    return html;
  }

  function transferJobFileIssuesHtml(jobId,issues){
    var items=Array.isArray(issues)?issues:[];
    if(!items.length){
      return '<div class="empty_state">No file issues recorded.</div>';
    }
    var html='<div class="transfer_job_failure_list">';
    for(var i=0;i<items.length;i++){
      var item=items[i]||{};
      html+='<div class="transfer_job_failure_item">'+
        '<div class="transfer_job_failure_head">'+
          '<div class="transfer_job_failure_scope">'+escapeHtml(String(item.reason_kind||'unknown'))+'</div>'+
          '<div class="transfer_job_failure_time">'+escapeHtml(formatUnixMs(item.updated_at_unix_ms))+'</div>'+
        '</div>'+
        '<div class="transfer_job_failure_message">'+escapeHtml(joinCompact([
          'batch '+String(item.batch_id||''),
          String(item.relpath||'')
        ]))+'</div>'+
        '<div class="transfer_job_tuning_actions">'+
          '<button class="btn" type="button" data-transfer-file-issue-open="1" data-transfer-job-id="'+escapeHtml(String(jobId||''))+'" data-transfer-batch-id="'+escapeHtml(String(item.batch_id||''))+'" data-transfer-relpath="'+escapeHtml(String(item.relpath||''))+'">View Reason</button>'+
        '</div>'+
      '</div>';
    }
    html+='</div>';
    return html;
  }

  function transferJobWorkersHtml(workers){
    var items=Array.isArray(workers)?workers:[];
    if(!items.length){
      return '<div class="empty_state">No live worker snapshot yet.</div>';
    }
    var html='<div class="transfer_job_worker_list">';
    for(var i=0;i<items.length;i++){
      var item=items[i]||{};
      html+='<div class="transfer_job_worker_item">'+
        '<div class="transfer_job_worker_head">'+
          '<div class="transfer_job_worker_title">'+escapeHtml(String(item.worker_id||''))+' / '+escapeHtml(String(item.batch_id||''))+'</div>'+
          '<div class="transfer_job_worker_meta">'+escapeHtml(joinCompact([
            String(item.state||'unknown'),
            'task '+String(item.worker_task_id||'')
          ]))+'</div>'+
        '</div>'+
        '<div class="transfer_job_worker_lines">'+
          '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
            'launch attempts '+String(Math.max(0,Number(item.launch_attempt_count)||0)),
            'visible files '+String(Math.max(0,Number(item.visible_file_count)||0)),
            'visible bytes '+formatBytes(item.visible_bytes)
          ]))+'</div>'+
          '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
            'writing '+formatBytesPerSec(item.current_bandwidth_bytes_per_sec),
            'written total '+formatBytes(item.total_written_bytes),
            'desired lanes '+String(Math.max(0,Number(item.desired_file_lanes)||0))
          ]))+'</div>'+
          '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
            'last heartbeat '+formatUnixMs(item.last_heartbeat_unix_ms),
            'lease expires '+formatUnixMs(item.lease_expire_unix_ms)
          ]))+'</div>'+
          '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
            item.stop_reason?'stop '+String(item.stop_reason):'',
            item.last_error?('last error '+String(item.last_error)):''
          ]))+'</div>'+
        '</div>'+
      '</div>';
    }
    html+='</div>';
    return html;
  }

  function transferJobWorkerAttemptsHtml(attempts){
    var items=Array.isArray(attempts)?attempts.slice():[];
    if(!items.length){
      return '<div class="empty_state">No worker attempt history yet.</div>';
    }
    items.sort(function(a,b){
      var aUpdated=Math.max(0,Number(a&&a.updated_at_unix_ms)||0);
      var bUpdated=Math.max(0,Number(b&&b.updated_at_unix_ms)||0);
      if(bUpdated!==aUpdated){return bUpdated-aUpdated;}
      var aTask=String(a&&a.worker_task_id||'');
      var bTask=String(b&&b.worker_task_id||'');
      return aTask<bTask?-1:(aTask>bTask?1:0);
    });
    var html='<div class="transfer_job_worker_list">';
    for(var i=0;i<items.length;i++){
      var item=items[i]||{};
      html+='<div class="transfer_job_worker_item">'+
        '<div class="transfer_job_worker_head">'+
          '<div class="transfer_job_worker_title">'+escapeHtml(String(item.worker_id||''))+' / '+escapeHtml(String(item.batch_id||''))+'</div>'+
          '<div class="transfer_job_worker_meta">'+escapeHtml(joinCompact([
            String(item.state||'unknown'),
            'task '+String(item.worker_task_id||''),
            'attempts '+String(Math.max(0,Number(item.launch_attempt_count)||0))
          ]))+'</div>'+
        '</div>'+
        '<div class="transfer_job_worker_lines">'+
          '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
            'updated '+formatUnixMs(item.updated_at_unix_ms),
            'created '+formatUnixMs(item.created_at_unix_ms),
            item.stop_reason?('stop '+String(item.stop_reason)):''
          ]))+'</div>'+
          '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
            'visible files '+String(Math.max(0,Number(item.visible_file_count)||0)),
            'visible bytes '+formatBytes(item.visible_bytes),
            item.dst_exporter_id?('dst exporter '+String(item.dst_exporter_id)):''
          ]))+'</div>'+
          '<div class="transfer_job_detail_item">'+escapeHtml(
            item.last_error?('retry reason '+String(item.last_error)):'no retry / stop error recorded'
          )+'</div>'+
        '</div>'+
      '</div>';
    }
    html+='</div>';
    return html;
  }

  function transferJobHistoryEntry(jobId){
    return transferJobState.historyByJobId[String(jobId||'')]||null;
  }

  function transferJobHistoryNeedsRefresh(jobId){
    var wanted=String(jobId||'');
    if(!wanted){return false;}
    var state=transferJobSectionState(wanted,'history');
    if(state.loading){return false;}
    var lastRequested=Math.max(0,Number(state.last_requested_unix_ms)||0);
    if(lastRequested>0&&Date.now()-lastRequested<TRANSFER_JOB_HISTORY_AUTO_REFRESH_MS){
      return false;
    }
    var summary=transferJobItemById(wanted);
    if(!summary||!summary.job){return false;}
    if(String(summary.job.state||'')!=='running'){
      return !transferJobHistoryEntry(wanted);
    }
    return true;
  }

  async function ensureTransferJobHistoryLoaded(jobId,forceReload){
    var wanted=String(jobId||'');
    if(!wanted){return;}
    var cached=transferJobHistoryEntry(wanted);
    var state=transferJobSectionState(wanted,'history');
    if(state.loading){return;}
    if(!forceReload&&cached){
      setTransferJobSectionState(wanted,'history',{loading:false,loaded:true,error:''});
      renderTransferJobDetail();
      return;
    }
    setTransferJobSectionState(wanted,'history',{
      loading:true,
      loaded:!!cached,
      error:'',
      last_requested_unix_ms:Date.now()
    });
    renderTransferJobDetail();
    try{
      var payload=await apiRequestWithAs(transferJobHistoryApiPath(wanted),currentAsUser);
      transferJobState.historyByJobId[wanted]=normalizeTransferJobHistoryPayload(payload);
      setTransferJobSectionState(wanted,'history',{loading:false,loaded:true,error:''});
      renderTransferJobDetail();
    }catch(err){
      setTransferJobSectionState(wanted,'history',{
        loading:false,
        loaded:!!cached,
        error:uiErrorMessage(err)
      });
      renderTransferJobDetail();
      throw err;
    }
  }

  function refreshSelectedTransferJobHistoryIfNeeded(){
    var item=selectedTransferJob();
    if(!item||!item.job){return;}
    var jobId=String(item.job.job_id||'');
    if(!jobId){return;}
    var state=transferJobSectionState(jobId,'history');
    if(!state.open){return;}
    if(!transferJobHistoryNeedsRefresh(jobId)){return;}
    ensureTransferJobHistoryLoaded(jobId,true).catch(function(){});
  }

  function transferJobHistoryMetricLabel(metricName){
    if(metricName==='bandwidth_bytes_per_sec'){return 'Bandwidth';}
    if(metricName==='running_worker_count'){return 'Running Workers';}
    if(metricName==='writing_batch_count'){return 'Writing Batches';}
    if(metricName==='total_written_bytes'){return 'Total Written';}
    return String(metricName||'');
  }

  function transferJobHistoryMetricFormat(metricName,value){
    var numeric=Math.max(0,Number(value)||0);
    if(metricName==='bandwidth_bytes_per_sec'){return formatBytesPerSec(numeric);}
    if(metricName==='running_worker_count'||metricName==='writing_batch_count'){
      return String(Math.round(numeric));
    }
    if(metricName==='total_written_bytes'){return formatBytes(numeric);}
    return String(numeric);
  }

  function transferJobHistoryLiveTailPoint(item){
    if(!item||!item.job){return null;}
    if(String(item.job.state||'').toLowerCase()!=='running'){return null;}
    var live=item.live_detail||null;
    var workers=live&&live.workers?live.workers:null;
    if(!workers){return null;}
    return {
      unix_ms:Date.now(),
      bandwidth_bytes_per_sec:Math.max(0,Number(workers.aggregate_live_bandwidth_bytes_per_sec)||0),
      running_worker_count:Math.max(0,Number(workers.running_worker_count)||0),
      writing_batch_count:Math.max(0,Number(workers.writing_batch_count)||0),
      total_written_bytes:Math.max(0,Number(workers.aggregate_total_written_bytes)||0)
    };
  }

  function transferJobHistoryForRender(item){
    if(!item||!item.job){return null;}
    var jobId=String(item.job.job_id||'');
    if(!jobId){return null;}
    var history=transferJobHistoryEntry(jobId);
    if(!history){return null;}
    var points=history&&Array.isArray(history.points)?history.points.slice():[];
    var liveTail=transferJobHistoryLiveTailPoint(item);
    if(liveTail){
      if(points.length){
        var lastPoint=points[points.length-1]||{};
        var lastUnixMs=Math.max(0,Number(lastPoint.unix_ms)||0);
        if(liveTail.unix_ms<=lastUnixMs){
          liveTail.unix_ms=lastUnixMs;
          points[points.length-1]=Object.assign({},lastPoint,liveTail);
        }else{
          points.push(liveTail);
        }
      }else{
        points.push(liveTail);
      }
    }
    if(!points.length){return null;}
    return {
      start_unix_ms:Math.max(
        0,
        Number(history&&history.start_unix_ms)||Math.max(0,Number(points[0]&&points[0].unix_ms)||0)
      ),
      end_unix_ms:Math.max(
        0,
        Number(history&&history.end_unix_ms)||0,
        Math.max(0,Number(points[points.length-1]&&points[points.length-1].unix_ms)||0)
      ),
      points:points,
      loaded_at_unix_ms:Math.max(0,Number(history&&history.loaded_at_unix_ms)||0)
    };
  }

  function transferJobHistoryPathData(points,metricName,width,height,pad){
    if(!Array.isArray(points)||!points.length){return null;}
    var minTs=0;
    var maxTs=0;
    var minValue=0;
    var maxValue=0;
    var firstValue=0;
    var lastValue=0;
    var initialized=false;
    for(var i=0;i<points.length;i++){
      var point=points[i]||{};
      var ts=Math.max(0,Number(point.unix_ms)||0);
      var value=Math.max(0,Number(point[metricName])||0);
      if(!initialized){
        minTs=ts;
        maxTs=ts;
        minValue=value;
        maxValue=value;
        firstValue=value;
        initialized=true;
      }else{
        if(ts<minTs){minTs=ts;}
        if(ts>maxTs){maxTs=ts;}
        if(value<minValue){minValue=value;}
        if(value>maxValue){maxValue=value;}
      }
      lastValue=value;
    }
    if(!initialized){return null;}
    var spanTs=Math.max(1,maxTs-minTs);
    var spanValue=Math.max(1e-9,maxValue-minValue);
    var commands=[];
    var scaledPoints=[];
    var lastX=pad;
    var lastY=height-pad;
    for(var j=0;j<points.length;j++){
      var item=points[j]||{};
      var ts=Math.max(0,Number(item.unix_ms)||0);
      var value=Math.max(0,Number(item[metricName])||0);
      var x=pad+((ts-minTs)/spanTs)*(width-pad-pad);
      var y=height-pad-((value-minValue)/spanValue)*(height-pad-pad);
      commands.push((j===0?'M':'L')+x.toFixed(2)+' '+y.toFixed(2));
      scaledPoints.push({
        index:j,
        unix_ms:ts,
        value:value,
        x:x,
        y:y,
      });
      lastX=x;
      lastY=y;
    }
    return {
      path:commands.join(' '),
      min_value:minValue,
      max_value:maxValue,
      first_value:firstValue,
      last_value:lastValue,
      last_x:lastX,
      last_y:lastY,
      scaled_points:scaledPoints
    };
  }

  function transferJobHistoryHoverBandsHtml(pathData,metricName,width,height,pad,chartKey){
    var scaledPoints=pathData&&Array.isArray(pathData.scaled_points)?pathData.scaled_points:[];
    if(!scaledPoints.length){return '';}
    var html='';
    for(var i=0;i<scaledPoints.length;i++){
      var point=scaledPoints[i];
      var left=i===0?pad:(point.x+scaledPoints[i-1].x)/2;
      var right=i===scaledPoints.length-1?(width-pad):(point.x+scaledPoints[i+1].x)/2;
      var bandX=Math.max(0,left);
      var bandWidth=Math.max(1,right-left);
      html+='<rect class="transfer_job_history_hover_band" '+
        'x="'+bandX.toFixed(2)+'" y="0" width="'+bandWidth.toFixed(2)+'" height="'+height+'" '+
        'data-transfer-history-hover="1" '+
        'data-transfer-history-chart-key="'+escapeHtml(chartKey)+'" '+
        'data-transfer-history-metric="'+escapeHtml(metricName)+'" '+
        'data-transfer-history-index="'+escapeHtml(String(point.index))+'" '+
        'data-transfer-history-unix-ms="'+escapeHtml(String(point.unix_ms))+'" '+
        'data-transfer-history-value="'+escapeHtml(String(point.value))+'" '+
        'data-transfer-history-x="'+escapeHtml(point.x.toFixed(2))+'" '+
        'data-transfer-history-y="'+escapeHtml(point.y.toFixed(2))+'"></rect>';
    }
    return html;
  }

  function transferJobHistoryTooltipHtml(chartKey,metricName){
    return '<div class="transfer_job_history_tooltip hidden" data-transfer-history-tooltip="'+escapeHtml(chartKey)+'" data-transfer-history-metric="'+escapeHtml(metricName)+'">'+
      '<div class="transfer_job_history_tooltip_metric"></div>'+
      '<div class="transfer_job_history_tooltip_value"></div>'+
      '<div class="transfer_job_history_tooltip_time"></div>'+
    '</div>';
  }

  function transferJobHistorySeriesHtml(history,metricName,color){
    var width=560;
    var height=72;
    var pad=6;
    var points=history&&Array.isArray(history.points)?history.points:[];
    var pathData=transferJobHistoryPathData(points,metricName,width,height,pad);
    if(!pathData){return '';}
    var chartKey=String(metricName||'')+'__'+String(Math.max(0,Number(history&&history.start_unix_ms)||0))+'__'+String(points.length);
    var startUnixMs=history&&history.start_unix_ms?history.start_unix_ms:(points[0]&&points[0].unix_ms||0);
    var endUnixMs=history&&history.end_unix_ms?history.end_unix_ms:(points[points.length-1]&&points[points.length-1].unix_ms||0);
    var dotHtml=points.length===1
      ? '<circle cx="'+pathData.last_x.toFixed(2)+'" cy="'+pathData.last_y.toFixed(2)+'" r="3.5" fill="'+escapeHtml(color)+'"></circle>'
      : '';
    var focusHtml=
      '<line class="transfer_job_history_focus_line hidden" x1="0" y1="0" x2="0" y2="'+height.toFixed(2)+'" data-transfer-history-focus-line="'+escapeHtml(chartKey)+'"></line>'+
      '<circle class="transfer_job_history_focus_dot hidden" cx="0" cy="0" r="4" fill="'+escapeHtml(color)+'" data-transfer-history-focus-dot="'+escapeHtml(chartKey)+'"></circle>';
    return '<div class="transfer_job_history_chart">'+
      '<div class="transfer_job_history_chart_head">'+
        '<div class="transfer_job_history_chart_title">'+escapeHtml(transferJobHistoryMetricLabel(metricName))+'</div>'+
        '<div class="transfer_job_history_chart_meta">'+escapeHtml(joinCompact([
          'start '+transferJobHistoryMetricFormat(metricName,pathData.first_value),
          'latest '+transferJobHistoryMetricFormat(metricName,pathData.last_value),
          'peak '+transferJobHistoryMetricFormat(metricName,pathData.max_value)
        ]))+'</div>'+
      '</div>'+
      transferJobHistoryTooltipHtml(chartKey,metricName)+
      '<svg class="transfer_job_history_svg" viewBox="0 0 '+width+' '+height+'" preserveAspectRatio="none" data-transfer-history-chart-key="'+escapeHtml(chartKey)+'" data-transfer-history-metric="'+escapeHtml(metricName)+'">'+
        '<line x1="0" y1="'+pad+'" x2="'+width+'" y2="'+pad+'" stroke="#e2e8f0" stroke-width="1"></line>'+
        '<line x1="0" y1="'+(height/2).toFixed(2)+'" x2="'+width+'" y2="'+(height/2).toFixed(2)+'" stroke="#e2e8f0" stroke-width="1"></line>'+
        '<line x1="0" y1="'+(height-pad)+'" x2="'+width+'" y2="'+(height-pad)+'" stroke="#e2e8f0" stroke-width="1"></line>'+
        '<path d="'+escapeHtml(pathData.path)+'" fill="none" stroke="'+escapeHtml(color)+'" stroke-width="2.25" stroke-linecap="round" stroke-linejoin="round"></path>'+
        focusHtml+
        dotHtml+
        transferJobHistoryHoverBandsHtml(pathData,metricName,width,height,pad,chartKey)+
      '</svg>'+
      '<div class="transfer_job_history_axis">'+
        '<span>'+escapeHtml(formatUnixMs(startUnixMs))+'</span>'+
        '<span>'+escapeHtml(formatUnixMs(endUnixMs))+'</span>'+
      '</div>'+
    '</div>';
  }

  function transferJobHistorySectionHtml(item){
    var history=transferJobHistoryForRender(item);
    if(!history||!Array.isArray(history.points)||!history.points.length){
      return '<div class="empty_state">No history samples recorded yet.</div>';
    }
    var startUnixMs=history.start_unix_ms||history.points[0].unix_ms||0;
    var endUnixMs=history.end_unix_ms||history.points[history.points.length-1].unix_ms||0;
    return '<div class="transfer_job_history_stack">'+
      '<div class="transfer_job_detail_item transfer_job_history_overview">'+
        '<span>'+escapeHtml('samples '+String(history.points.length))+'</span>'+
        '<span>'+escapeHtml('range '+formatUnixMs(startUnixMs)+' to '+formatUnixMs(endUnixMs))+'</span>'+
      '</div>'+
      transferJobHistorySeriesHtml(history,'bandwidth_bytes_per_sec','#0284c7')+
      transferJobHistorySeriesHtml(history,'running_worker_count','#ea580c')+
      transferJobHistorySeriesHtml(history,'writing_batch_count','#16a34a')+
      transferJobHistorySeriesHtml(history,'total_written_bytes','#475569')+
    '</div>';
  }

  function transferJobLazySectionHtml(jobId,sectionName,title,meta,bodyHtml){
    var state=transferJobSectionState(jobId,sectionName);
    var chevron=state.open?'▾':'▸';
    var body='';
    if(state.open){
      if(state.loading&&!state.loaded){
        body='<div class="transfer_job_detail_lazy_state empty_state">Loading '+escapeHtml(title.toLowerCase())+'...</div>';
      }else if(state.error&&!state.loaded){
        body='<div class="transfer_job_detail_lazy_state empty_state">'+escapeHtml(state.error)+'</div>';
      }else{
        body=typeof bodyHtml==='function'?String(bodyHtml()||''):String(bodyHtml||'');
      }
    }
    return '<div class="transfer_job_detail_block transfer_job_detail_section" data-transfer-job-section="'+escapeHtml(String(sectionName||''))+'" data-transfer-job-id="'+escapeHtml(String(jobId||''))+'">'+
      '<button class="transfer_job_detail_section_head" type="button" data-transfer-job-section-toggle="1" data-transfer-job-section-name="'+escapeHtml(String(sectionName||''))+'" data-transfer-job-id="'+escapeHtml(String(jobId||''))+'">'+
        '<div class="transfer_job_detail_section_title">'+escapeHtml(title)+'</div>'+
        '<div class="transfer_job_detail_section_meta">'+escapeHtml(meta||'')+'</div>'+
        '<div class="transfer_job_detail_section_chev">'+escapeHtml(chevron)+'</div>'+
      '</button>'+
      (state.open?'<div class="transfer_job_detail_section_body">'+body+'</div>':'')+
    '</div>';
  }

  function transferJobSectionBodyHtml(jobId,sectionName){
    var item=selectedTransferJob();
    if(!item||!item.job||String(item.job.job_id||'')!==String(jobId||'')){return '';}
    var live=item.live_detail||null;
    var scan=live&&live.scan?live.scan:null;
    var workers=live&&live.workers?live.workers:null;
    if(sectionName==='history'){
      return transferJobHistorySectionHtml(item);
    }
    if(sectionName==='running_batches'){
      return (Array.isArray(item.running_batches)&&item.running_batches.length?
        item.running_batches.map(function(batch){
          return '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
            String(batch.batch_id||''),
            String(batch.batch_kind||''),
            String(batch.root_relpath||'.'),
            'worker '+String(batch.owner_worker_id||'')
          ]))+'</div>';
        }).join('')
        :'<div class="empty_state">No running batch snapshot.</div>');
    }
    if(sectionName==='active_workers'){
      return transferJobWorkersHtml(live&&live.active_workers);
    }
    if(sectionName==='active_workers'){
      return transferJobWorkersHtml(live&&live.active_workers);
    }
    if(sectionName==='worker_attempts'){
      return transferJobWorkerAttemptsHtml(item.worker_attempts);
    }
    if(sectionName==='file_issues'){
      return transferJobFileIssuesHtml(item.job.job_id,item.failed_files);
    }
    if(sectionName==='intermediate_failures'){
      return transferJobFailuresHtml(item.job.job_id,live&&live.recent_failures);
    }
    if(sectionName==='summary'){
      return '<div class="transfer_job_detail_list">'+
        '<div class="transfer_job_detail_item"><strong>Source</strong> <span class="mono">'+escapeHtml(String(item.job.src_export||'')+':'+String(item.job.src_root_relpath||'.'))+'</span></div>'+
        '<div class="transfer_job_detail_item"><strong>Target</strong> <span class="mono">'+escapeHtml(String(item.job.dst_export||'')+':'+String(item.job.dst_root_relpath||'.'))+'</span></div>'+
        '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
          'desired scans '+String(Math.max(1,Number(item.job.desired_scan_concurrency)||DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY)),
          'desired workers '+String(Math.max(0,Number(item.job.desired_worker_count)||0))
        ]))+'</div>'+
        '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
          'open batches '+String(Math.max(0,Number(item.open_batches)||0)),
          'pending batches '+String(Math.max(0,Number(item.pending_batches)||0)),
          'done batches '+String(Math.max(0,Number(item.done_batches)||0)),
          'failed files '+String(Math.max(0,Number(item.failed_file_count)||0))
        ]))+'</div>'+
      '</div>';
    }
    if(sectionName==='scan'){
      return '<div class="transfer_job_detail_list">'+
        '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
          'queued '+String(scan?Math.max(0,Number(scan.queued_scan_unit_count)||0):0),
          'inflight '+String(scan?Math.max(0,Number(scan.inflight_scan_unit_count)||0):0),
          'completed '+String(scan?Math.max(0,Number(scan.completed_scan_unit_count)||0):0)
        ]))+'</div>'+
        '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
          'batches '+String(scan?Math.max(0,Number(scan.discovered_batch_count)||0):0),
          'files '+String(scan?Math.max(0,Number(scan.discovered_file_count)||0):0),
          'bytes '+formatBytes(scan&&scan.discovered_bytes)
        ]))+'</div>'+
        '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
          'scan rate '+(scan?String(Math.max(0,Number(scan.scan_rate_files_per_sec)||0)):'0')+' files/s',
          scan?formatBytesPerSec(scan.scan_rate_bytes_per_sec):'0 B/s',
          'last result '+formatUnixMs(scan&&scan.last_scan_result_unix_ms)
        ]))+'</div>'+
      '</div>';
    }
    if(sectionName==='workers'){
      return '<div class="transfer_job_detail_list">'+
        '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
          'launching '+String(workers?Math.max(0,Number(workers.launching_worker_count)||0):0),
          'running '+String(workers?Math.max(0,Number(workers.running_worker_count)||0):0),
          'writing '+String(workers?Math.max(0,Number(workers.writing_batch_count)||0):0)
        ]))+'</div>'+
        '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
          'stopped '+String(workers?Math.max(0,Number(workers.stopped_worker_count)||0):0),
          'finished '+String(workers?Math.max(0,Number(workers.finished_worker_count)||0):0),
          'live bandwidth '+(workers?formatBytesPerSec(workers.aggregate_live_bandwidth_bytes_per_sec):'0 B/s')
        ]))+'</div>'+
        '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
          'visible files '+String(workers?Math.max(0,Number(workers.aggregate_visible_file_count)||0):0),
          'visible bytes '+formatBytes(workers&&workers.aggregate_visible_bytes),
          'written total '+formatBytes(workers&&workers.aggregate_total_written_bytes)
        ]))+'</div>'+
      '</div>';
    }
    return '';
  }

  function transferJobSectionMeta(jobId,sectionName,item){
    var state=transferJobSectionState(jobId,sectionName);
    var loadedItem=transferJobState.detailByJobId[String(jobId||'')]||null;
    var source=loadedItem||item||{};
    if(state.loading&&!state.loaded){return 'loading...';}
    if(sectionName==='history'){
      var history=transferJobHistoryForRender(source);
      if(history){
        var suffix='';
        if(state.loading){suffix=' refreshing...';}
        else if(state.error){suffix=' refresh failed';}
        return 'loaded '+String(Math.max(0,Array.isArray(history.points)?history.points.length:0))+' points'+suffix;
      }
      return state.error&&!state.loaded ? 'retry to load' : 'click to load';
    }
    if(sectionName==='running_batches'){
      return loadedItem?'loaded '+String(Math.max(0,Array.isArray(source.running_batches)?source.running_batches.length:0))+' items':'click to load';
    }
    if(sectionName==='active_workers'){
      var live=source.live_detail||null;
      return loadedItem?'loaded '+String(Math.max(0,Array.isArray(live&&live.active_workers)?live.active_workers.length:0))+' items':'click to load';
    }
    if(sectionName==='worker_attempts'){
      return loadedItem?'loaded '+String(Math.max(0,Array.isArray(source.worker_attempts)?source.worker_attempts.length:0))+' items':'click to load';
    }
    if(sectionName==='file_issues'){
      return String(Math.max(0,Number(source.failed_file_count)||0))+' recorded';
    }
    if(sectionName==='intermediate_failures'){
      var live=source.live_detail||null;
      return loadedItem?
        'loaded '+String(Math.max(0,Array.isArray(live&&live.recent_failures)?live.recent_failures.length:0))+' items':
        String(Math.max(0,Number(live&&live.recent_failure_count)||0))+' recorded';
    }
    return 'click to expand';
  }

  function transferJobDetailHtml(item){
    if(!item||!item.job){
      return '<div class="empty_state">Select a transfer job to inspect live detail.</div>';
    }
    transferJobTuneEnsureDraft(item);
    var live=item.live_detail||null;
    var jobId=String(item.job.job_id||'');
    var jobRunning=transferJobIsRunning(item);
    var cancelInFlight=String(transferJobState.cancelSubmitInFlightJobId||'')===jobId;
    var tuningDisabled=(transferJobState.tuneSubmitInFlight||cancelInFlight||!jobRunning)?' disabled':'';
    var tuneScanText=String(transferJobState.tuneScanConcurrencyText||'');
    var tuneWorkerText=String(transferJobState.tuneWorkerCountText||'');
    return '<div class="transfer_job_detail_card">'+
      '<div class="transfer_job_detail_head">'+
        '<div>'+
          '<div class="transfer_job_detail_title">'+escapeHtml(transferJobDirectionText(item))+'</div>'+
          '<div class="transfer_job_detail_subtitle">'+escapeHtml(joinCompact([
            'job '+String(item.job.job_id||''),
            'state '+String(item.job.state||'unknown'),
            'updated '+formatUnixMs(item.job.updated_at_unix_ms)
          ]))+'</div>'+
        '</div>'+
        '<div class="transfer_job_card_meta">'+
          '<span class="transfer_job_pill '+transferJobStateClass(item)+'">'+escapeHtml(String(item.job.state||'unknown'))+'</span>'+
          '<span class="transfer_job_pill">batch ready '+escapeHtml(formatBytes(item.job.batch_ready_bytes))+'</span>'+
        '</div>'+
      '</div>'+
      '<div class="transfer_job_detail_grid">'+
        '<div class="transfer_job_detail_block">'+
          '<div class="transfer_job_detail_block_title">Transfer Detail</div>'+
          transferJobSectionBodyHtml(jobId,'summary')+
          '<div class="transfer_job_tuning_actions">'+
            '<button class="btn" type="button" data-transfer-job-open-workspace="'+escapeHtml(jobId)+'"'+(transferJobCanOpenInWorkspace(item)?'':' disabled')+'>Open Source/Target Panes</button>'+
            (jobRunning?'<button class="btn danger" type="button" data-transfer-job-cancel="'+escapeHtml(jobId)+'"'+(cancelInFlight?' disabled':'')+'>'+(cancelInFlight?'Stopping...':'Stop Transfer')+'</button>':'')+
          '</div>'+
        '</div>'+
        '<div class="transfer_job_detail_block">'+
          '<div class="transfer_job_detail_block_title">Tuning</div>'+
          '<form class="transfer_job_tuning_form" data-transfer-job-tune-form="'+escapeHtml(String(item.job.job_id||''))+'">'+
            '<div class="field">'+
              '<label>Scan Concurrency Target</label>'+
              '<input type="number" min="1" step="1" name="desired_scan_concurrency" value="'+escapeHtml(tuneScanText)+'"'+tuningDisabled+' />'+
            '</div>'+
            '<div class="field">'+
              '<label>Write Worker Target</label>'+
              '<input type="number" min="0" step="1" name="desired_worker_count" value="'+escapeHtml(tuneWorkerText)+'"'+tuningDisabled+' />'+
            '</div>'+
            '<div class="hint">'+escapeHtml(jobRunning?'The scheduler will keep scan dispatch and write worker launch close to these targets.':'Targets can only be changed while the job is running.')+'</div>'+
            '<div class="transfer_job_tuning_actions">'+
              '<button class="btn primary" type="submit"'+tuningDisabled+'>'+(transferJobState.tuneSubmitInFlight?'Applying...':'Apply Targets')+'</button>'+
            '</div>'+
          '</form>'+
        '</div>'+
        '<div class="transfer_job_detail_block">'+
          '<div class="transfer_job_detail_block_title">Scan</div>'+
          transferJobSectionBodyHtml(jobId,'scan')+
        '</div>'+
        '<div class="transfer_job_detail_block">'+
          '<div class="transfer_job_detail_block_title">Workers</div>'+
          transferJobSectionBodyHtml(jobId,'workers')+
      '</div>'+
      '</div>'+
      '<div class="transfer_job_detail_stack">'+
        transferJobLazySectionHtml(jobId,'history','History',transferJobSectionMeta(jobId,'history',item),function(){return transferJobSectionBodyHtml(jobId,'history');})+
        transferJobLazySectionHtml(jobId,'running_batches','Running Batches',transferJobSectionMeta(jobId,'running_batches',item),function(){return transferJobSectionBodyHtml(jobId,'running_batches');})+
        transferJobLazySectionHtml(jobId,'active_workers','Active Workers',transferJobSectionMeta(jobId,'active_workers',item),function(){return transferJobSectionBodyHtml(jobId,'active_workers');})+
        transferJobLazySectionHtml(jobId,'worker_attempts','Worker Attempt History',transferJobSectionMeta(jobId,'worker_attempts',item),function(){return transferJobSectionBodyHtml(jobId,'worker_attempts');})+
        transferJobLazySectionHtml(jobId,'file_issues','File Issues',transferJobSectionMeta(jobId,'file_issues',item),function(){return transferJobSectionBodyHtml(jobId,'file_issues');})+
        transferJobLazySectionHtml(jobId,'intermediate_failures','Intermediate Failures',transferJobSectionMeta(jobId,'intermediate_failures',item),function(){return transferJobSectionBodyHtml(jobId,'intermediate_failures');})+
      '</div>'+
    '</div>';
  }

  function renderTransferJobList(){
    if(!transferJobsHost){return;}
    if(!transferJobState.items.length){
      transferJobsHost.innerHTML='<div class="empty_state">No FluxonFS transfer jobs yet.</div>';
      return;
    }
    var html='';
    for(var i=0;i<transferJobState.items.length;i++){
      html+=transferJobCardHtml(transferJobState.items[i]);
    }
    transferJobsHost.innerHTML=html;
  }

  function renderTransferJobDetail(){
    if(!transferJobDetailHost){return;}
    transferJobDetailHost.innerHTML=transferJobDetailHtml(selectedTransferJob());
  }

  function hideTransferJobHistoryTooltipForChart(chart){
    if(!chart){return;}
    var tooltip=chart.querySelector('[data-transfer-history-tooltip]');
    if(tooltip){
      tooltip.classList.remove('visible');
      tooltip.classList.add('hidden');
    }
    var focusLine=chart.querySelector('[data-transfer-history-focus-line]');
    if(focusLine){
      focusLine.classList.add('hidden');
    }
    var focusDot=chart.querySelector('[data-transfer-history-focus-dot]');
    if(focusDot){
      focusDot.classList.add('hidden');
    }
  }

  function hideAllTransferJobHistoryTooltips(){
    if(!transferJobDetailHost){return;}
    var charts=transferJobDetailHost.querySelectorAll('.transfer_job_history_chart');
    for(var i=0;i<charts.length;i++){
      hideTransferJobHistoryTooltipForChart(charts[i]);
    }
  }

  function updateTransferJobHistoryTooltip(target,clientX){
    if(!transferJobDetailHost||!target||!target.getAttribute){return;}
    var chart=target.closest&&target.closest('.transfer_job_history_chart');
    if(!chart||!transferJobDetailHost.contains(chart)){return;}
    var metricName=String(target.getAttribute('data-transfer-history-metric')||'');
    if(!metricName){return;}
    var unixMs=Math.max(0,Number(target.getAttribute('data-transfer-history-unix-ms'))||0);
    var value=Math.max(0,Number(target.getAttribute('data-transfer-history-value'))||0);
    var anchorX=Math.max(0,Number(target.getAttribute('data-transfer-history-x'))||0);
    var anchorY=Math.max(0,Number(target.getAttribute('data-transfer-history-y'))||0);
    var svg=chart.querySelector('.transfer_job_history_svg');
    if(!svg){return;}
    hideAllTransferJobHistoryTooltips();
    var tooltip=chart.querySelector('[data-transfer-history-tooltip]');
    var focusLine=chart.querySelector('[data-transfer-history-focus-line]');
    var focusDot=chart.querySelector('[data-transfer-history-focus-dot]');
    if(!tooltip||!focusLine||!focusDot){return;}
    var chartRect=chart.getBoundingClientRect();
    var svgRect=svg.getBoundingClientRect();
    var svgWidth=Math.max(1,svgRect.width);
    var svgHeight=Math.max(1,svgRect.height);
    var viewBox=String(svg.getAttribute('viewBox')||'0 0 560 72').trim().split(/\s+/);
    var viewWidth=Math.max(1,Number(viewBox[2])||560);
    var viewHeight=Math.max(1,Number(viewBox[3])||72);
    var localX=(anchorX/viewWidth)*svgWidth;
    var localY=(anchorY/viewHeight)*svgHeight;
    var tooltipWidth=Math.max(132,tooltip.offsetWidth||132);
    var desiredLeft=12+localX;
    if(clientX){
      desiredLeft=clientX-chartRect.left+14;
    }
    var leftPx=Math.max(8,Math.min(chartRect.width-tooltipWidth-8,desiredLeft));
    var topPx=Math.max(42,Math.min(chartRect.height-56,8+localY-34));
    tooltip.style.left=leftPx.toFixed(2)+'px';
    tooltip.style.top=topPx.toFixed(2)+'px';
    var metricEl=tooltip.querySelector('.transfer_job_history_tooltip_metric');
    var valueEl=tooltip.querySelector('.transfer_job_history_tooltip_value');
    var timeEl=tooltip.querySelector('.transfer_job_history_tooltip_time');
    if(metricEl){metricEl.textContent=transferJobHistoryMetricLabel(metricName);}
    if(valueEl){valueEl.textContent=transferJobHistoryMetricFormat(metricName,value);}
    if(timeEl){timeEl.textContent=formatUnixMs(unixMs);}
    tooltip.classList.remove('hidden');
    tooltip.classList.add('visible');
    focusLine.setAttribute('x1',anchorX.toFixed(2));
    focusLine.setAttribute('x2',anchorX.toFixed(2));
    focusLine.classList.remove('hidden');
    focusDot.setAttribute('cx',anchorX.toFixed(2));
    focusDot.setAttribute('cy',anchorY.toFixed(2));
    focusDot.classList.remove('hidden');
  }

  function renderTransferFailureDetailModal(){
    if(!transferFailureDetailMeta||!transferFailureDetailMessage){return;}
    var modalState=transferJobState.failureModal||{};
    if(modalState.loading){
      transferFailureDetailMeta.textContent='Loading failure detail...';
      transferFailureDetailMessage.textContent='';
      return;
    }
    var detail=modalState.detail;
    if(!detail){
      transferFailureDetailMeta.textContent='No failure selected.';
      transferFailureDetailMessage.textContent='';
      return;
    }
    transferFailureDetailMeta.textContent=joinCompact([
      'job '+String(modalState.jobId||''),
      'failure '+String(detail.failure_index||''),
      String(detail.scope||'unknown'),
      formatUnixMs(detail.unix_ms)
    ]);
    transferFailureDetailMessage.textContent=String(detail.message||'');
  }

  function renderTransferFileIssueDetailModal(){
    if(!transferFileIssueDetailMeta||!transferFileIssueDetailMessage){return;}
    var modalState=transferJobState.fileIssueModal||{};
    if(modalState.loading){
      transferFileIssueDetailMeta.textContent='Loading file issue detail...';
      transferFileIssueDetailMessage.textContent='';
      return;
    }
    var detail=modalState.detail;
    if(!detail){
      transferFileIssueDetailMeta.textContent='No file issue selected.';
      transferFileIssueDetailMessage.textContent='';
      return;
    }
    transferFileIssueDetailMeta.textContent=joinCompact([
      'job '+String(detail.job_id||modalState.jobId||''),
      'batch '+String(detail.batch_id||modalState.batchId||''),
      String(detail.reason_kind||'unknown'),
      String(detail.relpath||modalState.relpath||''),
      formatUnixMs(detail.updated_at_unix_ms)
    ]);
    transferFileIssueDetailMessage.textContent=String(detail.reason_detail||'');
  }

  function renderTransferJobsPage(){
    if(!transferJobsHost&&!transferJobDetailHost){return;}
    ensureSelectedTransferJob();
    renderTransferJobList();
    renderTransferJobDetail();
  }

  function markTransferJobSectionsLoaded(jobId){
    var sections=['running_batches','active_workers','worker_attempts','file_issues','intermediate_failures'];
    for(var i=0;i<sections.length;i++){
      var state=transferJobSectionState(jobId,sections[i]);
      state.loading=false;
      state.loaded=true;
      state.error='';
      transferJobState.sectionLoadByJobId[transferJobSectionKey(sections[i],jobId)]=state;
    }
  }

  async function submitTransferJobTuning(jobId,desiredScanConcurrency,desiredWorkerCount){
    var body=new URLSearchParams();
    body.set('desired_scan_concurrency',String(desiredScanConcurrency));
    body.set('desired_worker_count',String(desiredWorkerCount));
    transferJobState.tuneSubmitInFlight=true;
    renderTransferJobDetail();
    try{
      await apiRequest(transferJobWorkersApiPath(jobId),{
        method:'POST',
        headers:{'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8'},
        body:body.toString(),
      });
      transferJobState.tuneDraftJobId='';
      setNotice(
        'Updated transfer targets for job '+String(jobId)+': scan '+String(desiredScanConcurrency)+', workers '+String(desiredWorkerCount)+'.',
        false
      );
      await syncTransferJobsOnce();
      await ensureTransferJobDetailLoaded(jobId,true);
    }finally{
      transferJobState.tuneSubmitInFlight=false;
      renderTransferJobDetail();
    }
  }

  async function submitTransferJobCancel(jobId){
    var wanted=String(jobId||'');
    if(!wanted){return;}
    transferJobState.cancelSubmitInFlightJobId=wanted;
    renderTransferJobDetail();
    try{
      await apiRequest(transferJobCancelApiPath(wanted),{
        method:'POST',
      });
      transferJobState.tuneDraftJobId='';
      setNotice('Cancelled transfer job '+wanted+'.',false);
      await syncTransferJobsOnce();
      await ensureTransferJobDetailLoaded(wanted,true);
    }finally{
      if(String(transferJobState.cancelSubmitInFlightJobId||'')===wanted){
        transferJobState.cancelSubmitInFlightJobId='';
      }
      renderTransferJobDetail();
    }
  }

  async function ensureTransferJobDetailLoaded(jobId,forceReload){
    var wanted=String(jobId||'');
    if(!wanted){return;}
    if(!forceReload&&transferJobState.detailByJobId[wanted]){return;}
    transferJobState.detailInFlightJobId=wanted;
    renderTransferJobDetail();
    try{
      var payload=await apiRequestWithAs(transferJobDetailApiPath(wanted),currentAsUser);
      if(payload&&payload.item){
        transferJobState.detailByJobId[wanted]=payload.item;
        markTransferJobSectionsLoaded(wanted);
      }
    }finally{
      if(String(transferJobState.detailInFlightJobId||'')===wanted){
        transferJobState.detailInFlightJobId='';
      }
      renderTransferJobDetail();
    }
  }

  async function openTransferFailureDetail(jobId,failureIndex){
    transferJobState.failureModal={
      jobId:String(jobId||''),
      failureIndex:String(failureIndex||''),
      loading:true,
      detail:null,
    };
    renderTransferFailureDetailModal();
    openModal('transfer_failure_detail_modal');
    try{
      var payload=await apiRequestWithAs(
        transferJobFailureDetailApiPath(jobId,failureIndex),
        currentAsUser
      );
      transferJobState.failureModal.loading=false;
      transferJobState.failureModal.detail=payload&&payload.failure?payload.failure:null;
      renderTransferFailureDetailModal();
    }catch(err){
      transferJobState.failureModal.loading=false;
      transferJobState.failureModal.detail={
        failure_index:String(failureIndex||''),
        unix_ms:0,
        scope:'error',
        message:uiErrorMessage(err),
      };
      renderTransferFailureDetailModal();
      throw err;
    }
  }

  async function openTransferFileIssueDetail(jobId,batchId,relpath){
    transferJobState.fileIssueModal={
      jobId:String(jobId||''),
      batchId:String(batchId||''),
      relpath:String(relpath||''),
      loading:true,
      detail:null,
    };
    renderTransferFileIssueDetailModal();
    openModal('transfer_file_issue_detail_modal');
    try{
      var payload=await apiRequestWithAs(
        transferJobFileIssueDetailApiPath(jobId,batchId,relpath),
        currentAsUser
      );
      transferJobState.fileIssueModal.loading=false;
      transferJobState.fileIssueModal.detail=payload&&payload.file_issue?payload.file_issue:null;
      renderTransferFileIssueDetailModal();
    }catch(err){
      transferJobState.fileIssueModal.loading=false;
      transferJobState.fileIssueModal.detail={
        job_id:String(jobId||''),
        batch_id:String(batchId||''),
        relpath:String(relpath||''),
        reason_kind:'error',
        reason_detail:uiErrorMessage(err),
        updated_at_unix_ms:Date.now()
      };
      renderTransferFileIssueDetailModal();
      throw err;
    }
  }

  function attachTransferJobEvents(){
    if(!transferJobsHost||transferJobsHost.__transferJobEventsBound){return;}
    transferJobsHost.__transferJobEventsBound=true;
    transferJobsHost.addEventListener('click',function(ev){
      var card=ev.target&&ev.target.closest&&ev.target.closest('[data-transfer-job-id]');
      if(!card||!transferJobsHost.contains(card)){return;}
      var nextSelected=String(card.getAttribute('data-transfer-job-id')||'');
      if(nextSelected!==String(transferJobState.selectedJobId||'')){
        resetTransferJobSectionState(nextSelected);
      }
      transferJobState.selectedJobId=nextSelected;
      renderTransferJobsPage();
    });
    if(transferJobDetailHost&&!transferJobDetailHost.__transferJobDetailEventsBound){
      transferJobDetailHost.__transferJobDetailEventsBound=true;
      transferJobDetailHost.addEventListener('click',function(ev){
        var sectionBtn=ev.target&&ev.target.closest&&ev.target.closest('[data-transfer-job-section-toggle][data-transfer-job-id][data-transfer-job-section-name]');
        var openWorkspaceBtn=ev.target&&ev.target.closest&&ev.target.closest('[data-transfer-job-open-workspace]');
        if(openWorkspaceBtn&&transferJobDetailHost.contains(openWorkspaceBtn)){
          var openJobId=String(openWorkspaceBtn.getAttribute('data-transfer-job-open-workspace')||'');
          var openItem=transferJobItemById(openJobId);
          if(openItem){
            openTransferJobInWorkspace(openItem);
          }
          return;
        }
        if(sectionBtn&&transferJobDetailHost.contains(sectionBtn)){
          var sectionJobId=String(sectionBtn.getAttribute('data-transfer-job-id')||'');
          var sectionName=String(sectionBtn.getAttribute('data-transfer-job-section-name')||'');
          if(!sectionJobId||!sectionName){return;}
          var state=transferJobSectionState(sectionJobId,sectionName);
          state.open=!state.open;
          state.error='';
          if(!state.open){
            renderTransferJobDetail();
            return;
          }
          if(sectionName==='history'){
            renderTransferJobDetail();
            ensureTransferJobHistoryLoaded(
              sectionJobId,
              transferJobHistoryNeedsRefresh(sectionJobId)
            ).catch(function(err){
              var failedState=transferJobSectionState(sectionJobId,sectionName);
              renderTransferJobDetail();
              if(failedState.open&&!failedState.loaded){
                setNotice(err.message,true);
              }
            });
            return;
          }
          if(!transferJobState.detailByJobId[sectionJobId]){
            state.loading=true;
            state.loaded=false;
            renderTransferJobDetail();
            ensureTransferJobDetailLoaded(sectionJobId,false).catch(function(err){
              var failedState=setTransferJobSectionState(sectionJobId,sectionName,{
                loading:false,
                loaded:false,
                error:uiErrorMessage(err),
              });
              renderTransferJobDetail();
              if(failedState.open){
                setNotice(err.message,true);
              }
            });
          }else{
            state.loaded=true;
            renderTransferJobDetail();
          }
          return;
        }
        var failureBtn=ev.target&&ev.target.closest&&ev.target.closest('[data-transfer-failure-open][data-transfer-job-id][data-transfer-failure-index]');
        if(failureBtn&&transferJobDetailHost.contains(failureBtn)){
          var jobId=String(failureBtn.getAttribute('data-transfer-job-id')||'');
          var failureIndex=String(failureBtn.getAttribute('data-transfer-failure-index')||'');
          openTransferFailureDetail(jobId,failureIndex).catch(function(err){
            setNotice(err.message,true);
          });
          return;
        }
        var fileIssueBtn=ev.target&&ev.target.closest&&ev.target.closest('[data-transfer-file-issue-open][data-transfer-job-id][data-transfer-batch-id][data-transfer-relpath]');
        if(fileIssueBtn&&transferJobDetailHost.contains(fileIssueBtn)){
          var issueJobId=String(fileIssueBtn.getAttribute('data-transfer-job-id')||'');
          var batchId=String(fileIssueBtn.getAttribute('data-transfer-batch-id')||'');
          var relpath=String(fileIssueBtn.getAttribute('data-transfer-relpath')||'');
          openTransferFileIssueDetail(issueJobId,batchId,relpath).catch(function(err){
            setNotice(err.message,true);
          });
          return;
        }
        var cancelBtn=ev.target&&ev.target.closest&&ev.target.closest('[data-transfer-job-cancel]');
        if(cancelBtn&&transferJobDetailHost.contains(cancelBtn)){
          var cancelJobId=String(cancelBtn.getAttribute('data-transfer-job-cancel')||'');
          if(transferJobState.cancelSubmitInFlightJobId){return;}
          submitTransferJobCancel(cancelJobId).catch(function(err){
            if(String(transferJobState.cancelSubmitInFlightJobId||'')===cancelJobId){
              transferJobState.cancelSubmitInFlightJobId='';
            }
            renderTransferJobDetail();
            setNotice(err.message,true);
          });
          return;
        }
      });
      transferJobDetailHost.addEventListener('input',function(ev){
        var target=ev.target;
        if(!target||!target.form||!target.form.hasAttribute('data-transfer-job-tune-form')){return;}
        var jobId=String(target.form.getAttribute('data-transfer-job-tune-form')||'');
        if(!jobId){return;}
        transferJobState.tuneDraftJobId=jobId;
        if(String(target.name||'')==='desired_scan_concurrency'){
          transferJobState.tuneScanConcurrencyText=String(target.value||'');
        }else if(String(target.name||'')==='desired_worker_count'){
          transferJobState.tuneWorkerCountText=String(target.value||'');
        }
      });
      transferJobDetailHost.addEventListener('submit',function(ev){
        var form=ev.target;
        if(!form||!form.hasAttribute||!form.hasAttribute('data-transfer-job-tune-form')){return;}
        ev.preventDefault();
        if(transferJobState.tuneSubmitInFlight||transferJobState.cancelSubmitInFlightJobId){return;}
        var jobId=String(form.getAttribute('data-transfer-job-tune-form')||'');
        var desiredScanConcurrency=String(transferJobState.tuneScanConcurrencyText||'').trim();
        var desiredWorkerCount=String(transferJobState.tuneWorkerCountText||'').trim();
        if(!desiredScanConcurrency){setNotice('scan concurrency target is required',true);return;}
        if(!desiredWorkerCount){setNotice('write worker target is required',true);return;}
        submitTransferJobTuning(jobId,desiredScanConcurrency,desiredWorkerCount).catch(function(err){
          transferJobState.tuneSubmitInFlight=false;
          renderTransferJobDetail();
          setNotice(err.message,true);
        });
      });
      transferJobDetailHost.addEventListener('mousemove',function(ev){
        var hoverTarget=ev.target&&ev.target.closest&&ev.target.closest('[data-transfer-history-hover]');
        if(!hoverTarget||!transferJobDetailHost.contains(hoverTarget)){
          hideAllTransferJobHistoryTooltips();
          return;
        }
        updateTransferJobHistoryTooltip(hoverTarget,ev.clientX);
      });
      transferJobDetailHost.addEventListener('mouseleave',function(){
        hideAllTransferJobHistoryTooltips();
      });
    }
  }

  async function syncTransferJobsOnce(){
    if(transferJobState.syncInFlight){return;}
    if(!transferJobsHost&&!transferJobDetailHost){return;}
    transferJobState.syncInFlight=true;
    try{
      var payload=await apiRequestWithAs(transferJobsApiPath(),currentAsUser);
      transferJobState.items=normalizeTransferJobItems(payload&&payload.items);
      var nextDetailByJobId={};
      var nextHistoryByJobId={};
      for(var i=0;i<transferJobState.items.length;i++){
        var summary=transferJobState.items[i];
        var jobId=String(summary&&summary.job&&summary.job.job_id||'');
        if(jobId&&transferJobState.detailByJobId[jobId]){
          nextDetailByJobId[jobId]=transferJobState.detailByJobId[jobId];
        }
        if(jobId&&transferJobState.historyByJobId[jobId]){
          nextHistoryByJobId[jobId]=transferJobState.historyByJobId[jobId];
        }
      }
      transferJobState.detailByJobId=nextDetailByJobId;
      transferJobState.historyByJobId=nextHistoryByJobId;
      ensureSelectedTransferJob();
      renderTransferJobsPage();
      refreshSelectedTransferJobHistoryIfNeeded();
    }catch(err){
      if(transferJobsHost&&!transferJobState.items.length){
        transferJobsHost.innerHTML='<div class="empty_state">'+escapeHtml(uiErrorMessage(err))+'</div>';
      }
      if(transferJobDetailHost&&!selectedTransferJob()){
        transferJobDetailHost.innerHTML='<div class="empty_state">'+escapeHtml(uiErrorMessage(err))+'</div>';
      }
    }finally{
      transferJobState.syncInFlight=false;
      scheduleTransferJobSync();
    }
  }

  function scheduleTransferJobSync(){
    if(transferJobState.syncTimer){
      window.clearTimeout(transferJobState.syncTimer);
      transferJobState.syncTimer=0;
    }
    if(!transferJobsHost&&!transferJobDetailHost){return;}
    transferJobState.syncTimer=window.setTimeout(function(){
      syncTransferJobsOnce().catch(function(){scheduleTransferJobSync();});
    },1000);
  }

  function startTransferJobSyncLoop(){
    attachTransferJobEvents();
    syncTransferJobsOnce().catch(function(){scheduleTransferJobSync();});
  }

  function transferPrescanCardHtml(item){
    var scan=item&&item.scan?item.scan:{};
    var candidates=Array.isArray(item&&item.source_export_candidates)?item.source_export_candidates:[];
    var disabledReason='';
    if(!candidates.length){
      disabledReason='No readable source export matches this scanned root.';
    }else if(!transferPrescanState.targetExports.length){
      disabledReason='No writable target export is available for this account.';
    }
    var actionHtml=disabledReason
      ? '<span class="transfer_prescan_hint">'+escapeHtml(disabledReason)+'</span>'
      : '<button class="btn primary" type="button" data-transfer-prescan-import="'+escapeHtml(String(item.job_id||''))+'">Import</button>';
    return '<div class="transfer_prescan_card">'+
      '<div class="transfer_prescan_head">'+
        '<div>'+
          '<div class="transfer_job_card_title">'+escapeHtml(String(item.src_root_dir_abs||''))+'</div>'+
          '<div class="transfer_job_card_line">'+escapeHtml(joinCompact([
            'job '+String(item.job_id||''),
            'state '+String(item.job_state||'unknown'),
            item.scan_finished?'scan finished':'scan running'
          ]))+'</div>'+
        '</div>'+
        '<div class="transfer_job_card_meta">'+
          '<span class="transfer_job_pill">'+escapeHtml('batch ready '+formatBytes(item.batch_ready_bytes))+'</span>'+
          '<span class="transfer_job_pill">'+escapeHtml('open batches '+String(Math.max(0,Number(item.open_batches)||0)))+'</span>'+
        '</div>'+
      '</div>'+
      '<div class="transfer_job_stats">'+
        '<div class="transfer_job_stat"><div class="transfer_job_stat_label">Scanned Batches</div><div class="transfer_job_stat_value">'+escapeHtml(String(Math.max(0,Number(scan.discovered_batch_count)||0)))+'</div></div>'+
        '<div class="transfer_job_stat"><div class="transfer_job_stat_label">Scanned Files</div><div class="transfer_job_stat_value">'+escapeHtml(String(Math.max(0,Number(scan.discovered_file_count)||0)))+'</div></div>'+
        '<div class="transfer_job_stat"><div class="transfer_job_stat_label">Scanned Bytes</div><div class="transfer_job_stat_value">'+escapeHtml(formatBytes(scan.discovered_bytes))+'</div></div>'+
        '<div class="transfer_job_stat"><div class="transfer_job_stat_label">Source Exports</div><div class="transfer_job_stat_value">'+escapeHtml(String(candidates.length))+'</div></div>'+
      '</div>'+
      '<div class="transfer_prescan_lines">'+
        '<div class="transfer_job_detail_item">'+escapeHtml(joinCompact([
          'scan epoch '+String(Math.max(0,Number(item.scan_epoch)||0)),
          'skip entries '+String(Math.max(0,Number(item.skip_entries_count)||0)),
          'updated '+formatUnixMs(item.updated_at_unix_ms)
        ]))+'</div>'+
        '<div class="transfer_job_detail_item">'+escapeHtml(candidates.length
          ? ('source exports '+candidates.map(function(candidate){
              return String(candidate.export_name||'')+':'+rootRelpathLabel(candidate.src_root_relpath);
            }).join(', '))
          : 'source exports unavailable')+'</div>'+
      '</div>'+
      '<div class="transfer_prescan_actions">'+actionHtml+'</div>'+
    '</div>';
  }

  function renderTransferPrescansPage(){
    if(!transferPrescansHost){return;}
    if(!transferPrescanState.items.length){
      transferPrescansHost.innerHTML='<div class="empty_state">No pre-scans yet.</div>';
      return;
    }
    var html='<div class="transfer_prescan_list">';
    for(var i=0;i<transferPrescanState.items.length;i++){
      html+=transferPrescanCardHtml(transferPrescanState.items[i]);
    }
    html+='</div>';
    transferPrescansHost.innerHTML=html;
  }

  function populateTransferPrescanImportModal(item){
    if(!transferPrescanImportForm||!item){return;}
    transferPrescanState.importItem=item;
    if(transferPrescanImportJobIdInput){transferPrescanImportJobIdInput.value=String(item.job_id||'');}
    if(transferPrescanImportSrcRootView){transferPrescanImportSrcRootView.textContent=String(item.src_root_dir_abs||'/');}
    if(transferPrescanImportBatchReadyView){transferPrescanImportBatchReadyView.textContent=formatBytes(item.batch_ready_bytes);}
    if(transferPrescanImportDstPrefixInput){transferPrescanImportDstPrefixInput.value='';}
    if(transferPrescanImportScanConcurrencyInput){
      transferPrescanImportScanConcurrencyInput.value=String(DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY);
    }
    if(transferPrescanImportWorkerCountInput){transferPrescanImportWorkerCountInput.value='';}
    if(transferPrescanImportSrcExportSelect){
      var candidates=Array.isArray(item.source_export_candidates)?item.source_export_candidates:[];
      var srcHtml='';
      if(candidates.length!==1){
        srcHtml+='<option value="">Select source export</option>';
      }
      for(var i=0;i<candidates.length;i++){
        var candidate=candidates[i]||{};
        var label=String(candidate.export_name||'')+':'+rootRelpathLabel(candidate.src_root_relpath);
        srcHtml+='<option value="'+escapeHtml(String(candidate.export_name||''))+'">'+escapeHtml(label)+'</option>';
      }
      transferPrescanImportSrcExportSelect.innerHTML=srcHtml;
      if(candidates.length===1){
        transferPrescanImportSrcExportSelect.value=String(candidates[0].export_name||'');
      }
    }
    if(transferPrescanImportDstExportSelect){
      var dstHtml='<option value="">Select target export</option>';
      for(var j=0;j<transferPrescanState.targetExports.length;j++){
        dstHtml+='<option value="'+escapeHtml(String(transferPrescanState.targetExports[j]||''))+'">'+escapeHtml(String(transferPrescanState.targetExports[j]||''))+'</option>';
      }
      transferPrescanImportDstExportSelect.innerHTML=dstHtml;
    }
  }

  async function submitTransferPrescanImport(ev){
    ev.preventDefault();
    if(!transferPrescanImportForm){throw new Error('pre-scan import form missing');}
    var item=transferPrescanState.importItem;
    if(!item||!item.job_id){throw new Error('no pre-scan selected');}
    var formData=new FormData(transferPrescanImportForm);
    var srcExport=String(formData.get('src_export')||'').trim();
    var dstExport=String(formData.get('dst_export')||'').trim();
    var dstPrefix=String(formData.get('dst_prefix')||'').trim();
    var desiredScanConcurrency=String(formData.get('desired_scan_concurrency')||'').trim();
    var desiredWorkerCount=String(formData.get('desired_worker_count')||'').trim();
    if(!srcExport){throw new Error('select a source export');}
    if(!dstExport){throw new Error('select a target export');}
    if(dstPrefix.indexOf('/')===0){throw new Error('target prefix must not start with "/"');}
    if(dstPrefix&&dstPrefix.charAt(dstPrefix.length-1)!=='/'){
      throw new Error('target prefix must end with "/" when non-empty');
    }
    if(desiredScanConcurrency===''){throw new Error('scan concurrency target is required');}
    if(desiredWorkerCount===''){throw new Error('desired worker count is required');}
    var body=new URLSearchParams();
    body.set('src_export',srcExport);
    body.set('dst_export',dstExport);
    body.set('dst_prefix',dstPrefix);
    body.set('desired_scan_concurrency',desiredScanConcurrency);
    body.set('desired_worker_count',desiredWorkerCount);
    var resp=await apiRequest(transferPrescanImportApiPath(item.job_id),{
      method:'POST',
      headers:{'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8'},
      body:body.toString(),
    });
    closeModal('transfer_prescan_import_modal');
    transferPrescanState.importItem=null;
    transferJobState.selectedJobId=resp&&resp.job&&resp.job.job_id?String(resp.job.job_id):'';
    setNotice('Imported pre-scan into FluxonFS transfer job '+String(item.job_id)+'.',false);
    syncTransferPrescansOnce().catch(function(){scheduleTransferPrescanSync();});
    syncTransferJobsOnce().catch(function(){scheduleTransferJobSync();});
  }

  function attachTransferPrescanEvents(){
    if(transferPrescansHost&&!transferPrescansHost.__transferPrescanEventsBound){
      transferPrescansHost.__transferPrescanEventsBound=true;
      transferPrescansHost.addEventListener('click',function(ev){
        var btn=ev.target&&ev.target.closest&&ev.target.closest('[data-transfer-prescan-import]');
        if(!btn||!transferPrescansHost.contains(btn)){return;}
        var jobId=String(btn.getAttribute('data-transfer-prescan-import')||'');
        var item=null;
        for(var i=0;i<transferPrescanState.items.length;i++){
          if(String(transferPrescanState.items[i]&&transferPrescanState.items[i].job_id||'')===jobId){
            item=transferPrescanState.items[i];
            break;
          }
        }
        if(!item){return;}
        populateTransferPrescanImportModal(item);
        openModal('transfer_prescan_import_modal');
      });
    }
  }

  async function syncTransferPrescansOnce(){
    if(transferPrescanState.syncInFlight){return;}
    if(!transferPrescansHost){return;}
    transferPrescanState.syncInFlight=true;
    try{
      var payload=await apiRequestWithAs(transferPrescansApiPath(),currentAsUser);
      transferPrescanState.items=Array.isArray(payload&&payload.items)?payload.items.slice().sort(function(a,b){
        var aUpdated=Math.max(0,Number(a&&a.updated_at_unix_ms)||0);
        var bUpdated=Math.max(0,Number(b&&b.updated_at_unix_ms)||0);
        if(bUpdated!==aUpdated){return bUpdated-aUpdated;}
        var aJobId=String(a&&a.job_id||'');
        var bJobId=String(b&&b.job_id||'');
        return aJobId<bJobId?-1:(aJobId>bJobId?1:0);
      }):[];
      transferPrescanState.targetExports=Array.isArray(payload&&payload.target_exports)?payload.target_exports:[];
      renderTransferPrescansPage();
    }catch(err){
      if(transferPrescansHost&&!transferPrescanState.items.length){
        transferPrescansHost.innerHTML='<div class="empty_state">'+escapeHtml(uiErrorMessage(err))+'</div>';
      }
    }finally{
      transferPrescanState.syncInFlight=false;
      scheduleTransferPrescanSync();
    }
  }

  function scheduleTransferPrescanSync(){
    if(transferPrescanState.syncTimer){
      window.clearTimeout(transferPrescanState.syncTimer);
      transferPrescanState.syncTimer=0;
    }
    if(!transferPrescansHost){return;}
    transferPrescanState.syncTimer=window.setTimeout(function(){
      syncTransferPrescansOnce().catch(function(){scheduleTransferPrescanSync();});
    },1000);
  }

  function startTransferPrescanSyncLoop(){
    attachTransferPrescanEvents();
    syncTransferPrescansOnce().catch(function(){scheduleTransferPrescanSync();});
  }

  function findPaneIndex(paneId){
    for(var i=0;i<state.panes.length;i++){
      if(state.panes[i].id===paneId){return i;}
    }
    return -1;
  }

  function paneById(paneId){
    var idx=findPaneIndex(paneId);
    return idx>=0?state.panes[idx]:null;
  }

  function findTabIndex(tabId){
    for(var i=0;i<state.tabs.length;i++){
      if(state.tabs[i].id===tabId){return i;}
    }
    return -1;
  }

  function tabById(tabId){
    var idx=findTabIndex(tabId);
    return idx>=0?state.tabs[idx]:null;
  }

  function paneForTab(tabId){
    for(var i=0;i<state.panes.length;i++){
      if(state.panes[i].tabIds.indexOf(tabId)>=0){return state.panes[i];}
    }
    return null;
  }

  function normalizeWorkspace(){
    if(!state.tabs.length){
      state.panes=[];
      state.activePaneId=0;
      return;
    }

    var seenTabs={};
    var nextPanes=[];
    for(var i=0;i<state.panes.length;i++){
      var pane=state.panes[i];
      var tabIds=[];
      for(var j=0;j<pane.tabIds.length;j++){
        var tabId=pane.tabIds[j];
        if(seenTabs[tabId]){continue;}
        if(tabById(tabId)){
          seenTabs[tabId]=true;
          tabIds.push(tabId);
        }
      }
      if(!tabIds.length){continue;}
      nextPanes.push({
        id:pane.id,
        tabIds:tabIds,
        activeTabId:tabIds.indexOf(pane.activeTabId)>=0?pane.activeTabId:tabIds[0],
      });
    }

    var orphanIds=[];
    for(var k=0;k<state.tabs.length;k++){
      if(!seenTabs[state.tabs[k].id]){
        orphanIds.push(state.tabs[k].id);
      }
    }
    if(orphanIds.length){
      if(nextPanes.length){
        nextPanes[0].tabIds=nextPanes[0].tabIds.concat(orphanIds);
      }else{
        nextPanes.push({id:1,tabIds:orphanIds.slice(),activeTabId:orphanIds[0]});
        if(state.nextPaneId<=1){state.nextPaneId=2;}
      }
    }

    state.panes=nextPanes;
    if(findPaneIndex(state.activePaneId)<0){
      state.activePaneId=state.panes[0].id;
    }
    if(state.nextPaneId<=0){state.nextPaneId=1;}
    if(state.nextTabId<=0){state.nextTabId=1;}
  }

  function workspaceSnapshot(){
    normalizeWorkspace();
    var tabs=[];
    for(var i=0;i<state.tabs.length;i++){
      tabs.push({
        id:state.tabs[i].id,
        bucket:state.tabs[i].bucket,
        prefix:state.tabs[i].prefix,
      });
    }
    var panes=[];
    for(var j=0;j<state.panes.length;j++){
      panes.push({
        id:state.panes[j].id,
        tabIds:state.panes[j].tabIds.slice(),
        activeTabId:state.panes[j].activeTabId,
      });
    }
    return {
      version:1,
      activePaneId:state.activePaneId,
      nextPaneId:state.nextPaneId,
      nextTabId:state.nextTabId,
      tabs:tabs,
      panes:panes,
    };
  }

  function cloneWorkspaceSnapshot(snapshot){
    var tabs=[];
    var panes=[];
    var srcTabs=Array.isArray(snapshot&&snapshot.tabs)?snapshot.tabs:[];
    var srcPanes=Array.isArray(snapshot&&snapshot.panes)?snapshot.panes:[];
    for(var i=0;i<srcTabs.length;i++){
      tabs.push({
        id:Number(srcTabs[i]&&srcTabs[i].id)||tabs.length+1,
        bucket:String(srcTabs[i]&&srcTabs[i].bucket||''),
        prefix:String(srcTabs[i]&&srcTabs[i].prefix||''),
      });
    }
    for(var j=0;j<srcPanes.length;j++){
      panes.push({
        id:Number(srcPanes[j]&&srcPanes[j].id)||j+1,
        tabIds:Array.isArray(srcPanes[j]&&srcPanes[j].tabIds)?srcPanes[j].tabIds.map(function(v){return Number(v);}):[],
        activeTabId:Number(srcPanes[j]&&srcPanes[j].activeTabId)||0,
      });
    }
    return {
      version:1,
      activePaneId:Number(snapshot&&snapshot.activePaneId)||0,
      nextPaneId:Number(snapshot&&snapshot.nextPaneId)||0,
      nextTabId:Number(snapshot&&snapshot.nextTabId)||0,
      tabs:tabs,
      panes:panes,
    };
  }

  function parseWorkspaceSnapshotValue(raw){
    var text=String(raw||'').trim();
    if(!text){return null;}
    try{
      var parsed=JSON.parse(text);
      if(!parsed||parsed.version!==1||!Array.isArray(parsed.tabs)||!Array.isArray(parsed.panes)){
        return null;
      }
      return cloneWorkspaceSnapshot(parsed);
    }catch(err){
      console.warn('workspace query parse failed',err);
      return null;
    }
  }

  function workspaceSnapshotFromLocation(){
    var params=new URLSearchParams(window.location.search||'');
    // Recover broken links emitted by older clients that wrote the snapshot under "undefined".
    var legacySnapshot=parseWorkspaceSnapshotValue(params.get('undefined'));
    if(legacySnapshot){return legacySnapshot;}
    return parseWorkspaceSnapshotValue(params.get(WORKSPACE_QUERY_KEY));
  }

  function activeTabSnapshotFromWorkspaceSnapshot(snapshot){
    if(!snapshot||!Array.isArray(snapshot.tabs)||!Array.isArray(snapshot.panes)||!snapshot.tabs.length){return null;}
    var paneId=Number(snapshot.activePaneId)||0;
    var activePane=null;
    for(var i=0;i<snapshot.panes.length;i++){
      if(Number(snapshot.panes[i].id)===paneId){
        activePane=snapshot.panes[i];
        break;
      }
    }
    if(!activePane){activePane=snapshot.panes[0]||null;}
    if(!activePane||!Array.isArray(activePane.tabIds)||!activePane.tabIds.length){return snapshot.tabs[0]||null;}
    var activeTabId=Number(activePane.activeTabId)||Number(activePane.tabIds[0])||0;
    for(var j=0;j<snapshot.tabs.length;j++){
      if(Number(snapshot.tabs[j].id)===activeTabId){
        return snapshot.tabs[j];
      }
    }
    return snapshot.tabs[0]||null;
  }

  function uiBucketPagePath(bucket){
    return uiRootBase()+encodeBucketName(bucket)+'/';
  }

  function workspaceQueryParamsForSnapshot(snapshot){
    var params=new URLSearchParams();
    if(currentAsUser){params.set('as',currentAsUser);}
    params.set(WORKSPACE_QUERY_KEY,JSON.stringify(snapshot));
    var activeTabSnapshot=activeTabSnapshotFromWorkspaceSnapshot(snapshot);
    if(!activeTabSnapshot||!activeTabSnapshot.bucket){return params;}
    var activePrefix=String(activeTabSnapshot.prefix||'');
    if(activePrefix){
      params.set('prefix',activePrefix);
    }
    return params;
  }

  function workspaceUrlForSnapshot(snapshot){
    var activeTabSnapshot=activeTabSnapshotFromWorkspaceSnapshot(snapshot);
    if(!activeTabSnapshot||!activeTabSnapshot.bucket){return '';}
    var params=workspaceQueryParamsForSnapshot(snapshot);
    var query=params.toString();
    return uiBucketPagePath(activeTabSnapshot.bucket)+(query?'?'+query:'');
  }

  function persistWorkspaceLocation(){
    if(!bootEl||!state.tabs.length){return;}
    if(currentPageMode===transferPageMode.TRANSFERS){return;}
    var nextUrl=workspaceUrlForSnapshot(workspaceSnapshot());
    if(!nextUrl){return;}
    var currentUrl=window.location.pathname+(window.location.search||'');
    if(nextUrl!==currentUrl){
      window.history.replaceState(null,'',nextUrl);
    }
  }

  function stashPendingWorkspaceFocusSpecs(specs){
    try{
      window.sessionStorage.setItem(WORKSPACE_FOCUS_KEY,JSON.stringify(Array.isArray(specs)?specs:[]));
    }catch(err){
      console.warn('workspace focus persist failed',err);
    }
  }

  function consumePendingWorkspaceFocusSpecs(){
    var raw='';
    try{
      raw=String(window.sessionStorage.getItem(WORKSPACE_FOCUS_KEY)||'');
      window.sessionStorage.removeItem(WORKSPACE_FOCUS_KEY);
    }catch(err){
      console.warn('workspace focus restore failed',err);
      return [];
    }
    if(!raw){return [];}
    try{
      var parsed=JSON.parse(raw);
      return Array.isArray(parsed)?parsed:[];
    }catch(err){
      console.warn('workspace focus parse failed',err);
      return [];
    }
  }

  async function restoreWorkspaceStateFromSnapshot(parsed,options){
    if(!parsed||parsed.version!==1||!Array.isArray(parsed.tabs)||!Array.isArray(parsed.panes)){
      return false;
    }

    var reuseLoadedTabs=!!(options&&options.reuseLoadedTabs);
    var requireAllTabs=!!(options&&options.requireAllTabs);
    var restoredTabs=[];
    var hadTabError=false;
    for(var i=0;i<parsed.tabs.length;i++){
      var snap=parsed.tabs[i];
      if(!snap||!snap.bucket||snap.prefix===undefined||snap.prefix===null){continue;}
      try{
        var tabId=Number(snap.id)||restoredTabs.length+1;
        var existingTab=null;
        if(reuseLoadedTabs){
          existingTab=tabById(tabId);
          if(
            existingTab&&
            existingTab.bucket===String(snap.bucket)&&
            existingTab.prefix===String(snap.prefix)
          ){
            restoredTabs.push(Object.assign({},existingTab));
            continue;
          }
        }
        var payload=await loadListing(String(snap.bucket),String(snap.prefix));
        restoredTabs.push(Object.assign({id:tabId},payload));
      }catch(err){
        hadTabError=true;
        console.warn('workspace restore tab failed',snap,err);
      }
    }
    if(hadTabError&&requireAllTabs){return false;}
    if(!restoredTabs.length){return false;}

    state.tabs=restoredTabs;
    state.panes=[];
    for(var j=0;j<parsed.panes.length;j++){
      var paneSnap=parsed.panes[j];
      if(!paneSnap||!Array.isArray(paneSnap.tabIds)){continue;}
      state.panes.push({
        id:Number(paneSnap.id)||j+1,
        tabIds:paneSnap.tabIds.map(function(v){return Number(v);}),
        activeTabId:Number(paneSnap.activeTabId)||0,
      });
    }
    state.activePaneId=Number(parsed.activePaneId)||1;

    var maxPaneId=0;
    for(var k=0;k<state.panes.length;k++){
      if(state.panes[k].id>maxPaneId){maxPaneId=state.panes[k].id;}
    }
    var maxTabId=0;
    for(var n=0;n<state.tabs.length;n++){
      if(state.tabs[n].id>maxTabId){maxTabId=state.tabs[n].id;}
    }
    state.nextPaneId=Math.max(Number(parsed.nextPaneId)||0,maxPaneId+1,1);
    state.nextTabId=Math.max(Number(parsed.nextTabId)||0,maxTabId+1,1);
    normalizeWorkspace();
    return true;
  }

  async function restoreWorkspaceStateFromLocation(){
    var snapshot=workspaceSnapshotFromLocation();
    if(!snapshot){return false;}
    return restoreWorkspaceStateFromSnapshot(snapshot);
  }

  function workspacePaneSnapshotById(snapshot,paneId){
    var wanted=Number(paneId)||0;
    for(var i=0;i<snapshot.panes.length;i++){
      if(Number(snapshot.panes[i].id)===wanted){
        return snapshot.panes[i];
      }
    }
    return null;
  }

  function workspaceTabSnapshotById(snapshot,tabId){
    var wanted=Number(tabId)||0;
    for(var i=0;i<snapshot.tabs.length;i++){
      if(Number(snapshot.tabs[i].id)===wanted){
        return snapshot.tabs[i];
      }
    }
    return null;
  }

  function activeWorkspaceTabSnapshotForPane(snapshot,paneId){
    var pane=workspacePaneSnapshotById(snapshot,paneId);
    if(!pane||!pane.tabIds.length){return null;}
    var tabId=Number(pane.activeTabId)||Number(pane.tabIds[0])||0;
    return workspaceTabSnapshotById(snapshot,tabId);
  }

  function nextWorkspacePaneSnapshotId(snapshot){
    var nextId=Math.max(1,Number(snapshot.nextPaneId)||0);
    snapshot.nextPaneId=nextId+1;
    return nextId;
  }

  function nextWorkspaceTabSnapshotId(snapshot){
    var nextId=Math.max(1,Number(snapshot.nextTabId)||0);
    snapshot.nextTabId=nextId+1;
    return nextId;
  }

  function createWorkspacePaneSnapshotAfter(snapshot,afterPaneId){
    var pane={id:nextWorkspacePaneSnapshotId(snapshot),tabIds:[],activeTabId:0};
    var insertAt=-1;
    var afterId=Number(afterPaneId)||0;
    for(var i=0;i<snapshot.panes.length;i++){
      if(Number(snapshot.panes[i].id)===afterId){
        insertAt=i;
        break;
      }
    }
    if(insertAt<0){
      snapshot.panes.push(pane);
    }else{
      snapshot.panes.splice(insertAt+1,0,pane);
    }
    return pane;
  }

  function ensureWorkspacePaneSnapshot(snapshot,paneId){
    var pane=workspacePaneSnapshotById(snapshot,paneId);
    if(pane){return pane;}
    if(!snapshot.panes.length){
      pane={id:nextWorkspacePaneSnapshotId(snapshot),tabIds:[],activeTabId:0};
      snapshot.panes.push(pane);
      return pane;
    }
    return createWorkspacePaneSnapshotAfter(snapshot,snapshot.panes[snapshot.panes.length-1].id);
  }

  async function openWorkspace(spec){
    if(!spec||!spec.snapshot){return;}
    var snapshot=cloneWorkspaceSnapshot(spec.snapshot);
    var focusSpecs=Array.isArray(spec.focusSpecs)?spec.focusSpecs.slice():[];
    if((spec.mode||'')===WORKSPACE_OPEN_MODE.NAVIGATE||!bootEl){
      if(focusSpecs.length){stashPendingWorkspaceFocusSpecs(focusSpecs);}
      window.location.href=workspaceUrlForSnapshot(snapshot);
      return;
    }
    pendingWorkspaceFocusSpecs=focusSpecs;
    var restored=await restoreWorkspaceStateFromSnapshot(snapshot,{reuseLoadedTabs:true,requireAllTabs:true});
    if(!restored){
      pendingWorkspaceFocusSpecs=[];
      throw new Error('open workspace failed');
    }
    render();
  }

  function findWorkspaceEntryElement(spec){
    if(!spec){return null;}
    var scope=spec.paneId?document.querySelector('.pane_shell[data-pane-id="'+Number(spec.paneId)+'"]'):document;
    if(!scope){return null;}
    if(spec.key!==undefined&&spec.key!==null){
      var fileRows=scope.querySelectorAll('[data-file-key]');
      for(var i=0;i<fileRows.length;i++){
        if(
          String(fileRows[i].getAttribute('data-file-bucket')||'')===String(spec.bucket||'')&&
          String(fileRows[i].getAttribute('data-file-key')||'')===String(spec.key||'')
        ){
          return fileRows[i];
        }
      }
      return null;
    }
    var folderRows=scope.querySelectorAll('[data-folder-prefix]');
    for(var j=0;j<folderRows.length;j++){
      if(
        String(folderRows[j].getAttribute('data-folder-bucket')||'')===String(spec.bucket||'')&&
        String(folderRows[j].getAttribute('data-folder-prefix')||'')===String(spec.prefix||'')
      ){
        return folderRows[j];
      }
    }
    return null;
  }

  function applyPendingWorkspaceFocusSpecs(){
    if(!pendingWorkspaceFocusSpecs.length){return;}
    var specs=pendingWorkspaceFocusSpecs.slice();
    pendingWorkspaceFocusSpecs=[];
    for(var i=0;i<specs.length;i++){
      var el=findWorkspaceEntryElement(specs[i]);
      if(!el){continue;}
      el.classList.add('focus_target');
      if(specs[i].scroll){
        el.scrollIntoView({block:'center',inline:'nearest',behavior:'smooth'});
      }
      (function(node){
        window.setTimeout(function(){node.classList.remove('focus_target');},1800);
      }(el));
    }
  }

  function activePane(){
    normalizeWorkspace();
    return paneById(state.activePaneId)||state.panes[0]||null;
  }

  function activeTabForPane(paneId){
    var pane=paneById(paneId);
    if(!pane||!pane.tabIds.length){return null;}
    if(pane.tabIds.indexOf(pane.activeTabId)<0){
      pane.activeTabId=pane.tabIds[0];
    }
    return tabById(pane.activeTabId);
  }

  function activeTab(){
    var pane=activePane();
    return pane?activeTabForPane(pane.id):null;
  }

  function setActivePaneTab(paneId,tabId){
    var pane=paneById(paneId);
    if(!pane){return;}
    state.activePaneId=pane.id;
    if(tabId&&pane.tabIds.indexOf(tabId)>=0){
      pane.activeTabId=tabId;
    }
  }

  function insertTabId(list,tabId,beforeTabId){
    var next=[];
    for(var i=0;i<list.length;i++){
      if(list[i]!==tabId){next.push(list[i]);}
    }
    var insertIndex=beforeTabId?next.indexOf(beforeTabId):-1;
    if(insertIndex<0){
      next.push(tabId);
    }else{
      next.splice(insertIndex,0,tabId);
    }
    return next;
  }

  function createPaneAfter(afterPaneId){
    var pane={id:state.nextPaneId++,tabIds:[],activeTabId:null};
    var idx=findPaneIndex(afterPaneId);
    if(idx<0){
      state.panes.push(pane);
    }else{
      state.panes.splice(idx+1,0,pane);
    }
    state.activePaneId=pane.id;
    return pane;
  }

  function addTabObjectToPane(paneId,tab){
    state.tabs.push(tab);
    var pane=paneById(paneId);
    if(!pane){
      pane=createPaneAfter(state.panes.length?state.panes[state.panes.length-1].id:0);
    }
    pane.tabIds.push(tab.id);
    pane.activeTabId=tab.id;
    state.activePaneId=pane.id;
    normalizeWorkspace();
  }

  function moveTabToPane(tabId,targetPaneId,beforeTabId){
    var srcPane=paneForTab(tabId);
    var dstPane=paneById(targetPaneId);
    if(!srcPane||!dstPane){return;}
    srcPane.tabIds=srcPane.tabIds.filter(function(id){return id!==tabId;});
    if(srcPane.activeTabId===tabId){
      srcPane.activeTabId=srcPane.tabIds[0]||null;
    }
    dstPane.tabIds=insertTabId(dstPane.tabIds,tabId,beforeTabId===tabId?null:beforeTabId);
    dstPane.activeTabId=tabId;
    state.activePaneId=dstPane.id;
    normalizeWorkspace();
  }

  function moveTabToNewPane(tabId){
    var srcPane=paneForTab(tabId);
    if(!srcPane){return;}
    var pane=createPaneAfter(srcPane.id);
    moveTabToPane(tabId,pane.id,null);
  }

  function collapsePane(paneId){
    normalizeWorkspace();
    if(state.panes.length<=1){return;}
    var idx=findPaneIndex(paneId);
    if(idx<0){return;}
    var pane=state.panes[idx];
    if(!pane||!pane.tabIds.length){
      state.panes.splice(idx,1);
      normalizeWorkspace();
      return;
    }
    var targetIdx=idx>0?idx-1:(idx+1<state.panes.length?idx+1:-1);
    if(targetIdx<0){return;}
    var targetPane=state.panes[targetIdx];
    targetPane.tabIds=targetPane.tabIds.concat(pane.tabIds);
    if(!targetPane.activeTabId&&targetPane.tabIds.length){
      targetPane.activeTabId=targetPane.tabIds[0];
    }
    state.panes.splice(idx,1);
    state.activePaneId=targetPane.id;
    normalizeWorkspace();
  }

  function setNotice(message,isError){
    if(!notice){return;}
    if(!message){
      notice.textContent='';
      notice.className='workspace_notice';
      return;
    }
    notice.textContent=message;
    notice.className=isError?'workspace_notice open error':'workspace_notice open info';
  }

  function setUploadStatus(title,done,total,detail,stage){
    if(!uploadProgressShell||!uploadProgressLabel||!uploadProgressStage||!uploadProgressFill||!uploadStatus){return;}
    if(!title){
      uploadProgressShell.className='progress_shell';
      uploadProgressLabel.textContent='';
      uploadProgressStage.textContent='';
      uploadProgressFill.style.width='0%';
      uploadStatus.textContent='';
      return;
    }
    var className='progress_shell open';
    if(stage===TRANSFER_STAGE.ERROR){
      className+=' error';
    }else if(stage===TRANSFER_STAGE.DONE){
      className+=' done';
    }
    uploadProgressShell.className=className;
    uploadProgressLabel.textContent=title;
    uploadProgressStage.textContent=formatProgressText(done,total);
    uploadProgressFill.style.width=progressPercent(done,total)+'%';
    uploadStatus.textContent=detail||'';
  }

  function setBusy(nodes,busy){
    for(var i=0;i<nodes.length;i++){
      if(nodes[i]){nodes[i].disabled=!!busy;}
    }
  }

  function closeContextMenu(){
    if(menu){
      menu.className='context_menu';
      menu.innerHTML='';
    }
  }

  function openContextMenu(x,y,items){
    if(!menu||!items.length){return;}
    var html='';
    for(var i=0;i<items.length;i++){
      var item=items[i];
      html+='<button class="context_item'+(item.danger?' danger':'')+'" type="button" data-menu-id="'+escapeHtml(item.id)+'">'+escapeHtml(item.label)+'</button>';
    }
    menu.innerHTML=html;
    menu.className='context_menu open';
    menu.style.left=x+'px';
    menu.style.top=y+'px';
    var buttons=menu.querySelectorAll('[data-menu-id]');
    for(var j=0;j<buttons.length;j++){
      buttons[j].addEventListener('click',function(ev){
        var id=ev.currentTarget.getAttribute('data-menu-id');
        closeContextMenu();
        for(var k=0;k<items.length;k++){
          if(items[k].id===id){items[k].run();return;}
        }
      });
    }
  }

  function withAsUser(path,asUser){
    var user=String(asUser||'').trim();
    if(!user){return path;}
    var s=String(path||'');
    if(/[?&]as=/.test(s)){return s;}
    return s+(s.indexOf('?')>=0?'&':'?')+'as='+encodeURIComponent(user);
  }

  function withAs(path){
    var asUser=state&&state.asUser?state.asUser:currentAsUser;
    if(!asUser){return path;}
    return withAsUser(path,asUser);
  }

  async function apiRequestWithAs(path,asUser,options){
    var resp=await fetch(withAsUser(path,asUser),options||{});
    var ct=resp.headers.get('content-type')||'';
    var data=ct.indexOf('application/json')>=0?await resp.json():await resp.text();
    if(!resp.ok){
      if(typeof data==='string'){throw new Error(data||('HTTP '+resp.status));}
      throw new Error((data&&data.error)||('HTTP '+resp.status));
    }
    return data;
  }

  async function apiRequest(path,options){
    return apiRequestWithAs(path,state.asUser,options);
  }

  async function requestOkWithAs(path,asUser,options){
    var resp=await fetch(withAsUser(path,asUser),options||{});
    if(resp.ok){return resp;}
    var text=await resp.text();
    var match=text.match(/<Message>([\s\S]*?)<\/Message>/i);
    if(match&&match[1]){throw new Error(match[1].trim());}
    match=text.match(/<Code>([\s\S]*?)<\/Code>/i);
    if(match&&match[1]){throw new Error(match[1].trim());}
    throw new Error(text||('HTTP '+resp.status));
  }

  async function requestOk(path,options){
    return requestOkWithAs(path,state.asUser,options);
  }

  function httpErrorFromText(text,status){
    var body=text||'';
    var match=body.match(/<Message>([\s\S]*?)<\/Message>/i);
    if(match&&match[1]){return new Error(match[1].trim());}
    match=body.match(/<Code>([\s\S]*?)<\/Code>/i);
    if(match&&match[1]){return new Error(match[1].trim());}
    return new Error(body||('HTTP '+status));
  }

  function decodeArrayBufferText(value){
    if(!(value instanceof ArrayBuffer)){return '';}
    return new TextDecoder().decode(new Uint8Array(value));
  }

  function xhrRequestWithAs(path,asUser,options){
    return new Promise(function(resolve,reject){
      var xhr=new XMLHttpRequest();
      var method=options&&options.method?options.method:'GET';
      xhr.open(method,withAsUser(path,asUser),true);
      if(options&&options.onReady){options.onReady(xhr);}
      if(options&&options.responseType){xhr.responseType=options.responseType;}
      var headers=options&&options.headers?options.headers:null;
      if(headers){
        for(var key in headers){
          if(Object.prototype.hasOwnProperty.call(headers,key)){xhr.setRequestHeader(key,headers[key]);}
        }
      }
      if(options&&options.onDownloadProgress){
        xhr.onprogress=function(ev){options.onDownloadProgress(ev);};
      }
      if(options&&options.onUploadProgress&&xhr.upload){
        xhr.upload.onprogress=function(ev){options.onUploadProgress(ev);};
      }
      xhr.onload=function(){
        if(xhr.status>=200&&xhr.status<300){
          resolve({
            status:xhr.status,
            body:(xhr.responseType&&xhr.responseType!=='text')?xhr.response:(xhr.responseText||''),
            etag:xhr.getResponseHeader('ETag')||'',
          });
          return;
        }
        var text=(xhr.responseType&&xhr.responseType!=='text')?decodeArrayBufferText(xhr.response):String(xhr.responseText||'');
        reject(httpErrorFromText(text,xhr.status));
      };
      xhr.onerror=function(){reject(new Error('network error'));};
      xhr.onabort=function(){reject(new Error('request aborted'));};
      xhr.send(options&&Object.prototype.hasOwnProperty.call(options,'body')?options.body:null);
    });
  }

  function xhrRequest(path,options){
    return xhrRequestWithAs(path,state.asUser,options);
  }

  function encodeSegments(path){
    var parts=String(path||'').split('/');
    for(var i=0;i<parts.length;i++){
      parts[i]=encodeURIComponent(parts[i]);
    }
    return parts.join('/');
  }

  function encodeBucketName(bucket){
    return encodeURIComponent(String(bucket||''));
  }

  function uiRootBaseFromPath(path){
    var value=String(path||'');
    var fluxonFsMarker='/fluxon/fs/';
    var fluxonFsIdx=value.indexOf(fluxonFsMarker);
    if(fluxonFsIdx>=0){
      return value.slice(0,fluxonFsIdx)+fluxonFsMarker;
    }
    if(/\/fluxon\/fs$/.test(value)){
      return value+'/';
    }
    var uiMarker='/ui/';
    var uiIdx=value.indexOf(uiMarker);
    if(uiIdx>=0){
      return value.slice(0,uiIdx)+uiMarker;
    }
    if(/\/ui$/.test(value)){
      return value+'/';
    }
    return '/ui/';
  }

  function uiRootBase(){
    return uiRootBaseFromPath(window.location.pathname||'');
  }

  function uiBucketBase(bucket){
    return uiRootBase()+encodeBucketName(bucket)+'/';
  }

  function uiRootApiPath(suffix){
    return uiRootBase()+String(suffix||'').replace(/^\/+/,'');
  }

  function uiBucketApiPath(bucket,suffix){
    return uiBucketBase(bucket)+'api/'+String(suffix||'');
  }

  function uiTransferTaskListPath(){
    return uiRootApiPath('api/transfers');
  }

  function uiTransferTaskPath(taskId){
    return uiRootApiPath('api/transfer/'+encodeURIComponent(String(taskId||'')));
  }

  function uiTransferTaskControlPath(taskId,action){
    return uiRootApiPath('api/transfer/'+encodeURIComponent(String(taskId||''))+'/'+encodeURIComponent(String(action||'')));
  }

  function encodeUiObjectPath(bucket,key){
    return withAs(uiBucketBase(bucket)+'obj/'+encodeSegments(key));
  }

  async function loadListing(bucket,prefix){
    return apiRequest(uiBucketApiPath(bucket,'ls')+'?prefix='+encodeURIComponent(prefix||''));
  }

  function bucketOptionExists(bucket){
    for(var i=0;i<state.availableBuckets.length;i++){
      if(state.availableBuckets[i]===bucket){return true;}
    }
    return false;
  }

  function ensureKnownBucket(bucket){
    if(!bucketOptionExists(bucket)){
      state.availableBuckets.push(bucket);
      state.availableBuckets.sort();
    }
  }

  function normalizeProviderItems(items,fallbackMountPath){
    var out=[];
    var seen={};
    var rawItems=Array.isArray(items)?items:[];
    for(var i=0;i<rawItems.length;i++){
      var raw=rawItems[i]||{};
      var agentInstanceKey=String(raw.agent_instance_key||'').trim();
      var remoteRootDirAbs=String(raw.remote_root_dir_abs||'').trim();
      if(!agentInstanceKey||!remoteRootDirAbs){continue;}
      var dedupeKey=agentInstanceKey+'\n'+remoteRootDirAbs;
      if(seen[dedupeKey]){continue;}
      seen[dedupeKey]=true;
      out.push({
        agent_instance_key:agentInstanceKey,
        remote_root_dir_abs:remoteRootDirAbs,
      });
    }
    if(!out.length){
      var fallback=String(fallbackMountPath||'').trim();
      if(fallback){
        out.push({
          agent_instance_key:'configured',
          remote_root_dir_abs:fallback,
        });
      }
    }
    return out;
  }

  function providerSummaryText(items,fallbackMountPath){
    var providers=normalizeProviderItems(items,fallbackMountPath);
    var parts=[];
    for(var i=0;i<providers.length;i++){
      parts.push(providers[i].agent_instance_key+': '+providers[i].remote_root_dir_abs);
    }
    return parts.join(' | ');
  }

  function tabLabel(tab){
    return tab.bucket+':'+prefixLabel(tab.prefix);
  }

  function clipboardActionLabel(clipboard){
    if(!clipboard){return 'copy';}
    if(clipboard.mode===CLIPBOARD_MODE.COPY){return 'copy';}
    return 'move';
  }

  function parentPrefixForObjectKey(key){
    var cutAt=key.lastIndexOf('/');
    if(cutAt<0){return '';}
    return key.slice(0,cutAt+1);
  }

  function targetRef(bucket,prefix){
    return {bucket:String(bucket||''),prefix:String(prefix||'')};
  }

  function targetRefForTab(tab){
    return targetRef(tab.bucket,tab.prefix);
  }

  function targetRefForObjectDirectory(bucket,key){
    return targetRef(bucket,parentPrefixForObjectKey(key));
  }

  function rootRelpathLabel(rootRelpath){
    var text=String(rootRelpath||'').trim();
    return text&&text!=='.' ? text : '.';
  }

  function transferUnavailableReasonText(){
    return 'This system has not configured the TiKV-backed transfer state store required for FluxonFS directory transfer. Configure fluxon_fs.master_panel.transfer_state_store and retry the cross-export folder drag.';
  }

  function rootRelpathFromFolderPrefix(prefix){
    var text=String(prefix||'').replace(/^\/+/,'').replace(/\/+$/,'');
    return text ? text : '.';
  }

  function folderNameFromPrefix(prefix){
    var text=String(prefix||'').replace(/\/+$/,'');
    if(!text){return '';}
    var cutAt=text.lastIndexOf('/');
    return cutAt>=0 ? text.slice(cutAt+1) : text;
  }

  function destinationRootRelpathForFolder(srcPrefix,dstPrefix){
    var folderName=folderNameFromPrefix(srcPrefix);
    var parentPrefix=String(dstPrefix||'');
    var combined=parentPrefix+folderName;
    combined=combined.replace(/^\/+/,'').replace(/\/+$/,'');
    return combined ? combined : '.';
  }

  function dragActionLabel(srcBucket,dstBucket){
    return srcBucket===dstBucket?'move':'copy';
  }

  function fileNameFromKey(key){
    var text=String(key||'');
    var cutAt=text.lastIndexOf('/');
    return cutAt>=0?text.slice(cutAt+1):text;
  }

  function destinationKeyForTransfer(srcKey,dstPrefix){
    var name=fileNameFromKey(srcKey)||String(srcKey||'');
    var prefix=String(dstPrefix||'');
    return prefix?prefix+name:name;
  }

  function isSameTransferTarget(srcBucket,srcKey,dstBucket,dstPrefix){
    return String(srcBucket||'')===String(dstBucket||'')&&String(srcKey||'')===destinationKeyForTransfer(srcKey,dstPrefix);
  }

  function uiErrorMessage(err){
    return err&&err.message?err.message:String(err);
  }

  function paneShellFromElement(el){
    return el&&el.closest?el.closest('.pane_shell'):null;
  }

  function setPaneShellDropTarget(el,isActive){
    var paneEl=paneShellFromElement(el);
    if(!paneEl){return;}
    if(isActive){
      paneEl.classList.add('drop_target');
      return;
    }
    paneEl.classList.remove('drop_target');
  }

  function claimDropEvent(ev){
    if(!ev){return false;}
    if(ev.__fluxonUiDropHandled){return false;}
    ev.__fluxonUiDropHandled=true;
    ev.preventDefault();
    ev.stopPropagation();
    if(typeof ev.stopImmediatePropagation==='function'){
      ev.stopImmediatePropagation();
    }
    return true;
  }

  function eventTargetsWorkspaceTable(ev){
    return !!(ev&&ev.target&&ev.target.closest&&ev.target.closest('.table_drop_target'));
  }

  function startUiTask(task){
    Promise.resolve()
      .then(task)
      .catch(function(err){setNotice(uiErrorMessage(err),true);});
  }

  function runUiTask(task){
    return function(){startUiTask(task);};
  }

  function progressPercent(done,total){
    var safeDone=Math.max(0,Number(done)||0);
    var safeTotal=Math.max(0,Number(total)||0);
    if(safeTotal<=0){
      return safeDone>0?100:0;
    }
    var pct=(safeDone*100)/safeTotal;
    if(pct<0){return 0;}
    if(pct>100){return 100;}
    return pct;
  }

  function joinCompact(parts){
    var out=[];
    for(var i=0;i<parts.length;i++){
      if(parts[i]){out.push(parts[i]);}
    }
    return out.join(' | ');
  }

  function transferKindLabel(kind){
    if(kind===TRANSFER_KIND.COPY){return 'Copy';}
    if(kind===TRANSFER_KIND.MOVE){return 'Move';}
    return 'Upload';
  }

  function transferStageLabel(stage){
    if(stage===TRANSFER_STAGE.PAUSED){return 'Paused';}
    if(stage===TRANSFER_STAGE.DONE){return 'Completed';}
    if(stage===TRANSFER_STAGE.ERROR){return 'Failed';}
    if(stage===TRANSFER_STAGE.CANCELLED){return 'Cancelled';}
    return 'Running';
  }

  function trimTransfers(){
    if(transferState.items.length<=MAX_TRANSFER_ITEMS){return;}
    var keepActive=[];
    var keepOther=[];
    for(var i=0;i<transferState.items.length;i++){
      if(isTransferActiveStage(transferState.items[i].stage)){
        keepActive.push(transferState.items[i]);
      }else{
        keepOther.push(transferState.items[i]);
      }
    }
    transferState.items=keepActive.concat(keepOther.slice(0,Math.max(0,MAX_TRANSFER_ITEMS-keepActive.length)));
  }

  function countActiveTransfers(ts){
    var n=0;
    var source=ts||transferState;
    for(var i=0;i<source.items.length;i++){
      if(isTransferActiveStage(source.items[i].stage)){n++;}
    }
    return n;
  }

  function countActiveTransfersForToast(){
    return countActiveTransfers(transferState);
  }

  function updateNavBadge(){
    var badge=qs('nav_transfer_badge');
    if(!badge){return;}
    var active=countActiveGlobal(transferState);
    if(active>0){
      badge.textContent=String(active);
      badge.className='nav_badge visible';
    }else{
      badge.textContent='';
      badge.className='nav_badge';
    }
  }

  function ensureToastContainer(){
    var el=qs('transfer_toast');
    if(el){return el;}
    var div=document.createElement('div');
    div.id='transfer_toast';
    div.className='transfer_toast';
    div.innerHTML='<div class="transfer_toast_head" id="transfer_toast_head"><div><div class="transfer_toast_title" id="transfer_toast_title">Transfers</div><div class="transfer_toast_meta" id="transfer_toast_meta"></div></div></div><div class="transfer_toast_body" id="transfer_toast_body"></div>';
    document.body.appendChild(div);
    var head=div.querySelector('#transfer_toast_head');
    if(head){
      head.addEventListener('click',function(){
        openTransfersSurface();
      });
    }
    return div;
  }

  function renderTransferToast(){
    trimTransfers();
    updateNavBadge();
    var activeCount=countActiveTransfersForToast();
    var isStandaloneTransfersPage=!!(transfersHost&&!bootEl);
    if(
      isStandaloneTransfersPage||
      currentPageMode===transferPageMode.TRANSFERS||
      !transferState.items.length||
      activeCount===0
    ){
      var existing=qs('transfer_toast');
      if(existing){existing.className='transfer_toast';}
      return;
    }
    var toast=ensureToastContainer();
    var titleEl=qs('transfer_toast_title');
    var metaEl=qs('transfer_toast_meta');
    var bodyEl=qs('transfer_toast_body');
    if(titleEl){titleEl.textContent=activeCount+' active transfer'+(activeCount>1?'s':'');}
    if(metaEl){metaEl.textContent='Click to view all';}
    if(bodyEl){
      var html='';
      var shown=0;
      for(var i=0;i<transferState.items.length&&shown<3;i++){
        var item=transferState.items[i];
        if(!isTransferActiveStage(item.stage)){continue;}
        var pct=progressPercent(item.doneBytes,item.totalBytes);
        var telemetry=transferTelemetryText(item);
        var itemClass='transfer_toast_item';
        if(item.stage===TRANSFER_STAGE.PAUSED){itemClass+=' paused';}
        html+='<div class="'+itemClass+'">';
        html+='<div class="transfer_toast_item_head"><div class="transfer_toast_item_name">'+escapeHtml(item.name)+'</div>';
        html+='<div class="transfer_toast_item_pct">'+pct.toFixed(0)+'%</div></div>';
        html+='<div class="transfer_toast_item_bar"><span class="transfer_toast_item_fill" style="width:'+pct+'%"></span></div>';
        html+='<div class="transfer_toast_item_detail">'+escapeHtml(joinCompact([transferStageLabel(item.stage),telemetry,item.detail||item.summary]))+'</div>';
        html+='</div>';
        shown++;
      }
      if(activeCount>3){
        html+='<div class="transfer_toast_item"><div class="transfer_toast_item_detail">+'+(activeCount-3)+' more...</div></div>';
      }
      bodyEl.innerHTML=html;
    }
    toast.className='transfer_toast open';
  }

  function transferCanOpenInWorkspace(item){
    return !!(
      item&&
      item.sourceBucket&&
      item.sourceKey&&
      item.targetBucket&&
      item.targetKey
    );
  }

  function transferJobRootRelpathToPrefix(rootRelpath){
    var text=String(rootRelpath||'').trim();
    if(!text||text==='.'||text==='/'){return '';}
    text=text.replace(/^\/+/,'').replace(/\/+$/,'');
    return text ? text+'/' : '';
  }

  function parentPrefixForFolderPrefix(prefix){
    var text=String(prefix||'').replace(/\/+$/,'');
    if(!text){return '';}
    var cutAt=text.lastIndexOf('/');
    if(cutAt<0){return '';}
    return text.slice(0,cutAt+1);
  }

  function transferJobCanOpenInWorkspace(item){
    return !!(
      item&&
      item.job&&
      String(item.job.src_export||'').trim()&&
      String(item.job.dst_export||'').trim()
    );
  }

  function transferJobWorkspaceSnapshot(item){
    var srcPrefix=transferJobRootRelpathToPrefix(item&&item.job&&item.job.src_root_relpath);
    var dstPrefix=transferJobRootRelpathToPrefix(item&&item.job&&item.job.dst_root_relpath);
    return {
      version:1,
      activePaneId:2,
      nextPaneId:3,
      nextTabId:3,
      tabs:[
        {id:1,bucket:String(item.job.src_export||''),prefix:parentPrefixForFolderPrefix(srcPrefix)},
        {id:2,bucket:String(item.job.dst_export||''),prefix:parentPrefixForFolderPrefix(dstPrefix)},
      ],
      panes:[
        {id:1,tabIds:[1],activeTabId:1},
        {id:2,tabIds:[2],activeTabId:2},
      ],
    };
  }

  function transferJobWorkspaceFocusSpecs(item){
    var out=[];
    var srcBucket=String(item&&item.job&&item.job.src_export||'');
    var dstBucket=String(item&&item.job&&item.job.dst_export||'');
    var srcPrefix=transferJobRootRelpathToPrefix(item&&item.job&&item.job.src_root_relpath);
    var dstPrefix=transferJobRootRelpathToPrefix(item&&item.job&&item.job.dst_root_relpath);
    if(srcBucket&&srcPrefix){
      out.push({paneId:1,bucket:srcBucket,prefix:srcPrefix,scroll:false});
    }
    if(dstBucket&&dstPrefix){
      out.push({paneId:2,bucket:dstBucket,prefix:dstPrefix,scroll:true});
    }
    return out;
  }

  function openTransferJobInWorkspace(item){
    if(!transferJobCanOpenInWorkspace(item)){return;}
    openWorkspace({
      mode:bootEl?WORKSPACE_OPEN_MODE.APPLY:WORKSPACE_OPEN_MODE.NAVIGATE,
      snapshot:transferJobWorkspaceSnapshot(item),
      focusSpecs:transferJobWorkspaceFocusSpecs(item),
    }).then(function(){
      if(bootEl){applyPageMode(transferPageMode.WORKSPACE,'replace');}
    }).catch(function(err){setNotice(uiErrorMessage(err),true);});
  }

  function transferWorkspaceSnapshot(item){
    return {
      version:1,
      activePaneId:2,
      nextPaneId:3,
      nextTabId:3,
      tabs:[
        {id:1,bucket:String(item.sourceBucket),prefix:String(item.sourcePrefix||'')},
        {id:2,bucket:String(item.targetBucket),prefix:String(item.targetPrefix||'')},
      ],
      panes:[
        {id:1,tabIds:[1],activeTabId:1},
        {id:2,tabIds:[2],activeTabId:2},
      ],
    };
  }

  function transferWorkspaceFocusSpecs(item){
    return [
      {paneId:1,bucket:String(item.sourceBucket),key:String(item.sourceKey),scroll:false},
      {paneId:2,bucket:String(item.targetBucket),key:String(item.targetKey),scroll:true},
    ];
  }

  function openTransferInWorkspace(item){
    if(!transferCanOpenInWorkspace(item)){return;}
    openWorkspace({
      mode:bootEl?WORKSPACE_OPEN_MODE.APPLY:WORKSPACE_OPEN_MODE.NAVIGATE,
      snapshot:transferWorkspaceSnapshot(item),
      focusSpecs:transferWorkspaceFocusSpecs(item),
    }).then(function(){
      if(bootEl){applyPageMode(transferPageMode.WORKSPACE,'replace');}
    }).catch(function(err){setNotice(uiErrorMessage(err),true);});
  }

  function transferRowPresentation(item){
    var pct=progressPercent(item.doneBytes,item.totalBytes);
    var telemetry=transferTelemetryText(item);
    var rowClass='transfer_row';
    if(transferCanOpenInWorkspace(item)){
      rowClass+=' clickable';
    }
    var icon='&#8593;';
    if(item.stage===TRANSFER_STAGE.PAUSED){rowClass+=' paused';icon='&#10074;&#10074;';}
    else if(item.stage===TRANSFER_STAGE.ERROR){rowClass+=' error';icon='&#10007;';}
    else if(item.stage===TRANSFER_STAGE.CANCELLED){rowClass+=' cancelled';icon='&#9632;';}
    else if(item.stage===TRANSFER_STAGE.DONE){rowClass+=' done';icon='&#10003;';}
    return {
      rowClass:rowClass,
      icon:icon,
      pct:pct,
      pctText:pct.toFixed(0)+'%',
      name:transferKindLabel(item.kind)+': '+item.name,
      detail:joinCompact([transferStageLabel(item.stage),telemetry,item.summary,item.detail]),
    };
  }

  function transferRowControlsInnerHtml(item){
    if(!item||!item.taskId){return '';}
    var html='';
    if(item.canPause){
      html+='<button class="btn" type="button" data-transfer-action="pause" data-transfer-task-id="'+escapeHtml(item.taskId)+'">Pause</button>';
    }
    if(item.canResume){
      html+='<button class="btn" type="button" data-transfer-action="resume" data-transfer-task-id="'+escapeHtml(item.taskId)+'">Start</button>';
    }
    if(item.canCancel){
      html+='<button class="btn danger" type="button" data-transfer-action="cancel" data-transfer-task-id="'+escapeHtml(item.taskId)+'">Cancel</button>';
    }
    return html;
  }

  function transferRowControlsHtml(item){
    var html=transferRowControlsInnerHtml(item);
    if(!html){return '';}
    return '<div class="transfer_row_controls">'+html+'</div>';
  }

  function createTransferRowHtml(item){
    var view=transferRowPresentation(item);
    var openAttr=transferCanOpenInWorkspace(item)?' data-transfer-open-id="'+item.id+'"':'';
    return '<div class="'+view.rowClass+'" data-transfer-item-id="'+item.id+'"'+openAttr+'>'+
      '<div class="transfer_row_icon">'+view.icon+'</div>'+
      '<div class="transfer_row_body">'+
        '<div class="transfer_row_name">'+escapeHtml(view.name)+'</div>'+
        '<div class="transfer_row_detail">'+escapeHtml(view.detail)+'</div>'+
        '<div class="transfer_row_bar"><span class="transfer_row_fill" style="width:'+view.pct+'%"></span></div>'+
      '</div>'+
      transferRowControlsHtml(item)+
      '<div class="transfer_row_pct">'+view.pctText+'</div>'+
    '</div>';
  }

  function patchTransferRowElement(row,item){
    var view=transferRowPresentation(item);
    row.className=view.rowClass;
    row.setAttribute('data-transfer-item-id',String(item.id));
    if(transferCanOpenInWorkspace(item)){
      row.setAttribute('data-transfer-open-id',String(item.id));
    }else{
      row.removeAttribute('data-transfer-open-id');
    }
    var iconEl=row.querySelector('.transfer_row_icon');
    if(iconEl){iconEl.innerHTML=view.icon;}
    var nameEl=row.querySelector('.transfer_row_name');
    if(nameEl){nameEl.textContent=view.name;}
    var detailEl=row.querySelector('.transfer_row_detail');
    if(detailEl){detailEl.textContent=view.detail;}
    var fillEl=row.querySelector('.transfer_row_fill');
    if(fillEl){fillEl.style.width=view.pct+'%';}
    var pctEl=row.querySelector('.transfer_row_pct');
    if(pctEl){pctEl.textContent=view.pctText;}
    var controlsEl=row.querySelector('.transfer_row_controls');
    var controlsInner=transferRowControlsInnerHtml(item);
    if(controlsInner){
      if(!controlsEl){
        var pctNode=row.querySelector('.transfer_row_pct');
        var wrapper=document.createElement('div');
        wrapper.className='transfer_row_controls';
        wrapper.innerHTML=controlsInner;
        row.insertBefore(wrapper,pctNode||null);
      }else{
        controlsEl.innerHTML=controlsInner;
      }
    }else if(controlsEl){
      controlsEl.remove();
    }
  }

  function canPatchTransferRows(host){
    var rows=host.querySelectorAll('.transfer_row[data-transfer-item-id]');
    if(rows.length!==transferState.items.length){return false;}
    for(var i=0;i<rows.length;i++){
      if(String(rows[i].getAttribute('data-transfer-item-id')||'')!==String(transferState.items[i].id)){
        return false;
      }
    }
    return true;
  }

  function patchTransferRowsInPlace(host){
    var rows=host.querySelectorAll('.transfer_row[data-transfer-item-id]');
    for(var i=0;i<rows.length;i++){
      patchTransferRowElement(rows[i],transferState.items[i]);
    }
  }

  function renderTransfersIntoHost(host){
    if(!host){return;}
    if(!transferState.items.length){
      host.innerHTML='<div class="empty_state">No transfers yet.</div>';
      return;
    }
    if(canPatchTransferRows(host)){
      patchTransferRowsInPlace(host);
      attachTransferActionEvents(host);
      return;
    }
    var html='';
    for(var i=0;i<transferState.items.length;i++){
      html+=createTransferRowHtml(transferState.items[i]);
    }
    host.innerHTML=html;
    attachTransferActionEvents(host);
  }

  function renderTransfersPage(){
    var hosts=transferRenderHosts();
    for(var i=0;i<hosts.length;i++){
      renderTransfersIntoHost(hosts[i]);
    }
  }

  function beginTransfer(kind,name,totalBytes){
    var item={
      id:transferState.nextId++,
      kind:kind,
      name:name,
      doneBytes:0,
      totalBytes:Math.max(0,Number(totalBytes)||0),
      stage:TRANSFER_STAGE.RUNNING,
      summary:'',
      detail:'',
      taskId:null,
      sourceBucket:null,
      sourceKey:null,
      sourcePrefix:null,
      targetBucket:null,
      targetKey:null,
      targetPrefix:null,
      startedAt:Date.now(),
      canPause:false,
      canResume:false,
      canCancel:false,
    };
    transferState.items.unshift(item);
    persistTransferState();
    renderTransferToast();
    renderTransfersPage();
    return item.id;
  }

  function updateTransfer(id,patch){
    for(var i=0;i<transferState.items.length;i++){
      if(transferState.items[i].id===id){
        transferState.items[i]=Object.assign({},transferState.items[i],patch);
        persistTransferState();
        renderTransferToast();
        renderTransfersPage();
        return;
      }
    }
  }

  function splitByteRanges(totalBytes,chunkBytes){
    var ranges=[];
    var start=0;
    var index=0;
    while(start<totalBytes){
      var end=Math.min(totalBytes,start+chunkBytes);
      ranges.push({index:index,partNumber:index+1,start:start,endExclusive:end,size:end-start});
      start=end;
      index+=1;
    }
    return ranges;
  }

  async function runLimitedWorkers(items,maxInflight,worker){
    var out=new Array(items.length);
    var nextIndex=0;
    async function runOne(){
      while(true){
        if(nextIndex>=items.length){return;}
        var current=nextIndex;
        nextIndex+=1;
        out[current]=await worker(items[current],current);
      }
    }
    var count=Math.min(maxInflight,items.length);
    var workers=[];
    for(var i=0;i<count;i++){
      workers.push(runOne());
    }
    await Promise.all(workers);
    return out;
  }

  function waitMs(ms){
    return new Promise(function(resolve){window.setTimeout(resolve,ms);});
  }

  var serverTransferSyncTimer=0;
  var serverTransferSyncInFlight=false;

  function applyTransferSnapshot(transferId,snapshot){
    var source=snapshot&&snapshot.source?snapshot.source:null;
    var target=snapshot&&snapshot.target?snapshot.target:null;
    updateTransfer(transferId,{
      taskId:snapshot&&snapshot.task_id?snapshot.task_id:null,
      kind:snapshot&&snapshot.kind?String(snapshot.kind):undefined,
      name:snapshot&&snapshot.name?String(snapshot.name):undefined,
      doneBytes:Math.max(0,Number(snapshot&&snapshot.done_bytes)||0),
      totalBytes:Math.max(0,Number(snapshot&&snapshot.total_bytes)||0),
      stage:snapshot&&snapshot.stage?String(snapshot.stage):TRANSFER_STAGE.RUNNING,
      summary:snapshot&&snapshot.summary?String(snapshot.summary):'',
      detail:snapshot&&snapshot.detail?String(snapshot.detail):'',
      sourceBucket:source&&source.bucket?String(source.bucket):null,
      sourceKey:source&&source.key?String(source.key):null,
      sourcePrefix:source&&source.prefix!==undefined&&source.prefix!==null?String(source.prefix):null,
      targetBucket:target&&target.bucket?String(target.bucket):null,
      targetKey:target&&target.key?String(target.key):null,
      targetPrefix:target&&target.prefix!==undefined&&target.prefix!==null?String(target.prefix):null,
      startedAt:Math.max(0,Number(snapshot&&snapshot.started_at_ms)||Date.now()),
      canPause:!!(snapshot&&snapshot.can_pause),
      canResume:!!(snapshot&&snapshot.can_resume),
      canCancel:!!(snapshot&&snapshot.can_cancel),
    });
    scheduleServerTransferSync();
  }

  async function waitForTransferCompletion(transferId,snapshot){
    var current=snapshot;
    while(current&&current.stage===TRANSFER_STAGE.RUNNING){
      await waitMs(150);
      current=await apiRequest(uiTransferTaskPath(current.task_id));
      applyTransferSnapshot(transferId,current);
    }
    return current;
  }

  function findTransferIndexByTaskId(taskId){
    var wanted=String(taskId||'');
    if(!wanted){return -1;}
    for(var i=0;i<transferState.items.length;i++){
      if(String(transferState.items[i].taskId||'')===wanted){return i;}
    }
    return -1;
  }

  function findTransferItemById(id){
    var wanted=Number(id)||0;
    if(!wanted){return null;}
    for(var i=0;i<transferState.items.length;i++){
      if(Number(transferState.items[i].id)===wanted){
        return transferState.items[i];
      }
    }
    return null;
  }

  function upsertServerTransferSnapshot(snapshot){
    if(!snapshot||!snapshot.task_id){return;}
    var taskId=String(snapshot.task_id);
    var stage=String(snapshot.stage||TRANSFER_STAGE.RUNNING);
    var source=snapshot.source||null;
    var target=snapshot.target||null;
    if(transferState.dismissedTaskIds.indexOf(taskId)>=0&&isTransferTerminalStage(stage)){
      return;
    }
    if(isTransferActiveStage(stage)){
      transferState.dismissedTaskIds=transferState.dismissedTaskIds.filter(function(v){return v!==taskId;});
    }
    var patch={
      kind:String(snapshot.kind||TRANSFER_KIND.COPY),
      name:String(snapshot.name||''),
      taskId:taskId,
      doneBytes:Math.max(0,Number(snapshot.done_bytes)||0),
      totalBytes:Math.max(0,Number(snapshot.total_bytes)||0),
      stage:stage,
      summary:String(snapshot.summary||''),
      detail:String(snapshot.detail||''),
      sourceBucket:source&&source.bucket?String(source.bucket):null,
      sourceKey:source&&source.key?String(source.key):null,
      sourcePrefix:source&&source.prefix!==undefined&&source.prefix!==null?String(source.prefix):null,
      targetBucket:target&&target.bucket?String(target.bucket):null,
      targetKey:target&&target.key?String(target.key):null,
      targetPrefix:target&&target.prefix!==undefined&&target.prefix!==null?String(target.prefix):null,
      startedAt:Math.max(0,Number(snapshot.started_at_ms)||Date.now()),
      canPause:!!snapshot.can_pause,
      canResume:!!snapshot.can_resume,
      canCancel:!!snapshot.can_cancel,
    };
    var idx=findTransferIndexByTaskId(taskId);
    if(idx>=0){
      transferState.items[idx]=Object.assign({},transferState.items[idx],patch);
      return;
    }
    transferState.items.unshift(Object.assign({id:transferState.nextId++},patch));
  }

  function mergeServerTransferSnapshots(snapshots){
    var seen={};
    for(var i=0;i<snapshots.length;i++){
      var snapshot=snapshots[i];
      if(!snapshot||!snapshot.task_id){continue;}
      seen[String(snapshot.task_id)]=true;
      upsertServerTransferSnapshot(snapshot);
    }
    for(var j=0;j<transferState.items.length;j++){
      var item=transferState.items[j];
      if(!item.taskId||seen[item.taskId]){continue;}
      if(isTransferActiveStage(item.stage)){
        transferState.items[j]=Object.assign({},item,{
          stage:TRANSFER_STAGE.ERROR,
          summary:'Interrupted',
          detail:'Gateway no longer reports this transfer',
          canPause:false,
          canResume:false,
          canCancel:false,
        });
      }
    }
  }

  function hasActiveServerTransfers(){
    for(var i=0;i<transferState.items.length;i++){
      if(isTransferActiveStage(transferState.items[i].stage)&&transferState.items[i].taskId){
        return true;
      }
    }
    return false;
  }

  function scheduleServerTransferSync(){
    if(serverTransferSyncTimer){window.clearTimeout(serverTransferSyncTimer);}
    serverTransferSyncTimer=0;
    if(!hasActiveServerTransfers()){return;}
    serverTransferSyncTimer=window.setTimeout(function(){
      syncServerTransferTasks().catch(function(){});
    },250);
  }

  async function syncServerTransferTasks(){
    if(serverTransferSyncInFlight){return;}
    serverTransferSyncInFlight=true;
    try{
      var prevStageByTaskId=transferTaskStageMap(transferState.items);
      var body=await apiRequestWithAs(uiTransferTaskListPath(),currentAsUser,{});
      var tasks=body&&Array.isArray(body.tasks)?body.tasks:[];
      mergeServerTransferSnapshots(tasks);
      var shouldRefreshWorkspace=!!(bootEl&&didAnyServerTransferReachTerminal(prevStageByTaskId));
      persistTransferState();
      if(shouldRefreshWorkspace){
        await refreshAllTabs();
      }else{
        renderTransfersPage();
        if(bootEl){renderTransferToast();}
      }
      updateNavBadgeGlobal();
    }finally{
      serverTransferSyncInFlight=false;
      scheduleServerTransferSync();
    }
  }

  function startServerTransferSyncLoop(){
    syncServerTransferTasks().catch(function(){scheduleServerTransferSync();});
  }

  async function controlServerTransferTask(taskId,action){
    var snapshot=await apiRequestWithAs(uiTransferTaskControlPath(taskId,action),currentAsUser,{method:'POST'});
    upsertServerTransferSnapshot(snapshot);
    persistTransferState();
    renderTransfersPage();
    if(bootEl){renderTransferToast();}
    updateNavBadgeGlobal();
    scheduleServerTransferSync();
  }

  function attachTransferActionEvents(host){
    if(!host||host.__transferEventsBound){return;}
    host.__transferEventsBound=true;
    host.addEventListener('click',function(ev){
      var actionEl=ev.target&&ev.target.closest('[data-transfer-action][data-transfer-task-id]');
      if(actionEl&&host.contains(actionEl)){
        ev.stopPropagation();
        var action=String(actionEl.getAttribute('data-transfer-action')||'');
        var taskId=String(actionEl.getAttribute('data-transfer-task-id')||'');
        if(!action||!taskId){return;}
        startUiTask(function(){return controlServerTransferTask(taskId,action);});
        return;
      }
      var rowEl=ev.target&&ev.target.closest('[data-transfer-open-id]');
      if(!rowEl||!host.contains(rowEl)){return;}
      var item=findTransferItemById(rowEl.getAttribute('data-transfer-open-id'));
      if(!item){return;}
      openTransferInWorkspace(item);
    });
  }

  function replaceTab(tabId,payload){
    for(var i=0;i<state.tabs.length;i++){
      if(state.tabs[i].id===tabId){
        state.tabs[i]=Object.assign({id:tabId},payload);
        ensureKnownBucket(state.tabs[i].bucket);
        normalizeWorkspace();
        return;
      }
    }
  }

  async function refreshAllTabs(){
    var tabs=state.tabs.slice();
    for(var i=0;i<tabs.length;i++){
      var payload=await loadListing(tabs[i].bucket,tabs[i].prefix);
      replaceTab(tabs[i].id,payload);
    }
    render();
  }

  function updateModalPrefixViews(){
    var tab=activeTab();
    var prefix=tab?tab.prefix:'';
    var bucket=tab?tab.bucket:'';
    var mkdirInput=qs('mkdir_prefix_input');
    var mkdirView=qs('mkdir_prefix_view');
    var uploadInput=qs('upload_prefix_input');
    var uploadView=qs('upload_prefix_view');
    if(mkdirInput){mkdirInput.value=prefix;}
    if(mkdirView){mkdirView.textContent=prefixLabel(prefix);}
    if(uploadInput){uploadInput.value=prefix;}
    if(uploadView){uploadView.textContent=prefixLabel(prefix);}
    if(openBucketSelect){
      populateOpenBucketOptions();
      if(bucket&&bucketOptionExists(bucket)){openBucketSelect.value=bucket;}
    }
    if(openBucketPrefixInput&&tab){openBucketPrefixInput.value=tab.prefix;}
    setUploadStatus('',0,0,'',TRANSFER_STAGE.RUNNING);
  }

  function filteredRows(tab){
    var filter=searchInput?(searchInput.value||'').trim().toLowerCase():'';
    var dirs=[];
    var files=[];
    for(var i=0;i<tab.dirs.length;i++){
      var dir=tab.dirs[i];
      var nextPrefix=(tab.prefix||'')+dir.name+'/';
      var text=('dir '+dir.name+' '+nextPrefix).toLowerCase();
      if(!filter||text.indexOf(filter)>=0){dirs.push(dir);}
    }
    for(var j=0;j<tab.files.length;j++){
      var file=tab.files[j];
      var ftext=('file '+file.name+' '+file.key).toLowerCase();
      if(!filter||ftext.indexOf(filter)>=0){files.push(file);}
    }
    return {dirs:dirs,files:files};
  }

  function pasteMenuItemForTarget(target,label){
    return {
      id:'paste',
      label:label||'Paste Here',
      run:runUiTask(function(){return pasteIntoTarget(target);})
    };
  }

  function clipboardHtml(tab){
    if(!state.clipboard||!tab){return '';}
    var css=state.clipboard.mode===CLIPBOARD_MODE.CUT?'cut':'copy';
    return '<span class="pill clipboard_pill '+css+'">'+escapeHtml(state.clipboard.mode)+': <strong class="mono">s3://'+escapeHtml(state.clipboard.bucket)+'/'+escapeHtml(state.clipboard.key)+'</strong> <button class="pill_button" type="button" id="paste_clipboard_btn">paste to '+escapeHtml(prefixLabel(tab.prefix))+'</button> <button class="pill_button" type="button" id="clear_clipboard_btn">clear</button></span>';
  }

  function formatBytes(value){
    value=Math.max(0,Number(value)||0);
    if(value<1024){return value+' B';}
    var units=['KB','MB','GB','TB'];
    var size=value;
    var idx=-1;
    while(size>=1024&&idx+1<units.length){size/=1024;idx++;}
    return size.toFixed(1)+' '+units[idx];
  }

  function formatDurationMs(value){
    var ms=Math.max(0,Number(value)||0);
    var totalSeconds=Math.floor(ms/1000);
    var hours=Math.floor(totalSeconds/3600);
    var minutes=Math.floor((totalSeconds%3600)/60);
    var seconds=totalSeconds%60;
    if(hours>0){
      return hours+'h '+String(minutes).padStart(2,'0')+'m '+String(seconds).padStart(2,'0')+'s';
    }
    if(minutes>0){
      return minutes+'m '+String(seconds).padStart(2,'0')+'s';
    }
    return seconds+'s';
  }

  function transferElapsedText(item){
    var startedAt=Math.max(0,Number(item&&item.startedAt)||0);
    if(!startedAt){return '';}
    return formatDurationMs(Date.now()-startedAt)+' elapsed';
  }

  function transferBandwidthText(item){
    if(!item){return '';}
    var recent=Math.max(0,Number(item.recentBytesPerSec)||0);
    if(isTransferActiveStage(item.stage)&&recent>0){
      return formatBytes(recent)+'/s recent';
    }
    var startedAt=Math.max(0,Number(item.startedAt)||0);
    var doneBytes=Math.max(0,Number(item.doneBytes)||0);
    var elapsedMs=startedAt>0?Math.max(0,Date.now()-startedAt):0;
    if(doneBytes<=0||elapsedMs<=0){return '';}
    return formatBytes(doneBytes*1000/elapsedMs)+'/s avg';
  }

  function transferTelemetryText(item){
    var parts=[];
    var bandwidth=transferBandwidthText(item);
    if(bandwidth){parts.push(bandwidth);}
    var elapsed=transferElapsedText(item);
    if(elapsed){parts.push(elapsed);}
    return parts.join(' | ');
  }

  function formatMtime(ns){
    if(!ns||ns<=0){return '-';}
    return new Date(ns/1000000).toISOString().replace('T',' ').replace('.000Z',' UTC');
  }

  function formatProgress(done,total){
    var pct=progressPercent(done,total);
    return pct.toFixed(pct>=10?0:1)+'%';
  }

  function formatProgressText(done,total){
    var safeDone=Math.max(0,Number(done)||0);
    var safeTotal=Math.max(0,Number(total)||0);
    if(safeTotal<=0){
      if(safeDone<=0){return '0%';}
      return formatBytes(safeDone)+' (100%)';
    }
    return formatBytes(safeDone)+' / '+formatBytes(safeTotal)+' ('+formatProgress(safeDone,safeTotal)+')';
  }

  function dataTransferHasFiles(dt){
    if(!dt){return false;}
    if(dt.files&&dt.files.length>0){return true;}
    if(!dt.types||typeof dt.types.length!=='number'){return false;}
    for(var i=0;i<dt.types.length;i++){
      if(String(dt.types[i])==='Files'){return true;}
    }
    return false;
  }

  function dataTransferFiles(dt){
    if(!dt||!dt.files||!dt.files.length){return [];}
    var out=[];
    for(var i=0;i<dt.files.length;i++){
      if(dt.files[i]){out.push(dt.files[i]);}
    }
    return out;
  }

  function actionIconSvg(kind){
    if(kind==='open_in_page'){
      return '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="4" width="18" height="16" rx="2"></rect><path d="M12 4v16"></path><path d="M15 9h3v3"></path><path d="m18 9-5 5"></path></svg>';
    }
    if(kind==='download'){
      return '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3v11"></path><path d="m7 10 5 5 5-5"></path><path d="M5 21h14"></path></svg>';
    }
    if(kind==='delete'){
      return '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16"></path><path d="M9 7V4h6v3"></path><path d="M7 7l1 12h8l1-12"></path><path d="M10 11v6"></path><path d="M14 11v6"></path></svg>';
    }
    return '';
  }

  function actionIconButtonHtml(iconKind,label,dataAttrs,danger){
    var html='<button class="btn action_icon_btn'+(danger?' danger':'')+'" type="button" title="'+escapeHtml(label)+'" aria-label="'+escapeHtml(label)+'"';
    if(dataAttrs){html+=dataAttrs;}
    html+='>'+actionIconSvg(iconKind)+'</button>';
    return html;
  }

  function actionIconLinkHtml(iconKind,label,href,danger){
    return '<a class="btn action_icon_btn'+(danger?' danger':'')+'" href="'+href+'" title="'+escapeHtml(label)+'" aria-label="'+escapeHtml(label)+'">'+actionIconSvg(iconKind)+'</a>';
  }

  function renderPanePage(pane){
    var tab=activeTabForPane(pane.id);
    if(!tab){
      return '<div class="pane_empty">Drop a page tab here.</div>';
    }
    var rows=filteredRows(tab);
    var nav='';
    if(tab.parent_prefix===null||tab.parent_prefix===undefined){
      nav+='<button class="btn" type="button" disabled>Up</button>';
    }else{
      nav+='<button class="btn" type="button" data-nav="up">Up</button>';
    }
    nav+='<button class="btn" type="button" data-nav="root"'+(tab.prefix?'':' disabled')+'>Root</button>';
    var html='';
    html+='<div class="page_panel">';
    html+='<div class="page_panel_head">';
    html+='<div class="page_meta">';
    html+='<div class="pathbar">';
    html+='<span class="pill">bucket: <strong class="mono">'+escapeHtml(tab.bucket)+'</strong></span>';
    html+='<span class="pill">providers: <span class="mono mono_scroll_x">'+escapeHtml(providerSummaryText(tab.provider_items,tab.mount_path))+'</span></span>';
    html+='<span class="pill">prefix: <span class="mono">'+escapeHtml(prefixLabel(tab.prefix))+'</span></span>';
    html+='<span class="pill">pages: <strong>'+state.tabs.length+'</strong></span>';
    html+='<span class="pill">panes: <strong>'+state.panes.length+'</strong></span>';
    html+=clipboardHtml(tab);
    html+='</div>';
    html+='<div class="page_actions">'+nav+'<span class="page_hint">Drag an object, folder, or page tab onto another pane, page, or folder. Drop local files into the page or onto a folder to upload. Same-bucket object drops move; cross-bucket object drops copy. Cross-export folder drops open a FluxonFS transfer job form.</span></div>';
    html+='</div>';
    html+='</div>';
    html+='<div class="page_surface" data-page-surface="1">';
    html+='<table class="table_drop_target"><thead><tr><th>Name</th><th>Size</th><th>Type</th><th>Last Modified</th><th style="text-align:right">Actions</th></tr></thead><tbody>';
    for(var i=0;i<rows.dirs.length;i++){
      var dir=rows.dirs[i];
      var dirPrefix=(tab.prefix||'')+dir.name+'/';
      html+='<tr draggable="true" data-folder-bucket="'+escapeHtml(tab.bucket)+'" data-folder-prefix="'+escapeHtml(dirPrefix)+'">';
      html+='<td class="object_name_cell"><a class="link mono object_name_link" href="#" data-open-folder="'+escapeHtml(dirPrefix)+'" title="'+escapeHtml(dirPrefix)+'"><span class="object_name_scroll">'+escapeHtml(dir.name)+'/</span></a></td>';
      html+='<td class="mono muted">-</td>';
      html+='<td class="muted">Folder</td>';
      html+='<td class="mono muted">'+escapeHtml(formatMtime(dir.mtime_ns))+'</td>';
      html+='<td style="text-align:right"><div class="row_actions">'+actionIconButtonHtml('open_in_page','Open in Page',' data-open-folder-new="'+escapeHtml(dirPrefix)+'"',false)+actionIconButtonHtml('delete','Delete Folder',' data-delete-folder-bucket="'+escapeHtml(tab.bucket)+'" data-delete-folder-prefix="'+escapeHtml(dirPrefix)+'"',true)+'</div></td>';
      html+='</tr>';
    }
    for(var j=0;j<rows.files.length;j++){
      var file=rows.files[j];
      html+='<tr draggable="true" data-file-bucket="'+escapeHtml(tab.bucket)+'" data-file-key="'+escapeHtml(file.key)+'">';
      html+='<td class="object_name_cell"><span class="mono object_name_scroll" title="'+escapeHtml(file.key)+'">'+escapeHtml(file.name)+'</span></td>';
      html+='<td class="mono muted">'+escapeHtml(formatBytes(file.size))+'</td>';
      html+='<td class="muted">Object</td>';
      html+='<td class="mono muted">'+escapeHtml(formatMtime(file.mtime_ns))+'</td>';
      html+='<td style="text-align:right"><div class="row_actions">'+actionIconLinkHtml('download','Download',encodeUiObjectPath(tab.bucket,file.key),false)+actionIconButtonHtml('delete','Delete',' data-delete-bucket="'+escapeHtml(tab.bucket)+'" data-delete-key="'+escapeHtml(file.key)+'"',true)+'</div></td>';
      html+='</tr>';
    }
    if(!rows.dirs.length&&!rows.files.length){
      html+='<tr><td colspan="5" class="empty_state">Empty</td></tr>';
    }
    html+='</tbody></table>';
    html+='</div>';
    html+='</div>';
    return html;
  }

  function renderWorkspace(){
    normalizeWorkspace();
    if(!workspaceHost){return;}
    if(!state.panes.length){
      workspaceHost.innerHTML='<div class="empty_state">No page is open.</div>';
      return;
    }
    var html='';
    for(var i=0;i<state.panes.length;i++){
      var pane=state.panes[i];
      var paneTab=activeTabForPane(pane.id);
      var paneActive=state.activePaneId===pane.id?' active':'';
      html+='<section class="pane_shell'+paneActive+'" data-pane-id="'+pane.id+'">';
      html+='<div class="pane_head">';
      html+='<div class="pane_title">';
      html+='<div><div class="pane_title_text">Pane '+(i+1)+'</div><div class="pane_title_meta">'+escapeHtml(paneTab?tabLabel(paneTab):'Empty')+'</div></div>';
      html+='</div>';
      html+='<div class="pane_tools">';
      html+='<button class="btn pane_tool_btn" type="button" data-pane-split="'+pane.id+'">Split Right</button>';
      html+='<button class="btn pane_tool_btn" type="button" data-pane-close="'+pane.id+'"'+(state.panes.length<=1?' disabled':'')+'>Close Pane</button>';
      html+='</div>';
      html+='</div>';
      html+='<div class="pane_tabbar" data-pane-drop="'+pane.id+'">';
      for(var j=0;j<pane.tabIds.length;j++){
        var tab=tabById(pane.tabIds[j]);
        if(!tab){continue;}
        var active=pane.activeTabId===tab.id?' active':'';
        html+='<div class="page_tab'+active+'" data-pane-id="'+pane.id+'" data-tab-id="'+tab.id+'" draggable="true">';
        html+='<button class="page_tab_main" type="button" data-tab-open="'+tab.id+'">'+escapeHtml(tabLabel(tab))+'</button>';
        html+='<button class="page_tab_close" type="button" data-tab-close="'+tab.id+'">×</button>';
        html+='</div>';
      }
      html+='</div>';
      html+=renderPanePage(pane);
      html+='</section>';
    }
    workspaceHost.innerHTML=html;
  }

  function render(){
    renderWorkspace();
    captureWorkspaceChromeState();
    updateModalPrefixViews();
    renderTransferToast();
    renderTransfersPage();
    updateNavBadge();
    attachRenderedEvents();
    applyPendingWorkspaceFocusSpecs();
    persistWorkspaceLocation();
  }

  function closeTab(id){
    if(state.tabs.length===1){return;}
    var pane=paneForTab(id);
    if(pane){
      pane.tabIds=pane.tabIds.filter(function(tabId){return tabId!==id;});
      if(pane.activeTabId===id){
        pane.activeTabId=pane.tabIds[0]||null;
      }
    }
    state.tabs=state.tabs.filter(function(tab){return tab.id!==id;});
    normalizeWorkspace();
    render();
  }

  async function openPrefixInPane(paneId,prefix){
    var snapshot=workspaceSnapshot();
    var pane=workspacePaneSnapshotById(snapshot,paneId);
    var tab=activeWorkspaceTabSnapshotForPane(snapshot,paneId);
    if(!pane||!tab){return;}
    tab.prefix=String(prefix||'');
    pane.activeTabId=tab.id;
    snapshot.activePaneId=pane.id;
    await openWorkspace({mode:WORKSPACE_OPEN_MODE.APPLY,snapshot:snapshot});
  }

  async function openBucketPrefixInNewTab(bucket,prefix,targetPaneId){
    var snapshot=workspaceSnapshot();
    var pane=ensureWorkspacePaneSnapshot(snapshot,targetPaneId||state.activePaneId);
    var tabId=nextWorkspaceTabSnapshotId(snapshot);
    snapshot.tabs.push({id:tabId,bucket:String(bucket),prefix:String(prefix||'')});
    pane.tabIds.push(tabId);
    pane.activeTabId=tabId;
    snapshot.activePaneId=pane.id;
    await openWorkspace({mode:WORKSPACE_OPEN_MODE.APPLY,snapshot:snapshot});
  }

  async function openBucketPrefixInNewPane(bucket,prefix,afterPaneId){
    var snapshot=workspaceSnapshot();
    var pane=createWorkspacePaneSnapshotAfter(snapshot,afterPaneId);
    var tabId=nextWorkspaceTabSnapshotId(snapshot);
    snapshot.tabs.push({id:tabId,bucket:String(bucket),prefix:String(prefix||'')});
    pane.tabIds.push(tabId);
    pane.activeTabId=tabId;
    snapshot.activePaneId=pane.id;
    await openWorkspace({mode:WORKSPACE_OPEN_MODE.APPLY,snapshot:snapshot});
  }

  async function splitActivePane(){
    var pane=activePane();
    var tab=activeTab();
    if(!pane||!tab){return;}
    await openBucketPrefixInNewPane(tab.bucket,tab.prefix,pane.id);
  }

  async function deleteObject(bucket,key){
    if(!window.confirm('Delete this object?')){return;}
    var body=new URLSearchParams();
    body.set('key',key);
    await apiRequest(uiBucketApiPath(bucket,'delete'),{
      method:'POST',
      headers:{'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8'},
      body:body.toString(),
    });
    await refreshAllTabs();
    setNotice('Deleted s3://'+bucket+'/'+key,false);
  }

  async function deleteFolder(bucket,prefix){
    if(!window.confirm('Delete this folder? Only empty folders can be deleted.')){return;}
    var body=new URLSearchParams();
    body.set('prefix',prefix);
    await apiRequest(uiBucketApiPath(bucket,'delete_folder'),{
      method:'POST',
      headers:{'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8'},
      body:body.toString(),
    });
    await refreshAllTabs();
    setNotice('Deleted folder s3://'+bucket+'/'+prefix,false);
  }

  function setClipboard(mode,bucket,key){
    state.clipboard={mode:mode,bucket:bucket,key:key};
    render();
    setNotice(mode+' ready: s3://'+bucket+'/'+key,false);
  }

  function mountPathForBucket(bucket){
    for(var i=0;i<state.tabs.length;i++){
      if(state.tabs[i].bucket===bucket&&state.tabs[i].mount_path){
        return state.tabs[i].mount_path;
      }
    }
    return '';
  }

  function resolveAbsPath(bucket,key){
    var mount=mountPathForBucket(bucket);
    if(!mount){return 's3://'+bucket+'/'+key;}
    var base=mount.replace(/\/+$/,'');
    return base+'/'+(key||'');
  }

  async function transferObject(endpoint,srcBucket,srcKey,dstBucket,dstPrefix){
    var srcAbs=resolveAbsPath(srcBucket,srcKey);
    var dstKey=destinationKeyForTransfer(srcKey,dstPrefix);
    var dstAbs=resolveAbsPath(dstBucket,dstKey);
    if(isSameTransferTarget(srcBucket,srcKey,dstBucket,dstPrefix)){
      setNotice('Source and target are identical. Nothing to do.',false);
      return;
    }
    var action=transferKindLabel(endpoint);
    var msg=action+' object?\n\nSource:\n  '+srcAbs+'\n\nTarget:\n  '+dstAbs;
    if(!window.confirm(msg)){return;}

    var transferId=beginTransfer(endpoint,fileNameFromKey(srcKey)||srcKey,0);
    updateTransfer(transferId,{
      stage:TRANSFER_STAGE.RUNNING,
      summary:'Requesting '+transferKindLabel(endpoint).toLowerCase()+' task',
      detail:'Waiting for gateway',
      sourceBucket:srcBucket,
      sourceKey:srcKey,
      sourcePrefix:parentPrefixForObjectKey(srcKey),
      targetBucket:dstBucket,
      targetKey:dstKey,
      targetPrefix:dstPrefix,
    });
    var body=new URLSearchParams();
    body.set('src_key',srcKey);
    body.set('dst_bucket',dstBucket);
    body.set('dst_prefix',dstPrefix);
    try{
      var snapshot=await apiRequest(uiBucketApiPath(srcBucket,endpoint),{
        method:'POST',
        headers:{'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8'},
        body:body.toString(),
      });
      applyTransferSnapshot(transferId,snapshot);
      snapshot=await waitForTransferCompletion(transferId,snapshot);
      if(snapshot&&snapshot.stage===TRANSFER_STAGE.PAUSED){
        setNotice(transferKindLabel(endpoint)+' paused. Resume it from Transfers.',false);
        return;
      }
      if(snapshot&&snapshot.stage===TRANSFER_STAGE.CANCELLED){
        await refreshAllTabs();
        setNotice(transferKindLabel(endpoint)+' cancelled.',false);
        return;
      }
      if(!snapshot||snapshot.stage!==TRANSFER_STAGE.DONE){
        throw new Error(snapshot&&snapshot.detail?snapshot.detail:(transferKindLabel(endpoint)+' failed'));
      }
      if(
        endpoint==='move'&&
        state.clipboard&&
        state.clipboard.mode===CLIPBOARD_MODE.CUT&&
        state.clipboard.bucket===srcBucket&&
        state.clipboard.key===srcKey
      ){
        state.clipboard=null;
      }
      await refreshAllTabs();
      setNotice(
        (endpoint==='move'?'Moved ':'Copied ')+'s3://'+srcBucket+'/'+srcKey+' -> s3://'+(snapshot&&snapshot.target&&snapshot.target.bucket?snapshot.target.bucket:dstBucket)+'/'+(snapshot&&snapshot.target&&snapshot.target.key?snapshot.target.key:fileNameFromKey(srcKey)),
        false
      );
    }catch(err){
      updateTransfer(transferId,{
        stage:TRANSFER_STAGE.ERROR,
        summary:transferKindLabel(endpoint)+' failed',
        detail:uiErrorMessage(err),
      });
      throw err;
    }
  }

  async function transferObjectToTarget(endpoint,srcBucket,srcKey,target){
    await transferObject(endpoint,srcBucket,srcKey,target.bucket,target.prefix);
  }

  async function transferDraggedObjectToTarget(payload,target){
    await transferObjectToTarget(dragActionLabel(payload.bucket,target.bucket),payload.bucket,payload.key,target);
  }

  async function transferDraggedFolderToTarget(payload,target){
    openFolderTransferModal(payload,target);
  }

  async function transferDraggedPayloadToTarget(payload,target){
    if(payload.kind===DRAG_KIND.OBJECT){
      await transferDraggedObjectToTarget(payload,target);
      return;
    }
    if(payload.kind===DRAG_KIND.FOLDER){
      await transferDraggedFolderToTarget(payload,target);
      return;
    }
    throw new Error('unsupported dragged payload kind: '+String(payload&&payload.kind||''));
  }

  async function pasteIntoTarget(target){
    if(!state.clipboard){
      setNotice('Clipboard is empty.',true);
      return;
    }
    await transferObjectToTarget(
      clipboardActionLabel(state.clipboard),
      state.clipboard.bucket,
      state.clipboard.key,
      target
    );
  }

  async function pasteIntoActiveTarget(){
    var tab=activeTab();
    if(!tab){
      throw new Error('No active page to paste into.');
    }
    await pasteIntoTarget(targetRefForTab(tab));
  }

  async function uploadFileDirectToTarget(bucket,prefix,file){
    var targetBucket=String(bucket||'').trim();
    if(!targetBucket){throw new Error('missing upload bucket');}
    var targetPrefix=String(prefix||'');
    var name=String(file&&file.name||'').trim();
    if(!name){
      throw new Error('missing file name');
    }
    var totalBytes=Math.max(0,Number(file&&file.size)||0);
    var transferId=beginTransfer(TRANSFER_KIND.UPLOAD,name,totalBytes);
    var doneBytes=0;
    function updateDirectUploadProgress(summary,detail,stage,nextDoneBytes){
      doneBytes=Math.max(0,Math.min(totalBytes,Math.max(doneBytes,Number(nextDoneBytes)||0)));
      updateTransfer(transferId,{
        doneBytes:doneBytes,
        totalBytes:totalBytes,
        stage:stage,
        summary:summary,
        detail:detail,
        targetBucket:targetBucket,
        targetPrefix:targetPrefix,
      });
    }
    updateDirectUploadProgress('Uploading '+name,'Starting direct upload',TRANSFER_STAGE.RUNNING,0);
    var body=new FormData();
    body.append('prefix',targetPrefix);
    body.append('file',file,name);
    try{
      var resp=await xhrRequest(uiBucketApiPath(targetBucket,'upload'),{
        method:'POST',
        body:body,
        onUploadProgress:function(progressEvent){
          var loaded=Math.max(0,Number(progressEvent&&progressEvent.loaded)||0);
          var detail=formatProgressText(loaded,totalBytes);
          updateDirectUploadProgress('Uploading '+name,detail,TRANSFER_STAGE.RUNNING,loaded);
        },
      });
      var result=resp&&resp.body?JSON.parse(resp.body):{};
      var storedKey=result&&result.key?String(result.key):((targetPrefix||'')+name);
      var storedPrefix=result&&result.prefix!==undefined&&result.prefix!==null?String(result.prefix):targetPrefix;
      updateTransfer(transferId,{
        doneBytes:totalBytes,
        totalBytes:totalBytes,
        stage:TRANSFER_STAGE.DONE,
        summary:'Upload completed',
        detail:storedKey,
        targetBucket:targetBucket,
        targetKey:storedKey,
        targetPrefix:storedPrefix,
      });
      return result;
    }catch(err){
      updateTransfer(transferId,{
        doneBytes:doneBytes,
        totalBytes:totalBytes,
        stage:TRANSFER_STAGE.ERROR,
        summary:'Upload failed',
        detail:uiErrorMessage(err),
        targetBucket:targetBucket,
        targetPrefix:targetPrefix,
      });
      throw err;
    }
  }

  async function uploadDroppedFilesToTarget(target,files){
    if(!target||!target.bucket){
      throw new Error('missing upload target');
    }
    var dropFiles=Array.isArray(files)?files.filter(function(file){return !!file;}):[];
    if(!dropFiles.length){return;}
    var prefixText=prefixLabel(target.prefix);
    setNotice('Uploading '+dropFiles.length+' file'+(dropFiles.length>1?'s':'')+' to '+prefixText+' ...',false);
    var uploadedKeys=[];
    for(var i=0;i<dropFiles.length;i++){
      var result=await uploadFileDirectToTarget(target.bucket,target.prefix,dropFiles[i]);
      if(result&&result.key){uploadedKeys.push(String(result.key));}
    }
    await refreshAllTabs();
    if(uploadedKeys.length===1){
      setNotice('Upload completed: '+uploadedKeys[0],false);
      return;
    }
    setNotice('Uploaded '+uploadedKeys.length+' files to '+prefixText+'.',false);
  }

  async function submitMkdir(ev){
    ev.preventDefault();
    var tab=activeTab();
    if(!tab){throw new Error('no active tab');}
    var formData=new FormData(mkdirForm);
    var body=new URLSearchParams();
    body.set('prefix',String(formData.get('prefix')||''));
    body.set('name',String(formData.get('name')||''));
    await apiRequest(uiBucketApiPath(tab.bucket,'mkdir'),{
      method:'POST',
      headers:{'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8'},
      body:body.toString(),
    });
    closeModal('mkdir_modal');
    mkdirForm.reset();
    await refreshAllTabs();
    setNotice('Folder created.',false);
  }

  function completeMultipartXml(parts){
    var xml='<?xml version="1.0" encoding="UTF-8"?>';
    xml+='<CompleteMultipartUpload>';
    for(var i=0;i<parts.length;i++){
      xml+='<Part><PartNumber>'+parts[i].partNumber+'</PartNumber><ETag>'+xmlEscape(parts[i].etag)+'</ETag></Part>';
    }
    xml+='</CompleteMultipartUpload>';
    return xml;
  }

  async function submitUpload(ev){
    ev.preventDefault();
    if(!uploadForm){throw new Error('upload form missing');}
    var tab=activeTab();
    if(!tab){throw new Error('no active tab');}
    var fileInput=uploadForm.querySelector('input[name="file"]');
    if(!fileInput||!fileInput.files||!fileInput.files.length){
      throw new Error('select a file first');
    }
    var file=fileInput.files[0];
    var formData=new FormData(uploadForm);
    var prefix=String(formData.get('prefix')||'');
    var name=String(file.name||'').trim();
    if(!name){
      throw new Error('missing file name');
    }

    var controls=uploadForm.querySelectorAll('button,input');
    setBusy(controls,true);
    setUploadStatus('Preparing upload',0,file.size,'Creating multipart session',TRANSFER_STAGE.RUNNING);
    setNotice('Uploading '+name+' ...',false);

    var createBody=new URLSearchParams();
    createBody.set('prefix',prefix);
    createBody.set('name',name);

    var created=null;
    var transferId=beginTransfer(TRANSFER_KIND.UPLOAD,name,file.size);
    var uploadedBytes=0;
    try{
      created=await apiRequest(uiBucketApiPath(tab.bucket,'multipart/create'),{
        method:'POST',
        headers:{'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8'},
        body:createBody.toString(),
      });
      updateTransfer(transferId,{
        targetBucket:tab.bucket,
        targetKey:created.key,
        targetPrefix:created.prefix,
      });

      var parts=splitByteRanges(file.size,MULTIPART_UPLOAD_PART_BYTES);
      var completedParts=0;
      var partLoaded={};

      function setPartLoaded(partNumber,totalPartBytes,loadedBytes){
        var nextLoaded=Math.min(totalPartBytes,Math.max(0,Number(loadedBytes)||0));
        var prevLoaded=partLoaded[partNumber]||0;
        if(nextLoaded===prevLoaded){return;}
        partLoaded[partNumber]=nextLoaded;
        uploadedBytes+=nextLoaded-prevLoaded;
      }

      function updateUploadProgress(){
        var msg='Uploading '+name;
        var detail='Parts '+completedParts+' / '+parts.length+' | '+formatBytes(uploadedBytes)+' / '+formatBytes(file.size);
        setUploadStatus(msg,uploadedBytes,file.size,detail,TRANSFER_STAGE.RUNNING);
        setNotice(msg,false);
        updateTransfer(transferId,{
          doneBytes:uploadedBytes,
          totalBytes:file.size,
          stage:TRANSFER_STAGE.RUNNING,
          summary:msg,
          detail:detail,
        });
      }
      updateUploadProgress();

      var uploadedParts=await runLimitedWorkers(parts,MULTIPART_UPLOAD_MAX_INFLIGHT,async function(part){
        var partResp=await xhrRequest(
          uiBucketApiPath(tab.bucket,'multipart/'+encodeURIComponent(created.upload_id)+'/part/'+part.partNumber),
          {
            method:'PUT',
            headers:{'Content-Type':'application/octet-stream'},
            responseType:'text',
            body:file.slice(part.start,part.endExclusive),
            onUploadProgress:function(progressEvent){
              setPartLoaded(part.partNumber,part.size,progressEvent.loaded);
              updateUploadProgress();
            },
          }
        );
        var partBody=partResp.body?JSON.parse(partResp.body):{};
        setPartLoaded(part.partNumber,part.size,part.size);
        completedParts+=1;
        updateUploadProgress();
        var etag=partBody.etag||partResp.etag;
        if(!etag){throw new Error('missing ETag for part '+part.partNumber);}
        return {partNumber:part.partNumber,etag:etag};
      });

      var finalizedBytes=file.size>0?file.size:1;
      var finalDetail='Completing object commit on gateway';
      setUploadStatus('Finalizing upload',finalizedBytes,file.size,finalDetail,TRANSFER_STAGE.RUNNING);
      updateTransfer(transferId,{
        doneBytes:finalizedBytes,
        totalBytes:file.size,
        stage:TRANSFER_STAGE.RUNNING,
        summary:'Finalizing upload',
        detail:finalDetail,
      });
      await requestOk(
        uiBucketApiPath(tab.bucket,'multipart/'+encodeURIComponent(created.upload_id)+'/complete'),
        {
          method:'POST',
          headers:{'Content-Type':'application/xml'},
          body:completeMultipartXml(uploadedParts),
        }
      );

      closeModal('upload_modal');
      uploadForm.reset();
      updateModalPrefixViews();
      await refreshAllTabs();
      setUploadStatus('Upload completed',finalizedBytes,file.size,'Stored at '+created.key,TRANSFER_STAGE.DONE);
      updateTransfer(transferId,{
        doneBytes:finalizedBytes,
        totalBytes:file.size,
        stage:TRANSFER_STAGE.DONE,
        summary:'Upload completed',
        detail:created.key,
        targetBucket:tab.bucket,
        targetKey:created.key,
        targetPrefix:created.prefix,
      });
      setNotice('Upload completed: '+created.key,false);
    }catch(err){
      if(created&&created.upload_id&&created.key){
        try{
          await requestOk(uiBucketApiPath(tab.bucket,'multipart/'+encodeURIComponent(created.upload_id)),{method:'DELETE'});
        }catch(_ignore){}
      }
      setUploadStatus('Upload failed',uploadedBytes,file.size,err&&err.message?err.message:String(err),TRANSFER_STAGE.ERROR);
      updateTransfer(transferId,{
        doneBytes:uploadedBytes,
        totalBytes:file.size,
        stage:TRANSFER_STAGE.ERROR,
        summary:'Upload failed',
        detail:err&&err.message?err.message:String(err),
      });
      throw err;
    }finally{
      setBusy(controls,false);
    }
  }

  function populateOpenBucketOptions(){
    if(!openBucketSelect){return;}
    var currentValue=openBucketSelect.value;
    var html='';
    for(var i=0;i<state.availableBuckets.length;i++){
      html+='<option value="'+escapeHtml(state.availableBuckets[i])+'">'+escapeHtml(state.availableBuckets[i])+'</option>';
    }
    openBucketSelect.innerHTML=html;
    if(currentValue&&bucketOptionExists(currentValue)){
      openBucketSelect.value=currentValue;
    }else if(state.availableBuckets.length){
      openBucketSelect.value=state.availableBuckets[0];
    }
  }

  function dragPayloadForFolder(bucket,prefix){
    return JSON.stringify({kind:DRAG_KIND.FOLDER,bucket:bucket,prefix:prefix});
  }

  function dragPayloadForObject(bucket,key){
    return JSON.stringify({kind:DRAG_KIND.OBJECT,bucket:bucket,key:key});
  }

  function dragPayloadForTab(tabId){
    return JSON.stringify({kind:DRAG_KIND.TAB,tabId:tabId});
  }

  function parseDragPayload(raw){
    if(!raw){throw new Error('missing dragged payload');}
    var payload;
    try{
      payload=JSON.parse(raw);
    }catch(_ignore){
      throw new Error('invalid dragged payload');
    }
    if(!payload||!payload.kind){
      throw new Error('incomplete dragged payload');
    }
    if(payload.kind===DRAG_KIND.OBJECT){
      if(!payload.bucket||!payload.key){
        throw new Error('incomplete dragged object payload');
      }
      return payload;
    }
    if(payload.kind===DRAG_KIND.FOLDER){
      if(!payload.bucket||!payload.prefix){
        throw new Error('incomplete dragged folder payload');
      }
      return payload;
    }
    if(payload.kind===DRAG_KIND.TAB){
      if(!payload.tabId||!tabById(Number(payload.tabId))){
        throw new Error('invalid dragged page payload');
      }
      payload.tabId=Number(payload.tabId);
      return payload;
    }
    throw new Error('unknown dragged payload kind');
  }

  function parseDropPayloadOrNotify(ev){
    try{
      return parseDragPayload(ev&&ev.dataTransfer?ev.dataTransfer.getData('text/plain'):'');
    }catch(_ignore){
      setNotice(
        'Failed to read dragged item. Start the drag from a FluxonFS page tab, folder, or object row and drop it again.',
        true
      );
      return null;
    }
  }

  function openFolderTransferModal(payload,target){
    if(!folderTransferForm){throw new Error('folder transfer form missing');}
    if(!payload||payload.kind!==DRAG_KIND.FOLDER){throw new Error('invalid dragged folder payload');}
    if(!target||!target.bucket){throw new Error('missing transfer target');}
    if(String(payload.bucket||'')===String(target.bucket||'')){
      throw new Error('folder drag transfer currently requires different source and target exports');
    }
    if(state.transferEnabled!==true){
      openTransferUnavailableModal(payload,target);
      return;
    }
    var srcRootRelpath=rootRelpathFromFolderPrefix(payload.prefix);
    var dstRootRelpath=destinationRootRelpathForFolder(payload.prefix,target.prefix);
    if(folderTransferSrcExportInput){folderTransferSrcExportInput.value=String(payload.bucket||'');}
    if(folderTransferSrcRootRelpathInput){folderTransferSrcRootRelpathInput.value=srcRootRelpath;}
    if(folderTransferDstExportInput){folderTransferDstExportInput.value=String(target.bucket||'');}
    if(folderTransferDstRootRelpathInput){folderTransferDstRootRelpathInput.value=dstRootRelpath;}
    if(folderTransferSrcView){
      folderTransferSrcView.textContent=String(payload.bucket||'')+':'+rootRelpathLabel(srcRootRelpath);
    }
    if(folderTransferDstView){
      folderTransferDstView.textContent=String(target.bucket||'')+':'+rootRelpathLabel(dstRootRelpath);
    }
    if(folderTransferScanConcurrencyInput){
      folderTransferScanConcurrencyInput.value=String(DEFAULT_TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY);
    }
    if(folderTransferWorkerCountInput){folderTransferWorkerCountInput.value='';}
    if(folderTransferBatchReadyBytesInput){folderTransferBatchReadyBytesInput.value='';}
    openModal('folder_transfer_modal');
  }

  function openTransferUnavailableModal(payload,target){
    if(!payload||payload.kind!==DRAG_KIND.FOLDER){throw new Error('invalid dragged folder payload');}
    if(!target||!target.bucket){throw new Error('missing transfer target');}
    var srcRootRelpath=rootRelpathFromFolderPrefix(payload.prefix);
    var dstRootRelpath=destinationRootRelpathForFolder(payload.prefix,target.prefix);
    if(transferUnavailableSrcView){
      transferUnavailableSrcView.textContent=String(payload.bucket||'')+':'+rootRelpathLabel(srcRootRelpath);
    }
    if(transferUnavailableDstView){
      transferUnavailableDstView.textContent=String(target.bucket||'')+':'+rootRelpathLabel(dstRootRelpath);
    }
    if(transferUnavailableReasonView){
      transferUnavailableReasonView.textContent=transferUnavailableReasonText();
    }
    openModal('transfer_unavailable_modal');
  }

  async function submitFolderTransfer(ev){
    ev.preventDefault();
    if(!folderTransferForm){throw new Error('folder transfer form missing');}
    var formData=new FormData(folderTransferForm);
    var desiredScanConcurrency=String(formData.get('desired_scan_concurrency')||'').trim();
    var desiredWorkerCount=String(formData.get('desired_worker_count')||'').trim();
    var batchReadyBytes=String(formData.get('batch_ready_bytes')||'').trim();
    if(desiredScanConcurrency===''){throw new Error('scan concurrency target is required');}
    if(desiredWorkerCount===''){throw new Error('desired worker count is required');}
    if(batchReadyBytes===''){throw new Error('batch ready bytes is required');}
    var body=new URLSearchParams();
    body.set('src_export',String(formData.get('src_export')||''));
    body.set('src_root_relpath',String(formData.get('src_root_relpath')||''));
    body.set('dst_export',String(formData.get('dst_export')||''));
    body.set('dst_root_relpath',String(formData.get('dst_root_relpath')||''));
    body.set('desired_scan_concurrency',desiredScanConcurrency);
    body.set('desired_worker_count',desiredWorkerCount);
    body.set('batch_ready_bytes',batchReadyBytes);
    var resp=await apiRequest(transferJobsApiPath(),{
      method:'POST',
      headers:{'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8'},
      body:body.toString(),
    });
    closeModal('folder_transfer_modal');
    transferJobState.selectedJobId=resp&&resp.job&&resp.job.job_id?String(resp.job.job_id):'';
    setNotice('Created FluxonFS folder transfer job '+String(resp&&resp.job&&resp.job.job_id||'')+'.',false);
  }

  function paneIdFromElement(el){
    var paneEl=el&&el.closest('[data-pane-id]');
    return paneEl?Number(paneEl.getAttribute('data-pane-id')):0;
  }

  function attachRenderedEvents(){
    closeContextMenu();
    var paneShells=document.querySelectorAll('.pane_shell');
    for(var p=0;p<paneShells.length;p++){
      paneShells[p].addEventListener('mousedown',function(ev){
        var paneId=paneIdFromElement(ev.currentTarget);
        if(paneId){state.activePaneId=paneId;}
      });
    }
    var paneSplitBtns=document.querySelectorAll('[data-pane-split]');
    for(var s=0;s<paneSplitBtns.length;s++){
      paneSplitBtns[s].addEventListener('click',function(ev){
        var paneId=Number(ev.currentTarget.getAttribute('data-pane-split'));
        var tab=activeTabForPane(paneId);
        if(!tab){return;}
        state.activePaneId=paneId;
        startUiTask(function(){return openBucketPrefixInNewPane(tab.bucket,tab.prefix,paneId);});
      });
    }
    var paneCloseBtns=document.querySelectorAll('[data-pane-close]');
    for(var q=0;q<paneCloseBtns.length;q++){
      paneCloseBtns[q].addEventListener('click',function(ev){
        var paneId=Number(ev.currentTarget.getAttribute('data-pane-close'));
        collapsePane(paneId);
        render();
      });
    }
    var openTabs=document.querySelectorAll('[data-tab-open]');
    for(var i=0;i<openTabs.length;i++){
      openTabs[i].addEventListener('click',function(ev){
        var paneId=paneIdFromElement(ev.currentTarget);
        setActivePaneTab(paneId,Number(ev.currentTarget.getAttribute('data-tab-open')));
        render();
      });
    }
    var closeTabs=document.querySelectorAll('[data-tab-close]');
    for(var j=0;j<closeTabs.length;j++){
      closeTabs[j].addEventListener('click',function(ev){
        ev.stopPropagation();
        closeTab(Number(ev.currentTarget.getAttribute('data-tab-close')));
      });
    }
    var tabEls=document.querySelectorAll('.page_tab');
    for(var k=0;k<tabEls.length;k++){
      tabEls[k].addEventListener('dragstart',function(ev){
        var tabId=Number(ev.currentTarget.getAttribute('data-tab-id'));
        ev.dataTransfer.setData('text/plain',dragPayloadForTab(tabId));
        ev.dataTransfer.effectAllowed='move';
      });
      tabEls[k].addEventListener('dragover',function(ev){ev.preventDefault();ev.currentTarget.classList.add('drop_target');});
      tabEls[k].addEventListener('dragleave',function(ev){ev.currentTarget.classList.remove('drop_target');});
      tabEls[k].addEventListener('drop',function(ev){
        if(!claimDropEvent(ev)){return;}
        ev.currentTarget.classList.remove('drop_target');
        var payload=parseDropPayloadOrNotify(ev);
        if(!payload){return;}
        var paneId=paneIdFromElement(ev.currentTarget);
        var tabId=Number(ev.currentTarget.getAttribute('data-tab-id'));
        var tab=tabById(tabId);
        if(!tab){return;}
        if(payload.kind===DRAG_KIND.TAB){
          if(payload.tabId===tabId){return;}
          moveTabToPane(payload.tabId,paneId,tabId);
          render();
          return;
        }
        startUiTask(function(){return transferDraggedPayloadToTarget(payload,targetRefForTab(tab));});
      });
      tabEls[k].addEventListener('contextmenu',function(ev){
        ev.preventDefault();
        ev.stopPropagation();
        var paneId=paneIdFromElement(ev.currentTarget);
        var tabId=Number(ev.currentTarget.getAttribute('data-tab-id'));
        var tab=tabById(tabId);
        if(!tab){return;}
        var items=[
          {
            id:'move_to_new_pane',
            label:'Move To New Pane',
            run:function(id){return function(){moveTabToNewPane(id);render();};}(tabId),
          },
          {
            id:'split_right',
            label:'Split Right',
            run:runUiTask(function(bucket,prefix,targetPaneId){return function(){return openBucketPrefixInNewPane(bucket,prefix,targetPaneId);};}(tab.bucket,tab.prefix,paneId)),
          }
        ];
        if(state.clipboard){
          items.push(pasteMenuItemForTarget(targetRefForTab(tab),'Paste Here'));
        }
        openContextMenu(ev.clientX,ev.clientY,items);
      });
    }

    var paneDrops=document.querySelectorAll('[data-pane-drop]');
    for(var n=0;n<paneDrops.length;n++){
      paneDrops[n].addEventListener('dragover',function(ev){
        ev.preventDefault();
        setPaneShellDropTarget(ev.currentTarget,true);
      });
      paneDrops[n].addEventListener('dragleave',function(ev){
        setPaneShellDropTarget(ev.currentTarget,false);
      });
      paneDrops[n].addEventListener('drop',function(ev){
        if(!claimDropEvent(ev)){return;}
        setPaneShellDropTarget(ev.currentTarget,false);
        var payload=parseDropPayloadOrNotify(ev);
        if(!payload){return;}
        var paneId=Number(ev.currentTarget.getAttribute('data-pane-drop'));
        if(payload.kind===DRAG_KIND.TAB){
          moveTabToPane(payload.tabId,paneId,null);
          render();
          return;
        }
        var tab=activeTabForPane(paneId);
        if(!tab){return;}
        startUiTask(function(){return transferDraggedPayloadToTarget(payload,targetRefForTab(tab));});
      });
    }

    var navBtns=document.querySelectorAll('[data-nav]');
    for(var a=0;a<navBtns.length;a++){
      navBtns[a].addEventListener('click',function(ev){
        var paneId=paneIdFromElement(ev.currentTarget);
        var tab=activeTabForPane(paneId);
        if(!tab){return;}
        state.activePaneId=paneId;
        var mode=ev.currentTarget.getAttribute('data-nav');
        var nextPrefix=mode==='up'?tab.parent_prefix:'';
        startUiTask(function(){return openPrefixInPane(paneId,nextPrefix||'');});
      });
    }

    var openFolderBtns=document.querySelectorAll('[data-open-folder]');
    for(var b=0;b<openFolderBtns.length;b++){
      openFolderBtns[b].addEventListener('click',function(ev){
        ev.preventDefault();
        var paneId=paneIdFromElement(ev.currentTarget);
        state.activePaneId=paneId;
        startUiTask(function(){return openPrefixInPane(paneId,ev.currentTarget.getAttribute('data-open-folder'));});
      });
    }
    var openFolderPageBtns=document.querySelectorAll('[data-open-folder-new]');
    for(var c=0;c<openFolderPageBtns.length;c++){
      openFolderPageBtns[c].addEventListener('click',function(ev){
        ev.preventDefault();
        var paneId=paneIdFromElement(ev.currentTarget);
        var tab=activeTabForPane(paneId);
        if(!tab){return;}
        state.activePaneId=paneId;
        startUiTask(function(){return openBucketPrefixInNewTab(tab.bucket,ev.currentTarget.getAttribute('data-open-folder-new'),paneId);});
      });
    }

    var pageTables=document.querySelectorAll('.table_drop_target');
    for(var c=0;c<pageTables.length;c++){
      pageTables[c].addEventListener('dragover',function(ev){
        ev.preventDefault();
        if(dataTransferHasFiles(ev.dataTransfer)){ev.dataTransfer.dropEffect='copy';}
        setPaneShellDropTarget(ev.currentTarget,true);
      });
      pageTables[c].addEventListener('dragleave',function(ev){
        setPaneShellDropTarget(ev.currentTarget,false);
      });
      pageTables[c].addEventListener('drop',function(ev){
        if(!claimDropEvent(ev)){return;}
        setPaneShellDropTarget(ev.currentTarget,false);
        var paneId=Number(paneIdFromElement(ev.currentTarget));
        if(dataTransferHasFiles(ev.dataTransfer)){
          var tabForFiles=activeTabForPane(paneId);
          if(!tabForFiles){return;}
          var files=dataTransferFiles(ev.dataTransfer);
          startUiTask(function(){return uploadDroppedFilesToTarget(targetRefForTab(tabForFiles),files);});
          return;
        }
        var payload=parseDropPayloadOrNotify(ev);
        if(!payload){return;}
        if(payload.kind===DRAG_KIND.TAB){
          moveTabToPane(payload.tabId,paneId,null);
          render();
          return;
        }
        var tab=activeTabForPane(paneId);
        if(!tab){return;}
        startUiTask(function(){return transferDraggedPayloadToTarget(payload,targetRefForTab(tab));});
      });
    }

    var folderRows=document.querySelectorAll('[data-folder-prefix]');
    for(var d=0;d<folderRows.length;d++){
      folderRows[d].addEventListener('dragstart',function(ev){
        var bucket=ev.currentTarget.getAttribute('data-folder-bucket');
        var prefix=ev.currentTarget.getAttribute('data-folder-prefix');
        ev.dataTransfer.setData('text/plain',dragPayloadForFolder(bucket,prefix));
        ev.dataTransfer.effectAllowed='copy';
      });
      folderRows[d].addEventListener('dragover',function(ev){
        ev.preventDefault();
        if(dataTransferHasFiles(ev.dataTransfer)){ev.dataTransfer.dropEffect='copy';}
        ev.currentTarget.classList.add('drop_target');
      });
      folderRows[d].addEventListener('dragleave',function(ev){ev.currentTarget.classList.remove('drop_target');});
      folderRows[d].addEventListener('drop',function(ev){
        if(!claimDropEvent(ev)){return;}
        ev.currentTarget.classList.remove('drop_target');
        if(dataTransferHasFiles(ev.dataTransfer)){
          var target=targetRef(
            ev.currentTarget.getAttribute('data-folder-bucket'),
            ev.currentTarget.getAttribute('data-folder-prefix')
          );
          var files=dataTransferFiles(ev.dataTransfer);
          startUiTask(function(){return uploadDroppedFilesToTarget(target,files);});
          return;
        }
        var payload=parseDropPayloadOrNotify(ev);
        if(!payload){return;}
        var dstBucket=ev.currentTarget.getAttribute('data-folder-bucket');
        var dstPrefix=ev.currentTarget.getAttribute('data-folder-prefix');
        startUiTask(function(){return transferDraggedPayloadToTarget(payload,targetRef(dstBucket,dstPrefix));});
      });
      folderRows[d].addEventListener('contextmenu',function(ev){
        ev.preventDefault();
        var paneId=paneIdFromElement(ev.currentTarget);
        var dstBucket=ev.currentTarget.getAttribute('data-folder-bucket');
        var dstPrefix=ev.currentTarget.getAttribute('data-folder-prefix');
        var items=[
          {id:'open',label:'Open',run:runUiTask(function(targetPaneId,prefix){return function(){state.activePaneId=targetPaneId;return openPrefixInPane(targetPaneId,prefix);};}(paneId,dstPrefix))},
          {id:'page',label:'Open in Page',run:runUiTask(function(bucket,prefix,targetPaneId){return function(){state.activePaneId=targetPaneId;return openBucketPrefixInNewTab(bucket,prefix,targetPaneId);};}(dstBucket,dstPrefix,paneId))}
        ];
        if(state.clipboard){
          items.push(pasteMenuItemForTarget(targetRef(dstBucket,dstPrefix),'Paste Here'));
        }
        openContextMenu(ev.clientX,ev.clientY,items);
      });
    }

    var fileRows=document.querySelectorAll('[data-file-key]');
    for(var e=0;e<fileRows.length;e++){
      fileRows[e].addEventListener('dragstart',function(ev){
        var bucket=ev.currentTarget.getAttribute('data-file-bucket');
        var key=ev.currentTarget.getAttribute('data-file-key');
        ev.dataTransfer.setData('text/plain',dragPayloadForObject(bucket,key));
        ev.dataTransfer.effectAllowed='copyMove';
      });
      fileRows[e].addEventListener('contextmenu',function(ev){
        ev.preventDefault();
        var bucket=ev.currentTarget.getAttribute('data-file-bucket');
        var key=ev.currentTarget.getAttribute('data-file-key');
        var items=[
          {id:'copy',label:'Copy',run:function(){setClipboard(CLIPBOARD_MODE.COPY,bucket,key);}},
          {id:'cut',label:'Cut',run:function(){setClipboard(CLIPBOARD_MODE.CUT,bucket,key);}}
        ];
        if(state.clipboard){
          items.push(pasteMenuItemForTarget(targetRefForObjectDirectory(bucket,key),'Paste Into This Directory'));
        }
        openContextMenu(ev.clientX,ev.clientY,items);
      });
    }

    var deleteBtns=document.querySelectorAll('[data-delete-key]');
    for(var f=0;f<deleteBtns.length;f++){
      deleteBtns[f].addEventListener('click',function(ev){
        deleteObject(
          ev.currentTarget.getAttribute('data-delete-bucket'),
          ev.currentTarget.getAttribute('data-delete-key')
        ).catch(function(err){setNotice(uiErrorMessage(err),true);});
      });
    }

    var deleteFolderBtns=document.querySelectorAll('[data-delete-folder-prefix]');
    for(var g=0;g<deleteFolderBtns.length;g++){
      deleteFolderBtns[g].addEventListener('click',function(ev){
        deleteFolder(
          ev.currentTarget.getAttribute('data-delete-folder-bucket'),
          ev.currentTarget.getAttribute('data-delete-folder-prefix')
        ).catch(function(err){setNotice(uiErrorMessage(err),true);});
      });
    }

    var pageSurfaces=document.querySelectorAll('[data-page-surface]');
    for(var h=0;h<pageSurfaces.length;h++){
      pageSurfaces[h].addEventListener('dragover',function(ev){
        if(eventTargetsWorkspaceTable(ev)){return;}
        ev.preventDefault();
        if(dataTransferHasFiles(ev.dataTransfer)){ev.dataTransfer.dropEffect='copy';}
        setPaneShellDropTarget(ev.currentTarget,true);
      });
      pageSurfaces[h].addEventListener('dragleave',function(ev){
        if(eventTargetsWorkspaceTable(ev)){return;}
        setPaneShellDropTarget(ev.currentTarget,false);
      });
      pageSurfaces[h].addEventListener('drop',function(ev){
        if(eventTargetsWorkspaceTable(ev)){return;}
        if(!claimDropEvent(ev)){return;}
        setPaneShellDropTarget(ev.currentTarget,false);
        if(dataTransferHasFiles(ev.dataTransfer)){
          var paneIdForFiles=paneIdFromElement(ev.currentTarget);
          var tabForFiles=activeTabForPane(paneIdForFiles);
          if(!tabForFiles){return;}
          var files=dataTransferFiles(ev.dataTransfer);
          startUiTask(function(){return uploadDroppedFilesToTarget(targetRefForTab(tabForFiles),files);});
          return;
        }
        var payload=parseDropPayloadOrNotify(ev);
        if(!payload){return;}
        var paneId=paneIdFromElement(ev.currentTarget);
        if(payload.kind===DRAG_KIND.TAB){
          moveTabToPane(payload.tabId,paneId,null);
          render();
          return;
        }
        var tab=activeTabForPane(paneId);
        if(!tab){return;}
        startUiTask(function(){return transferDraggedPayloadToTarget(payload,targetRefForTab(tab));});
      });
      pageSurfaces[h].addEventListener('contextmenu',function(ev){
        if(ev.target&&ev.target.closest('[data-file-key],[data-folder-prefix],.page_tab')){return;}
        ev.preventDefault();
        var paneId=paneIdFromElement(ev.currentTarget);
        var tab=activeTabForPane(paneId);
        if(!tab){return;}
        var items=[];
        if(state.clipboard){
          items.push(pasteMenuItemForTarget(targetRefForTab(tab),'Paste Here'));
        }
        openContextMenu(ev.clientX,ev.clientY,items);
      });
    }

    var pasteClipboardBtn=qs('paste_clipboard_btn');
    if(pasteClipboardBtn){
      pasteClipboardBtn.addEventListener('click',runUiTask(pasteIntoActiveTarget));
    }

    var clearClipboardBtn=qs('clear_clipboard_btn');
    if(clearClipboardBtn){
      clearClipboardBtn.addEventListener('click',function(){state.clipboard=null;render();setNotice('Clipboard cleared.',false);});
    }
  }

  if(searchInput){searchInput.addEventListener('input',render);}
  document.addEventListener('click',function(ev){if(menu&&menu.classList.contains('open')&&!menu.contains(ev.target)){closeContextMenu();}});
  document.addEventListener('keydown',function(ev){
    if(ev.key==='Escape'){
      closeContextMenu();
      if(currentPageMode===transferPageMode.TRANSFERS){
        closeTransfersSurface();
        return;
      }
      closeModal('open_bucket_modal');
      closeModal('mkdir_modal');
      closeModal('upload_modal');
      closeModal('folder_transfer_modal');
      closeModal('transfer_unavailable_modal');
      closeModal('transfer_prescan_import_modal');
      closeModal('fs_master_export_modal');
    }
  });
  window.addEventListener('scroll',closeContextMenu,true);
  window.addEventListener('resize',closeContextMenu);

  attachWorkspaceChromeEvents();
  if(openBucketForm){
    openBucketForm.addEventListener('submit',function(ev){
      ev.preventDefault();
      var bucket=openBucketSelect?String(openBucketSelect.value||''):'';
      var prefix=openBucketPrefixInput?String(openBucketPrefixInput.value||''):'';
      if(!bucket){
        setNotice('select a bucket first',true);
        return;
      }
      if(prefix.indexOf('/')===0){
        setNotice('prefix must not start with "/"',true);
        return;
      }
      if(prefix&&prefix.charAt(prefix.length-1)!=='/'){
        setNotice('prefix must end with "/" when non-empty',true);
        return;
      }
      openBucketPrefixInNewTab(bucket,prefix,state.activePaneId).then(function(){
        closeModal('open_bucket_modal');
      }).catch(function(err){setNotice(err.message,true);});
    });
  }
  if(mkdirForm){mkdirForm.addEventListener('submit',function(ev){submitMkdir(ev).catch(function(err){setNotice(err.message,true);});});}
  if(uploadForm){uploadForm.addEventListener('submit',function(ev){submitUpload(ev).catch(function(err){setNotice(err.message,true);});});}
  if(folderTransferForm){
    folderTransferForm.addEventListener('submit',function(ev){
      submitFolderTransfer(ev).catch(function(err){setNotice(err.message,true);});
    });
  }
  if(transferPrescanImportForm){
    transferPrescanImportForm.addEventListener('submit',function(ev){
      submitTransferPrescanImport(ev).catch(function(err){setNotice(err.message,true);});
    });
  }

  async function bootWorkspace(){
    var bootstrap=JSON.parse(bootEl.textContent||'{}');
    state.availableBuckets=Array.isArray(bootstrap.available_buckets)?bootstrap.available_buckets.slice().sort():[];
    state.transferEnabled=bootstrap.transfer_enabled===true;
    var initial=bootstrap.initial_tab||{};
    ensureKnownBucket(initial.bucket);
    populateOpenBucketOptions();
    pendingWorkspaceFocusSpecs=consumePendingWorkspaceFocusSpecs();
    var restored=await restoreWorkspaceStateFromLocation();
    if(!restored){
      state.tabs=[Object.assign({id:1},initial)];
      state.panes=[{id:1,tabIds:[1],activeTabId:1}];
      state.activePaneId=1;
      state.nextPaneId=2;
      state.nextTabId=2;
      normalizeWorkspace();
    }
    render();
    startServerTransferSyncLoop();
    startTransferPrescanSyncLoop();
    startTransferJobSyncLoop();
    if(/\/transfers\/?$/.test(window.location.pathname||'')){
      applyPageMode(transferPageMode.TRANSFERS,'replace');
    }else{
      applyPageMode(transferPageMode.WORKSPACE,'replace');
    }
  }

  bootWorkspace().catch(function(err){
    console.warn('workspace boot failed',err);
    var bootstrap=JSON.parse(bootEl.textContent||'{}');
    state.availableBuckets=Array.isArray(bootstrap.available_buckets)?bootstrap.available_buckets.slice().sort():[];
    state.transferEnabled=bootstrap.transfer_enabled===true;
    var initial=bootstrap.initial_tab||{};
    ensureKnownBucket(initial.bucket);
    populateOpenBucketOptions();
    pendingWorkspaceFocusSpecs=consumePendingWorkspaceFocusSpecs();
    state.tabs=[Object.assign({id:1},initial)];
    state.panes=[{id:1,tabIds:[1],activeTabId:1}];
    state.activePaneId=1;
    state.nextPaneId=2;
    state.nextTabId=2;
    normalizeWorkspace();
    render();
    startServerTransferSyncLoop();
    startTransferPrescanSyncLoop();
    startTransferJobSyncLoop();
    if(/\/transfers\/?$/.test(window.location.pathname||'')){
      applyPageMode(transferPageMode.TRANSFERS,'replace');
    }else{
      applyPageMode(transferPageMode.WORKSPACE,'replace');
    }
  });

  window.addEventListener('popstate',function(){
    if(!bootEl){return;}
    if(/\/transfers\/?$/.test(window.location.pathname||'')){
      applyPageMode(transferPageMode.TRANSFERS,null);
      return;
    }
    applyPageMode(transferPageMode.WORKSPACE,null);
  });
})();
"##;
