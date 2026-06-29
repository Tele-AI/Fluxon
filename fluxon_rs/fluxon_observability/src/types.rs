#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FluxonMemberKind {
    Kv,
    Mq,
    Fs,
    Rpc,
}

impl FluxonMemberKind {
    pub fn as_str(self) -> &'static str {
        match self {
            FluxonMemberKind::Kv => "kv",
            FluxonMemberKind::Mq => "mq",
            FluxonMemberKind::Fs => "fs",
            FluxonMemberKind::Rpc => "rpc",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FluxonMemberRole {
    Master,
    Broker,
    OwnerClient,
    ExternalClient,
    SideTransferWorker,
    Unknown,
}

impl FluxonMemberRole {
    pub fn as_str(self) -> &'static str {
        match self {
            FluxonMemberRole::Master => "master",
            FluxonMemberRole::Broker => "broker",
            FluxonMemberRole::OwnerClient => "owner_client",
            FluxonMemberRole::ExternalClient => "external_client",
            FluxonMemberRole::SideTransferWorker => "side_transfer_worker",
            FluxonMemberRole::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FsMountKind {
    Export,
    Shm,
    Tmp,
}

impl FsMountKind {
    pub fn as_str(self) -> &'static str {
        match self {
            FsMountKind::Export => "export",
            FsMountKind::Shm => "shm",
            FsMountKind::Tmp => "tmp",
        }
    }

    pub fn parse_label(s: &str) -> Option<Self> {
        match s.trim() {
            "export" => Some(FsMountKind::Export),
            "shm" => Some(FsMountKind::Shm),
            "tmp" => Some(FsMountKind::Tmp),
            _ => None,
        }
    }
}
