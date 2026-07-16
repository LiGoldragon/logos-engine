use std::{
    convert::Infallible,
    path::{Path, PathBuf},
};

use kameo::{
    Actor,
    actor::{ActorRef, Spawn},
    message::{Context, Message},
};
use name_table::NameTable;
use signal_logos::{ProjectionEvent, Rejection, Reply, Request};
use signal_sema_storage::{
    DocumentKind, DocumentPayload, Reply as SemaReply, Request as SemaRequest,
    SubscriptionIdentifier,
};
use textual_rust::RustSource;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    sync::broadcast,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("projection: {0}")]
    Projection(String),
    #[error("actor: {0}")]
    Actor(String),
}
type Result<T> = std::result::Result<T, Error>;

async fn exchange(path: &Path, request: &SemaRequest) -> Result<SemaReply> {
    let mut stream = UnixStream::connect(path).await?;
    let bytes = signal_sema_storage::Wire::encode_request(request)
        .map_err(|error| Error::Projection(error.to_string()))?;
    stream.write_u32_le(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    let length = stream.read_u32_le().await? as usize;
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    rkyv::from_bytes::<SemaReply, rkyv::rancor::Error>(&bytes)
        .map_err(|error| Error::Projection(error.to_string()))
}

pub struct SemaPlane {
    socket: PathBuf,
    reads: u64,
}
impl Actor for SemaPlane {
    type Args = Self;
    type Error = Infallible;
    async fn on_start(
        actor: Self::Args,
        _: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}
pub struct Fetch(pub SemaRequest);
impl Message<Fetch> for SemaPlane {
    type Reply = Result<SemaReply>;
    async fn handle(&mut self, message: Fetch, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.reads += 1;
        exchange(&self.socket, &message.0).await
    }
}

pub struct NexusPlane {
    sema: ActorRef<SemaPlane>,
    events: broadcast::Sender<ProjectionEvent>,
    projected: u64,
}
impl Actor for NexusPlane {
    type Args = Self;
    type Error = Infallible;
    async fn on_start(
        actor: Self::Args,
        _: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}
pub struct Dispatch(pub Request);
impl Message<Dispatch> for NexusPlane {
    type Reply = Result<Reply>;
    async fn handle(
        &mut self,
        message: Dispatch,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match message.0 {
            Request::ProjectRust { scope: _, logos } => {
                self.projected += 1;
                let fetched = self
                    .sema
                    .ask(Fetch(SemaRequest::HashFetch { hash: logos }))
                    .send()
                    .await
                    .map_err(|error| Error::Actor(error.to_string()))?;
                let SemaReply::Document(Some(document)) = fetched else {
                    return Ok(Reply::Rejected(Rejection::LogosNotFound));
                };
                let summary = signal_sema_storage::SlotSummary {
                    key: document.key.clone(),
                    version: document.version,
                    hash: document.hash,
                };
                let DocumentPayload::Logos { items, names } = document.payload else {
                    return Ok(Reply::Rejected(Rejection::WrongDocumentKind));
                };
                let names = NameTable::from_archive_bytes(&names.0)
                    .map_err(|error| Error::Projection(error.to_string()))?;
                let mut rust = String::new();
                for item in items {
                    rust.push_str(
                        RustSource::project_item(&item, &names)
                            .map_err(|error| Error::Projection(error.to_string()))?
                            .as_str(),
                    );
                    rust.push('\n');
                }
                let _ = self.events.send(ProjectionEvent {
                    logos,
                    rust: rust.clone(),
                    source: summary.clone(),
                });
                Ok(Reply::RustProjected {
                    rust,
                    source: summary,
                })
            }
            Request::List { scope } => {
                let reply = self
                    .sema
                    .ask(Fetch(SemaRequest::List {
                        scope,
                        kind: Some(DocumentKind::Logos),
                    }))
                    .send()
                    .await
                    .map_err(|error| Error::Actor(error.to_string()))?;
                Ok(match reply {
                    SemaReply::Listed(values) => Reply::Listed(values),
                    _ => Reply::Rejected(Rejection::StorageFailed),
                })
            }
            Request::Subscribe { scope } => {
                let reply = self
                    .sema
                    .ask(Fetch(SemaRequest::List {
                        scope,
                        kind: Some(DocumentKind::Logos),
                    }))
                    .send()
                    .await
                    .map_err(|error| Error::Actor(error.to_string()))?;
                Ok(match reply {
                    SemaReply::Listed(initial) => Reply::Subscribed {
                        identifier: SubscriptionIdentifier(0),
                        initial,
                    },
                    _ => Reply::Rejected(Rejection::StorageFailed),
                })
            }
        }
    }
}

pub struct SignalPlane {
    nexus: ActorRef<NexusPlane>,
    admitted: u64,
}
impl Actor for SignalPlane {
    type Args = Self;
    type Error = Infallible;
    async fn on_start(
        actor: Self::Args,
        _: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}
impl Message<Dispatch> for SignalPlane {
    type Reply = Result<Reply>;
    async fn handle(
        &mut self,
        message: Dispatch,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.admitted += 1;
        self.nexus
            .ask(message)
            .send()
            .await
            .map_err(|error| Error::Actor(error.to_string()))
    }
}

#[derive(Clone)]
pub struct Runtime {
    signal: ActorRef<SignalPlane>,
    events: broadcast::Sender<ProjectionEvent>,
}
impl Runtime {
    pub fn new(socket: PathBuf) -> Self {
        let sema = SemaPlane::spawn(SemaPlane { socket, reads: 0 });
        let (events, _) = broadcast::channel(64);
        let nexus = NexusPlane::spawn(NexusPlane {
            sema,
            events: events.clone(),
            projected: 0,
        });
        Self {
            signal: SignalPlane::spawn(SignalPlane { nexus, admitted: 0 }),
            events,
        }
    }
    pub async fn request(&self, request: Request) -> Result<Reply> {
        self.signal
            .ask(Dispatch(request))
            .send()
            .await
            .map_err(|error| Error::Actor(error.to_string()))
    }
    pub fn subscribe(&self) -> broadcast::Receiver<ProjectionEvent> {
        self.events.subscribe()
    }
}
