use std::{convert::Infallible, path::PathBuf};

use core_logos::standard_name_table;
use kameo::{
    Actor,
    actor::{ActorRef, Spawn},
    message::{Context, Message},
};
use name_table::NameTable;
use signal_logos::{ProjectionEvent, Rejection, Reply, Request};
use signal_sema_storage::{
    DocumentKind, DocumentPayload, FrameMessage, NameTableBytes, Reply as SemaReply,
    Request as SemaRequest, SubscriptionIdentifier, Wire,
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

pub struct SemaPlane {
    socket: PathBuf,
    reads: u64,
}
impl SemaPlane {
    async fn read_frame(&self, stream: &mut UnixStream) -> Result<Vec<u8>> {
        let length = stream.read_u32().await? as usize;
        let mut frame = Vec::with_capacity(length + 4);
        frame.extend_from_slice(&(length as u32).to_be_bytes());
        frame.resize(length + 4, 0);
        stream.read_exact(&mut frame[4..]).await?;
        Ok(frame)
    }

    async fn exchange(&self, request: &SemaRequest) -> Result<SemaReply> {
        let mut stream = UnixStream::connect(&self.socket).await?;
        stream
            .write_all(
                &Wire::frame_current_handshake_request()
                    .map_err(|error| Error::Projection(error.to_string()))?,
            )
            .await?;
        let handshake = Wire::decode_frame(&self.read_frame(&mut stream).await?)
            .map_err(|error| Error::Projection(error.to_string()))?;
        if !handshake.is_accepted_handshake() {
            return Err(Error::Projection("Sema rejected frame protocol".into()));
        }
        let payload =
            Wire::encode_request(request).map_err(|error| Error::Projection(error.to_string()))?;
        stream
            .write_all(
                &Wire::frame_request(payload, self.reads)
                    .map_err(|error| Error::Projection(error.to_string()))?,
            )
            .await?;
        let FrameMessage::Reply { payload, .. } =
            Wire::decode_frame(&self.read_frame(&mut stream).await?)
                .map_err(|error| Error::Projection(error.to_string()))?
        else {
            return Err(Error::Projection("Sema returned a non-reply frame".into()));
        };
        rkyv::from_bytes::<SemaReply, rkyv::rancor::Error>(&payload)
            .map_err(|error| Error::Projection(error.to_string()))
    }
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
        self.exchange(&message.0).await
    }
}

pub struct NexusPlane {
    sema: ActorRef<SemaPlane>,
    events: broadcast::Sender<ProjectionEvent>,
    projected: u64,
}
impl NexusPlane {
    /// Recompose the fixed Logos vocabulary after reading a persisted Logos home slice.
    ///
    /// Persisted NameTable bytes intentionally contain only their owned `Logos` slice.
    /// Rust projection also names fixed `LogosStandard` objects, so the projection
    /// boundary restores that independently owned slice before resolving any item.
    fn projection_names(bytes: &NameTableBytes) -> Result<NameTable> {
        let stored = NameTable::from_archive_bytes(&bytes.0)
            .map_err(|error| Error::Projection(error.to_string()))?;
        let standards =
            standard_name_table().map_err(|error| Error::Projection(error.to_string()))?;
        stored
            .compose(&standards)
            .map_err(|error| Error::Projection(error.to_string()))
    }
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
                let names = Self::projection_names(&names)?;
                // The generated module opens with Nomos's fixed head: the
                // `// @generated` marker and canonical support imports. It contains
                // no transparent type aliases and is projected before the document's
                // declarations through the same TextualRust codec.
                let mut rust = core_nomos::ModuleHead::fixed()
                    .render()
                    .map_err(|error| Error::Projection(error.to_string()))?;
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

#[cfg(test)]
mod tests {
    use core_logos::INTEGER;
    use name_table::{IdentifierNamespace, NameTable};
    use signal_sema_storage::NameTableBytes;

    use super::NexusPlane;

    #[test]
    fn persisted_logos_names_recompose_the_standard_slice_for_projection() {
        let standards = core_logos::standard_name_table().expect("standard vocabulary");
        let names = NameTable::new(IdentifierNamespace::Logos)
            .compose(&standards)
            .expect("compose standard vocabulary");
        let persisted = NameTableBytes(
            names
                .to_archive_bytes()
                .expect("archive Logos slice")
                .to_vec(),
        );

        let restored = NexusPlane::projection_names(&persisted).expect("restore projection names");

        assert_eq!(
            restored.resolve(INTEGER).expect("resolve Integer").as_str(),
            "Integer"
        );
    }
}
