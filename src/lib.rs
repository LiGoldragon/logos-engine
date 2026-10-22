use std::{convert::Infallible, path::PathBuf};

use kameo::{
    Actor,
    actor::{ActorRef, Spawn},
    message::{Context, Message},
};
use name_table::{Name, TableAddress};
use rust_logos::{FixtureRustVocabulary, FixtureRustVocabularyIds, RustLogos};
use signal_logos::{ProjectionEvent, Rejection, Reply, Request};
use signal_memory_storage::{
    CodecError as StorageCodecError, DocumentKind, DocumentPayload, FrameMessage,
    Reply as SemaReply, Request as SemaRequest, SubscriptionIdentifier, VocabularyNameTableArchive,
    WholeLogosArchive, Wire,
};
use signal_sema_translator::{VocabularyEncodedId, VocabularyNameTable, VocabularyRoot};
use structural_codec::EncodedNameResolver;
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
    #[error("stored Logos archive: {0}")]
    StoredArchive(#[from] StorageCodecError),
    #[error("Rust Logos projection: {0}")]
    RustLogos(#[from] rust_logos::Error),
    #[error("Nomos module head: {0}")]
    ModuleHead(#[from] core_nomos::NomosError),
    #[error("vocabulary authority: {0}")]
    VocabularyAuthority(String),
    #[error("Rust vocabulary is missing authority allocation for {0}")]
    MissingRustVocabulary(&'static str),
    #[error("actor: {0}")]
    Actor(String),
}
type Result<T> = std::result::Result<T, Error>;

struct AuthorityNames<'names> {
    names: &'names VocabularyNameTable,
}

impl EncodedNameResolver<VocabularyRoot> for AuthorityNames<'_> {
    fn resolve(&self, encoded_id: &VocabularyEncodedId) -> Option<&Name> {
        self.names.resolve(encoded_id).ok()
    }
}

fn required_rust_vocabulary_id(
    names: &VocabularyNameTable,
    spelling: &'static str,
) -> Result<VocabularyEncodedId> {
    names
        .lookup(
            &TableAddress::root(VocabularyRoot::Rust),
            &Name::new(spelling),
        )
        .map_err(|error| Error::VocabularyAuthority(error.to_string()))?
        .ok_or(Error::MissingRustVocabulary(spelling))
}

fn rust_logos(names: &VocabularyNameTable) -> Result<RustLogos> {
    let ids = FixtureRustVocabularyIds::new(
        required_rust_vocabulary_id(names, "NewtypeItemRecord")?,
        required_rust_vocabulary_id(names, "EnumerationItemRecord")?,
        required_rust_vocabulary_id(names, "VariantRecord")?,
        required_rust_vocabulary_id(names, "TupleFieldRecord")?,
        required_rust_vocabulary_id(names, "TypeReferenceRecord")?,
        required_rust_vocabulary_id(names, "struct")?,
        required_rust_vocabulary_id(names, "enum")?,
        required_rust_vocabulary_id(names, "pub")?,
        required_rust_vocabulary_id(names, ",")?,
        required_rust_vocabulary_id(names, ";")?,
    );
    let resolver = AuthorityNames { names };
    Ok(RustLogos::new(FixtureRustVocabulary::seal(ids, &resolver)?))
}

fn project_archived_logos(
    logos: WholeLogosArchive,
    names: VocabularyNameTableArchive,
) -> Result<String> {
    let logos = logos.to_logos()?;
    let names = names.to_name_table()?;
    let resolver = AuthorityNames { names: &names };
    let mut rust = core_nomos::ModuleHead::fixed()?.render()?;
    rust.push_str(&rust_logos(&names)?.emit(&logos, &resolver)?);
    Ok(rust)
}

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
                let summary = signal_memory_storage::SlotSummary {
                    key: document.key.clone(),
                    version: document.version,
                    hash: document.hash,
                };
                let DocumentPayload::Logos {
                    logos: archived_logos,
                    names,
                } = document.payload
                else {
                    return Ok(Reply::Rejected(Rejection::WrongDocumentKind));
                };
                // The generated module opens with the fixed head — the `// @generated`
                // marker, the four scalar aliases, and the cfg-gated NOTA import —
                // rendered from Nomos's fixed prelude package through the same
                // prettyplease projection, ahead of the document's declarations.
                let rust = project_archived_logos(archived_logos, names)?;
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
    use std::{fs, process::Command};

    use core_logos::{
        WholeLogos, WholeLogosEnumeration, WholeLogosItem, WholeLogosTupleFields,
        WholeLogosTypeReference, WholeLogosVariant, WholeLogosVariantPayload, WholeLogosVisibility,
    };
    use name_table::{
        Declaration, Name, OperationKey, RootDeclaration, SealRequest, TableAddress,
        TableMutability,
    };
    use signal_memory_storage::{VocabularyNameTableArchive, WholeLogosArchive};
    use signal_sema_translator::{VocabularyEncodedId, VocabularyNameTable, VocabularyRoot};

    use super::{Error, project_archived_logos};

    const RUST_VOCABULARY: &[&str] = &[
        "NewtypeItemRecord",
        "EnumerationItemRecord",
        "VariantRecord",
        "TupleFieldRecord",
        "TypeReferenceRecord",
        "struct",
        "enum",
        "pub",
        ",",
        ";",
        "u64",
    ];

    fn authority_names(complete_rust_vocabulary: bool) -> VocabularyNameTable {
        let rust_names = if complete_rust_vocabulary {
            RUST_VOCABULARY
        } else {
            &["u64"]
        };
        let mut names = VocabularyNameTable::new();
        names
            .seal(SealRequest::new(
                OperationKey::new([7; 32]),
                vec![
                    RootDeclaration::new(
                        VocabularyRoot::Universal,
                        TableMutability::Mutable,
                        ["Choice", "Unit", "Unary", "Product"]
                            .into_iter()
                            .map(|spelling| Declaration::Member(Name::new(spelling)))
                            .collect(),
                    ),
                    RootDeclaration::new(
                        VocabularyRoot::Rust,
                        TableMutability::Immutable,
                        rust_names
                            .iter()
                            .map(|spelling| Declaration::Member(Name::new(*spelling)))
                            .collect(),
                    ),
                ],
                Vec::new(),
            ))
            .expect("seal authority vocabulary");
        names
    }

    fn identity(
        names: &VocabularyNameTable,
        root: VocabularyRoot,
        spelling: &str,
    ) -> VocabularyEncodedId {
        names
            .lookup(&TableAddress::root(root), &Name::new(spelling))
            .expect("lookup authority name")
            .expect("authority name exists")
    }

    fn product_logos(names: &VocabularyNameTable) -> WholeLogos {
        let integer =
            WholeLogosTypeReference::Identity(identity(names, VocabularyRoot::Rust, "u64"));
        WholeLogos::new(vec![WholeLogosItem::Enumeration(
            WholeLogosEnumeration::new(
                WholeLogosVisibility::Public,
                identity(names, VocabularyRoot::Universal, "Choice"),
                vec![
                    WholeLogosVariant::new(
                        identity(names, VocabularyRoot::Universal, "Unit"),
                        WholeLogosVariantPayload::Unit,
                    ),
                    WholeLogosVariant::new(
                        identity(names, VocabularyRoot::Universal, "Unary"),
                        WholeLogosVariantPayload::Tuple(
                            WholeLogosTupleFields::new(vec![integer.clone()])
                                .expect("unary variant"),
                        ),
                    ),
                    WholeLogosVariant::new(
                        identity(names, VocabularyRoot::Universal, "Product"),
                        WholeLogosVariantPayload::Tuple(
                            WholeLogosTupleFields::new(vec![integer.clone(), integer])
                                .expect("product variant"),
                        ),
                    ),
                ],
            ),
        )])
    }

    #[test]
    fn validated_archives_project_through_authority_vocabulary_and_compile() {
        let names = authority_names(true);
        let logos = product_logos(&names);
        let rust = project_archived_logos(
            WholeLogosArchive::from_logos(&logos).expect("archive WholeLogos"),
            VocabularyNameTableArchive::from_name_table(&names)
                .expect("archive authority vocabulary"),
        )
        .expect("project stored WholeLogos");

        assert!(rust.starts_with(core_nomos::GENERATED_MARKER));
        assert!(rust.contains("u64"), "{rust}");
        for human_spelling in ["Choice", "Unit", "Unary", "Product"] {
            assert!(!rust.contains(human_spelling), "{rust}");
        }

        let directory = tempfile::tempdir().expect("temporary compile directory");
        let source = directory.path().join("lib.rs");
        fs::write(&source, format!("#![allow(non_camel_case_types)]\n{rust}"))
            .expect("write generated Rust");
        let status = Command::new("rustc")
            .arg("--edition=2024")
            .arg("--crate-type=lib")
            .arg("--out-dir")
            .arg(directory.path())
            .arg(source)
            .status()
            .expect("run rustc");
        assert!(status.success(), "projected Rust must compile");
    }

    #[test]
    fn projection_refuses_missing_authority_rust_vocabulary() {
        let names = authority_names(false);
        let error = project_archived_logos(
            WholeLogosArchive::from_logos(&WholeLogos::new(Vec::new()))
                .expect("archive empty WholeLogos"),
            VocabularyNameTableArchive::from_name_table(&names)
                .expect("archive incomplete authority vocabulary"),
        )
        .expect_err("missing structural vocabulary must be refused");
        assert!(matches!(
            error,
            Error::MissingRustVocabulary("NewtypeItemRecord")
        ));
    }
}
