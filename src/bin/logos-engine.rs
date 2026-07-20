use std::{env, path::PathBuf};

use logos_engine::Runtime;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use signal_logos::{Reply, Request, encode_reply, encode_request};
use signal_sema_storage::{ContentHash, FixtureScope, FrameMessage, Wire};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot},
};

struct FramedSocket {
    stream: UnixStream,
    sequence: u64,
}
impl FramedSocket {
    async fn connect(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut socket = Self {
            stream: UnixStream::connect(path).await?,
            sequence: 0,
        };
        socket
            .stream
            .write_all(&Wire::frame_current_handshake_request()?)
            .await?;
        if !Wire::decode_frame(&socket.read_frame().await?)?.is_accepted_handshake() {
            return Err("daemon rejected shared frame protocol".into());
        }
        Ok(socket)
    }
    async fn accept(stream: UnixStream) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut socket = Self {
            stream,
            sequence: 0,
        };
        let FrameMessage::HandshakeRequest(peer) = Wire::decode_frame(&socket.read_frame().await?)?
        else {
            return Err("first frame was not a protocol handshake".into());
        };
        socket
            .stream
            .write_all(&Wire::frame_handshake_reply(Wire::handshake_reply(peer))?)
            .await?;
        Ok(socket)
    }
    async fn read_frame(&mut self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let length = self.stream.read_u32().await? as usize;
        let mut frame = Vec::with_capacity(length + 4);
        frame.extend_from_slice(&(length as u32).to_be_bytes());
        frame.resize(length + 4, 0);
        self.stream.read_exact(&mut frame[4..]).await?;
        Ok(frame)
    }
    async fn request(
        &mut self,
        payload: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let frame = Wire::frame_request(payload, self.sequence)?;
        self.sequence += 1;
        self.stream.write_all(&frame).await?;
        Ok(())
    }
    async fn reply_payload(&mut self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let FrameMessage::Reply { payload, .. } = Wire::decode_frame(&self.read_frame().await?)?
        else {
            return Err("expected shared reply frame".into());
        };
        Ok(payload)
    }
}

struct SocketReadiness {
    path: PathBuf,
}
impl SocketReadiness {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
    async fn changed(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let parent = self
            .path
            .parent()
            .ok_or("upstream socket has no parent")?
            .to_path_buf();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |event| {
            let _ = sender.send(event);
        })?;
        watcher.watch(&parent, RecursiveMode::NonRecursive)?;
        while let Some(event) = receiver.recv().await {
            let event = event?;
            if event.paths.iter().any(|path| path == &self.path) {
                return Ok(());
            }
        }
        Err("upstream readiness watcher closed".into())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut arguments = env::args().skip(1);
    match arguments.next().as_deref() {
        Some("daemon") => {
            let socket = PathBuf::from(arguments.next().unwrap_or_else(|| "/tmp/new-language-engine/logos.sock".into()));
            let sema = PathBuf::from(arguments.next().unwrap_or_else(|| "/tmp/new-language-engine/sema.sock".into()));
            let nomos = PathBuf::from(arguments.next().unwrap_or_else(|| "/tmp/new-language-engine/nomos.sock".into()));
            if let Some(parent) = socket.parent() { std::fs::create_dir_all(parent)?; }
            let _ = std::fs::remove_file(&socket);
            let listener = UnixListener::bind(&socket)?;
            let runtime = Runtime::new(sema);
            let relay_runtime = runtime.clone();
            let (readiness_sender, readiness_receiver) = oneshot::channel();
            tokio::spawn(async move {
                Relay::supervise(nomos, relay_runtime, readiness_sender).await;
            });
            readiness_receiver
                .await
                .map_err(|_| std::io::Error::other("Nomos relay stopped before subscribing"))?;
            println!("READY {}", socket.display());
            loop {
                let (stream, _) = listener.accept().await?;
                let runtime = runtime.clone();
                tokio::spawn(async move { let _ = Server::serve(stream, runtime).await; });
            }
        }
        Some("project") => {
            let socket = PathBuf::from(arguments.next().ok_or("socket")?);
            let hash = ContentHash(Parser::hash(&arguments.next().ok_or("logos hash")?)?);
            println!("{:?}", Client::exchange(&socket, &Request::ProjectRust { scope: FixtureScope(1), logos: hash }).await?);
            Ok(())
        }
        Some("subscribe") => {
            let socket = PathBuf::from(arguments.next().ok_or("socket")?);
            Client::subscribe(&socket).await
        }
        _ => Err("usage: logos-engine daemon [socket] [sema-socket] [nomos-socket] | project <socket> <hash> | subscribe <socket>".into()),
    }
}

struct Relay;
impl Relay {
    async fn supervise(nomos: PathBuf, runtime: Runtime, readiness_sender: oneshot::Sender<()>) {
        let mut readiness_sender = Some(readiness_sender);
        loop {
            if let Err(error) = Self::connection(&nomos, &runtime, &mut readiness_sender).await {
                eprintln!("logos relay failed: {error}");
            }
            if SocketReadiness::new(nomos.clone()).changed().await.is_err() {
                return;
            }
        }
    }
    async fn connection(
        nomos: &PathBuf,
        runtime: &Runtime,
        readiness_sender: &mut Option<oneshot::Sender<()>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut socket = FramedSocket::connect(nomos).await?;
        socket
            .request(signal_nomos::encode_request(
                &signal_nomos::Request::Subscribe {
                    scope: FixtureScope(1),
                },
            )?)
            .await?;
        let _: signal_nomos::Reply = Decoder::value(&socket.reply_payload().await?)?;
        if let Some(readiness_sender) = readiness_sender.take() {
            let _ = readiness_sender.send(());
        }
        loop {
            if let signal_nomos::Reply::Event(event) =
                Decoder::value::<signal_nomos::Reply>(&socket.reply_payload().await?)?
            {
                runtime
                    .request(Request::ProjectRust {
                        scope: event.logos.key.scope,
                        logos: event.logos.hash,
                    })
                    .await?;
            }
        }
    }
}

struct Server;
impl Server {
    async fn serve(
        stream: UnixStream,
        runtime: Runtime,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut socket = FramedSocket::accept(stream).await?;
        let FrameMessage::Request { exchange, payload } =
            Wire::decode_frame(&socket.read_frame().await?)?
        else {
            return Err("expected shared request frame".into());
        };
        let request: Request = Decoder::value(&payload)?;
        let subscribed = matches!(request, Request::Subscribe { .. });
        let mut events = runtime.subscribe();
        socket
            .stream
            .write_all(&Wire::frame_reply(
                exchange,
                encode_reply(&runtime.request(request).await?)?,
            )?)
            .await?;
        if subscribed {
            while let Ok(event) = events.recv().await {
                socket
                    .stream
                    .write_all(&Wire::frame_reply(
                        exchange,
                        encode_reply(&Reply::Event(event))?,
                    )?)
                    .await?;
            }
        }
        Ok(())
    }
}

struct Client;
impl Client {
    async fn exchange(
        path: &PathBuf,
        request: &Request,
    ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
        let mut socket = FramedSocket::connect(path).await?;
        socket.request(encode_request(request)?).await?;
        Decoder::value(&socket.reply_payload().await?)
    }
    async fn subscribe(path: &PathBuf) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut socket = FramedSocket::connect(path).await?;
        socket
            .request(encode_request(&Request::Subscribe {
                scope: FixtureScope(1),
            })?)
            .await?;
        loop {
            println!(
                "{:?}",
                Decoder::value::<Reply>(&socket.reply_payload().await?)?
            );
        }
    }
}

struct Decoder;
impl Decoder {
    fn value<T>(bytes: &[u8]) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        T: rkyv::Archive,
        T::Archived: for<'a> rkyv::bytecheck::CheckBytes<
                rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
            > + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
    {
        Ok(rkyv::from_bytes::<T, rkyv::rancor::Error>(bytes)?)
    }
}

struct Parser;
impl Parser {
    fn hash(value: &str) -> Result<[u8; 32], Box<dyn std::error::Error + Send + Sync>> {
        if value.len() != 64 {
            return Err("hash must have 64 hexadecimal digits".into());
        }
        let mut output = [0; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            output[index] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
        }
        Ok(output)
    }
}
