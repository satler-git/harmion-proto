use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use notify::{EventKind, RecursiveMode, Watcher};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use std::{borrow::Cow, net::SocketAddr, path::PathBuf, sync::Arc};

use color_eyre::Result;

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{RwLock, mpsc},
    task::JoinSet,
};

use loro::{ExportMode, LoroDoc, VersionVector};

use serde::{Deserialize, Serialize};

use clap::Parser;

#[derive(Debug)]
struct AppState {
    doc: LoroDoc,
    tx: mpsc::Sender<Event>,
    local_write_flag: Arc<AtomicBool>,
}

#[derive(Debug)]
enum Event {
    Write,
    Read,
}

async fn handler(
    state: Arc<RwLock<AppState>>,
    mut rx: mpsc::Receiver<Event>,
    remotes: Vec<SocketAddr>,
    path: PathBuf,
) -> Result<()> {
    let local_write_flag = {
        let s = state.read().await;
        s.local_write_flag.clone()
    };

    while let Some(event) = rx.recv().await {
        dbg!(&event);
        match event {
            Event::Write => {
                local_write_flag.store(true, Ordering::SeqCst);
                let content = (*state).read().await.doc.get_text("content").to_string();

                let mut file = File::create(&path).await?;

                file.write_all(content.as_bytes()).await?;

                tokio::time::sleep(Duration::from_millis(100)).await;
                local_write_flag.store(false, Ordering::SeqCst);
            }
            Event::Read => {
                let text = (*state).write().await.doc.get_text("content");

                let mut file = File::open(&path).await?;
                let mut contents = String::new();
                file.read_to_string(&mut contents).await?;

                text.update(&contents, Default::default())?;

                // ちゃんと制御できてると信じたい。時間の問題
                if local_write_flag.load(Ordering::SeqCst) {
                    continue;
                }
                let mut set: JoinSet<Result<()>> = JoinSet::new();

                // ちょっとパフォーマンス落ちるけどまあ
                for ri in remotes.clone() {
                    let state = state.clone();
                    set.spawn(async move {
                        let client = reqwest::Client::new();

                        eprintln!("fetching version");

                        let version: VersionVector = reqwest::get(format!("http://{ri}/version"))
                            .await?
                            .json()
                            .await?;

                        let update = Update {
                            bytes: (*state).read().await.doc.export(ExportMode::Updates {
                                from: Cow::Borrowed(&version),
                            })?,
                        };

                        eprintln!("sending updates");

                        client
                            .post(format!("http://{ri}/update"))
                            .json(&update)
                            .send()
                            .await?;

                        Ok(())
                    });
                }

                let _ = set.join_all().await;
            }
        }
    }
    Ok(())
}

async fn watcher(tx: mpsc::Sender<Event>, path: PathBuf, local_write_flag: Arc<AtomicBool>) {
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if !local_write_flag.load(Ordering::SeqCst) && event.is_ok() {
            match event.as_ref().unwrap().kind {
                EventKind::Modify(_) | EventKind::Remove(_) => {
                    let _ = tx.blocking_send(Event::Read);
                }
                _ => {
                    dbg!(&event);
                }
            }
        } else {
            eprintln!("むしだよ");
        }
    })
    .expect("failed to create watcher");

    watcher
        .watch(&path, RecursiveMode::NonRecursive)
        .expect("failed to start watching");

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, short, default_values_t = Vec::<SocketAddr>::new())]
    remotes: Vec<SocketAddr>,
    #[arg(long, short)]
    file: PathBuf,
    #[arg(long, short)]
    addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Args::parse();

    let (tx, rx) = mpsc::channel(100);

    let local_write_flag = Arc::new(AtomicBool::new(false));

    let state = Arc::new(RwLock::new(AppState {
        doc: {
            let doc = LoroDoc::default();

            let text = doc.get_text("content");

            let mut file = File::open(&args.file).await?;
            let mut contents = String::new();
            file.read_to_string(&mut contents).await?;

            text.update(&contents, Default::default())?;

            doc
        },
        tx: tx.clone(),
        local_write_flag: local_write_flag.clone(),
    }));

    tx.send(Event::Read).await?;

    tokio::spawn(handler(
        state.clone(),
        rx,
        args.remotes.clone(),
        args.file.clone(),
    ));

    tokio::spawn(watcher(
        tx.clone(),
        args.file.clone(),
        local_write_flag.clone(),
    ));

    let app = Router::new()
        .route("/version", get(version))
        .route("/update", post(update))
        .with_state(state.clone());

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(args.addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
struct Update {
    bytes: Vec<u8>,
}

async fn version(State(state): State<Arc<RwLock<AppState>>>) -> (StatusCode, Json<VersionVector>) {
    eprintln!("version");
    let r = (*state).read().await;

    (StatusCode::OK, Json(r.doc.oplog_vv()))
}

async fn update(
    State(state): State<Arc<RwLock<AppState>>>,
    Json(payload): Json<Update>,
) -> StatusCode {
    eprintln!("update");
    let w = (*state).write().await;

    if let Err(err) = w.doc.import(&payload.bytes) {
        eprintln!("{err:?}");

        StatusCode::NOT_ACCEPTABLE
    } else {
        let _ = w.tx.send(Event::Write).await;

        StatusCode::OK
    }
}
