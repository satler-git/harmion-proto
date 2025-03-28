use color_eyre::Result;
use crdts::{Map, Orswot};
use std::collections::HashMap;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use std::sync::Arc;

use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::{api::APIBuilder, peer_connection::peer_connection_state::RTCPeerConnectionState};

use tokio::sync::mpsc;

mod signal;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct NodeInfo {
    // TODO: node_idとかもいるはず
    sdp: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct State;

#[derive(Serialize, Deserialize, Debug)]
enum Message {
    Merge(State),
    AddNode(NodeInfo),
}

struct Server<'a> {
    remotes: Map<&'a str, Orswot<&'a str, NodeInfo>, NodeInfo>,
    conn: HashMap<String, RTCPeerConnection>,
    local: RTCSessionDescription,
}

async fn message_handler(rx: mpsc::Receiver<Message>) -> Result<()> {
    // while let rx

    Ok(())
}

async fn new(local: String, remote: String) -> Result<()> {
    let api = APIBuilder::new().build();

    let config = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: vec![
                "stun:stun.l.google.com:19302".to_owned(),
                "stun:stun1.l.google.com:19302".to_owned(),
                "stun:stun2.l.google.com:19302".to_owned(),
                "stun:stun3.l.google.com:19302".to_owned(),
                "stun:stun4.l.google.com:19302".to_owned(),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let peer_connection = Arc::new(api.new_peer_connection(config).await?);

    let desc_data = signal::decode(remote.as_str())?;
    let offer = serde_json::from_str::<RTCSessionDescription>(&desc_data)?;

    // Set the remote SessionDescription
    peer_connection.set_remote_description(offer).await?;

    let (tx, rx) = mpsc::channel::<Message>(100);
    tokio::spawn(message_handler(rx));

    let (done_tx, done_rx) = mpsc::channel::<()>(1);

    peer_connection.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        println!("Peer Connection State has changed: {s}");

        if s == RTCPeerConnectionState::Failed {
            // Wait until PeerConnection has had no network activity for 30 seconds or another failure. It may be reconnected using an ICE Restart.
            // Use webrtc.PeerConnectionStateDisconnected if you are interested in detecting faster timeout.
            // Note that the PeerConnection may come back from PeerConnectionStateDisconnected.
            println!("Peer Connection has gone to failed exiting");
            let _ = done_tx.try_send(());
        }

        Box::pin(async {})
    }));

    // DataChannel の作成（"data" というラベルを付与）
    let dc_init = RTCDataChannelInit {
        ..Default::default()
    };

    let data_channel = peer_connection
        .create_data_channel("data", Some(dc_init))
        .await?;

    data_channel.on_close(Box::new(move || {
        println!("Data channel closed");
        Box::pin(async {})
    }));

    // DataChannel の on_message コールバック設定

    {
        let tx_clone = tx.clone();
        data_channel.on_message(Box::new(move |msg| {
            let tx = tx_clone.clone();
            Box::pin(async move {
                // 受信したバイト列を文字列に変換
                let text = String::from_utf8(msg.data.to_vec()).unwrap_or_default();
                // JSON 文字列を Message 構造体にデシリアライズ
                match serde_json::from_str::<Message>(&text) {
                    Ok(message) => {
                        // チャンネルへ送信
                        if let Err(e) = tx.send(message).await {
                            eprintln!("メッセージの送信に失敗しました: {}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("JSON のパースに失敗しました: {}", e);
                    }
                }
            })
        }));
    }

    data_channel.on_open(Box::new(|| {
        println!("DataChannel がオープンしました");
        Box::pin(async {})
    }));

    Ok(())
}
