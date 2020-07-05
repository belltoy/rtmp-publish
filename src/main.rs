#![recursion_limit="1024"]
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::pin::Pin;
use std::sync::Arc;

use clap::{
    crate_version, crate_authors,
    App, Arg, ArgGroup,
};

use bytes::Bytes;

use futures::{
    stream::{
        StreamExt,
    },
};
use pin_utils::pin_mut;

use rml_rtmp::{
    sessions::StreamMetadata,
    time::RtmpTimestamp,
};
use slog::{info};

mod error;
mod rtmp;
mod flv;
mod logger;

const USAGE: &str = "
    rtmp-publish --input <INPUT> <DEST_LIST_FILE>
    rtmp-publish --input <INPUT> --concurrency <CONCURRENCY> --prefix <PREFIX>";

const EXAMPLE: &str = "
EXAMPLES:

    ## Auto-Generated destinations

    > rtmp-publish --input test.flv -c 100 -p rtmp://test.example.com/app/stream_prefix_

    This command will read from test.flv, push RTMP stream to the following destinations concurrently:

        rtmp:://test.example.com/app/stream_prefix_0
        rtmp:://test.example.com/app/stream_prefix_1
        rtmp:://test.example.com/app/stream_prefix_2
        ...
        rtmp:://test.example.com/app/stream_prefix_99

    ## From destinations list file

    > rtmp-publish --input test.flv target_list.txt

    This command will read from test.flv, push RTMP stream to the destinations read from `target_list.txt` concurrently:

    > cat target_list.txt

        rtmp:://test.example.com/app/stream_LIho834J
        rtmp:://test.example.com/app/stream_HliH234L
        rtmp:://test.example.com/app/stream_AhBhi33j
        ...
        rtmp:://test.example.com/app/stream_Eie83lrF
";

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let (root_logger, _guard) = logger::init();
    #[allow(deprecated)]
    let matches = App::new("RTMP Publish Bench Tool")
        .version(crate_version!())
        .author(crate_authors!("\n"))
        .about("This tool read flv packages from a specified file and push it to destinations from list or generated path, concurrently.")
        .usage(USAGE)
        .after_help(EXAMPLE)
        .arg(Arg::with_name("INPUT")
            .short("i")
            .long("input")
            .help("Input FLV file path")
            .required(true)
            .takes_value(true))

        .arg(Arg::with_name("CONCURRENCY")
            .short("c")
            .long("concurrency")
            .takes_value(true))
        .arg(Arg::with_name("PREFIX")
            .short("p")
            .long("prefix")
            .help("RTMP destinations prefix, e.g. `rtmp://example.com/app/stream_`")
            .takes_value(true))

        .arg(Arg::with_name("DEST_LIST_FILE")
             .help("Sets the input file to use")
             .index(1))

        .group(ArgGroup::with_name("prefix group")
            .args(&["PREFIX"])
            .conflicts_with("DEST_LIST_FILE")
            .requires("CONCURRENCY"))
        .group(ArgGroup::with_name("list group")
            .arg("DEST_LIST_FILE")
            .conflicts_with_all(&["prefix group", "CONCURRENCY"]))
        .get_matches();

    let urls: Box<dyn Iterator<Item = String>> = if matches.is_present("PREFIX") {
        let concurrency = matches.value_of("CONCURRENCY").map(|c| {
            c.parse::<usize>().expect("Cannot parse `CONCURRENCY`")
        }).unwrap_or(1);
        let prefix = matches.value_of("PREFIX").unwrap();
        let urls = (0..concurrency).map(move |c| format!("{}{}", prefix, c));
        Box::new(urls)
    } else {
        // Read from list file
        let dest_file_path = matches.value_of("DEST_LIST_FILE").unwrap();
        let list_file = File::open(dest_file_path)?;
        let reader = BufReader::new(list_file);
        let urls = reader.lines().map(|r| r.unwrap());
        Box::new(urls)
    };
    let urls = urls.map(|u| parse_rtmp_url(u.as_str())).collect::<Vec<Result<Url, _>>>();

    if let Some(Err(e)) = urls.iter().find(|u| u.is_err()) {
        panic!("RTMP url error: {}", e);
    }

    let urls = urls.into_iter().map(|r| r.unwrap()).collect::<Vec<Url>>();

    let num_threads = matches.value_of("threads").map(|n| {
        n.parse::<usize>().expect("Cannot parse `threads`")
    });

    let input_file_path = matches.value_of("INPUT").unwrap();
    assert!(input_file_path.ends_with(".flv") || input_file_path.ends_with(".FLV"),
        "Only FLV files are supported");

    let mut builder = tokio::runtime::Builder::new();
    builder.threaded_scheduler().enable_all();

    if let Some(threads) = num_threads {
        info!(root_logger, "Use {} cores as threads number", threads);
        builder.core_threads(threads);
    } else {
        info!(root_logger, "Use the number of cores available to the system as threads number");
    }

    let (tx, _rx) = tokio::sync::broadcast::channel(16);

    let clients = futures::stream::futures_unordered::FuturesUnordered::new();
    for Url{ host, port, app, stream } in urls {
        let rx = tx.subscribe();
        let client_fut = rtmp::client::Client::new(host, port, app, stream, rx, root_logger.clone());
        clients.push(client_fut);
    }

    let msgs = flv::read_flv_tag(input_file_path).await?;
    pin_mut!(msgs);
    let mut msgs: Pin<&mut _> = msgs;

    // await for all publish client ready
    let _ = clients.collect::<Vec<_>>().await;

    // broadcast
    while let Some(Ok(msg)) = msgs.next().await {
        if tx.receiver_count() <= 0 {
            break;
        }
        let _ = tx.send(msg);
    }

    info!(root_logger, "End");
    Ok(())
}

#[derive(Clone, Debug)]
pub enum PacketType {
    Metadata(Arc<StreamMetadata>),
    Video {
        data: Bytes,
        ts: RtmpTimestamp,
    },
    Audio {
        data: Bytes,
        ts: RtmpTimestamp,
    },
}

#[derive(Debug)]
pub enum ReceivedType {
    FromClient {
        message: rml_rtmp::messages::MessagePayload,
        bytes_read: usize,
    },
    Broadcast(Arc<PacketType>),
}

#[derive(Debug)]
struct Url {
    host: String,
    port: u16,
    app: String,
    stream: String,
}

fn parse_rtmp_url(rtmp_url: &str) -> Result<Url, String> {
    let parsed = url::Url::parse(rtmp_url).map_err(|e| e.to_string())?;
    let host = if let Some(host) = parsed.host_str() {
        host.to_owned()
    } else {
        return Err("EmptyHost".into());
    };
    let port = parsed.port().unwrap_or(1935);
    let parts: Vec<_> = parsed.path().trim_start_matches('/').split('/').collect();
    if parts.len() != 2 {
        return Err("Wrong path".into());
    }
    Ok(Url {
        host,
        port,
        app: parts[0].into(),
        stream: parts[1].into(),
    })
}
