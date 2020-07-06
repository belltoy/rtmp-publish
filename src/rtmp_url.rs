
#[derive(Debug)]
pub struct Url {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) app: String,
    pub(crate) stream: String,
    pub(crate) vhost: Option<String>,
}

pub fn parse_rtmp_url(rtmp_url: &str) -> Result<Url, String> {
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

    let mut vhost = None;
    for (k, v) in parsed.query_pairs() {
        if k.as_ref() == "vhost" {
            vhost = Some(v.into_owned());
        }
    }

    Ok(Url {
        host,
        port,
        app: parts[0].into(),
        stream: parts[1].into(),
        vhost,
    })
}
