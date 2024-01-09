pub trait Sanitize {
    fn sanitize(&self) -> Self;
}

impl Sanitize for http::HeaderMap {
    fn sanitize(&self) -> Self {
        let mut sanitized = self.clone();
        if let Some(authorization) = sanitized.get_mut("Authorization") {
            *authorization = http::HeaderValue::from_static("<redacted>");
        }
        sanitized
    }
}
