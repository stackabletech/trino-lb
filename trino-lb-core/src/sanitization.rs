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

#[cfg(test)]
mod tests {
    use http::{
        header::{CONTENT_LENGTH, HOST},
        HeaderMap, HeaderValue,
    };

    use super::*;

    #[test]
    fn test_sanitize() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "example.com".parse().unwrap());
        headers.insert(CONTENT_LENGTH, "123".parse().unwrap());

        let sanitized = headers.sanitize();
        assert_eq!(sanitized, headers);

        headers.insert("Authorization", HeaderValue::from_static("secure"));
        let sanitized = headers.sanitize();
        assert_eq!(sanitized.get("Authorization").unwrap(), "<redacted>");

        // Also test lowercase variant
        headers.insert("authorization", HeaderValue::from_static("secure"));
        let sanitized = headers.sanitize();
        assert_eq!(sanitized.get("authorization").unwrap(), "<redacted>");
    }
}
