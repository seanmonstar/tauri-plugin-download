use headers::{Date, ETag, HeaderMapExt, IfRange, LastModified};
use reqwest::header::{ETAG, HeaderMap};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// A validator for the exact representation saved in the temporary file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ResumeValidator {
   ETag(String),
   LastModified(SystemTime),
}

impl ResumeValidator {
   pub(crate) fn from_headers(headers: &HeaderMap) -> Option<Self> {
      if headers.contains_key(ETAG) {
         let tag = headers.typed_get::<ETag>()?;
         // If-Range requires strong comparison, including against itself.
         if IfRange::etag(tag.clone()).is_modified(Some(&tag), None) {
            return None;
         }
         return Some(Self::ETag(headers.get(ETAG)?.to_str().ok()?.to_owned()));
      }

      let modified = SystemTime::from(headers.typed_get::<LastModified>()?);
      let date = SystemTime::from(headers.typed_get::<Date>()?);
      // RFC 9110 §8.8.2.2 permits dates when sufficiently separated to make
      // clock skew unlikely. Retain the conservative 60-second margin from
      // RFC 7232; a recent or missing Date cannot establish a strong validator.
      if date.duration_since(modified).ok()? < Duration::from_secs(60) {
         return None;
      }
      Some(Self::LastModified(modified))
   }

   pub(crate) fn if_range(&self) -> Option<IfRange> {
      match self {
         Self::ETag(value) => {
            let tag: ETag = value.parse().ok()?;
            let condition = IfRange::etag(tag.clone());
            (!condition.is_modified(Some(&tag), None)).then_some(condition)
         }
         Self::LastModified(time) => Some(IfRange::date(*time)),
      }
   }
}

#[cfg(test)]
mod tests {
   use super::*;
   use reqwest::header::{DATE, LAST_MODIFIED};

   #[test]
   fn selects_only_strong_validators() {
      let mut headers = HeaderMap::new();
      headers.insert(
         LAST_MODIFIED,
         "Tue, 15 Nov 1994 12:45:26 GMT".parse().unwrap(),
      );
      assert_eq!(ResumeValidator::from_headers(&headers), None);
      headers.insert(DATE, "Tue, 15 Nov 1994 12:46:25 GMT".parse().unwrap());
      assert_eq!(ResumeValidator::from_headers(&headers), None);
      headers.insert(DATE, "Tue, 15 Nov 1994 12:46:26 GMT".parse().unwrap());
      assert!(matches!(
         ResumeValidator::from_headers(&headers),
         Some(ResumeValidator::LastModified(_))
      ));
      for tag in ["W/\"weak\"", "invalid"] {
         headers.insert(ETAG, tag.parse().unwrap());
         assert_eq!(ResumeValidator::from_headers(&headers), None);
      }
      headers.insert(ETAG, "\"strong\"".parse().unwrap());
      assert_eq!(
         ResumeValidator::from_headers(&headers),
         Some(ResumeValidator::ETag("\"strong\"".into()))
      );
   }

   #[test]
   fn validators_round_trip_to_conditional_headers() {
      let mut headers = HeaderMap::new();
      headers.insert(
         LAST_MODIFIED,
         "Tue, 15 Nov 1994 12:45:26 GMT".parse().unwrap(),
      );
      headers.insert(DATE, "Tue, 15 Nov 1994 12:46:26 GMT".parse().unwrap());
      let date = ResumeValidator::from_headers(&headers).unwrap();
      for (validator, expected) in [
         (date, "Tue, 15 Nov 1994 12:45:26 GMT"),
         (ResumeValidator::ETag("\"strong\"".into()), "\"strong\""),
      ] {
         let saved = serde_json::to_string(&validator).unwrap();
         let loaded: ResumeValidator = serde_json::from_str(&saved).unwrap();
         let mut request = HeaderMap::new();
         request.typed_insert(loaded.if_range().unwrap());
         assert_eq!(request[reqwest::header::IF_RANGE], expected);
      }
   }
}
