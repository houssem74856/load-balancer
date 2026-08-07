use http_body_util::{BodyExt, combinators::BoxBody};
use hyper::body::{Body, Bytes, Frame, Incoming};
use std::pin::Pin;
use std::task::{Context, Poll};

const MAX_BUFFER_BYTES: usize = 1024 * 1024; // 1MB
const MAX_RETRY_ATTEMPTS_FOR_SEND_RETRIABLE_BODY: u8 = 3;

struct PrefixBody {
    prefix: Option<Bytes>,
    rest: Incoming,
}

impl Body for PrefixBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        if let Some(bytes) = this.prefix.take() {
            return Poll::Ready(Some(Ok(Frame::data(bytes))));
        }

        Pin::new(&mut this.rest).poll_frame(cx)
    }
}

pub enum RequestBody {
    Retriable { bytes: Bytes },
    OneShot(BoxBody<Bytes, hyper::Error>),
}

impl RequestBody {
    pub fn max_attempts(&self) -> u8 {
        match self {
            RequestBody::Retriable { .. } => MAX_RETRY_ATTEMPTS_FOR_SEND_RETRIABLE_BODY,
            RequestBody::OneShot(_) => 1,
        }
    }
}

pub async fn handle_body(
    mut body: Incoming,
) -> Result<RequestBody, Box<dyn std::error::Error + Send + Sync>> {
    let mut buffered_bytes = Vec::new();
    let mut current_size = 0;
    let mut exceeded = false;

    while let Some(frame_res) = body.frame().await {
        let frame = frame_res?;

        if let Some(data) = frame.data_ref() {
            current_size += data.len();

            if current_size > MAX_BUFFER_BYTES {
                buffered_bytes.extend_from_slice(data);
                exceeded = true;
                break;
            }

            buffered_bytes.extend_from_slice(data);
        }
    }

    let buffered_bytes = Bytes::from(buffered_bytes);

    if exceeded {
        Ok(RequestBody::OneShot(
            PrefixBody {
                prefix: Some(buffered_bytes),
                rest: body,
            }
            .boxed(),
        ))
    } else {
        Ok(RequestBody::Retriable {
            bytes: buffered_bytes,
        })
    }
}
