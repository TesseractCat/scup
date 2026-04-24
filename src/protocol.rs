use anyhow::{Context, Result};
use crate::{Object, ObjectId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Status,
    Push {
        repo_uuid: [u8; 32],
    },
    PullSnapshotIds {
        repo_uuid: [u8; 32],
    },
    PullObjects {
        repo_uuid: [u8; 32],
        object_ids: Vec<ObjectId>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Status {
        head: ObjectId,
        object_count: usize,
    },
    PullSnapshotIds {
        head: ObjectId,
        snapshot_ids: Vec<ObjectId>,
    },
    PullObjects {
        objects: Vec<(ObjectId, Object)>,
        chunks: Vec<(ObjectId, Vec<u8>)>,
    },
    PushComplete,
    Error(String),
}

fn frame(payload: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

fn unframe(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < 4 {
        anyhow::bail!("framed message too short: {} bytes", bytes.len());
    }
    let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let payload = &bytes[4..];
    if payload.len() < len {
        anyhow::bail!("incomplete framed message: expected {len} bytes, got {}", payload.len());
    }
    Ok(&payload[..len])
}

pub fn encode_request(req: &Request) -> Result<Vec<u8>> {
    Ok(frame(postcard::to_allocvec(req).context("serialize request")?))
}

pub fn decode_request(bytes: &[u8]) -> Result<Request> {
    Ok(postcard::from_bytes(unframe(bytes)?).context("deserialize request")?)
}

pub fn encode_response(resp: &Response) -> Result<Vec<u8>> {
    Ok(frame(postcard::to_allocvec(resp).context("serialize response")?))
}

pub fn decode_response(bytes: &[u8]) -> Result<Response> {
    Ok(postcard::from_bytes(unframe(bytes)?).context("deserialize response")?)
}

pub fn next_framed_message_len(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < 4 {
        return None;
    }
    let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let total = 4 + len;
    if bytes.len() < total {
        None
    } else {
        Some(total)
    }
}

pub fn pop_framed_message(bytes: &mut Vec<u8>) -> Option<Vec<u8>> {
    let total = next_framed_message_len(bytes.as_slice())?;
    Some(bytes.drain(..total).collect())
}
