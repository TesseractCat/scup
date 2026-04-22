use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use crate::{Object, ObjectId, Repository, protocol, scan, to_hex};

pub(crate) fn object_refs(obj: &Object) -> Vec<ObjectId> {
    match obj {
        Object::Chunk(_) => vec![],
        Object::Blob(b) => vec![b.chunks],
        Object::Map(m) => m.entries.values().copied().collect(),
        Object::List(l) => l.entries.clone(),
        Object::Snapshot(s) => {
            let mut out = Vec::with_capacity(1 + s.parents.len());
            out.push(s.tree);
            out.extend_from_slice(&s.parents);
            out
        }
    }
}

pub(crate) fn local_pull_response(base: &Path, req: protocol::Request) -> protocol::Response {
    let repo = Repository::load(base);
    let repo_uuid = repo.repo_uuid;

    let ensure = |requested: [u8; 32]| -> Result<Repository, String> {
        if requested != repo_uuid {
            Err(format!(
                "unknown repo_uuid: requested={}, available={}",
                to_hex(&requested),
                to_hex(&repo_uuid)
            ))
        } else {
            Ok(repo.clone())
        }
    };

    match req {
        protocol::Request::PullSnapshotIds { repo_uuid } => match ensure(repo_uuid) {
            Ok(repo) => {
                let snapshot_ids = repo
                    .objects
                    .iter()
                    .filter_map(|(id, obj)| match obj {
                        Object::Snapshot(_) => Some(*id),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                protocol::Response::PullSnapshotIds {
                    head: repo.head,
                    snapshot_ids,
                }
            }
            Err(err) => protocol::Response::Error(err),
        },
        protocol::Request::PullObjects {
            repo_uuid,
            object_ids,
        } => match ensure(repo_uuid) {
            Ok(repo) => {
                let mut objects = Vec::new();
                let mut chunks = Vec::new();
                for id in object_ids {
                    if let Some(obj) = repo.objects.get(&id) {
                        objects.push((id, obj.clone()));
                        if matches!(obj, Object::Chunk(_)) {
                            let path = base.join(format!(".syncup/chunks/{}", to_hex(&id.0)));
                            if let Ok(bytes) = std::fs::read(path) {
                                chunks.push((id, bytes));
                            }
                        }
                    }
                }
                protocol::Response::PullObjects { objects, chunks }
            }
            Err(err) => protocol::Response::Error(err),
        },
        _ => protocol::Response::Error("unsupported local pull request".into()),
    }
}

pub(crate) async fn pull_and_merge_from_host(
    base: &Path,
    host: &scan::ScannedHost,
) -> anyhow::Result<()> {
    pull_and_merge_with(base, |req| async move { crate::rpc(host, "pull", Some(&req), base).await })
        .await
}

pub(crate) async fn pull_and_merge_with<F, Fut>(
    base: &Path,
    mut send: F,
) -> anyhow::Result<()>
where
    F: FnMut(protocol::Request) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<protocol::Response>>,
{
    let local = Repository::load(base);
    let local_object_ids: BTreeSet<ObjectId> = local.objects.keys().copied().collect();

    let response = send(protocol::Request::PullSnapshotIds {
        repo_uuid: local.repo_uuid,
    })
    .await?;

    let (remote_head, snapshot_ids) = match response {
        protocol::Response::PullSnapshotIds { head, snapshot_ids } => (head, snapshot_ids),
        protocol::Response::Error(err) => anyhow::bail!("snapshot id pull failed: {err}"),
        _ => anyhow::bail!("unexpected response to PullSnapshotIds"),
    };

    let mut need: BTreeSet<ObjectId> = snapshot_ids
        .into_iter()
        .filter(|id| !local_object_ids.contains(id))
        .collect();

    let mut fetched_objects: BTreeMap<ObjectId, Object> = BTreeMap::new();
    let mut fetched_chunks: BTreeMap<ObjectId, Vec<u8>> = BTreeMap::new();

    while !need.is_empty() {
        let req_ids: Vec<ObjectId> = need.iter().copied().collect();
        need.clear();

        let response = send(protocol::Request::PullObjects {
            repo_uuid: local.repo_uuid,
            object_ids: req_ids,
        })
        .await?;

        let (objects, chunks) = match response {
            protocol::Response::PullObjects { objects, chunks } => (objects, chunks),
            protocol::Response::Error(err) => anyhow::bail!("object pull failed: {err}"),
            _ => anyhow::bail!("unexpected response to PullObjects"),
        };

        for (id, data) in chunks {
            fetched_chunks.entry(id).or_insert(data);
        }

        for (id, obj) in objects {
            if local_object_ids.contains(&id) || fetched_objects.contains_key(&id) {
                continue;
            }
            for child in object_refs(&obj) {
                if !local_object_ids.contains(&child) && !fetched_objects.contains_key(&child) {
                    need.insert(child);
                }
            }
            fetched_objects.insert(id, obj);
        }
    }

    std::fs::create_dir_all(base.join(".syncup/chunks")).expect("failed to create .syncup/chunks");
    for (id, data) in fetched_chunks {
        let path = base.join(format!(".syncup/chunks/{}", to_hex(&id.0)));
        if !path.exists() {
            std::fs::write(path, data).expect("failed to write chunk");
        }
    }

    let mut remote_repo = local.clone();
    for (id, obj) in fetched_objects {
        remote_repo.objects.insert(id, obj);
    }
    remote_repo.head = remote_head;

    let mut merged = Repository::load(base);
    merged.merge(remote_repo.clone());
    merged.save(base);

    Ok(())
}

pub async fn pull_all(base: &Path) -> anyhow::Result<()> {
    let local = Repository::load(base);
    let hosts = scan::scan_hosts(3)?;

    for host in hosts {
        if !host.repo_uuids.iter().any(|id| id == &local.repo_uuid) {
            continue;
        }

        match pull_and_merge_from_host(base, &host).await {
            Ok(()) => println!("- pulled from {}", host.fullname),
            Err(err) => println!("- pull from {} failed: {}", host.fullname, err),
        }
    }

    Ok(())
}
