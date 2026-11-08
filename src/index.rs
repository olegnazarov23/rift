//! The chunk index on local disk: where every stored chunk is, and each pack's size. It caches the
//! bucket's .idx files, so it's rebuilt from them whenever it's missing or out of step.

use std::{collections::HashMap, path::Path};

use anyhow::Result;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

/// pack, offset, length
pub type Ref = [u32; 3];
/// hash, offset, length of a chunk inside its pack
pub type Entry = ([u8; 32], u32, u32);

const CHUNKS: TableDefinition<[u8; 32], (u32, u32, u32)> = TableDefinition::new("chunks");
/// stored size, bytes of chunks
const PACKS: TableDefinition<u32, (u64, u64)> = TableDefinition::new("packs");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
const VERSION: u64 = 1;

pub struct Index {
    db: Database,
}

impl Index {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        Ok(Self { db: Database::create(path)? })
    }

    /// The packs it knows, with (stored size, chunk bytes); empty when new or from another version.
    pub fn packs(&self) -> Result<HashMap<u32, (u64, u64)>> {
        let r = self.db.begin_read()?;
        let version = match r.open_table(META) {
            Ok(meta) => meta.get("version")?.map(|v| v.value()),
            Err(redb::TableError::TableDoesNotExist(_)) => None,
            Err(e) => return Err(e.into()),
        };
        if version != Some(VERSION) {
            return Ok(HashMap::new());
        }
        let mut out = HashMap::new();
        for row in r.open_table(PACKS)?.iter()? {
            let (k, v) = row?;
            out.insert(k.value(), v.value());
        }
        Ok(out)
    }

    /// Starts over with these packs: (id, stored size, chunks).
    pub fn rebuild(&self, packs: impl IntoIterator<Item = (u32, u64, Vec<Entry>)>) -> Result<()> {
        let w = self.db.begin_write()?;
        w.delete_table(CHUNKS)?;
        w.delete_table(PACKS)?;
        w.delete_table(META)?;
        {
            let (mut chunks, mut table) = (w.open_table(CHUNKS)?, w.open_table(PACKS)?);
            for (id, size, entries) in packs {
                table.insert(id, (size, entries.iter().map(|e| u64::from(e.2)).sum::<u64>()))?;
                for (h, at, len) in entries {
                    chunks.insert(h, (id, at, len))?;
                }
            }
            w.open_table(META)?.insert("version", VERSION)?;
        }
        w.commit()?;
        Ok(())
    }

    pub fn lookup(&self, hash: &[u8; 32]) -> Result<Option<Ref>> {
        let r = self.db.begin_read()?;
        let chunks = match r.open_table(CHUNKS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(chunks.get(hash)?.map(|v| {
            let (p, at, len) = v.value();
            [p, at, len]
        }))
    }

    /// Records stored packs and their chunks; a chunk already indexed elsewhere now points here.
    pub fn add_packs(&self, packs: &[(u32, u64, Vec<Entry>)]) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let (mut chunks, mut table) = (w.open_table(CHUNKS)?, w.open_table(PACKS)?);
            for (id, size, entries) in packs {
                table.insert(*id, (*size, entries.iter().map(|e| u64::from(e.2)).sum::<u64>()))?;
                for &(h, at, len) in entries {
                    chunks.insert(h, (*id, at, len))?;
                }
            }
            w.open_table(META)?.insert("version", VERSION)?;
        }
        w.commit()?;
        Ok(())
    }

    /// Forgets packs, and their chunks unless those now point somewhere else.
    pub fn remove_packs(&self, packs: &[(u32, Vec<Entry>)]) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let (mut chunks, mut table) = (w.open_table(CHUNKS)?, w.open_table(PACKS)?);
            for (id, entries) in packs {
                table.remove(*id)?;
                for (h, ..) in entries {
                    let here = chunks.get(h)?.is_some_and(|v| v.value().0 == *id);
                    if here {
                        chunks.remove(h)?;
                    }
                }
            }
        }
        w.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packs_chunks_and_rebuild() {
        let path = std::env::temp_dir().join(format!("rift-index-test-{}.redb", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let index = Index::open(&path).unwrap();
        assert!(index.packs().unwrap().is_empty(), "a new index knows nothing");
        let (a, b) = ([1u8; 32], [2u8; 32]);
        index.add_packs(&[(0, 100, vec![(a, 0, 10), (b, 10, 20)])]).unwrap();
        assert_eq!(index.lookup(&a).unwrap(), Some([0, 0, 10]));
        assert_eq!(index.packs().unwrap()[&0], (100, 30));
        // b moves to pack 1; removing pack 0 keeps it there and drops a
        index.add_packs(&[(1, 50, vec![(b, 0, 20)])]).unwrap();
        index.remove_packs(&[(0, vec![(a, 0, 10), (b, 10, 20)])]).unwrap();
        assert_eq!(index.lookup(&a).unwrap(), None);
        assert_eq!(index.lookup(&b).unwrap(), Some([1, 0, 20]));
        assert_eq!(index.packs().unwrap().keys().copied().collect::<Vec<_>>(), vec![1]);
        index.rebuild([(7, 9, vec![(a, 0, 10)])]).unwrap();
        assert_eq!(index.lookup(&b).unwrap(), None);
        assert_eq!(index.lookup(&a).unwrap(), Some([7, 0, 10]));
        drop(index);
        let _ = std::fs::remove_file(&path);
    }
}
