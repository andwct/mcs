"""
Pydantic models for the {version}.meta sidecar written next to every
partially-encrypted MODEL/KERNEL artifact on PVC.
See documents/partial-encryption.md for the full format spec.
"""
from pydantic import BaseModel


class ChunkEntry(BaseModel):
    """One chunk of the on-disk artifact file, in file order."""
    encrypted: bool
    stored_length: int      # bytes on disk (Fernet token length when encrypted)
    plaintext_length: int   # bytes after decryption (== stored_length when plaintext)


class ArtifactMeta(BaseModel):
    meta_version: int = 1
    algorithm: str = "fernet"
    plaintext_size: int
    chunks: list[ChunkEntry]

    @property
    def stored_size(self) -> int:
        return sum(c.stored_length for c in self.chunks)
