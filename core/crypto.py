"""
Secure Secret Management & AES-256-GCM Encryption for RnR Linux IoT Hub.
File: core/crypto.py

Implements Customization 3 specification:
- AES-256-GCM authenticated encryption for sensitive configuration (RabbitMQ passwords, credentials).
- Random 96-bit (12-byte) nonce generated per encryption.
- Authenticated metadata (config_key, key_version) as Associated Authenticated Data (AAD).
- Master encryption key stored separately in protected file (/etc/iothub/keys/master.key),
  never stored in the SQLite database or committed to Git.
- Systemd LoadCredential= integration supported ($CREDENTIALS_DIRECTORY/master_key).
- Works with cryptography package if available, or pure-Python AES-256-GCM standard implementation.
"""

from __future__ import annotations
import base64
import os
import secrets
from pathlib import Path
from typing import Optional, Tuple


class CryptoError(Exception):
    """Base exception for cryptographic operations."""
    pass


class DecryptionError(CryptoError):
    """Raised when authentication tag verification or decryption fails."""
    pass


# ---------------------------------------------------------------------------
# AES-256-GCM Pure-Python Engine (NIST SP 800-38D Compliant Fallback)
# ---------------------------------------------------------------------------

class _AES256Cipher:
    """Standard AES-256 block cipher implementation for 128-bit blocks."""

    SBOX = [
        0x63, 0x7c, 0x77, 0x7b, 0xf2, 0x6b, 0x6f, 0xc5, 0x30, 0x01, 0x67, 0x2b, 0xfe, 0xd7, 0xab, 0x76,
        0xca, 0x82, 0xc9, 0x7d, 0xfa, 0x59, 0x47, 0xf0, 0xad, 0xd4, 0xa2, 0xaf, 0x9c, 0xa4, 0x72, 0xc0,
        0xb7, 0xfd, 0x93, 0x26, 0x36, 0x3f, 0xf7, 0xcc, 0x34, 0xa5, 0xe5, 0xf1, 0x71, 0xd8, 0x31, 0x15,
        0x04, 0xc7, 0x23, 0xc3, 0x18, 0x96, 0x05, 0x9a, 0x07, 0x12, 0x80, 0xe2, 0xeb, 0x27, 0xb2, 0x75,
        0x09, 0x83, 0x2c, 0x1a, 0x1b, 0x6e, 0x5a, 0xa0, 0x52, 0x3b, 0xd6, 0xb3, 0x29, 0xe3, 0x2f, 0x84,
        0x53, 0xd1, 0x00, 0xed, 0x20, 0xfc, 0xb1, 0x5b, 0x6a, 0xcb, 0xbe, 0x39, 0x4a, 0x4c, 0x58, 0xcf,
        0xd0, 0xef, 0xaa, 0xfb, 0x43, 0x4d, 0x33, 0x85, 0x45, 0xf9, 0x02, 0x7f, 0x50, 0x3c, 0x9f, 0xa8,
        0x51, 0xa3, 0x40, 0x8f, 0x92, 0x9d, 0x38, 0xf5, 0xbc, 0xb6, 0xda, 0x21, 0x10, 0xff, 0xf3, 0xd2,
        0xcd, 0x0c, 0x13, 0xec, 0x5f, 0x97, 0x44, 0x17, 0xc4, 0xa7, 0x7e, 0x3d, 0x64, 0x5d, 0x19, 0x73,
        0x60, 0x81, 0x4f, 0xdc, 0x22, 0x2a, 0x90, 0x88, 0x46, 0xee, 0xb8, 0x14, 0xde, 0x5e, 0x0b, 0xdb,
        0xe0, 0x32, 0x3a, 0x0a, 0x49, 0x06, 0x24, 0x5c, 0xc2, 0xd3, 0xac, 0x62, 0x91, 0x95, 0xe4, 0x79,
        0xe7, 0xc8, 0x37, 0x6d, 0x8d, 0xd5, 0x4e, 0xa9, 0x6c, 0x56, 0xf4, 0xea, 0x65, 0x7a, 0xae, 0x08,
        0xba, 0x78, 0x25, 0x2e, 0x1c, 0xa6, 0xb4, 0xc6, 0xe8, 0xdd, 0x74, 0x1f, 0x4b, 0xbd, 0x8b, 0x8a,
        0x70, 0x3e, 0xb5, 0x66, 0x48, 0x03, 0xf6, 0x0e, 0x61, 0x35, 0x57, 0xb9, 0x86, 0xc1, 0x1d, 0x9e,
        0xe1, 0xf8, 0x98, 0x11, 0x69, 0xd9, 0x8e, 0x94, 0x9b, 0x1e, 0x87, 0xe9, 0xce, 0x55, 0x28, 0xdf,
        0x8c, 0xa1, 0x89, 0x0d, 0xbf, 0xe6, 0x42, 0x68, 0x41, 0x99, 0x2d, 0x0f, 0xb0, 0x54, 0xbb, 0x16,
    ]
    RCON = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1b, 0x36]

    def __init__(self, key: bytes):
        if len(key) != 32:
            raise ValueError("AES-256 requires exactly a 32-byte key.")
        self.round_keys = self._key_expansion(key)

    @classmethod
    def _sub_word(cls, word: bytes) -> bytes:
        return bytes(cls.SBOX[b] for b in word)

    @staticmethod
    def _rot_word(word: bytes) -> bytes:
        return word[1:] + word[:1]

    def _key_expansion(self, key: bytes) -> list[bytes]:
        w = [key[4 * i : 4 * i + 4] for i in range(8)]
        for i in range(8, 60):
            temp = w[i - 1]
            if i % 8 == 0:
                temp = bytes(
                    a ^ b
                    for a, b in zip(
                        self._sub_word(self._rot_word(temp)),
                        bytes([self.RCON[i // 8 - 1], 0, 0, 0]),
                    )
                )
            elif i % 8 == 4:
                temp = self._sub_word(temp)
            w.append(bytes(a ^ b for a, b in zip(w[i - 8], temp)))
        return [b"".join(w[4 * r : 4 * r + 4]) for r in range(15)]

    @staticmethod
    def _xtime(a: int) -> int:
        return ((a << 1) ^ 0x1b) & 0xff if (a & 0x80) else (a << 1) & 0xff

    def encrypt_block(self, block: bytes) -> bytes:
        state = list(block)
        # Round 0
        rk = self.round_keys[0]
        state = [s ^ k for s, k in zip(state, rk)]

        # Rounds 1 to 13
        for r in range(1, 14):
            # SubBytes
            state = [self.SBOX[b] for b in state]
            # ShiftRows
            state = [
                state[0], state[5], state[10], state[15],
                state[4], state[9], state[14], state[3],
                state[8], state[13], state[2], state[7],
                state[12], state[1], state[6], state[11],
            ]
            # MixColumns
            new_state = [0] * 16
            for c in range(4):
                i = c * 4
                s0, s1, s2, s3 = state[i], state[i + 1], state[i + 2], state[i + 3]
                new_state[i] = self._xtime(s0) ^ (s1 ^ self._xtime(s1)) ^ s2 ^ s3
                new_state[i + 1] = s0 ^ self._xtime(s1) ^ (s2 ^ self._xtime(s2)) ^ s3
                new_state[i + 2] = s0 ^ s1 ^ self._xtime(s2) ^ (s3 ^ self._xtime(s3))
                new_state[i + 3] = (s0 ^ self._xtime(s0)) ^ s1 ^ s2 ^ self._xtime(s3)
            state = new_state
            # AddRoundKey
            rk = self.round_keys[r]
            state = [s ^ k for s, k in zip(state, rk)]

        # Final Round 14
        state = [self.SBOX[b] for b in state]
        state = [
            state[0], state[5], state[10], state[15],
            state[4], state[9], state[14], state[3],
            state[8], state[13], state[2], state[7],
            state[12], state[1], state[6], state[11],
        ]
        rk = self.round_keys[14]
        return bytes(s ^ k for s, k in zip(state, rk))


class _GCMMode:
    """Galois/Counter Mode (GCM) GHASH and counter execution."""

    def __init__(self, aes: _AES256Cipher):
        self.aes = aes
        self.H = int.from_bytes(aes.encrypt_block(b"\x00" * 16), "big")

    def _gf_mult(self, x: int, y: int) -> int:
        R = 0xE1000000000000000000000000000000
        z = 0
        v = x
        for i in range(128):
            if (y >> (127 - i)) & 1:
                z ^= v
            if v & 1:
                v = (v >> 1) ^ R
            else:
                v >>= 1
        return z

    def _ghash(self, data: bytes) -> int:
        y = 0
        for i in range(0, len(data), 16):
            block = data[i : i + 16].ljust(16, b"\x00")
            b_int = int.from_bytes(block, "big")
            y = self._gf_mult(y ^ b_int, self.H)
        return y

    def encrypt(self, nonce: bytes, plaintext: bytes, aad: bytes) -> Tuple[bytes, bytes]:
        if len(nonce) != 12:
            raise ValueError("GCM requires a 12-byte (96-bit) nonce.")

        # J0 = nonce || 0x00000001
        j0 = nonce + b"\x00\x00\x00\x01"
        ctr_val = int.from_bytes(j0, "big")

        # CTR mode encryption
        ciphertext = bytearray()
        for i in range(0, len(plaintext), 16):
            ctr_val = (ctr_val & ~0xFFFFFFFF) | (((ctr_val & 0xFFFFFFFF) + 1) & 0xFFFFFFFF)
            ctr_block = ctr_val.to_bytes(16, "big")
            keystream = self.aes.encrypt_block(ctr_block)
            chunk = plaintext[i : i + 16]
            ciphertext.extend(a ^ b for a, b in zip(chunk, keystream))

        # GHASH over AAD || Ciphertext || lengths
        padded_aad = aad + b"\x00" * ((16 - (len(aad) % 16)) % 16)
        padded_ct = bytes(ciphertext) + b"\x00" * ((16 - (len(ciphertext) % 16)) % 16)
        len_block = (len(aad) * 8).to_bytes(8, "big") + (len(ciphertext) * 8).to_bytes(8, "big")
        s = self._ghash(padded_aad + padded_ct + len_block)

        # Tag = S ^ AES(J0)
        t_mask = int.from_bytes(self.aes.encrypt_block(j0), "big")
        tag = (s ^ t_mask).to_bytes(16, "big")
        return bytes(ciphertext), tag

    def decrypt(self, nonce: bytes, ciphertext: bytes, tag: bytes, aad: bytes) -> bytes:
        if len(nonce) != 12:
            raise ValueError("GCM requires a 12-byte (96-bit) nonce.")

        j0 = nonce + b"\x00\x00\x00\x01"
        padded_aad = aad + b"\x00" * ((16 - (len(aad) % 16)) % 16)
        padded_ct = ciphertext + b"\x00" * ((16 - (len(ciphertext) % 16)) % 16)
        len_block = (len(aad) * 8).to_bytes(8, "big") + (len(ciphertext) * 8).to_bytes(8, "big")
        s = self._ghash(padded_aad + padded_ct + len_block)

        t_mask = int.from_bytes(self.aes.encrypt_block(j0), "big")
        expected_tag = (s ^ t_mask).to_bytes(16, "big")

        if not secrets.compare_digest(tag, expected_tag):
            raise DecryptionError("Authentication tag verification failed. Ciphertext has been tampered with.")

        # CTR mode decryption
        ctr_val = int.from_bytes(j0, "big")
        plaintext = bytearray()
        for i in range(0, len(ciphertext), 16):
            ctr_val = (ctr_val & ~0xFFFFFFFF) | (((ctr_val & 0xFFFFFFFF) + 1) & 0xFFFFFFFF)
            ctr_block = ctr_val.to_bytes(16, "big")
            keystream = self.aes.encrypt_block(ctr_block)
            chunk = ciphertext[i : i + 16]
            plaintext.extend(a ^ b for a, b in zip(chunk, keystream))

        return bytes(plaintext)


# ---------------------------------------------------------------------------
# Master Key Management & CryptoManager API
# ---------------------------------------------------------------------------

class CryptoManager:
    """
    Coordinates master encryption key isolation and AES-256-GCM authenticated encryption.
    """

    def __init__(self, key_path: Optional[str] = None):
        self.key_path = self._resolve_key_path(key_path)
        self._master_key: Optional[bytes] = None

    @staticmethod
    def _resolve_key_path(explicit_path: Optional[str]) -> Path:
        if explicit_path:
            return Path(explicit_path)

        # 1. Systemd LoadCredential runtime directory
        cred_dir = os.environ.get("CREDENTIALS_DIRECTORY")
        if cred_dir:
            p = Path(cred_dir) / "master_key"
            if p.exists():
                return p

        # 2. Environment override
        env_key = os.environ.get("IOTHUB_MASTER_KEY_FILE")
        if env_key:
            return Path(env_key)

        # 3. Standard production location
        prod_path = Path("/etc/iothub/keys/master.key")
        if prod_path.exists():
            return prod_path

        # 4. Project local fallback for development / test environments
        dev_dir = Path(__file__).resolve().parent.parent / ".keys"
        return dev_dir / "master.key"

    def get_or_create_master_key(self) -> bytes:
        """Loads existing master key or creates a fresh 256-bit key with 0600 permissions."""
        if self._master_key:
            return self._master_key

        if self.key_path.exists():
            key = self.key_path.read_bytes().strip()
            if len(key) == 32:
                self._master_key = key
                return self._master_key
            elif len(key) == 64:  # Hex-encoded
                self._master_key = bytes.fromhex(key.decode("ascii"))
                return self._master_key
            elif len(key) == 44:  # Base64-encoded
                self._master_key = base64.b64decode(key)
                return self._master_key
            else:
                raise CryptoError(f"Invalid master key length ({len(key)} bytes) at {self.key_path}")

        # Generate fresh 32-byte (256-bit) key
        self.key_path.parent.mkdir(parents=True, exist_ok=True)
        key = secrets.token_bytes(32)
        self.key_path.write_bytes(key)
        try:
            self.key_path.chmod(0o600)
        except OSError:
            pass

        self._master_key = key
        return self._master_key

    def encrypt_secret(
        self,
        plaintext: str,
        config_key: str,
        key_version: int = 1,
    ) -> Tuple[bytes, bytes]:
        """
        Encrypts plaintext string with AES-256-GCM.
        Returns:
            Tuple[ciphertext_with_tag (BLOB), nonce (BLOB)]
        """
        master_key = self.get_or_create_master_key()
        nonce = secrets.token_bytes(12)  # 96-bit random nonce
        aad = f"{config_key}:v{key_version}".encode("utf-8")
        pt_bytes = plaintext.encode("utf-8")

        try:
            # Check if cryptography package is installed
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            aesgcm = AESGCM(master_key)
            ct_with_tag = aesgcm.encrypt(nonce, pt_bytes, aad)
            return ct_with_tag, nonce
        except ImportError:
            # Fallback pure-python AES-256-GCM
            cipher = _AES256Cipher(master_key)
            gcm = _GCMMode(cipher)
            ct, tag = gcm.encrypt(nonce, pt_bytes, aad)
            return ct + tag, nonce

    def decrypt_secret(
        self,
        ciphertext_with_tag: bytes,
        nonce: bytes,
        config_key: str,
        key_version: int = 1,
    ) -> str:
        """
        Decrypts ciphertext with AES-256-GCM, verifying authenticity tag and AAD.
        Raises DecryptionError if authentication fails.
        """
        master_key = self.get_or_create_master_key()
        aad = f"{config_key}:v{key_version}".encode("utf-8")

        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            aesgcm = AESGCM(master_key)
            pt_bytes = aesgcm.decrypt(nonce, ciphertext_with_tag, aad)
            return pt_bytes.decode("utf-8")
        except ImportError:
            if len(ciphertext_with_tag) < 16:
                raise DecryptionError("Ciphertext too short to contain authentication tag.")
            ct = ciphertext_with_tag[:-16]
            tag = ciphertext_with_tag[-16:]
            cipher = _AES256Cipher(master_key)
            gcm = _GCMMode(cipher)
            pt_bytes = gcm.decrypt(nonce, ct, tag, aad)
            return pt_bytes.decode("utf-8")
        except Exception as exc:
            raise DecryptionError(f"Failed to decrypt secret for key '{config_key}': {exc}")
