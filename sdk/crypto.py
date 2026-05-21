"""
aicq.crypto — 基于 pynacl 的轻量加密工具模块

提供 Ed25519 签名、X25519 密钥交换、XSalsa20-Poly1305 加解密等功能。
不依赖 shared/crypto，完全基于 pynacl 实现，保持 SDK 自包含。
"""

from __future__ import annotations

import os
from typing import Tuple

try:
    from nacl import utils as nacl_utils
    from nacl.encoding import HexEncoder
    from nacl.signing import SigningKey, VerifyKey
    from nacl.public import PrivateKey, PublicKey, Box
    from nacl.bindings import (
        crypto_secretbox,
        crypto_secretbox_open,
    )
except ImportError:
    raise ImportError(
        "PyNaCl is required for AICQ encryption. "
        "Install it with: pip install pynacl>=1.5"
    ) from None


# ─── Ed25519 签名密钥对 ───────────────────────────────────────────

def generate_signing_keypair() -> Tuple[str, str]:
    """生成 Ed25519 签名密钥对。

    Returns:
        (public_key_hex, secret_key_hex) — 64 字符和 128 字符的十六进制字符串
    """
    signing_key = SigningKey.generate()
    public_key_hex = signing_key.verify_key.encode(encoder=HexEncoder).decode()
    secret_key_hex = signing_key.encode(encoder=HexEncoder).decode()
    return public_key_hex, secret_key_hex


# ─── X25519 密钥交换密钥对 ────────────────────────────────────────

def generate_exchange_keypair() -> Tuple[str, str]:
    """生成 X25519 密钥交换密钥对。

    Returns:
        (public_key_hex, secret_key_hex) — 64 字符和 64 字符的十六进制字符串
    """
    private_key = PrivateKey.generate()
    public_key_hex = private_key.public_key.encode(encoder=HexEncoder).decode()
    secret_key_hex = private_key.encode(encoder=HexEncoder).decode()
    return public_key_hex, secret_key_hex


# ─── 签名 / 验证 ──────────────────────────────────────────────────

def sign(message: str, secret_key_hex: str) -> str:
    """使用 Ed25519 私钥对消息签名。

    Args:
        message: 消息字符串（普通文本或十六进制字符串均可）
        secret_key_hex: Ed25519 私钥的十六进制字符串

    Returns:
        签名的十六进制字符串（128 字符）
    """
    signing_key = SigningKey(secret_key_hex, encoder=HexEncoder)
    # 尝试将消息解析为十六进制，如果失败则作为普通文本处理
    try:
        message_bytes = bytes.fromhex(message)
    except ValueError:
        message_bytes = message.encode("utf-8")
    signed = signing_key.sign(message_bytes)
    return signed.signature.hex()


def verify(message: str, signature_hex: str, public_key_hex: str) -> bool:
    """使用 Ed25519 公钥验证签名。

    Args:
        message: 原始消息字符串（普通文本或十六进制字符串均可）
        signature_hex: 签名的十六进制字符串
        public_key_hex: Ed25519 公钥的十六进制字符串

    Returns:
        签名是否有效
    """
    try:
        verify_key = VerifyKey(public_key_hex, encoder=HexEncoder)
        # 尝试将消息解析为十六进制，如果失败则作为普通文本处理
        try:
            message_bytes = bytes.fromhex(message)
        except ValueError:
            message_bytes = message.encode("utf-8")
        signature_bytes = bytes.fromhex(signature_hex)
        verify_key.verify(message_bytes, signature_bytes)
        return True
    except Exception:
        return False


# ─── 对称加密 / 解密（XSalsa20-Poly1305）────────────────────────────

def encrypt(plaintext: str, nonce_hex: str, key_hex: str) -> str:
    """使用 XSalsa20-Poly1305 对称加密。

    Args:
        plaintext: 明文字符串
        nonce_hex: 24 字节随机数的十六进制字符串（48 字符）
        key_hex: 32 字节密钥的十六进制字符串（64 字符）

    Returns:
        密文的十六进制字符串
    """
    plaintext_bytes = plaintext.encode("utf-8")
    nonce_bytes = bytes.fromhex(nonce_hex)
    key_bytes = bytes.fromhex(key_hex)
    ciphertext = crypto_secretbox(plaintext_bytes, nonce_bytes, key_bytes)
    return ciphertext.hex()


def decrypt(ciphertext_hex: str, nonce_hex: str, key_hex: str) -> str:
    """使用 XSalsa20-Poly1305 对称解密。

    Args:
        ciphertext_hex: 密文的十六进制字符串
        nonce_hex: 24 字节随机数的十六进制字符串（48 字符）
        key_hex: 32 字节密钥的十六进制字符串（64 字符）

    Returns:
        解密后的明文字符串
    """
    ciphertext_bytes = bytes.fromhex(ciphertext_hex)
    nonce_bytes = bytes.fromhex(nonce_hex)
    key_bytes = bytes.fromhex(key_hex)
    plaintext_bytes = crypto_secretbox_open(ciphertext_bytes, nonce_bytes, key_bytes)
    return plaintext_bytes.decode("utf-8")


# ─── X25519 盒式加密 / 解密 ───────────────────────────────────────

def box_encrypt(
    plaintext: str,
    nonce_hex: str,
    sender_sec_hex: str,
    recipient_pub_hex: str,
) -> str:
    """使用 X25519 盒式加密（发送方私钥 + 接收方公钥）。

    Args:
        plaintext: 明文字符串
        nonce_hex: 24 字节随机数的十六进制字符串
        sender_sec_hex: 发送方 X25519 私钥的十六进制字符串
        recipient_pub_hex: 接收方 X25519 公钥的十六进制字符串

    Returns:
        密文的十六进制字符串
    """
    sender_private = PrivateKey(sender_sec_hex, encoder=HexEncoder)
    recipient_public = PublicKey(recipient_pub_hex, encoder=HexEncoder)
    box = Box(sender_private, recipient_public)
    plaintext_bytes = plaintext.encode("utf-8")
    nonce_bytes = bytes.fromhex(nonce_hex)
    encrypted = box.encrypt(plaintext_bytes, nonce_bytes)
    return encrypted.ciphertext.hex()


def box_decrypt(
    ciphertext_hex: str,
    nonce_hex: str,
    recipient_sec_hex: str,
    sender_pub_hex: str,
) -> str:
    """使用 X25519 盒式解密（接收方私钥 + 发送方公钥）。

    Args:
        ciphertext_hex: 密文的十六进制字符串
        nonce_hex: 24 字节随机数的十六进制字符串
        recipient_sec_hex: 接收方 X25519 私钥的十六进制字符串
        sender_pub_hex: 发送方 X25519 公钥的十六进制字符串

    Returns:
        解密后的明文字符串
    """
    recipient_private = PrivateKey(recipient_sec_hex, encoder=HexEncoder)
    sender_public = PublicKey(sender_pub_hex, encoder=HexEncoder)
    box = Box(recipient_private, sender_public)
    nonce_bytes = bytes.fromhex(nonce_hex)
    ciphertext_bytes = bytes.fromhex(ciphertext_hex)
    plaintext_bytes = box.decrypt(ciphertext_bytes, nonce_bytes)
    return plaintext_bytes.decode("utf-8")


# ─── 辅助函数 ──────────────────────────────────────────────────────

def generate_nonce() -> str:
    """生成 24 字节随机 nonce。

    Returns:
        48 字符的十六进制字符串
    """
    return nacl_utils.random(24).hex()


def compute_fingerprint(public_key_hex: str) -> str:
    """计算公钥指纹（冒号分隔的十六进制）。

    Args:
        public_key_hex: 公钥的十六进制字符串

    Returns:
        冒号分隔的指纹字符串，如 "ab:cd:ef:12:..."
    """
    raw = bytes.fromhex(public_key_hex)
    # 取 SHA-256 的前 8 字节作为指纹（简单起见直接用公钥前 8 字节）
    fingerprint_bytes = raw[:8]
    return ":".join(f"{b:02x}" for b in fingerprint_bytes)
