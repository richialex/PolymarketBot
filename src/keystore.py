"""Minimal encrypted Ethereum-keystore support for local and server launches.

The private key is encrypted at rest with a password supplied interactively at
process startup.  The password is deliberately never read from .env.
"""
from __future__ import annotations

import getpass
import json
import os
from pathlib import Path

from eth_account import Account


def _prompt_password(confirm: bool = False) -> str:
    password = getpass.getpass("Keystore password: ")
    if not password:
        raise RuntimeError("Keystore password must not be empty.")
    if confirm and password != getpass.getpass("Repeat keystore password: "):
        raise RuntimeError("Passwords do not match.")
    return password


def decrypt_private_key(filename: str) -> str:
    """Return a private key from a standard Web3 Secret Storage keystore."""
    path = Path(filename).expanduser()
    if not path.is_file():
        raise RuntimeError(f"Keystore file was not found: {path}")
    try:
        encrypted = json.loads(path.read_text(encoding="utf-8"))
        key = Account.decrypt(encrypted, _prompt_password())
    except ValueError as exc:
        raise RuntimeError("Could not decrypt keystore: wrong password or invalid file.") from exc
    return "0x" + key.hex()


def create_keystore(filename: str) -> None:
    """Interactively create an encrypted keystore without exposing input in shell history."""
    path = Path(filename).expanduser()
    if path.exists():
        raise RuntimeError(f"Refusing to overwrite existing keystore: {path}")

    private_key = getpass.getpass("Private key to encrypt (input hidden): ").strip()
    try:
        Account.from_key(private_key)
    except ValueError as exc:
        raise RuntimeError("The supplied value is not a valid Ethereum private key.") from exc

    encrypted = Account.encrypt(private_key, _prompt_password(confirm=True))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(encrypted, indent=2), encoding="utf-8")
    if os.name != "nt":
        path.chmod(0o600)
    print(f"Encrypted keystore created: {path}")
    print("Remove PRIVATE_KEY from .env before starting the bot.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Create an encrypted Ethereum keystore.")
    parser.add_argument("command", choices=("create", "address"), help="operation to perform")
    parser.add_argument("--file", default="secrets/wallet.json", help="keystore file path")
    args = parser.parse_args()
    if args.command == "create":
        create_keystore(args.file)
    else:
        # This prints only the public address derived from the encrypted key.
        print(Account.from_key(decrypt_private_key(args.file)).address)
