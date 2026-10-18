#!/usr/bin/env python3
"""Create local server secrets without printing them; never overwrite .env."""
import base64, os, secrets
from pathlib import Path
root = Path(__file__).resolve().parent.parent
path = root / ".env"
if path.exists():
    print(".env already exists; preserving existing configuration.")
else:
    content = (root / ".env.example").read_text()
    content = content.replace("replace-with-generated-secret", secrets.token_urlsafe(48))
    content = content.replace("replace-with-generated-fernet-key", base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())
    content = content.replace("replace-with-generated-password", secrets.token_hex(24))
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(content)
    print("Created .env with private local secrets. Add OPENAI_API_KEY for real AI mode.")
