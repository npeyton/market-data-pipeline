# make_keys.py
# Creates a matching pair of keys for Dagster's Snowflake login.
# You only need to run this ONCE.

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

# Create a brand-new random key pair.
key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

# Save the PRIVATE half to a file called snowflake_key.p8.
# This file is the secret. Never share it or upload it anywhere.
private_bytes = key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)
with open("snowflake_key.p8", "wb") as f:
    f.write(private_bytes)

# Turn the PUBLIC half into one long line of text, which is the format
# Snowflake wants, and print it on screen so you can copy it.
public_bytes = key.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
)
public_text = (
    public_bytes.decode()
    .replace("-----BEGIN PUBLIC KEY-----", "")
    .replace("-----END PUBLIC KEY-----", "")
    .replace("\n", "")
)
print("Copy everything on the next line (it starts with MII):")
print(public_text)