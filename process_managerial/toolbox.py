import datetime
import hashlib
from typing import Tuple

def generate_time_based_hash() -> Tuple[datetime.datetime, str]:
  """
  Generates a SHA-256 hash based on the current high-resolution timestamp.

  This function captures the current time, including microseconds, converts
  it to a standard string format (ISO 8601), encodes it to bytes, and
  then computes the SHA-256 hash of those bytes.

  The resulting hash changes rapidly with time. While not guaranteed to be
  globally unique like a UUID (as two calls could theoretically happen
  within the same microsecond or a hash collision could occur, though unlikely
  for SHA-256), it's highly unique for practical purposes where uniqueness
  is tied to the time of generation.

  Returns:
      datetime.datetime: A datetime of when the time based hash function was called
      str: A hexadecimal string representation of the SHA-256 hash.
  """
  # Get the current timestamp with microsecond precision
  now = datetime.datetime.now(tz=datetime.timezone.utc)

  # Convert the timestamp to a string format (ISO 8601 is standard)
  # Example: '2025-04-02T11:36:16.123456'
  time_str = now.isoformat()

  # Encode the string into bytes (hash functions require bytes)
  time_bytes = time_str.encode('utf-8')

  # Create a SHA-256 hash object
  hasher = hashlib.sha256()

  # Feed the time bytes into the hasher
  hasher.update(time_bytes)

  # Get the hexadecimal representation of the digest
  hash_hex = hasher.hexdigest()

  return now, hash_hex

