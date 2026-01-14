from dataclasses import dataclass


@dataclass
class HashConfig:
    required_hash_algorithms = ["md5", "sha1", "sha256"]
