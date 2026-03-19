import hashlib

def generate_hash(value):
    return hashlib.md5(str(value).encode()).hexdigest()
