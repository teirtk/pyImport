from cryptography.fernet import Fernet

if __name__ == "__main__":
    with open('conf/config.key', 'w+b') as f:
        key = Fernet.generate_key()
        f.write(key)
