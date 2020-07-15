from cryptography.fernet import Fernet

if __name__ == "__main__":
    with open('config.key','w+b') as f:
        key=Fernet.generate_key()
        print(key)
        f.write(key)