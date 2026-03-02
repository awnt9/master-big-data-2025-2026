from src.minio import MinioManager

def main():
    paco = MinioManager()
    paco.upload_image("manolo","data/vicente_payaso.png")


if __name__ == "__main__":
    main()
