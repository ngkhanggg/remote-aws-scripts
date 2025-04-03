import boto3
import os

from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from settings import *

def download_s3_folder(bucket, prefix, dest_local_folder_path):
    try:
        os.makedirs(dest_local_folder_path, exist_ok=True)

        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get('Contents'):
                s3_file_path = obj['Key']
                if s3_file_path.endswith('/'):
                    continue

                relative_path = os.path.relpath(s3_file_path, prefix)
                local_file_path = os.path.join(dest_local_folder_path, relative_path)
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                print(f"Downloading {s3_file_path} to {local_file_path} ...")

                s3_client.download_file(bucket, s3_file_path, local_file_path)
        print('Download completed!')
    except NoCredentialsError:
        print('AWS credentials not found!')
    except PartialCredentialsError:
        print('Incomplete AWS credentials!')
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    BUCKET_NAME = ''
    FOLDER_NAME = ''
    LOCAL_FOLDER = ''

    download_files(BUCKET_NAME, FOLDER_NAME, LOCAL_FOLDER)
