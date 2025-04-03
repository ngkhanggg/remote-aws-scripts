import boto3

from settings import *

def recover_deleted_s3_folder(bucket_name, prefix=''):
    s3_client = boto3.client('s3', region_name='ap-southeast-1')

    if not prefix.endswith('/'):
        prefix += '/'

    try:
        response = s3_client.list_object_versions(Bucket=bucket_name, Prefix=prefix)
        versions = response.get('Versions', [])
        delete_markers = response.get('DeleteMarkers', [])

        for marker in delete_markers:
            if marker.get('IsLatest', False):
                key = marker['Key']
                version_id = marker['VersionId']

                print(f"Recovering: {key} | VersionId: {version_id}")

                previous_version = next(
                    (v for v in versions if v['Key'] == key and v['VersionId'] != version_id),
                    None
                )

                if previous_version:
                    s3_client.copy_object(
                        Bucket=bucket_name,
                        CopySource={
                            'Bucket': bucket_name,
                            'Key': key,
                            'VersionId': previous_version['VersionId']
                        },
                        Key=key
                    )

                    print(f"Restored {key} to version {previous_version['VersionId']}")
    except Exception as e:
        raise(e)

if __name__ == '__main__':
    bucket_name = ''
    folder_name = ''
    recover_deleted_s3_folder(bucket_name, folder_name)
