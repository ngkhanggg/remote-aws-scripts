import boto3

from settings import *

def delete_s3_folder(bucket_name, folder_name):
  s3_client = boto3.client('s3', region_name='ap-southeast-1')

  response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

  if 'Contents' not in response:
    return

  while 'Contents' in response:
    try:
      objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]

      delete_response = s3_client.delete_objects(
        Bucket=bucket_name,
        Delete={'Objects': objects_to_delete}
      )

      deleted_files = [item['Key'] for item in delete_response.get('Deleted', [])]

      print(f"Deleted files:\n{deleted_files}")

      response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    except Exception as e:
      print(f"An error occurred: {str(e)}")

if __name__ == '__main__':
  BUCKET_TO_DELETE = ''
  FOLDER_TO_DELETE = ''

  delete_s3_folder(BUCKET_TO_DELETE, FOLDER_TO_DELETE)
