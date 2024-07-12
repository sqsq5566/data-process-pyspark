import requests
import boto3
from io import BytesIO
from tqdm import tqdm
import zipfile

# 替换为您的 S3 存储桶名称
s3_bucket_name = '47-data-test'

# 指定 ZIP 文件 URL
zip_file_url = 'https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2019.zip'

# 创建 S3 客户端
#s3 = boto3.Session(profile_name='default').resource('s3')
s3=boto3.client('s3')


# 定义下载进度条
def download_progress(response):
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024
    progress = tqdm(total=total_size, unit='iB', unit_scale=True)

    # 使用 BytesIO 存储下载的数据
    download_bytes = BytesIO()

    for chunk in response.iter_content(chunk_size=block_size):
        if chunk:
            progress.update(len(chunk))
            download_bytes.write(chunk)

    progress.close()
    return download_bytes

# 下载并上传文件到 S3
with requests.get(zip_file_url, stream=True,timeout=(10,6000)) as response:
    response.raise_for_status()
    zip_file_bytes = download_progress(response)

    with zipfile.ZipFile(zip_file_bytes) as zip_ref:
        for zip_info in zip_ref.infolist():
            filename = zip_info.filename
            if not filename.startswith("__MACOSX/"):
                s3_key = f'data/{filename}'
        # 上传文件到 S3
                s3.upload_fileobj(
                   zip_ref.open(filename),
                   s3_bucket_name,
                   s3_key
                )

print("File uploaded to S3 successfully.")