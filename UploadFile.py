from ibm_botocore.client import Config
import ibm_boto3
import requests
from datetime import datetime

cos_credentials={
  "apikey": "",
  "endpoints": "",
  "iam_apikey_description": ",
  "iam_apikey_name": "",
  "iam_role_crn": "",
  "iam_serviceid_crn": "",
  "resource_instance_id": ""
}

bucketName="<bucket_name>"
upload_filename="test"

auth_endpoint = 'https://iam.cloud.ibm.com/identity/token'
service_endpoint = 'https://s3.eu-de.cloud-object-storage.appdomain.cloud'

cos = ibm_boto3.client('s3',
                         ibm_api_key_id=cos_credentials['apikey'],
                         ibm_service_instance_id=cos_credentials['resource_instance_id'],
                         ibm_auth_endpoint=auth_endpoint,
                         config=Config(signature_version='oauth'),
                         endpoint_url=service_endpoint)

###############################LIST ALL BUCKETS###########################
for bucket in cos.list_buckets()['Buckets']:
    print(bucket['Name'])


################################HTTPS UPLOAD##############################
print(datetime.now())
cos.upload_file(upload_filename, bucketName,upload_filename)
print(datetime.now())


#########################ASPERA FASP TRANSFER##############################

from ibm_s3transfer.aspera.manager import AsperaTransferManager
from ibm_s3transfer.aspera.manager import AsperaConfig
ms_transfer_config = AsperaConfig(multi_session=10,
                                  multi_session_threshold_mb=60)

# Create the Aspera Transfer Manager
transfer_manager = AsperaTransferManager(client=cos,
                                         transfer_config=ms_transfer_config)


print(datetime.now())
# Create Transfer manager
with AsperaTransferManager(cos) as transfer_manager:

    # Perform upload
    future = transfer_manager.upload(upload_filename, bucketname, cos_credentials['resource_instance_id'])

    # Wait for upload to complete
    future.result()
    
print(datetime.now())
