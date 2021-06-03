# Splunk Azure Blob Buckets

## INDEX
### 1. Description
### 2. How to use and import library
### 3. CALLS - APPS
#### 3.1 CALL: getContainers
#### 3.2 CALL: getAppData
#### 3.3 CALL: getAppNames
#### 3.4 CALL: getAppVersions


## - 1 - Description
Using custom azure lib, get and put data related to Azure blob storage

## - 2 - How to use in your own script:
1. put wr_azure_lib.py where you can import it, e.g. in a subfolder called "lib"
2. from lib import wr_azure_lib as wazure
3. In your main script: blob_service = wazure.BlobService('DefaultEndpointsProtocol=https;AccountName=splunkcloudblobdownload;AccountKey=YuAaifdfsdfsdfsdfsfsfsdfsHeyZ+jXfKc2zeDu/tTvUE1mDh9dfwf3f323f23r2f2f23f2f2f2m80sA2BwBDXA==;EndpointSuffix=core.windows.net')
4. Get the data you want

## - 3 - CALLS - APPS -----------------------------------------
### - 3.1 - getContainers() -> list:
	container_list = blob_service.getContainers()
#### RETURNS: list of dicts containing blob storage containers (and optional metadata) OR a simple list of container names if specified
#### RETURN EXAMPLE:
	{'name': '707fa789-b998-4d55-a887-026a8234343f', 'last_modified': datetime.datetime(2021, 5, 17, 21, 42, 55, tzinfo=datetime.timezone.utc), 'etag': '"0x8D9197CBADA4256"', 'lease': {'status':  'unlocked', 'state': 'available', 'duration': None}, 'public_access': None, 'has_immutability_policy': False, 'deleted': None, 'version': None, 'has_legal_hold': False, 'metadata': None,   'encryption_scope': <azure.storage.blob._models.ContainerEncryptionScope object at 0x7f2848a4c3c8>}
	{'name': 'db452702-97f5-4020-aeef-5753c75a28a9', 'last_modified': datetime.datetime(2021, 5, 17, 21, 42, 17, tzinfo=datetime.timezone.utc), 'etag': '"0x8D9197CA4A046BA"', 'lease': {'status':  'unlocked', 'state': 'available', 'duration': None}, 'public_access': None, 'has_immutability_policy': False, 'deleted': None, 'version': None, 'has_legal_hold': False, 'metadata': None,   'encryption_scope': <azure.storage.blob._models.ContainerEncryptionScope object at 0x7f284576a6d8>}
	{'name': 'dfe', 'last_modified': datetime.datetime(2021, 5, 17, 21, 45, 9, tzinfo=datetime.timezone.utc), 'etag': '"0x8D9197D0AAECB31"', 'lease': {'status': 'unlocked', 'state': 'available',  'duration': None}, 'public_access': None, 'has_immutability_policy': False, 'deleted': None, 'version': None, 'has_legal_hold': False, 'metadata': None, 'encryption_scope': <azure.storage.blob.	_models.ContainerEncryptionScope object at 0x7f284576a3c8>}
	{'name': 'meglintestcontainer', 'last_modified': datetime.datetime(2021, 4, 12, 9, 26, 54, tzinfo=datetime.timezone.utc), 'etag': '"0x8D8FD951C6D36F9"', 'lease': {'status': 'unlocked', 'state':   'available', 'duration': None}, 'public_access': None, 'has_immutability_policy': False, 'deleted': None, 'version': None, 'has_legal_hold': False, 'metadata': None, 'encryption_scope': <azure.storage.blob._models.ContainerEncryptionScope object at 0x7f2848ab5860>}

#### ( OPTIONAL: ) specify True to only get container names
	container_list = blob_service.getContainers(names_only=True)
#### Returned Raw Data
	for c in container_list:
		print(c)

	OUTPUT:

		Azure Blob Storage v12.8.1
		707fa789-b998-4d55-a887-026a8234343f
		db452702-97f5-4020-aeef-5753c75a28a9
		dfe
		meglintestcontainer
