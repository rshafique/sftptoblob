import pysftp
import os
import paramiko
import config
import func
import time
import uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# from azure.storage.blob import BlockBlobService

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

# This is used during development phase for testing purpose
if config.send_email_on == 'Yes':
    try:
        func.send_email(config.fromaddr,config.toaddr,'SFTP To Blob Python Job Started','Started')
    except:
        print("Email Sending Failed")
        func.error_log('Email Sending Failed')

# Making sure the solution does not fail even if there are large files in the sftp location

# paramiko.sftp_file.SFTPFile.MAX_REQUEST_SIZE = pow(2, 22) # 4MB per chunk # Slow but reliable with large files >30GB
paramiko.sftp_file.SFTPFile.MAX_REQUEST_SIZE = pow(2, 40)



# Current working directory
dir = config.local_path

myHostname = config.myHostname
myUsername = config.myUsername
myPassword = config.myPassword

Iterator = 0

for Iterator in  range(20):
    Iterator = Iterator + 1

    try:

        print("Attempting to Connect to SFTP")

        sftp = pysftp.Connection(host=myHostname, username = myUsername, password = myPassword, cnopts=cnopts)

        print("Connection to SFTP successfully established")


        # This is used during development phase for testing purpose
        func.send_email(config.fromaddr, config.toaddr, 'SFTP To Blob Python SFTP Connection Established', 'Connection to SFTP successfully established')

        sftp.cwd('/Optimum/Australia/')

        remote_file_list = sftp.listdir_attr(config.remote_path)


        for attr in remote_file_list:

            print("Downloading File: %s" % attr.filename)

            try:
                if attr.st_mtime == os.path.getmtime(os.path.join(config.local_path, attr.filename)):
                    print("Latest file already exists")
                else:
                    remote_file_name_with_path = [config.remote_path, attr.filename]
                    remote_file_name_with_path = '/'.join(remote_file_name_with_path)

                    local_file_name_with_path = [dir, attr.filename]
                    local_file_name_with_path = '\\'.join(local_file_name_with_path)

                    sftp.get(remote_file_name_with_path, local_file_name_with_path, preserve_mtime=True)

                    print("Downloaded File: %s" % attr.filename)

            except:
                remote_file_name_with_path = [config.remote_path, attr.filename]
                remote_file_name_with_path = '/'.join(remote_file_name_with_path)

                local_file_name_with_path = [dir, attr.filename]
                local_file_name_with_path = '\\'.join(local_file_name_with_path)

                sftp.get(remote_file_name_with_path, local_file_name_with_path, preserve_mtime=True)

                print("Downloaded File: %s" % attr.filename)

        # The following code is for downloading all items in a directory in the sftp location
        # sftp.get_d('FP_PROD',dir,preserve_mtime=True)

        # Get ouf of the loop as the connection worked and files downloaded

        break

    except:

        print("The attempt to connect to SFTP failed for %s time(s)" % Iterator)
        func.error_log('The attempt to connect to SFTP failed ')
        Iterator = Iterator + 1
        time.sleep(10)

        # This is used during development phase for testing purpose
        if config.send_email_on == 'Yes':
            try:
                func.send_email(config.fromaddr, config.toaddr, 'SFTP To Blob Python Job Failed', 'The attempt to connect to SFTP failed')
            except:
                print("Email Sending Failed")
                func.error_log('Email Sending Failed')



        # transport.close()

        #import os
        #os.system(". C:\Anaconda\evvs\sftptoblob2\Scripts\activate && python && sftptoblob.py")
        md = r'c:\Anaconda\envs\sftptoblob3\Scripts\python c:\sftptoblob\sftptoblob.py'
        # md = 'c:\\Anaconda\\envs\\sftptoblob2\\Scripts\\python c:\\Users\\Donovan.Gregory\\PycharmProjects\\untitled\\sftptoblob.py'

        # The following code makes the iterator invalid, however, this is the only way the connection is 100% reliably established
        # as with iterator even after 20 trial the connection failed
        # it was noted that the python code had to break and run again to establish connection through repeated trial
        # therefore, this technique was used.

        os.system(md)




sftp.close()




## Pushing to Blob Storage

# Getting list of files to upload

file_list = os.listdir(config.local_path)

print("The following files will be uploaded to Blob Storage")
print(file_list)




# Blob Storage Connection String Retrieval from environment

try:
    connect_str = os.getenv('CONNECT_STR')

    # List the blobs in the container
    print("\nListing blobs...")

    service = BlobServiceClient.from_connection_string(connect_str)
    blob_service_client = service

    # Getting List of Blobs in the Container

    container_client = ContainerClient(account_url=config.account_name, container_name=config.container_name,
                                       credential=config.account_key)

    blobs_list = container_client.list_blobs()

    blob_file_list = []

    for blob in blobs_list:
        print(blob.name + '\n')
        # print(blob.last_modified)

        blob_file_list.append(blob.name)

except:
    print("Blob Connection could not be established")
    func.error_log('Blob Connection could not be established')
    if config.send_email_on == 'Yes':
        try:
            func.send_email(config.fromaddr, config.toaddr, 'Blob Connection could not be establishedd','Blob Connection could not be established')
        except:
            print("Email Sending Failed")
            func.error_log('Email Sending Failed')


# Upload Blobs in the container

try:
    for i in range(len(file_list)):

        print(i)

        # Establish file connection details
        blob_client = blob_service_client.get_blob_client(container='blobparking', blob=file_list[i])

        print("\nUploading to Azure Storage as blob:\n\t" + file_list[i])

        # Create a file in local Documents directory to upload and download
        local_path = config.local_path
        local_file_name = file_list[i]
        local_file_name_with_path = os.path.join(local_path, local_file_name)
        upload_file_path = os.path.join(local_path, local_file_name)

        # Upload the file
        with open(upload_file_path, "rb") as data:

            if file_list[i] in blob_file_list:
                blob_client.delete_blob()

            blob_client.upload_blob(data)

except:
    print("Blob Upload Failed")
    func.error_log('Blob Upload Failed')

    if config.send_email_on == 'Yes':
        try:
            func.send_email(config.fromaddr, config.toaddr, 'Blob Upload Failed','Blob Upload Failed')
        except:
            print("Email Sending Failed")
            func.error_log('Email Sending Failed')


# This is used during development phase for testing purpose
if config.send_email_on == 'Yes':
    try:
        func.send_email(config.fromaddr, config.toaddr, 'SFTP To Blob Python Job Completed', 'The Job is now complete')
    except:
        print("Email Sending Failed")
        func.error_log('Email Sending Failed')