import os


# local_path is the local directory in which we want to download the data from sftp location

# Setting working directory
os.chdir('C:\\sftptoblob')

directory_list = os.listdir(os.getcwd())
directory_name = 'DownloadedFiles'

if (directory_name in directory_list) is True:
    print("Directory to download files is found")
else:
    print("Directory to download files is not found; creating the directory")

    try:
        os.mkdir(os.path.join(os.getcwd(),directory_name))
    except OSError:
        print("Creation of the directory %s failed" % os.getcwd())
    else:
        print("Successfully created the directory %s" % os.path.join(os.getcwd(),directory_name))


local_path = os.path.join(os.getcwd(),directory_name)
print("Local Path in which Files will be downloaded is {}".format(local_path))


# Remote Path

remote_path = '/Optimum/Australia/FP_PROD'


# SFTP Details

myHostname = os.getenv('SFTPHostName')
myUsername = os.getenv('SFTPUserName')
myPassword = os.getenv('SFTPPassword')


connect_str = os.getenv('CONNECT_STR')

# Azure Blob Storage Details

# Set Environment variable using cmd and use it if you want

# setx CONNECT_STR "<yourconnectionstring>"

url1 = r'https://'
url2 = os.getenv('BlobAccountName')
url3 = r'.blob.core.windows.net'

url = []
url.append(url1)
url.append(url2)
url.append(url3)

account_name = ''.join(url)
account_key = os.getenv('BlobAccountKey')
container_name = os.getenv('BlobContainerName')

# Email List

fromaddr = "Donovan.Gregory@sodexo.com"
toaddr = 'raihan.shafique@sodexo.com'  # Seperate more email address with comma but inside the quote all of them
subject = 'Test'
body = 'Test'

# Send Email Functionality on or off ('Yes' or 'No')

send_email_on = 'Yes'