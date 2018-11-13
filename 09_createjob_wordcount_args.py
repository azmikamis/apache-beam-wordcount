from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
from datetime import datetime

credentials = GoogleCredentials.get_application_default()
service = build('dataflow', 'v1b3', credentials=credentials)
request = service.projects().templates().create(
              projectId='PROJECT-ID',
              body={"gcsPath":'gs://BUCKET-NAME/wordcount_template',
                    "jobName":"JOBNAME-USERNAME-" + datetime.strftime(datetime.now(),'%Y%m%d-%H%M%S%z'),
                    "parameters": {"input" : "gs://BUCKET-NAME/macbeth.txt",},})
response = request.execute()
print(response)