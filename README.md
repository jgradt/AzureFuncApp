# AzureFuncApp

The AzureFuncApp project is an Azure function project written in C# with the following Http Trigger endpoints:

* ProcessEmployees - This endpoint accepts a GET request with a filename parameter of a json file.  The file should exist in an Azure blob container.  It will be transformed via some simple ETL processing into another file format that is uploaded to the same container.  A test file with data is included in the project (Test data is [MOCKED](https://mockaroo.com/).  No personal data in this file is real).
