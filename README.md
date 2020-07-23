## Atac Roma open data ingestion system
Data ingestion system to analyze the status of public transports in Rome.

The open data is given in real time by [Atac Roma](https://romamobilita.it/it/tecnologie/open-data).

This project has been developed using [ATAC-Monitor](http://www.atacmonitor.com/) as an example of data ingestion infrastructure.

You can visualize an example of the results of the data elaboration [here](https://lsparkzwz.github.io/atacmonitor/), under Average waiting minutes, Longest waiting time and Average waiting time by Location and Waiting times divided by Rome districts and neighborhoods.

The queries used to elaborate the data can be found in '/lambda functions/layers/updates_feed/python/queries.py'.

### Setup:
0. This project is meant to be run in a Standard AWS Account (a free trial standard account is fine).
1. Clone the repository.

### AWS buckets
3. Create three S3 buckets:
   * One for storing the data ingestion feed, 
   * One for storing the Athena query results,
   * One for storing static files.
	
4. In the root of the static files bucket create two folders: locations and routes,
   * Insert in the locations folder the locations.csv file found in the /static_files/locations/ folder of this project.
   * Insert in the routes folder the routes.csv file found in the /static_files/routes/ folder of this project.
5. Change the read policy of the Athena query results bucket according to your preferences, that is where the final results will be stored.

### AWS Athena
6. Create an AWS Athena database.
7. Create three tables using the three queries found in the /athena/create_table.md file of this project.
   * Modify the LOCATION query line by replacing the square brackets with the name of the respective bucket you create.
   * ex. LOCATION 's3://[ NAME OF THE BUCKET FOR THE DATA INGESTION FEED ]/' becomes LOCATION 's3://stops-feed/', where stops-feed is the name of the bucket.

### Data ingestion AWS Lambda Function
8. Create a Lambda Function from the AWS Management Console, with Python 3.8 as Runtime.
9. In the Lambda Function page, in the Permissions tab, click on the Execution Roles:
   * Attach one policy to grant the Lambda S3 Access permissions and one policy to grant it Athena Access permissions.
10. Delete the code found in lambda_function.py found in the Function Code windows found in the Lambda Function page.	
11. Copy and paste the code found in /lambda_functions/lambda_function.py file of this project into the Function Code windows found in the Lambda Function page.	
12. From the AWS Lambda page, click on Layers found in the menu on the left.
![layer menu](https://i.imgur.com/W8DEdb9.png)
14. Go to the /lambda functions/layers/gtfs/ folder of the project, and create a zip file with the entire python folder that was inside the folder.
13. Create a layer named 'gtfs' with Python 3.8 as Runtime and upload the zip file you just created by clicking the upload button.
14. Go to the /lambda functions/layers/updates_feed/ folder of the project, and create a zip file with the entire python folder that was inside the folder.
15. Create a layer named 'updates_feed' with Python 3.8 as Runtime and upload the zip file you just created by clicking the upload button.
16. Go back into the page of the Lambda you created before and click on Layers.
![layer menu](https://i.imgur.com/xqr1LCS.png)
17. Choose Add Layer -> Custom Layer, and add the two Layers you just created.
18. From the main page of the Lambda function, set these four environment variables:
   * ATHENA_DB = [name of your Athena DB]
   * BUCKET_NAME = [name of the bucket used for data ingestion]
   * ATHENA_TABLE = stops-feed
   * TRIP_UPDATES_FEED_URL = https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb
![lambda env](https://i.imgur.com/mxbU6mE.png)        

### Elaboration results AWS Lambda Function
19. Create a Lambda Function from the AWS Management Console, with Python 3.8 as Runtime.
20. In the Lambda Function page, in the Permissions tab, click on the Execution Roles:
    * Attach one policy to grant the Lambda S3 Access permissions.
21. Delete the code found in lambda_function.py found in the Function Code windows found in the Lambda Function page.	
22. Copy and paste the code found in /lambda_functions/results_manager.py file of this project into the Function Code windows found in the Lambda Function page.
23. Set the following environment variable:
    * RESULTS_BUCKET = [name of the bucked used to store the Athena query results]
24. From the main page of the Lambda function, click the button "Add trigger" and insert the following parameters:
    * Select trigger = S3
    * Bucket = [name of the bucket used for data ingestion]
    * Event type = All object create events
    * Prefix = results/
    * Suffix = .csv

## Run:
After following the setup steps you just need to run first Lambda you created and retrieve the final results inside the bucket you created to store Athena query results.
If you want the entire system to keep running on a fixed schedule you can use Amazon EventBridge to create a rule that runs the first Lambda on a fixed schedule.
While the system runs by itself you just need to retrieve the results you find in the apposite bucket.



For any questions please feel free to contact me.
