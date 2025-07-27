# Medallion Architecture implementation from S3 to Hive to Redshift

### About the project
This project is for understanding more clearly in how to implement the Medallion. We will walk through from setup to actually load the data to Redshift.

### Introduction
This project is the implementation of data pipeline from S3 to HIVE external table (stored in S3) to Redshift. First we gonna read from S3 and process it in Amazon EMR like this is actually a big data pipeline, including format string to date with pyspark. Once it's cleaned, we store it in Hive external table, data in S3, metastore in Glue data catalog. Then we gonna to easy dimensional modeling and load them into Redshift.  
> We will skip the ingestion from data source to bronze layer because each company has their own way to ingest and we cannot predict that. You can actually add a ingestion method in some way if you want to get more advanced.

### Setting up
How to set up your cluster for this project in detail. Keep in my that this is an **emr-7.9.0**.  
#### Set up your cluster
1. Application Bundle
  - Hadoop
  - Hive
  - Spark
  
  Or simply select **Spark Interactive**  
  
2. AWS Glue Data Catalog settings  
  - choose **Use for Hive table metadata**
  - if you don't do so metadata will be stored in MySQL in the driver node in the cluster, once it's terminated, you will lose the metadata.
3. Cluster configuration
  - choose the instance type (resource for processing) that suit your need the most
  - I recommend you to select it on your own even you are using free trial account because EMR cluster will waste your credit for free if it is too big. If you leave with the default, you can see your credit is decreased significantly.
4. Networking
  - search for VPC in the console and create new VPC with public subnet (don't create with 0 public subnet)
  - You can research more detail on the online stuff about subnet because I just followed the other along.
5. Steps
  - Steps are the steps allows you to run your application in the cluster using UI instead of CLI.
  - But in this project we will use CLI to do the jobs through ssh server.
6. Cluster logs
  - I recommend you to create `logs` folder in your s3 as well to store the logs
7. Security configuration and EC2 key pair (required)
  - select your existed key pair or create new
8. IAM roles
  - Amazon EMR service role
    - I recommend you to select **Create a service role** because you will be lack some services for EMR if you create a role by your own.
  - EC2 instance profile for Amazon EMR
    - choose your existed instance profile, if you don't have one, just select **Create an instance profile**  
  
You are all set! Now just click on **Create cluster**.

#### Set up the permission
You need to add additional permission for you IAM role and instace profile, just follow along.
1. Go to IAM and select Roles
2. Select your instance profile and click on Add permission then Attach policies
3. Add **AWSGlueServiceRole** to the role
4. Do the same with your IAM role ,but for IAM role, you also need to add AmazonS3FullAccess (allow full access may not be a good practice but this is just self implemented project for better understanding so we simplify the task)

#### Set up for managing the cluster
We need `openssh-server` to get into the cluster. First, install the package.    
On Debian based:  
```bash
sudo apt update
sudo apt install openssh-server
```
On Arch based:
```bash
sudo pacman -Syu
sudo pacman -S openssh
```
Rather than `ssh`, we need 2 things to make us be able to get into the cluster  
  - key pair
  - make the master node to see our IP
  
Once you finish the ssh installation, we need to adjust your key pair permission to make it not too open (required). Just simply run:
```bash
chmod 400 ~/path/to/your-keypair.pem
```
Now, it's time to manage the master node.  
1. Go to the EC2 and click on **Security Groups** then **Security group ID** that has **Security group name** "ElasticMapReduce-master".  
2. Click on Edit inbound rules
3. Scroll to the bottom and click on Add rule
4. On **Type** drop down, select **SSH**
5. On **Source** drop down, select **MyIP**

After you create your cluster, it will start your cluster. Once it's done, the status will be **Waiting**.  
- Go to EMR >> Cluster under EMR on EC2 >> click on your cluster ID and it will show the cluster summary  
- Look for **Primary node public DNS** and click on **Connect to the Primary node using SSH**
- Copy the command and paste it to your terminal then press enter, the command will look something like:
  ```
  ssh -i ~/your-keypair.pem hadoop@ec2-your-ec2-or-something.your-region.compute.amazonaws.com
  ```
Now you are connected to the cluster!  

### Process your data
First, don't forget to upload the `samplesuperstore.csv` into your s3 bucket. Once you finish it, you're ready to go.  
  
You can submit the job either manually submitting in the cluster or using steps from EMR. But in this project, we will manually submit.  
2 ways to do it is:  
1. create and empty file by typing
```bash
nano run.py
```
then copy every thing in `runtohive.py` and paste it to the script. Keep in mind that you need to adjust your s3 path in the script.  
Then  
```bash
spark-submit run.py
```
After the job is finished, you will get the metadata in your Glue data catalog and have output csv files in your output path.  
  
2. use aws [script-runner.jar](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html)
```
aws emr add-steps \
--cluster-id j-2AXXXXXXGAPLF \
--steps Type=CUSTOM_JAR,Name="Run spark-submit using command-runner.jar",ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=[spark-submit,S3://amzn-s3-demo-bucket/my-app.py,ArgName1,ArgValue1,ArgName2,ArgValue2]
```
I've never tried this method before but you can it if you would.  

### Load to Redshift
Once you have cleaned data you can dimensional model and load to Redshift using Glue job.

### Create connection between Glue and Redshift
1. go to Glue then Connections
2. create connection under Connections
3. select Redshift as an input
4. 
