# ETL from s3 to hive table in s3  

### About the project
This project is for understanding more clearly in how to process big data with pyspark and load it into hive table on Amazon EMR cluster. We will leave the extraction and destination for now because there are tons of the combination of how to integrate data and load into data storage, this project will focus on how to make your big data processing works in the cluster.

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
  - if you don't do so metadata will be stored in MySQL in the driver node in the cluster, once it's terminated, you will lose the metadata
3. Cluster configuration
  - choose the instance type (resource for processing) that suit your need the most
  - if you are using free trial version of AWS, you can leave it with the default
4. Networking
  - search for VPC in the console and create new VPC with public subnet (don't create with 0 public subnet)
  - You can research more detail on the online stuff because I just followed the other along.
5. Steps
  - Steps are the steps allows you to run your application in the cluster.
  - But in this project we will use command line to do the jobs through ssh server
6. Cluster logs
  - I recommend you to create `logs` folder in your s3 as well to store the logs
7. Security configuration and EC2 key pair (required)
  - select your existed key pair or create new
8. IAM roles
  - Amazon EMR service role
    - I recommend you to select **Create a service role** because you will lack some service for EMR if you create a role by your own separately.
  - EC2 instance profile for Amazon EMR
    - choose your existed instance profile, if you don't have, just select **Create an instance profile**
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
- Copy the command and paste it to your terminal and enter, the command will look something like:
  ```
  ssh -i ~/your-keypair.pem hadoop@ec2-your-ec2-or-something.your-region.compute.amazonaws.com
  ```
Now you are connected to the cluster!  

### Process your data
First, don't forget to upload the `samplesuperstore.csv` into your s3 bucket. Once you finish it, you're ready to go.  
  
You can submit the job either manually submitting in the cluster or using steps from EMR. But in this project, we will manually submit.  
2 ways to do it is:  
1. create and empty file by type
```bash
nano run.py
```
then copy every thing in `runtohive.py` and paste to the script. Keep in mind that you need to adjust your 23 path in the script.  
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

And that's all you need to do just to make things work on EMR, hope this was helpful!
