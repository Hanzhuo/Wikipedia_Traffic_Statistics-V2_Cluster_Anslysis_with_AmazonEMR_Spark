# Wikipedia Traffic Statistics-V2 Cluster Anslysis with Amazon AWS EMR Spark

This Readme provides step-by-step instruction on how to conduct data analysis (using K-means++ cluster analysis as example) through Amazon EMR Spark and Zeppelin Notebook, using 1TB **Wikipedia Traffic Statistics V2** AWS public dataset [here](https://aws.amazon.com/datasets/wikipedia-traffic-statistics-v2/)

This instruction is for new users who want to try Amazon EMR on data analysis. This instruction provides detailed process on how to set up AWS service data anslysis environment and run Spark with Zeppelin notebook.

**AWS services and techniques used in this instruction**:

* AWS EC2 instance (will be the master and slaves of EMR)
* AWS EMR (see [here](https://zeppelin.apache.org/))
* AWS S3 bucket for data storage (see [here](http://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/Welcome.html))
* AWS EBS storage
* AWS cli (see [here](https://aws.amazon.com/cli/))
* PySpark (see [here](http://spark.apache.org/))
* Zeppelin Notebook (see [here](https://zeppelin.apache.org/))

##General workflow

Generally, I create an AWS EMR Spark analysis environment and use Zepplin notebook to do data analysis. As for this project, I created EMR clusters with Spark that actually are several EC2 instances. These instances (clusters) include one master instance and other slave instances. Then, I copied the data from Amazon public dataset to EBS volume and attached to the master instance. After that, I choosed the data with master instance shell and uploaded the chosen data to S3 bucket with AWS cli (AWS command line). Finally, I used the Zepplin Notebook to run Python and Pyspark scripts to conduct data process and analysis.

The reason why I uploaded the data to S3 is because that EMR PySpark only can read data from S3 bucket rather than read data from main instance. 

Besides useing Zepling Notebook, Another way to do this project is to upload Pyspark code to master instance through S3 and cli. Then you can run the code with terminal of master instance. For this readme, I just list the process of using Zepplin Notebook. In this repository, I also post python files for data processing and analysis(One is for python, and another is for Pyspark).


##Set up AWS EMR data analysis environment

First, go to <https://aws.amazon.com/> and create an account or login in your account.

Second, please set up AWS IAM User as step 1 in this document [here](https://aws.amazon.com/getting-started/tutorials/backup-to-s3-cli/?nc1=h_ls) (It is important to set up your security configue correctly).

Third, go to your Amazon EMR services and click **create cluster** according to your need on hardware. You can either go through the setting up process with *Quick Options* or use *advanced options*. Please ensure that you create or use **EC2 key pair** and **IAM roles** correctly.

* You can see [here](https://aws.amazon.com/ec2/instance-types/?nc1=h_ls) for instance hardware type choice.
* As for this project, I created the 5 instances as below:
![instances](https://github.com/Hanzhuo/Wikipedia_Traffic_Statistics-V2_Cluster_Anslysis_with_AmazonEMR_Spark/blob/master/ImageDocument/5instances.jpeg)

Forth, We can create a new file folder in our S3 bucket for data storage (or you can create the file folder with AWS cli later).

* We have to use EC2 instance to manipulate the S3 bucket. We use AWS cli to handle that. Please see official document on how to set up, configure, upload and download data at: 
<https://aws.amazon.com/getting-started/tutorials/backup-to-s3-cli/?nc1=h_ls>

##Prepare data for analysis

### 1. Copy data from AWS public dataset 
As I said above, this project used 1TB **Wikipedia Traffic Statistics V2** AWS public dataset [here](https://aws.amazon.com/datasets/wikipedia-traffic-statistics-v2/). You can download same data from links in Wikipedia Traffic Statistics V2 website to your local machine. It also is available in AWS public datasets that can be accessed through AWS EBS (see detailed document about how to access public datasets [here](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-public-data-sets.html)).

* Go to your EC2 services console, and click **Snapshots** under **ELASTIC BLOCK STROE**. Search **US Snapshot ID (Linux/Unix): snap-0c155c67** of wikipedia-traffic-statistics-v2 in **Public Snapshot** and **N.Virginia** (see image below). 
![Snapshots](https://github.com/Hanzhuo/Wikipedia_Traffic_Statistics-V2_Cluster_Anslysis_with_AmazonEMR_Spark/blob/master/ImageDocument/snapshots.png)
* Click **Actions** and choose **Create Volume**. Then we can get a volume in **Volumes** under **ELASTIC BLOCK STROE**. 

By now, we have copied the data from public snapshots to our own volume. 

### 2. Attach EBS volume to main EC2 instance and prepare data

* Go to AWS EC2 console and click **Volumes** under **ELASTIC BLOCK STROE**. 
* Click **Actions** and **Attach Volume**. Then choose your master EC2 instance.
* Open terminal on your local machine and connect your master EC2 instance with SSH. 
* Then use command line (shell) to check the data files locations, uncompress and check data. Please add permissions of reading and writing on these files (we need permission to read and wirte `titles-sorted.txt` and `links-simple-sorted.txt` for future data pre-processing). 
* As for my project, I only choosed one week data (02/02/2009-02/08/2009 around 136 million records
)


### 3. Copy the data from attached EBS volume to S3 bucket through EC2 instance with AWS CLI

I used AWS cli to upload data to S3 bucket. See official document [here](https://aws.amazon.com/getting-started/tutorials/backup-to-s3-cli/?nc1=h_ls).

* Use SHH to connect to your master EC2 instance with terminal and enter `aws configure` in EC2 instance command line to make sure that we can upload and download data to S3. See detailed steps at Step2 Max/Linux [here](https://aws.amazon.com/getting-started/tutorials/backup-to-s3-cli/?nc1=h_ls)
* Enter `aws s3 mb s3://yourBucketDocument` to create a new file folder in S3 if you do not create file folder before. 
* Upload the choosed data and files to S3 according to step3 b [here](https://aws.amazon.com/getting-started/tutorials/backup-to-s3-cli/?nc1=h_ls)
AWS cli example is showed below:
![AWS cli](https://github.com/Hanzhuo/Wikipedia_Traffic_Statistics-V2_Cluster_Anslysis_with_AmazonEMR_Spark/blob/master/ImageDocument/cop%20file%20to%20s3.png)

By now, we have prepared the data for data analysis.

##Use Zeppelin Notebook and PySpark to conduct 

It is super easy to use Zeppelin Notebook. Just go to your EMR Management Console and click Zeppelin. Make sure that your IP address is in corresponding security group in your AWS.
![Click Zeppelin](https://github.com/Hanzhuo/Wikipedia_Traffic_Statistics-V2_Cluster_Anslysis_with_AmazonEMR_Spark/blob/master/ImageDocument/useZeppelin.jpeg)

The Zepplein notebook looks like:
![Zeppelin example](https://github.com/Hanzhuo/Wikipedia_Traffic_Statistics-V2_Cluster_Anslysis_with_AmazonEMR_Spark/blob/master/ImageDocument/zeppelin.png)
Attention: In Zepplin Notebook, the first line `%pyspark` means that you want to use pyspark in this textbox. If you want to use Python just type `%python` at first line, then you can use Python in the textbox.

###Explaination of Python and Pyspark scripts in Zepplin Notebook
As for Python and PySpark code, you can see the the Jupyter notebook **Zepplin_code.ipynb**

The first textbox in the notebook is to process the number of links data (I want to get the number of other wiki web pages that have links directed to one specific wiki page). After run these python scripts, we should upload the output file `name_inputNum.txt` to S3 as we do for uploading prior data. 

Then we can run Pyspark code to process data and conduct cluster analysis. The results will be showed in Zepplin Notebook as Jupyter Notebook.

###Some results
Here is sample data after pre-processing
![sample_processed_data](https://github.com/Hanzhuo/Wikipedia_Traffic_Statistics-V2_Cluster_Anslysis_with_AmazonEMR_Spark/blob/master/ImageDocument/sample_processed_data.png)

Here is general data descriptions for 9 million selected highe view pages
![sample_processed_data](https://github.com/Hanzhuo/Wikipedia_Traffic_Statistics-V2_Cluster_Anslysis_with_AmazonEMR_Spark/blob/master/ImageDocument/9mhighviewsdatadesc.png)

