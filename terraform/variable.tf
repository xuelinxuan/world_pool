#region
variable "aws_region"       {default = "us-east-1"}

#key_name_prefix 
variable "key_name"         {default = "world_pool"}

#instance
variable "instance_type"    {default = "t3.medium"}

#alert email
variable "alert_email_id"   {default = "linxuanxue@gmail.com"}

#clone repositoy
variable "repo_url"         {default = "https://github.com/xuelinxuan/world_pool.git"}

# AWS Access Key
variable "aws_access_key"   {default = "AKIAWDYU6IA6HRAXK7XW"}

# AWS Secret Key
variable "aws_secret_key"   {default = "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"}



#Lambda
#variable "lambda_function_name" {default = "IGTIexecutaEMRaovivo"}

#SSH 
#variable "key_pair_name" {default = "ssh_ec2_instance"}

#subnet
#variable "airflow_subnet_id" {default = "subnet-08b465766ec396cde"}

#vpc
#variable "vpc_id" {default = "vpc-06767fed36ee2d510"}

