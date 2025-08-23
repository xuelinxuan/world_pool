#region
variable "aws_region"    {default = "us-east-1"}

#key_name_prefix 
variable ="key_name"     {default = "world_pool"}

#instance
variable "instance_type" {default = "world_pool"}

#clone repositoy
variable "repo_url"      {default = "https://github.com/xuelinxuan/world_pool.git"}

#alert email
variable "alert_email_id"{default = "linxuanxue@gmail.com"}

#Lambda
#variable "lambda_function_name" {default = "IGTIexecutaEMRaovivo"}

#SSH 
#variable "key_pair_name" {default = "ssh_ec2_instance"}

#subnet
#variable "airflow_subnet_id" {default = "subnet-08b465766ec396cde"}

#vpc
#variable "vpc_id" {default = "vpc-06767fed36ee2d510"}

