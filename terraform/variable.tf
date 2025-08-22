#region
variable "aws_region" {default = "us-east-1"}

#Lambda
variable "lambda_function_name" {default = "IGTIexecutaEMRaovivo"}

#SSH 
variable "key_pair_name" {default = "ssh_ec2_instance"}

#subnet
variable "airflow_subnet_id" {default = "subnet-08b465766ec396cde"}

#vpc
variable "vpc_id" {default = "vpc-06767fed36ee2d510"}

