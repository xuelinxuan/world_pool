provider "aws" {
  region       = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  # profile    = "default" 就是已经把 acces ,secert 写到了cli 中，直接调取
}
