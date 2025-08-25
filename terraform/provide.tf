provider "aws" {
  region       = var.aws_region
  #access_key = var.aws_access_key  #有了role就不需要这个两个
  #secret_key = var.aws_secret_key  #有了role就不需要这个两个
  # profile    = "default" 就是已经把 acces ,secert 写到了cli 中，直接调取
}
