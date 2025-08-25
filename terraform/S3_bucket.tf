# 创建 bucket
resource "aws_s3_bucket" "datalake" {
  bucket = "world-pool-bucket-version-1"   # bucket 名必须全局唯一
  acl    = "private"

  tags = {
    Environment = "Dev"
    Project     = "DataLake"
  }

  # 开启默认加密（AES-256）
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
}

# 创建文件夹结构（用占位符对象）
resource "aws_s3_object" "folders" {
  for_each = toset([
    "bronze/market/streaming/",
    "bronze/fundamentals/streaming/",
    "bronze/macro_economy/streaming/",
    "silver/market/streaming/",
    "silver/fundamentals/streaming/",
    "silver/macro_economy/streaming/",
    "gold/market/streaming/",
    "gold/fundamentals/streaming/",
    "gold/macro_economy/streaming/"
  ])

  bucket = aws_s3_bucket.datalake.bucket
  key    = each.value

  # 占位文件，防止目录为空被 AWS 控制台不显示
  content = ""
}
