
magic-html专项
1. 如何在jupyter中调用llm_web_kit抽取html数据？
- llm-webkit-mirror github repp：https://github.com/ccprocessor/llm-webkit-mirror
- llm_web_kit中标准输入数据格式规范：llm-webkit-mirror/docs/specification/input_format/html_spec.md
| 字段名字                  | 格式                           | 意义                                                                                                                | 是否必填        |
| ------------------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------- | --------------- |
| track_id                  | uuid                           | 全局唯一的ID                                                                                                        | 是              |
| dataset_name              | str                            | 数据集的名字（全局唯一），这个名字是管理员输入的，然后做索引的时候带到index里来                                     | 是              |
| data_source_category      | str                            | 这一行数据代表的是HTML，PDF，EBOOK,CC,labCC类型                                                                     | 是，此处是 HTML |
| html                      | 字符串                         | 以UTF-8为编码的HTML文件全文                                                                                         | 是              |
| url                       | 字符串                         | 这个文件的来源网址                                                                                                  | 是              |
| file_bytes                | 整数                           | 文件的size, 单位是byte                                                                                              | 是              |
| meta_info                 | 字典                           | 存放关于文件的元信息:如果能从文件里获取到作者，制作日期等信息。或者数据本身就带有一些其他的信息都放入到这个字段里。 | 是              |
| meta_info->input_datetime | 其格式为 `yyyy-mm-dd HH:MM:SS` | 生成这个json索引文件这一条数据的时间，可以不用那么精确                                                              | 是              |

样例：
{"track_id": "f7b3b1b4-0b1b", "url":"http://example.com/1.html", "dataset_name": "CC-TEST", "data_source_type": "HTML",  "html": "<html>.../html>", "file_bytes": 1000, "meta_info": {"input_datetime": "2020-01-01 00:00:00"}}
{"track_id": "f7b3b1b4-0b1c", "url":"http://example.com/2.html", "dataset_name": "CC-TEST", "data_source_type": "HTML",  "html": "<html>...</html>", "file_bytes": 1000, "meta_info": {"input_datetime": "2020-01-01 00:00:00"}}

1. jupter环境部署
新建python env环境，安装仓库requirements下的runtime.txt和dev.txt依赖。
- 如果lxml安装遇到报错，可以尝试使用命令安装：
pip install --platform manylinux2014_x86_64 --only-binary=:all: lxml --trusted-host mirrors.aliyun.com --target （替换为对应环境路径）[~/my_envs/my_cpu_3.10/lib/python3.10/site-packages]
- 安装pyspark包，pip install pyspark==3.4.1

2. 在用户的home路径下新建 .llm-web-kit.jsonc  文件
配置参考：llm-webkit-mirror/llm_web_kit/config/README.MD
3. jupyter中spark代码示例
- 启动spark session，注意这里需要配置（替换为自己的路径） "spark.executorEnv.LLM_WEB_KIT_CFG_PATH": "/share/yujia/.llm-web-kit.jsonc"
# create spark session
from pyspark.sql import Row, DataFrame

from app.common.spark_ext import *
from app.common.json_util import *

from app.common.s3 import *

config = {
    "spark_conf_name": "spark_2",
    "skip_success_check": True,
    # 根据个人路径进行替换
    "spark.executorEnv.LLM_WEB_KIT_CFG_PATH": "/share/yujia/.llm-web-kit.jsonc",
    "spark.dynamicAllocation.maxExecutors": "400",
}

spark = new_spark_session("llm_kit_cc", config)
spark
- 读取ceph输入数据
input_path = ["s3://web-parse-huawei/CC/pre-dedup/v002/unique_html/CC-MAIN-2024-33/"]
input_df = read_any_path(spark, ",".join(input_path), config)
- 这里是对输入的压缩html数据进行处理，可根据具体的业务场景进行处理
import zlib
import base64

def compress_and_decompress_str(input_data: Union[str, bytes], compress: bool = True, base: bool = False) -> Union[str, bytes]:
    compressed_bytes = base64.b64decode(input_data)
    decompressed_bytes = zlib.decompress(compressed_bytes)
    return decompressed_bytes.decode('utf-8')


def uncompress_html(_iter):
    
    for row in _iter:
        d = json_loads(row.value)
        raw_html = d.get("html", "")        
        d.pop("html", None)
        d["html"] = compress_and_decompress_str(raw_html)
        d["dataset_name"] = "cc"
        d["data_source_category"] = "html"                

        yield Row(value=json_dumps(d))


input_map_rdd = input_df.rdd.mapPartitions(uncompress_html)
input_map_rdd.take(1)
- 抽取html数据
这里有3个注意点：
1. 使用os.environ设置环境变量：LLM_WEB_KIT_CFG_PATH
2. PipelineSuit初始化时，定义html_pipeline_formatter_disable.jsonc配置，这里定义了数据处理流程format/extract等，这里的关键流程是使用extract对html数据进行抽取，llm_web_kit.pipeline.extractor.html.extractor.HTMLFileFormatExtractor
3. 将错误日志写入error_log_path，llm_web_kit有完整的exception异常，可以根据具体业务场景进行处理
import os
os.environ["LLM_WEB_KIT_CFG_PATH"] = "/share/yujia/.llm-web-kit.jsonc"

import uuid
import traceback
from datetime import datetime
from llm_web_kit.input.datajson import DataJson
from llm_web_kit.pipeline.pipeline_suit import PipelineSuit


def extract_data(_iter):
    pipesuit = PipelineSuit('/share/yujia/notebooks/llm_web_kit/html_pipeline_formatter_disable.jsonc')
    # 为每个分区创建唯一的错误日志文件
    partition_id = str(uuid.uuid4())
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    error_log_path = f"s3://xyz-llm-users/xyz-users/yujia/CC-MAIN-2024-33/output/v002/error_logs/{current_time}_{partition_id}.json"
    s3_doc_writer = S3DocWriter(path=error_log_path)

    for row in _iter:
        try:
            d = json_loads(row.value)
            input_data = DataJson(d)
            data: DataJson = pipesuit.format(input_data)
            data_e: DataJson = pipesuit.extract(input_data)
    
            yield Row(value=json_dumps(data_e.to_json()))
        except Exception as e:
            # 记录更详细的错误信息
            error_info = {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "input_data": row.value if hasattr(row, 'value') else str(row),
                "timestamp": datetime.now().isoformat()
            }
            s3_doc_writer.write(error_info)

    # 确保在分区处理完成后刷新并关闭文件
    try:
        s3_doc_writer.flush()
    except Exception as e:
        print(f"Error flushing error logs: {str(e)}")

cc_main3_rdd = input_map_rdd.mapPartitions(extract_data)
cc_main3_rdd.take(1)
# cc_main_33_rdd.count()
- 结果数据写入ceph
cc_main3_path = "s3://xyz-llm-users/xyz-users/yujia/CC-MAIN-2024-33/output/v002/"
write_any_path(cc_main3_rdd.toDF(), cc_main3_path, config)