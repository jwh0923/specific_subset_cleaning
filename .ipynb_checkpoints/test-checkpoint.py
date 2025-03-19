from pyspark.sql import Row, SparkSession
from typing import Dict, Any
import json
import uuid
from functools import partial
from your_common_modules import read_any_path, write_any_path, get_s3_client  # 替换为实际工具模块

class WebDataProcessor:
    def __init__(self, spark_config: Dict[str, Any]):
        self.spark = self._create_spark_session(spark_config)
        self.platform_configs = self._load_platform_configs()
        
    @staticmethod
    def _create_spark_session(config: Dict[str, Any]) -> SparkSession:
        """创建并返回Spark会话"""
        return SparkSession.builder \
            .appName(config.get("app_name", "web_data_processor")) \
            .config(conf=config) \
            .getOrCreate()

    def _load_platform_configs(self) -> Dict[str, Any]:
        """加载平台配置（可替换为从文件/数据库读取）"""
        return {
            "netease": {
                "field_mappings": {
                    "page_layout_type": {
                        "论坛": "forum",
                        "个人网站": "article",
                        # ...其他映射
                    },
                    "dataset_name": "net-ease",
                    "domain_field": "f_domain_sec"
                },
                "extractor_config": "/path/to/netease_config.jsonc",
                "output_template": "s3://bucket/{platform}/v{version}/"
            },
            "other_platform": {
                # 其他平台配置
            }
        }

    def process_platform_data(self, platform: str, input_paths: list, version: str = "001"):
        """处理指定平台数据的主流程"""
        # 获取平台配置
        config = self.platform_configs.get(platform)
        if not config:
            raise ValueError(f"Unsupported platform: {platform}")

        # 读取原始数据
        input_df = read_any_path(self.spark, ",".join(input_paths), config)
        
        # 数据转换
        transformed_rdd = input_df.rdd.map(
            partial(self._transform_row, platform=platform)
        ).repartition(6000)

        # 数据抽取
        processed_rdd = transformed_rdd.mapPartitions(
            partial(self._extract_data, platform=platform)
        )

        # 写入输出
        output_path = config["output_template"].format(
            platform=platform, 
            version=version.zfill(3)
        )
        write_any_path(
            processed_rdd.map(lambda x: Row(value=json.dumps(x))).toDF(),
            output_path,
            {"skip_output_check": True}
        )

    def _transform_row(self, row: Row, platform: str) -> Row:
        """根据平台配置转换行数据"""
        config = self.platform_configs[platform]
        mappings = config["field_mappings"]
        
        return Row(
            track_id=str(uuid.uuid4()),
            url=row.url,
            html=row.content,
            page_layout_type=mappings["page_layout_type"].get(
                getattr(row, 'f_name', ''),
                "article"
            ),
            domain=getattr(row, mappings["domain_field"], None),
            dataset_name=mappings["dataset_name"],
            data_source_category='HTML',
            meta_info={},
            platform=platform  # 添加平台标识字段
        )

    def _extract_data(self, partition: Iterable[Row], platform: str) -> Iterable[Dict]:
        """数据抽取处理（带平台感知）"""
        from llm_web_kit import ExtractSimpleFactory  # 按需延迟导入
        config = self.platform_configs[platform]
        
        # 初始化提取器
        extractor = ExtractSimpleFactory.create(config["extractor_config"])
        
        for row in partition:
            try:
                # 执行数据抽取
                data = self._safe_extract(row.asDict(), extractor)
                yield data
            except Exception as e:
                yield self._handle_error(row, e)

    @staticmethod
    def _safe_extract(data: Dict, extractor, timeout: int = 10) -> Dict:
        """带超时机制的抽取"""
        # 这里实现实际的抽取逻辑
        return data

    @staticmethod
    def _handle_error(row: Dict, error: Exception) -> Dict:
        """统一错误处理"""
        return {
            ​**row,
            "__error": {
                "type": type(error).__name__,
                "message": str(error),
                "traceback": traceback.format_exc()
            }
        }

# 使用示例
if __name__ == "__main__":
  
    config = {
        "spark_conf_name": "spark_4",
        "skip_success_check": True,
        # 根据个人路径进行替换1
        "spark.executorEnv.LLM_WEB_KIT_CFG_PATH": "/share/jiangwenhao/.llm-web-kit.jsonc",
        "spark.dynamicAllocation.maxExecutors": "400",
    }

    spark = new_spark_session("llm_kit_cc", config)

    processor = WebDataProcessor(config)
    
    # 处理网易数据
    processor.process_platform_data(
        platform="netease",
        input_paths=["s3://path/to/netease/data/*.json.gz"],
        version="001"
    )
    
    # 处理其他平台数据
    # processor.process_platform_data("other_platform", [...])