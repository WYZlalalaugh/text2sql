"""
Text2SQL 智能体系统配置文件
"""
import os
from dataclasses import dataclass, field
from typing import Optional

# 自动加载 .env 文件
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv 未安装时跳过


@dataclass
class LLMConfig:
    """LLM 配置 (兼容 OpenAI 格式)"""
    api_base: str = os.getenv("LLM_API_BASE", "http://localhost:11434/v1")
    api_key: str = os.getenv("LLM_API_KEY", "ollama")
    model_name: str = os.getenv("LLM_MODEL_NAME", "qwen2.5:7b")
    temperature: float = 0.0
    max_tokens: int = 2048


@dataclass
class FineTunedModelConfig:
    """微调模型配置 (Ollama API)"""
    api_base: str = os.getenv("FINETUNED_API_BASE", "http://localhost:11434")
    model_name: str = os.getenv("FINETUNED_MODEL_NAME", "text2sql-finetuned")
    temperature: float = 0.0
    max_tokens: int = 1024


@dataclass
class EmbeddingConfig:
    """Embedding 模型配置"""
    api_base: str = os.getenv("EMBEDDING_API_BASE", "http://localhost:11434/v1")
    api_key: str = os.getenv("EMBEDDING_API_KEY", "ollama")
    model_name: str = os.getenv("EMBEDDING_MODEL_NAME", "bge-m3")
    dimension: int = 1024


@dataclass
class DatabaseConfig:
    """MySQL 数据库配置"""
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "3306"))
    user: str = os.getenv("DB_USER", "root")
    password: str = os.getenv("DB_PASSWORD", "your_password_here")
    database: str = os.getenv("DB_NAME", "education_metrics")
    charset: str = "utf8mb4"


@dataclass
class PathConfig:
    """路径配置"""
    base_dir: str = os.path.dirname(os.path.abspath(__file__))
    schema_path: str = field(default="")
    metrics_path: str = field(default="")
    
    def __post_init__(self):
        self.schema_path = os.path.join(self.base_dir, "test_number.json")
        self.metrics_path = os.path.join(self.base_dir, "基教指标.json")


@dataclass
class AppConfig:
    """应用总配置"""
    llm: LLMConfig = field(default_factory=LLMConfig)
    finetuned_model: FineTunedModelConfig = field(default_factory=FineTunedModelConfig)
    embedding: EmbeddingConfig = field(default_factory=EmbeddingConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    
    # 向量检索配置
    vector_top_k: int = 5
    similarity_threshold: float = 0.7


# 全局配置实例
config = AppConfig()
