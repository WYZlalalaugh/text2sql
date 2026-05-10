"""
Text2SQL 智能体系统配置文件
"""
import os
from dataclasses import dataclass, field

# 自动加载 .env 文件
try:
    from dotenv import load_dotenv
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    _ = load_dotenv(os.path.join(_BASE_DIR, ".env"))
except ImportError:
    pass  # python-dotenv 未安装时跳过


def _get_env_bool(name: str, default: bool = False) -> bool:
    """读取布尔环境变量"""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


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
class AuthConfig:
    """Authentication settings for the web UI."""
    jwt_secret: str = os.getenv("JWT_SECRET", "text2sql-dev-secret")
    jwt_expire_minutes: int = int(os.getenv("JWT_EXPIRE_MINUTES", "10080"))
    bootstrap_admin_username: str = os.getenv("BOOTSTRAP_ADMIN_USERNAME", "admin")
    bootstrap_admin_password: str = os.getenv("BOOTSTRAP_ADMIN_PASSWORD", "")


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
    auth: AuthConfig = field(default_factory=AuthConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    use_workspace_context: bool = _get_env_bool("USE_WORKSPACE_CONTEXT", False)
    
    # 向量检索配置
    vector_top_k: int = 5
    similarity_threshold: float = 0.7

    def refresh_feature_flags(self):
        """按需从环境变量刷新功能开关和相关运行时配置"""
        self.use_workspace_context = _get_env_bool("USE_WORKSPACE_CONTEXT", False)


# 全局配置实例
config = AppConfig()
