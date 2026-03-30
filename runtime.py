"""
Shared LLM/embedding factory functions for API and CLI.
"""
from typing import Any

from config import config


def create_llm_client():
    """创建 LLM 客户端"""
    try:
        from langchain_openai import ChatOpenAI  # pyright: ignore[reportMissingImports]

        return ChatOpenAI(
            base_url=config.llm.api_base,
            api_key=config.llm.api_key,
            model=config.llm.model_name,
            temperature=config.llm.temperature,
            max_tokens=config.llm.max_tokens
        )
    except ImportError:
        print("警告: langchain_openai 未安装，使用简易 LLM 客户端")
        return SimpleLLMClient()


def create_embedding_client():
    """创建 Embedding 客户端"""
    # 使用自定义 Ollama Embedding 客户端
    return OllamaEmbeddingClient()


class OllamaEmbeddingClient:
    """Ollama Embedding 客户端"""

    def __init__(self):
        import requests
        self.base_url = config.embedding.api_base.rstrip('/v1').rstrip('/')
        self.model = config.embedding.model_name
        self.session = requests.Session()

    def embed_query(self, text: str) -> list[Any]:
        """生成文本嵌入向量"""
        import requests

        url = f"http://localhost:11434/api/embeddings"
        payload = {
            "model": self.model,
            "prompt": text
        }

        try:
            response = self.session.post(url, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()
            return result.get("embedding", [])
        except Exception as e:
            print(f"Embedding 生成失败: {e}")
            return []


class SimpleLLMClient:
    """简易 LLM 客户端（用于测试）"""

    def invoke(self, prompt: str):
        """模拟 LLM 调用"""
        class Response:
            content = '{"intent_type": "chitchat", "analysis": "测试模式", "identified_metrics": []}'
        return Response()
