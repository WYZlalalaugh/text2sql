"""
向量检索模块 - 用于将用户口语化查询映射到标准指标
"""
import json
import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import numpy as np

from config import config
from state import MetricInfo


class MetricVectorStore:
    """指标向量存储"""
    
    def __init__(self, embedding_client=None):
        """
        初始化向量存储
        
        Args:
            embedding_client: Embedding 客户端，需要有 embed_query 方法
        """
        self.embedding_client = embedding_client
        self.metrics_data: List[Dict[str, Any]] = []
        self.embeddings: Optional[np.ndarray] = None
        self.index = None  # FAISS index
        
        # 加载指标数据
        self._load_metrics()
    
    def _load_metrics(self):
        """加载并解析指标体系 JSON"""
        metrics_path = config.paths.metrics_path
        
        if not os.path.exists(metrics_path):
            raise FileNotFoundError(f"指标文件不存在: {metrics_path}")
        
        with open(metrics_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # 展平为列表，每个元素包含一级和二级指标信息
        self.metrics_data = []
        
        for level1_name, level1_info in raw_data.items():
            level1_desc = level1_info.get("一级指标解释", "")
            
            # 添加一级指标本身
            self.metrics_data.append({
                "level1_name": level1_name,
                "level1_description": level1_desc,
                "level2_name": None,
                "level2_description": None,
                "text_for_embedding": f"{level1_name}: {level1_desc}"
            })
            
            # 添加二级指标
            level2_dict = level1_info.get("二级指标", {})
            for level2_name, level2_info in level2_dict.items():
                level2_desc = level2_info.get("二级指标解释", "")
                self.metrics_data.append({
                    "level1_name": level1_name,
                    "level1_description": level1_desc,
                    "level2_name": level2_name,
                    "level2_description": level2_desc,
                    "text_for_embedding": f"{level1_name} - {level2_name}: {level2_desc}"
                })
        
        print(f"已加载 {len(self.metrics_data)} 个指标项")
    
    def build_index(self):
        """构建向量索引"""
        if self.embedding_client is None:
            raise ValueError("未配置 embedding_client")
        
        # 获取所有指标文本
        texts = [m["text_for_embedding"] for m in self.metrics_data]
        
        # 生成 embeddings
        embeddings_list = []
        for text in texts:
            emb = self.embedding_client.embed_query(text)
            embeddings_list.append(emb)
        
        self.embeddings = np.array(embeddings_list, dtype=np.float32)
        
        # 构建 FAISS 索引
        try:
            import faiss
            dimension = self.embeddings.shape[1]
            self.index = faiss.IndexFlatIP(dimension)  # 使用内积（余弦相似度需要先归一化）
            
            # 归一化
            faiss.normalize_L2(self.embeddings)
            self.index.add(self.embeddings)
            
            print(f"向量索引构建完成，维度: {dimension}")
        except ImportError:
            print("警告: FAISS 未安装，将使用简单的余弦相似度搜索")
            self.index = None
    
    def search(self, query: str, top_k: int = None) -> List[MetricInfo]:
        """
        搜索与查询最相关的指标
        
        Args:
            query: 用户查询
            top_k: 返回结果数量
            
        Returns:
            匹配的指标列表
        """
        if top_k is None:
            top_k = config.vector_top_k
        
        if self.embedding_client is None:
            # 没有 embedding 客户端时，使用简单的关键词匹配
            return self._keyword_search(query, top_k)
        
        # 生成查询向量
        query_embedding = np.array([self.embedding_client.embed_query(query)], dtype=np.float32)
        
        if self.index is not None:
            # 使用 FAISS 搜索
            import faiss
            faiss.normalize_L2(query_embedding)
            scores, indices = self.index.search(query_embedding, top_k)
            
            results = []
            for score, idx in zip(scores[0], indices[0]):
                if idx < 0:
                    continue
                metric_data = self.metrics_data[idx]
                results.append(MetricInfo(
                    level1_name=metric_data["level1_name"],
                    level1_description=metric_data["level1_description"],
                    level2_name=metric_data["level2_name"],
                    level2_description=metric_data["level2_description"],
                    similarity_score=float(score)
                ))
            return results
        else:
            # 使用简单的余弦相似度
            return self._cosine_search(query_embedding[0], top_k)
    
    def _keyword_search(self, query: str, top_k: int) -> List[MetricInfo]:
        """简单的关键词匹配搜索"""
        results = []
        query_lower = query.lower()
        
        for metric_data in self.metrics_data:
            text = metric_data["text_for_embedding"].lower()
            # 计算匹配分数（包含的关键词越多分数越高）
            score = sum(1 for word in query_lower.split() if word in text)
            if score > 0:
                results.append((score, metric_data))
        
        # 按分数排序
        results.sort(key=lambda x: x[0], reverse=True)
        
        return [
            MetricInfo(
                level1_name=m["level1_name"],
                level1_description=m["level1_description"],
                level2_name=m["level2_name"],
                level2_description=m["level2_description"],
                similarity_score=float(s) / 10.0  # 归一化
            )
            for s, m in results[:top_k]
        ]
    
    def _cosine_search(self, query_embedding: np.ndarray, top_k: int) -> List[MetricInfo]:
        """使用余弦相似度搜索"""
        if self.embeddings is None:
            return []
        
        # 归一化查询向量
        query_norm = query_embedding / np.linalg.norm(query_embedding)
        
        # 计算余弦相似度
        similarities = np.dot(self.embeddings, query_norm)
        
        # 获取 top_k 索引
        top_indices = np.argsort(similarities)[-top_k:][::-1]
        
        results = []
        for idx in top_indices:
            metric_data = self.metrics_data[idx]
            results.append(MetricInfo(
                level1_name=metric_data["level1_name"],
                level1_description=metric_data["level1_description"],
                level2_name=metric_data["level2_name"],
                level2_description=metric_data["level2_description"],
                similarity_score=float(similarities[idx])
            ))
        
        return results
    
    def get_all_metrics(self) -> List[Dict[str, Any]]:
        """获取所有指标数据"""
        return self.metrics_data
    
    def get_metric_definition(self, level1_name: str, level2_name: str = None) -> Optional[str]:
        """
        获取指标定义
        
        Args:
            level1_name: 一级指标名称
            level2_name: 二级指标名称（可选）
            
        Returns:
            指标定义描述
        """
        for metric in self.metrics_data:
            if metric["level1_name"] == level1_name:
                if level2_name is None or metric["level2_name"] == level2_name:
                    if metric["level2_name"]:
                        return f"**{metric['level1_name']} - {metric['level2_name']}**: {metric['level2_description']}"
                    else:
                        return f"**{metric['level1_name']}**: {metric['level1_description']}"
        return None


# 全局向量存储实例（延迟初始化）
_vector_store: Optional[MetricVectorStore] = None


def get_vector_store(embedding_client=None) -> MetricVectorStore:
    """获取向量存储实例"""
    global _vector_store
    if _vector_store is None:
        _vector_store = MetricVectorStore(embedding_client)
        if embedding_client is not None:
            _vector_store.build_index()
    return _vector_store
