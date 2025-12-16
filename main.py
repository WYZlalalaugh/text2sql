"""
Text2SQL 智能体系统入口
"""
import sys
from typing import Optional

from config import config
from state import AgentState
from graph import create_graph, process_clarification


def create_llm_client():
    """创建 LLM 客户端"""
    try:
        from langchain_openai import ChatOpenAI
        
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
    
    def embed_query(self, text: str) -> list:
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


class Text2SQLAgent:
    """Text2SQL 智能体"""
    
    def __init__(self, llm_client=None, embedding_client=None, db_connection=None):
        """
        初始化智能体
        
        Args:
            llm_client: LLM 客户端
            embedding_client: Embedding 客户端
            db_connection: 数据库连接
        """
        self.llm_client = llm_client or create_llm_client()
        self.embedding_client = embedding_client or create_embedding_client()
        self.db_connection = db_connection
        
        # 创建 Graph
        self.app = create_graph(
            llm_client=self.llm_client,
            embedding_client=None,  # 废弃：不再使用向量检索
            db_connection=self.db_connection
        )
        
        # 当前状态（用于多轮对话）
        self.current_state: Optional[AgentState] = None
        self.waiting_for_clarification = False
    
    def chat(self, user_input: str) -> str:
        """
        处理用户输入
        
        Args:
            user_input: 用户输入
            
        Returns:
            智能体回复
        """
        if self.waiting_for_clarification and self.current_state:
            # 处理澄清回复
            self.current_state = process_clarification(
                self.app, 
                self.current_state, 
                user_input
            )
        else:
            # 新查询
            initial_state: AgentState = {
                "user_query": user_input,
                "messages": [],
                "clarification_count": 0
            }
            self.current_state = self.app.invoke(initial_state)
        
        # 检查是否需要澄清
        final_response = self.current_state.get("final_response", "")
        clarification_question = self.current_state.get("clarification_question", "")
        
        if self.current_state.get("ambiguity_detected", False) and clarification_question:
            self.waiting_for_clarification = True
            return clarification_question
        else:
            self.waiting_for_clarification = False
            return final_response
    
    def reset(self):
        """重置对话状态"""
        self.current_state = None
        self.waiting_for_clarification = False


def main():
    """主函数 - 交互式对话"""
    print("=" * 60)
    print("Text2SQL 智能体系统")
    print("=" * 60)
    print("输入 'quit' 或 'exit' 退出")
    print("输入 'reset' 重置对话")
    print("-" * 60)
    
    try:
        agent = Text2SQLAgent()
        print("智能体初始化完成！\n")
    except Exception as e:
        print(f"初始化失败: {e}")
        print("将使用测试模式运行...\n")
        agent = Text2SQLAgent(llm_client=SimpleLLMClient())
    
    while True:
        try:
            user_input = input("用户: ").strip()
            
            if not user_input:
                continue
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("再见！")
                break
            
            if user_input.lower() == 'reset':
                agent.reset()
                print("对话已重置。\n")
                continue
            
            response = agent.chat(user_input)
            print(f"\n助手: {response}\n")
            
        except KeyboardInterrupt:
            print("\n再见！")
            break
        except Exception as e:
            print(f"错误: {e}\n")


if __name__ == "__main__":
    main()
