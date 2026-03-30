"""
Text2SQL 智能体系统入口
"""
from typing import Optional, cast

from state import AgentState
from graph import process_clarification
from runtime import SimpleLLMClient
from runtime_bootstrap import create_runtime_graph  # pyright: ignore[reportMissingImports]


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
        self.app, self.llm_client, self.embedding_client = create_runtime_graph(
            llm_client=llm_client,
            embedding_client=embedding_client,
            db_connection=db_connection,
            enable_embedding_in_graph=False,
        )
        self.db_connection = db_connection

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
            self.current_state = cast(AgentState, self.app.invoke(initial_state))
        
        # 检查是否需要澄清
        state = self.current_state or {}
        final_response = state.get("final_response", "")
        clarification_question = state.get("clarification_question", "")
        
        if state.get("ambiguity_detected", False) and clarification_question:
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
