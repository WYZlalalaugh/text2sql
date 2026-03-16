
import sys
import os
import pandas as pd
import numpy as np

# 确保能找到 tools 和 config
# 当前文件在 text2sql/test/ 目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 项目根目录是 text2sql/ (上一级)
project_root = os.path.dirname(current_dir)

if project_root not in sys.path:
    sys.path.append(project_root)

print(f"当前路径: {current_dir}")
print(f"项目根路径 (已加入 sys.path): {project_root}")

try:
    from tools.db_client import load_data
    from sqlalchemy import create_engine
    print("✅ 成功导入 tools.db_client.load_data")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    sys.exit(1)

def test_load_data():
    print("\n" + "="*50)
    print("开始测试 load_data 功能 (SQLAlchemy 版)")
    print("="*50)

    # 1. 测试自动初始化 & 基本查询
    print("\n[测试 1] 自动初始化 & 基本查询 (SELECT 1)...")
    try:
        df = load_data("SELECT 1 as test_col")
        print(f"查询结果:\n{df}")
        if not df.empty and df.iloc[0]['test_col'] == 1:
            print("✅ 自动初始化成功")
        else:
            print("❌ 查询结果不符合预期")
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return

    # 2. 测试数值类型转换 (Decimal -> Float/Int)
    print("\n[测试 2] 数值类型自动转换 (SELECT q.level2_weight)...")
    try:
        # 假设 questions 表存在且有 level2_weight 字段
        sql = "SELECT level2_weight, level2_name FROM questions LIMIT 5"
        # 如果表名不对，可能需要调整。这里尽量用通用的。
        # 为了更健壮，先查一下表名
        try:
             df_tables = load_data("SHOW TABLES")
             # print(f"Tables: {df_tables.values.flatten()[:5]}")
             if 'questions' in str(df_tables.values):
                 sql = "SELECT level2_weight, level2_name FROM questions LIMIT 5"
             else:
                 # 随便找个表测试
                 table_name = df_tables.iloc[0,0]
                 sql = f"SELECT * FROM {table_name} LIMIT 1"
        except:
             pass

        print(f"Executing SQL: {sql}")
        df = load_data(sql)
        print(f"查询结果 (前5行):\n{df}")
        print(f"列类型:\n{df.dtypes}")
        
        # 检查数值列 (假设 level2_weight 是数值)
        for col in df.columns:
            if 'weight' in col or 'value' in col or 'id' in col:
                dtype = df[col].dtype
                if pd.api.types.is_numeric_dtype(dtype):
                    print(f"✅ 列 {col} 是数值类型 ({dtype})")
                else:
                    print(f"⚠️ 列 {col} 是 {dtype}，期望是数值类型")
                    print(f"样本值类型: {type(df[col].iloc[0])}")
        
    except Exception as e:
        print(f"❌ 测试 2 失败: {e}")

    # 3. 测试安全性检查
    print("\n[测试 3] 安全性检查 (禁止 DROP/DELETE)...")
    unsafe_sqls = [
        "DROP TABLE test",
        "DELETE FROM questions WHERE id = 1",
    ]
    
    for sql in unsafe_sqls:
        try:
            load_data(sql)
            print(f"❌ 未拦截不安全 SQL: {sql}")
        except ValueError as e:
            if "安全检查失败" in str(e):
                print(f"✅ 成功拦截: {sql[:30]}...")
            else:
                 print(f"❌ 抛出了错误的异常: {e}")
        except Exception as e:
            print(f"❌ 抛出了未预期的异常: {e}")

    print("\n" + "="*50)
    print("测试完成")
    print("="*50)

if __name__ == "__main__":
    test_load_data()
