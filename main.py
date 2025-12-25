import pandas as pd
import requests
import io
import os
from datetime import datetime

# 文件名
FILE_NAME = 'gld_data.csv'

def fetch_and_save():
    url = "https://www.spdrgoldshares.com/assets/dynamic/GLD/GLD_US_archive_EN.csv"
    
    try:
        # 1. 获取官网数据
        response = requests.get(url, verify=True)
        content = response.content.decode('utf-8')
        
        # 寻找 Header 行
        lines = content.split('\n')
        header_row = 0
        for i, line in enumerate(lines[:20]):
            if "Date" in line and "Tonnes" in line:
                header_row = i
                break
        
        df = pd.read_csv(io.StringIO(content), skiprows=header_row)
        df.columns = df.columns.str.strip()
        
        # 找到需要的列
        col_map = [c for c in df.columns if 'Tonnes' in c]
        if not col_map: return
        tonnes_col = col_map[0]
        
        # 获取最新一条数据
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.dropna(subset=['Date'])
        latest = df.iloc[-1]
        
        date_str = latest['Date'].strftime('%Y-%m-%d')
        tonnes = float(latest[tonnes_col])
        
        print(f"官网最新数据: {date_str} - {tonnes}吨")

        # 2. 读取或创建本地 CSV
        if os.path.exists(FILE_NAME):
            history = pd.read_csv(FILE_NAME)
        else:
            history = pd.DataFrame(columns=['Date', 'Tonnes', 'Change_Pct'])
        
        # 确保 Date 列是字符串格式
        history['Date'] = history['Date'].astype(str)
        
        # === 核心修改点 ===
        # 判断条件：(1) 日期不存在 OR (2) 列缺失
        # 只要满足其中一个，就进入更新流程
        need_update = False
        
        if date_str not in history['Date'].values:
            print(">>> 发现新日期，准备追加...")
            new_row = pd.DataFrame({'Date': [date_str], 'Tonnes': [tonnes]})
            history = pd.concat([history, new_row], ignore_index=True)
            need_update = True
        elif 'Change_Pct' not in history.columns:
            print(">>> 发现缺少 Change_Pct 列，准备重算结构...")
            need_update = True
            
        # 3. 只有需要更新时才进行计算和保存
        if need_update:
            # A. 排序
            history['Date'] = pd.to_datetime(history['Date'])
            history = history.sort_values('Date').reset_index(drop=True)
            
            # B. 确保数字类型
            history['Tonnes'] = history['Tonnes'].astype(float)
            
            # C. 计算百分比
            history['Change_Pct'] = history['Tonnes'].pct_change() * 100
            history['Change_Pct'] = history['Change_Pct'].round(4)
            
            # D. 格式化日期
            history['Date'] = history['Date'].dt.strftime('%Y-%m-%d')
            
            # E. 保存
            history.to_csv(FILE_NAME, index=False)
            
            latest_change = history.iloc[-1]['Change_Pct']
            change_disp = f"{latest_change:+.4f}%" if not pd.isna(latest_change) else "0.00%"
            print(f"✅ 数据已更新: {date_str} | 持仓: {tonnes}吨 | 日环比: {change_disp}")
            
        else:
            print(f"数据已存在且格式完整 ({date_str})，跳过写入")
            
    except Exception as e:
        print(f"出错: {e}")

if __name__ == "__main__":
    fetch_and_save()
