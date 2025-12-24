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
        
        print(f"官网最新数据: {date_str} - {tonnes}")

        # 2. 读取或创建本地 CSV
        if os.path.exists(FILE_NAME):
            history = pd.read_csv(FILE_NAME)
        else:
            history = pd.DataFrame(columns=['Date', 'Tonnes'])
        
        # 3. 查重并写入
        if date_str not in history['Date'].values:
            new_row = pd.DataFrame({'Date': [date_str], 'Tonnes': [tonnes]})
            history = pd.concat([history, new_row], ignore_index=True)
            history.to_csv(FILE_NAME, index=False)
            print("✅ 数据已更新")
        else:
            print("数据已存在，跳过写入")
            
    except Exception as e:
        print(f"出错: {e}")

if __name__ == "__main__":
    fetch_and_save()
