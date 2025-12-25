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
            # 初始化时增加 Change_Pct 列
            history = pd.DataFrame(columns=['Date', 'Tonnes', 'Change_Pct'])
        
        # 确保 Date 列是字符串格式，防止比较出错
        history['Date'] = history['Date'].astype(str)
        
        # 3. 查重并写入
        if date_str not in history['Date'].values:
            # 创建新行
            new_row = pd.DataFrame({'Date': [date_str], 'Tonnes': [tonnes]})
            history = pd.concat([history, new_row], ignore_index=True)
            
            # --- 新增核心功能：计算环比变化 ---
            
            # A. 确保按日期排序 (防止数据乱序导致计算错误)
            history['Date'] = pd.to_datetime(history['Date'])
            history = history.sort_values('Date').reset_index(drop=True)
            
            # B. 确保吨数是数字类型
            history['Tonnes'] = history['Tonnes'].astype(float)
            
            # C. 使用 pct_change() 计算每日变化率 (乘以100变百分比)
            history['Change_Pct'] = history['Tonnes'].pct_change() * 100
            
            # D. 保留 4 位小数，看起来更整洁
            history['Change_Pct'] = history['Change_Pct'].round(4)
            
            # E. 格式化日期回字符串 (为了保存 CSV)
            history['Date'] = history['Date'].dt.strftime('%Y-%m-%d')
            
            # 获取最新算出来的变化
            latest_change = history.iloc[-1]['Change_Pct']
            
            # 保存文件
            history.to_csv(FILE_NAME, index=False)
            
            # 打印带符号的变化率 (例如: +0.2500%)
            if pd.isna(latest_change):
                change_disp = "0.00%" # 第一天没有前值
            else:
                change_disp = f"{latest_change:+.4f}%"
                
            print(f"✅ 数据已更新: {date_str} | 持仓: {tonnes}吨 | 日环比: {change_disp}")
            
        else:
            print(f"数据已存在 ({date_str})，跳过写入")
            
    except Exception as e:
        print(f"出错: {e}")

if __name__ == "__main__":
    fetch_and_save()
