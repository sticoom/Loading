import streamlit as st
import pandas as pd
import math
import io

# ================= 页面初始化 =================
st.set_page_config(page_title="亚马逊智能排柜系统", page_icon="📦", layout="wide")

# ================= 业务配置区 =================
SHORT_NAME_MAP = {
    "铧胜": "铧胜", "凯乐": "凯乐", "云仓": "云仓", "深圳仓": "深圳仓", 
    "成品一区": "深圳仓", "捷鹏": "捷鹏", "云晴": "云晴", "畅艺鑫": "畅艺鑫", 
    "深凡": "深凡", "枫悦": "枫悦", "启森": "启森", "蓓圣美": "蓓圣美",
    "坤茂": "坤茂", "天源": "云仓"
}

LOCAL_WH = ["云仓", "深圳仓"]

# ================= 核心工具函数 =================
def extract_short_name(full_name):
    if pd.isna(full_name): return "未知"
    for key, short in SHORT_NAME_MAP.items():
        if key in str(full_name): return short
    return str(full_name)

def safe_sum(df, col='待发货体积(CBM)'):
    return df[col].sum() if not df.empty else 0.0

# ================= 核心排柜算法 =================
def process_core_pool(pool_df, pool_name):
    if pool_df.empty: return pool_df
    
    df = pool_df.copy()
    
    # 【修复重点 1】: 强制清理 Excel 带来的 NaN 空值，防止判定失效
    for col in ['最终库区简称', '系统分配柜号', '装柜地址', '排柜备注']:
        if col not in df.columns: 
            df[col] = ""
        else:
            df[col] = df[col].fillna("").astype(str).replace('nan', '').str.strip()
            
    df['最终库区简称'] = df['当前库区'].apply(extract_short_name)
    df['待发货体积(CBM)'] = pd.to_numeric(df['待发货体积(CBM)'], errors='coerce').fillna(0)
    df['是否商检'] = df.get('是否商检', '').fillna('').astype(str).str.strip()
    
    global_cab_idx = 1
    regions = ['华东', '华南']
    remain_dfs = []
    initial_totals = {}
    
    # ---------------- 第二阶段：同区域宏观算账与整柜 ----------------
    for region in regions:
        # 只抓取还未排柜的数据
        r_df = df[(df['当前区域'] == region) & (df['装柜地址'] == "")].copy()
        if r_df.empty: 
            initial_totals[region] = 0
            continue
            
        total_vol = safe_sum(r_df)
        initial_totals[region] = total_vol
        
        # 1. 摸底定额
        if total_vol < 60:
            remain_dfs.append(r_df)
            continue
            
        X = math.floor(total_vol / 71.0)
        if (total_vol % 71.0) >= 60: X += 1 # 尾数如果>=60，也算一个整柜
        if X == 0: X = 1
        
        # 2. 商检强绑定 (占用名额)
        inspections = r_df[r_df['是否商检'] == '是']
        insp_cabs = 0
        if not inspections.empty:
            main_insp_wh = inspections.groupby('最终库区简称')['待发货体积(CBM)'].sum().idxmax()
            insp_vol = safe_sum(inspections)
            insp_cabs = max(1, math.ceil(insp_vol / 71.0))
            
            for i in range(insp_cabs):
                cab_name = f"{pool_name}-商检柜{global_cab_idx:02d}"
                global_cab_idx += 1
                for idx in inspections.index:
                    r_df.at[idx, '装柜地址'] = f"{main_insp_wh}装柜-{region}"
                    r_df.at[idx, '系统分配柜号'] = cab_name
            X = max(0, X - insp_cabs)
            
        # 3. 选定剩余主导地址
        normal_df = r_df[(r_df['装柜地址'] == "") & (~r_df['最终库区简称'].isin(LOCAL_WH))]
        wh_vols = normal_df.groupby('最终库区简称')['待发货体积(CBM)'].sum().sort_values(ascending=False)
        
        lead_whs = []
        for wh, vol in wh_vols.items():
            if len(lead_whs) >= X: break
            if wh == "捷鹏" and vol < 50: continue # 捷鹏拦截规则
            lead_whs.append(wh)
            
        # 4. 【修复重点 2】: 完整补齐瀑布流精准填缝 (贪心算法)
        for lead_wh in lead_whs:
            cab_name = f"{pool_name}-整柜{global_cab_idx:02d}"
            global_cab_idx += 1
            current_vol = 0.0
            
            # (1) 先装主导库区自己的货
            lead_items = r_df[(r_df['装柜地址'] == "") & (r_df['最终库区简称'] == lead_wh)]
            for idx in lead_items.index:
                r_df.at[idx, '装柜地址'] = f"{lead_wh}装柜-{region}"
                r_df.at[idx, '系统分配柜号'] = cab_name
                current_vol += r_df.at[idx, '待发货体积(CBM)']
                
            # (2) 去别的库区抓货填满 60~71方 (优先抓取大块头)
            other_items = r_df[r_df['装柜地址'] == ""].sort_values(by='待发货体积(CBM)', ascending=False)
            for idx in other_items.index:
                if current_vol >= 71.0: break # 已经装满
                item_vol = r_df.at[idx, '待发货体积(CBM)']
                # 如果塞得下这票货 (最高容差放宽至71.5)
                if current_vol + item_vol <= 71.5:  
                    r_df.at[idx, '装柜地址'] = f"{lead_wh}装柜-{region}"
                    r_df.at[idx, '系统分配柜号'] = cab_name
                    current_vol += item_vol

        # 把这轮排好的结果存下来，没排上的丢入尾货池
        remain_dfs.append(r_df[r_df['装柜地址'] == ""])
        df.update(r_df[r_df['装柜地址'] != ""])

    # ---------------- 第三阶段：尾货清算 (跨区调拨 vs 散货) ----------------
    if remain_dfs:
        remain_all = pd.concat(remain_dfs)
        hd_rem = remain_all[remain_all['当前区域'] == '华东']
        hn_rem = remain_all[remain_all['当前区域'] == '华南']
        
        hd_vol = safe_sum(hd_rem)
        hn_vol = safe_sum(hn_rem)
        
        # 分支 A：触发跨区合体调拨
        if 60 <= (hd_vol + hn_vol) <= 71 and hd_vol > 0 and hn_vol > 0:
            sender = '华东' if hd_vol < hn_vol else '华南'
            receiver = '华南' if sender == '华东' else '华东'
            
            recv_df = hn_rem if receiver == '华南' else hd_rem
            send_df = hd_rem if sender == '华东' else hn_rem
            
            recv_main_wh = recv_df.groupby('最终库区简称')['待发货体积(CBM)'].sum().idxmax()
            cab_name = f"{pool_name}-跨区合体柜{global_cab_idx:02d}"
            global_cab_idx += 1
            
            # 接收方打标
            for idx in recv_df.index:
                df.at[idx, '装柜地址'] = f"{recv_main_wh}装柜-{receiver}"
                df.at[idx, '系统分配柜号'] = cab_name
                
            # 发出方打标与备注
            for idx in send_df.index:
                sender_wh = df.at[idx, '最终库区简称']
                df.at[idx, '装柜地址'] = f"{recv_main_wh}装柜-{receiver}"
                df.at[idx, '系统分配柜号'] = cab_name
                df.at[idx, '排柜备注'] = f"拆分调拨：{recv_main_wh}装柜-{sender}发往{receiver}"
                
        # 分支 B：触发本地散货发车
        else:
            for region, rem_df in [('华东', hd_rem), ('华南', hn_rem)]:
                if rem_df.empty: continue
                vol = safe_sum(rem_df)
                
                is_b1 = initial_totals.get(region, 0) < 60
                cab_count = max(1, math.ceil(vol / 40.0))
                default_wh = "云仓" if region == "华东" else "深圳仓"
                
                scatter_vols = rem_df.groupby('最终库区简称')['待发货体积(CBM)'].sum()
                def_vol = scatter_vols.get(default_wh, 0)
                
                final_addr = default_wh
                is_reversed = False
                
                if is_b1:
                    for wh, v in scatter_vols.items():
                        if wh not in LOCAL_WH and v > (def_vol + 5):
                            final_addr = wh
                            is_reversed = True
                            break
                            
                for i in range(cab_count):
                    cab_name = f"{region}-散货柜{global_cab_idx:02d}"
                    global_cab_idx += 1
                    
                    if is_b1:
                        prefix = f"{final_addr}{i+1}{i+1}"
                        addr_str = f"{prefix}-AMP散货-{region}"
                    else:
                        addr_str = f"AMP散货-{region}"
                        
                    for idx in rem_df.index: 
                        if df.at[idx, '装柜地址'] == "":
                            df.at[idx, '装柜地址'] = addr_str
                            df.at[idx, '系统分配柜号'] = cab_name
                            if is_b1 and is_reversed:
                                df.at[idx, '排柜备注'] = f"{default_wh}调往-{final_addr}"

    return df

# ================= 网页 UI 渲染 =================
st.title("📦 亚马逊智能排柜系统 (V1.1 修复版)")
st.markdown("已修复空值识别故障，并全量挂载瀑布流分配算法。")

uploaded_file = st.file_uploader("请上传最新的《排柜草稿》Excel/CSV 文件", type=["xlsx", "csv"])

if uploaded_file is not None:
    try:
        if uploaded_file.name.endswith('.csv'):
            raw_df = pd.read_csv(uploaded_file)
        else:
            raw_df = pd.read_excel(uploaded_file)
            
        st.success(f"✅ 数据读取成功！共加载 {len(raw_df)} 行。")
        
        if st.button("🚀 启动全局排柜算法", type="primary"):
            with st.spinner('正在进行数据分流与矩阵运算...'):
                
                # 安全清理条件列
                raw_df['尺寸类型'] = raw_df['尺寸类型'].fillna('').astype(str).str.strip()
                raw_df['运输方式'] = raw_df['运输方式'].fillna('').astype(str).str.strip()
                raw_df['入库配置方式'] = raw_df['入库配置方式'].fillna('').astype(str).str.strip()
                
                mask_s1 = raw_df['尺寸类型'].str.contains('标准') & raw_df['运输方式'].str.contains('AGL') & raw_df['入库配置方式'].isin(['AOSS', 'AMP'])
                mask_s2 = raw_df['尺寸类型'].str.contains('标准') & raw_df['运输方式'].str.contains('AGL') & (raw_df['入库配置方式'] == 'MSS')
                mask_s3 = raw_df['尺寸类型'].str.contains('标准') & (raw_df['入库配置方式'] == 'SMP')
                
                sheet1_df = raw_df[mask_s1].copy()
                sheet2_df = raw_df[mask_s2].copy()
                sheet3_df = raw_df[mask_s3].copy()
                sheet4_df = raw_df[~(mask_s1 | mask_s2 | mask_s3)].copy()
                
                # 核心运算
                res_sheet1 = process_core_pool(sheet1_df, "AOSS/AMP")
                res_sheet2 = process_core_pool(sheet2_df, "MSS")
                
                # 生成说明文档
                readme_df = pd.DataFrame({
                    "排柜逻辑说明": [
                        "1. AGL快-标准尺寸-AOSS+AMP：系统已执行瀑布流凑柜与调拨算法。",
                        "2. MSS：逻辑同上，已独立运算完毕。",
                        "3. SMP：未参与排柜，原样保留。",
                        "4. 其它方式：过滤出的无需排柜的数据。",
                        "注：新增列为【最终库区简称】、【系统分配柜号】、【装柜地址】、【排柜备注】。"
                    ]
                })
                
                def reorder_cols(df):
                    if df.empty: return df
                    cols = list(df.columns)
                    added = ['最终库区简称', '系统分配柜号', '装柜地址', '排柜备注']
                    for c in added:
                        if c in cols: cols.remove(c)
                        cols.append(c)
                    return df[cols]

            st.success("🎉 全局排柜运算完成！")
            
            st.subheader("📊 运算结果预览 (核心池1: AOSS+AMP)")
            st.dataframe(reorder_cols(res_sheet1).head(15))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                reorder_cols(res_sheet1).to_excel(writer, sheet_name="AGL快-标准-AOSS+AMP", index=False)
                reorder_cols(res_sheet2).to_excel(writer, sheet_name="MSS", index=False)
                sheet3_df.to_excel(writer, sheet_name="SMP", index=False)
                sheet4_df.to_excel(writer, sheet_name="其它方式", index=False)
                readme_df.to_excel(writer, sheet_name="系统排柜逻辑说明", index=False)
            
            processed_data = output.getvalue()
            
            st.download_button(
                label="⬇️ 一键下载最终多表盘 Excel",
                data=processed_data,
                file_name="智能排柜_最终结果.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary"
            )
            
    except Exception as e:
        st.error(f"❌ 运行报错: {str(e)}")
