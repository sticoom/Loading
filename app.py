"""
亚马逊智能排柜系统 (V-Final 终极三明治架构版)
包含：前置清洗提取、商检强绑、核心打分穷举沙盘、尾盘跨区大决战、后置行级切分回写。
"""
import streamlit as st
import pandas as pd
import math
import itertools
import copy
import io

# ==========================================
# 0. 全局配置与字典
# ==========================================
st.set_page_config(page_title="亚马逊智能排柜引擎", page_icon="📦", layout="wide")

SHORT_NAME_MAP = {
    "铧胜": "铧胜", "凯乐": "凯乐", "云仓": "云仓", "深圳仓": "深圳仓", 
    "成品一区": "深圳仓", "捷鹏": "捷鹏", "云晴": "云晴", "畅艺鑫": "畅艺鑫", 
    "深凡": "深凡", "枫悦": "枫悦", "启森": "启森", "蓓圣美": "蓓圣美",
    "坤茂": "坤茂", "天源": "云仓"
}
LOCAL_WHS = ["云仓", "深圳仓"]

# 全局执行日志，用于网页展示和导出Excel
process_logs = []

def log(msg):
    """记录推演日志"""
    process_logs.append(msg)

# ==========================================
# 1. 中间的肉：核心沙盘打分引擎 (The Sandbox Engine)
# ==========================================
def score_plan(plan_type, num_stops, split_local, split_normal, cross_region, leftover_vols):
    """全局策略打分器 (分数越低越优)"""
    score = 0
    # A. 局部惩罚
    score += num_stops * 15
    if split_local: score += 25
    if split_normal: score += 1000 # 绝对大忌
    if cross_region: score += 10

    # B. 全局预判碎片惩罚
    total_left = sum(leftover_vols)
    if 60 <= total_left <= 71:
        score -= 50 # 完美遗留，重赏
    
    tiny_frags = [v for v in leftover_vols if 0 < v < 15]
    if len(tiny_frags) >= 2:
        score += 30 * len(tiny_frags) # 碎片遍地，重罚
        
    return score

def run_sandbox_engine(inventory_dict, is_endgame=False):
    """
    沙盘引擎：接收字典 {(区域, 库区): 体积}，输出装柜指令列表。
    """
    allocations = []
    cab_counter = 1
    
    # 深拷贝，避免污染原字典
    inv = {k: v for k, v in inventory_dict.items() if v > 0.01}
    
    # --- 阶段 1 & 2: 循环寻找整柜 (同区优先，尾盘跨区) ---
    while sum(inv.values()) >= 60:
        # 获取合法的主导厂 (体积最大，且若是捷鹏需>=50)
        valid_leads = {k: v for k, v in inv.items() if not (k[1] == "捷鹏" and v < 50)}
        if not valid_leads:
            break # 没有合法主导厂了
            
        lead_key = max(valid_leads.items(), key=lambda x: x[1])[0]
        lead_reg, lead_wh = lead_key
        lead_vol = inv[lead_key]
        
        # 优先级1：大户直通车
        if lead_vol >= 60:
            take_vol = min(lead_vol, 71.0)
            allocations.append({
                "cab_id": f"整柜-{cab_counter:02d}",
                "addr": f"{lead_wh}装柜-{lead_reg}",
                "remark": "大户独立成柜",
                "items": {lead_key: take_vol}
            })
            inv[lead_key] -= take_vol
            log(f"[优先级1] {lead_wh}({lead_reg}) 大户直通，截取 {take_vol:.1f}方。")
            cab_counter += 1
            continue
            
        # 优先级2-4：准备穷举拼凑方案
        gap_min, gap_max = 60 - lead_vol, 71 - lead_vol
        
        # 如果不是尾盘大决战，只能同区拼；如果是尾盘，允许全盘跨区找！
        if not is_endgame:
            candidate_keys = [k for k in inv.keys() if k != lead_key and k[0] == lead_reg]
        else:
            candidate_keys = [k for k in inv.keys() if k != lead_key]
            
        possible_plans = []
        
        # 方案A & B: 1家或2家无损拼合
        for combo_size in [1, 2]:
            for combo in itertools.combinations(candidate_keys, combo_size):
                combo_vol = sum(inv[k] for k in combo)
                if gap_min <= combo_vol <= gap_max:
                    cross = any(k[0] != lead_reg for k in combo)
                    # 计算剩下的货
                    leftovers = []
                    for k, v in inv.items():
                        if k != lead_key and k not in combo: leftovers.append(v)
                    
                    score = score_plan('exact', len(combo)+1, False, False, cross, leftovers)
                    possible_plans.append({
                        "type": "exact", "combo": combo, "take_vols": {k: inv[k] for k in combo},
                        "score": score, "desc": "无损拼图"
                    })
                    
        # 方案C: 切分本地仓 (云仓/深仓)
        local_keys = [k for k in candidate_keys if k[1] in LOCAL_WHS and inv[k] >= gap_min]
        for lk in local_keys:
            take_vol = min(inv[lk], gap_max) # 优先切满
            leftovers = [v for k, v in inv.items() if k != lead_key and k != lk]
            leftovers.append(inv[lk] - take_vol) # 切剩的
            cross = (lk[0] != lead_reg)
            
            score = score_plan('split_local', 2, True, False, cross, leftovers)
            possible_plans.append({
                "type": "split_local", "combo": (lk,), "take_vols": {lk: take_vol},
                "score": score, "desc": f"切分本地仓({lk[1]})"
            })
            
        # 方案D: 强行切分普通厂
        normal_keys = [k for k in candidate_keys if k[1] not in LOCAL_WHS and inv[k] >= gap_min]
        for nk in normal_keys:
            take_vol = min(inv[nk], gap_max)
            leftovers = [v for k, v in inv.items() if k != lead_key and k != nk]
            leftovers.append(inv[nk] - take_vol)
            cross = (nk[0] != lead_reg)
            
            score = score_plan('split_normal', 2, False, True, cross, leftovers)
            possible_plans.append({
                "type": "split_normal", "combo": (nk,), "take_vols": {nk: take_vol},
                "score": score, "desc": f"强切普通厂({nk[1]})"
            })
            
        # 抉择最优方案
        if possible_plans:
            best_plan = min(possible_plans, key=lambda x: x['score'])
            
            alloc_items = {lead_key: lead_vol}
            alloc_items.update(best_plan["take_vols"])
            
            remark_str = best_plan["desc"]
            if any(k[0] != lead_reg for k in best_plan["combo"]):
                remark_str = "跨区调拨：" + remark_str
                
            allocations.append({
                "cab_id": f"整柜-{cab_counter:02d}",
                "addr": f"{lead_wh}装柜-{lead_reg}",
                "remark": remark_str,
                "items": alloc_items
            })
            log(f"[AI决策] 主导:{lead_wh}({lead_reg}) 缺{gap_min:.1f}方。采用【{remark_str}】方案，代价分:{best_plan['score']}。")
            
            # 扣减库存
            inv[lead_key] = 0
            for k, v in best_plan["take_vols"].items():
                inv[k] -= v
            inv = {k: v for k, v in inv.items() if v > 0.01}
            cab_counter += 1
        else:
            # 彻底找不到任何补齐方案，只能把这个主导厂剥离，让它去散货池
            log(f"[沙盘死局] {lead_wh}({lead_reg}) {lead_vol:.1f}方 无法凑齐整柜，掉入散货池。")
            break

    # --- 阶段 3: 优先级5 散货清算 ---
    inv = {k: v for k, v in inv.items() if v > 0.01}
    for region in ["华东", "华南"]:
        reg_inv = {k: v for k, v in inv.items() if k[0] == region}
        if not reg_inv: continue
        
        total_scatter = sum(reg_inv.values())
        cab_count = max(1, math.ceil(total_scatter / 40.0))
        
        default_wh = "云仓" if region == "华东" else "深圳仓"
        final_addr_wh = default_wh
        is_reverse = False
        
        # 散货反转判定 (如果普通厂 > 本地仓+5)
        def_vol = sum(v for k, v in reg_inv.items() if k[1] == default_wh)
        for k, v in reg_inv.items():
            if k[1] not in LOCAL_WHS and v > (def_vol + 5):
                final_addr_wh = k[1]
                is_reverse = True
                break
                
        for i in range(cab_count):
            if is_endgame:
                # B1 先天不足或跨区失败，带前缀
                addr_str = f"{final_addr_wh}{i+1}{i+1}-AMP散货-{region}"
            else:
                # B2 拼凑残渣，不带前缀
                addr_str = f"AMP散货-{region}"
                
            remark = f"{default_wh}调往-{final_addr_wh}" if is_reverse else "正常散货"
            
            allocations.append({
                "cab_id": f"散货柜-{cab_counter:02d}",
                "addr": addr_str,
                "remark": remark,
                "items": reg_inv # 把剩下的全塞进去
            })
            log(f"[散货清算] {region}剩余 {total_scatter:.1f}方。打包装入【{addr_str}】。")
            cab_counter += 1
            
    return allocations

# ==========================================
# 2. 上层面包：数据前置处理与商检拦截 (Preprocessing)
# ==========================================
def extract_short(name):
    if pd.isna(name): return "未知"
    for k, v in SHORT_NAME_MAP.items():
        if k in str(name): return v
    return str(name)

# ==========================================
# 3. 下层面包：物理行切分与结果回写 (Postprocessing)
# ==========================================
def apply_allocations_to_df(df, allocations, pool_name):
    """
    这步是核心现实映射：把沙盘计算的体积扣减，真实地切分到 Excel 每一行上。
    """
    out_rows = []
    
    # 将DataFrame转为字典列表，方便动态拆分行
    records = df.to_dict('records')
    
    for alloc in allocations:
        cab_id = f"{pool_name}-{alloc['cab_id']}"
        addr = alloc['addr']
        remark = alloc['remark']
        
        for (reg, wh), target_vol in alloc['items'].items():
            needed = round(target_vol, 2)
            
            for row in records:
                if needed <= 0.01: break
                if row.get('系统分配柜号') != "": continue # 已分配
                if row['当前区域'] == reg and row['最终库区简称'] == wh:
                    
                    row_vol = float(row.get('待发货体积(CBM)', 0))
                    if row_vol <= 0: continue
                    
                    if row_vol <= needed + 0.05: # 容差，整行吸纳
                        row['系统分配柜号'] = cab_id
                        row['装柜地址'] = addr
                        row['排柜备注'] = remark
                        needed -= row_vol
                    else:
                        # 核心：物理行切分 (SKU切割)
                        new_row = copy.deepcopy(row)
                        new_row['待发货体积(CBM)'] = needed
                        new_row['系统分配柜号'] = cab_id
                        new_row['装柜地址'] = addr
                        new_row['排柜备注'] = "SKU拆分：" + remark
                        out_rows.append(new_row)
                        
                        row['待发货体积(CBM)'] -= needed # 原行扣减，留在池里
                        needed = 0
                        
    # 把已经分配好的和剩下没分配的(如果有漏网之鱼)拼起来
    assigned = [r for r in records if r.get('系统分配柜号') != ""]
    unassigned = [r for r in records if r.get('系统分配柜号') == ""]
    
    final_records = assigned + out_rows + unassigned
    res_df = pd.DataFrame(final_records)
    
    # 排列展示顺序
    cols = list(res_df.columns)
    for c in ['最终库区简称', '系统分配柜号', '装柜地址', '排柜备注']:
        if c in cols: cols.remove(c)
        cols.append(c)
    return res_df[cols]

# ==========================================
# 4. 主控桥梁 (The Controller)
# ==========================================
def process_full_pipeline(df, pool_name):
    global process_logs
    if df.empty: return df
    
    df = df.copy()
    for col in ['系统分配柜号', '装柜地址', '排柜备注', '最终库区简称']:
        df[col] = ""
        
    df['待发货体积(CBM)'] = pd.to_numeric(df['待发货体积(CBM)'], errors='coerce').fillna(0)
    df['最终库区简称'] = df['当前库区'].apply(extract_short)
    df['是否商检'] = df.get('是否商检', '').fillna('').astype(str).str.strip()
    
    log(f"\n======== 开始处理 {pool_name} 数据池 ========")
    
    # 步骤一：提取商检货 (优先级0)
    inspections = df[df['是否商检'] == '是']
    insp_allocations = []
    if not inspections.empty:
        log(f"[商检处理] 检出 {len(inspections)} 行商检货。开始强制捆绑。")
        for region in inspections['当前区域'].unique():
            reg_insp = inspections[inspections['当前区域'] == region]
            main_wh = reg_insp.groupby('最终库区简称')['待发货体积(CBM)'].sum().idxmax()
            insp_vol = reg_insp['待发货体积(CBM)'].sum()
            cabs = max(1, math.ceil(insp_vol / 71.0))
            
            for i in range(cabs):
                # 商检占用柜号，从DataFrame中打标剥离
                for idx in reg_insp.index:
                    df.at[idx, '系统分配柜号'] = f"商检柜-{i+1:02d}"
                    df.at[idx, '装柜地址'] = f"{main_wh}装柜-{region}"
                    df.at[idx, '排柜备注'] = "商检强制捆绑"
    
    # 步骤二：数据聚合提取 (将DF映射为沙盘字典)
    normal_df = df[df['系统分配柜号'] == ""]
    inventory_dict = normal_df.groupby(['当前区域', '最终库区简称'])['待发货体积(CBM)'].sum().to_dict()
    
    log(f"[大盘扫描] 准备进入沙盘的普通货分布：")
    for k, v in inventory_dict.items(): log(f"  - {k[0]} {k[1]}: {v:.1f} 方")
    
    # 步骤三：启动核心沙盘！
    # 1. 同区沙盘优先
    log("\n--- 启动第一轮：同区沙盘计算 ---")
    final_allocations = run_sandbox_engine(inventory_dict, is_endgame=False)
    
    # 2. 尾盘跨区大决战沙盘
    log("\n--- 启动第二轮：跨区尾盘大决战沙盘 ---")
    # 此时 inventory_dict 里剩下的就是同区凑不齐的尾货
    endgame_allocations = run_sandbox_engine(inventory_dict, is_endgame=True)
    
    final_allocations.extend(endgame_allocations)
    
    # 步骤四：将沙盘虚拟指令真实回写到 Excel 中
    log("\n[执行回写] 开始将沙盘逻辑物理切分到每一行 SKU 上...")
    res_df = apply_allocations_to_df(normal_df, final_allocations, pool_name)
    
    # 合并最初的商检货
    final_df = pd.concat([df[df['系统分配柜号'] != ""], res_df], ignore_index=True)
    return final_df

# ==========================================
# 5. 网页 UI 展示层
# ==========================================
st.title("📦 亚马逊智能排柜引擎 (三明治架构版)")
st.markdown("搭载 `全量穷举打分沙盘` 与 `SKU 物理行级切分` 功能，追求全局运费成本最低。")

uploaded_file = st.file_uploader("请上传最新的《排柜草稿》", type=["xlsx", "csv"])

if uploaded_file is not None:
    try:
        raw_df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('.xlsx') else pd.read_csv(uploaded_file)
        
        if st.button("🚀 启动全局运算", type="primary"):
            process_logs.clear() # 清空旧日志
            
            with st.spinner('正在进行数据流重组与沙盘穷举演算...'):
                # 清洗空值
                for c in ['尺寸类型', '运输方式', '入库配置方式']:
                    if c in raw_df.columns: raw_df[c] = raw_df[c].fillna('').astype(str).str.strip()
                
                # 分流
                mask_s1 = raw_df['尺寸类型'].str.contains('标准') & raw_df['运输方式'].str.contains('AGL') & raw_df['入库配置方式'].isin(['AOSS', 'AMP'])
                mask_s2 = raw_df['尺寸类型'].str.contains('标准') & raw_df['运输方式'].str.contains('AGL') & (raw_df['入库配置方式'] == 'MSS')
                mask_s3 = raw_df['尺寸类型'].str.contains('标准') & (raw_df['入库配置方式'] == 'SMP')
                
                # 执行管线
                res_s1 = process_full_pipeline(raw_df[mask_s1].copy(), "AOSS+AMP")
                res_s2 = process_full_pipeline(raw_df[mask_s2].copy(), "MSS")
                
                # 生成日志 DataFrame
                log_df = pd.DataFrame({"沙盘AI推演日志 (Dispatch Logs)": process_logs})
                
            st.success("🎉 排柜计算完成！")
            
            # 展示前端 UI
            col1, col2 = st.columns([3, 1])
            with col1:
                st.subheader("运算结果预览 (AOSS+AMP)")
                st.dataframe(res_s1.head(15))
            with col2:
                st.subheader("沙盘思维推演日志")
                st.dataframe(log_df, height=350)
                
            # 导出 Excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                res_s1.to_excel(writer, sheet_name="AOSS+AMP排柜结果", index=False)
                res_s2.to_excel(writer, sheet_name="MSS排柜结果", index=False)
                raw_df[mask_s3].to_excel(writer, sheet_name="SMP保留", index=False)
                raw_df[~(mask_s1 | mask_s2 | mask_s3)].to_excel(writer, sheet_name="其它隔离区", index=False)
                log_df.to_excel(writer, sheet_name="系统沙盘推演日志", index=False)
                
            st.download_button(
                label="⬇️ 一键下载【最终多表盘排柜表】(含日志)",
                data=output.getvalue(),
                file_name="智能排柜_V终极解耦版.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary"
            )
            
    except Exception as e:
        st.error(f"❌ 运行报错: {str(e)}")
