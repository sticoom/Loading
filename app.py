"""
亚马逊智能排柜系统 (V-Final 终极三明治架构版)
包含：前置清洗、商检强绑、核心打分穷举沙盘、尾盘跨区大决战、精确散货截断、行级物理切分与双列备注回写。
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

process_logs = []

def log(msg):
    """记录推演日志"""
    process_logs.append(msg)

# ==========================================
# 1. 中间的肉：核心沙盘打分引擎
# ==========================================
def score_plan(plan_type, num_stops, split_local, split_normal, cross_region, leftover_vols):
    """全局策略打分器 (分数越低越优)"""
    score = 0
    # A. 局部惩罚
    score += num_stops * 15
    if split_local: score += 25
    if split_normal: score += 1000 # 绝对大忌
    if cross_region: score += 10

    # B. 全局预判碎片惩罚 (大局观)
    total_left = sum(leftover_vols)
    if 60 <= total_left <= 71:
        score -= 50 # 完美遗留，重赏
    
    tiny_frags = [v for v in leftover_vols if 0 < v < 15]
    if len(tiny_frags) >= 2:
        score += 30 * len(tiny_frags) # 碎片遍地，重罚
        
    return score

def run_sandbox_engine(inventory_dict, is_endgame=False):
    """沙盘引擎：输出包含双列备注、严格限额的装柜指令"""
    allocations = []
    cab_counter = 1
    
    inv = {k: v for k, v in inventory_dict.items() if v > 0.01}
    
    # --- 阶段 1 & 2: 循环寻找整柜 (同区优先，尾盘跨区) ---
    while sum(inv.values()) >= 60:
        valid_leads = {k: v for k, v in inv.items() if not (k[1] == "捷鹏" and v < 50)}
        if not valid_leads: break
            
        lead_key = max(valid_leads.items(), key=lambda x: x[1])[0]
        lead_reg, lead_wh = lead_key
        lead_vol = inv[lead_key]
        
        # 优先级1：大户直通车
        if lead_vol >= 60:
            take_vol = min(lead_vol, 71.0)
            allocations.append({
                "cab_id": f"整柜-{cab_counter:02d}",
                "addr": f"{lead_wh}装柜-{lead_reg}",
                "remark1": f"全部在原地仓。(最终装柜:{take_vol:.2f}方)",
                "remark2": "大户独立成柜",
                "items": {lead_key: take_vol}
            })
            inv[lead_key] -= take_vol
            log(f"[优先级1] {lead_wh}({lead_reg}) 大户直通，截取 {take_vol:.1f}方。")
            cab_counter += 1
            continue
            
        # 优先级2-4：穷举拼凑
        gap_min, gap_max = 60 - lead_vol, 71 - lead_vol
        candidate_keys = [k for k in inv.keys() if k != lead_key] if is_endgame else [k for k in inv.keys() if k != lead_key and k[0] == lead_reg]
        possible_plans = []
        
        # 方案A & B: 1或2家无损拼
        for combo_size in [1, 2]:
            for combo in itertools.combinations(candidate_keys, combo_size):
                combo_vol = sum(inv[k] for k in combo)
                if gap_min <= combo_vol <= gap_max:
                    cross = any(k[0] != lead_reg for k in combo)
                    leftovers = [v for k, v in inv.items() if k != lead_key and k not in combo]
                    score = score_plan('exact', len(combo)+1, False, False, cross, leftovers)
                    possible_plans.append({
                        "type": "exact", "combo": combo, "take_vols": {k: inv[k] for k in combo},
                        "score": score, "desc": "无损拼图"
                    })
                    
        # 方案C: 切分本地仓
        local_keys = [k for k in candidate_keys if k[1] in LOCAL_WHS and inv[k] >= gap_min]
        for lk in local_keys:
            take_vol = min(inv[lk], gap_max)
            leftovers = [v for k, v in inv.items() if k != lead_key and k != lk] + [inv[lk] - take_vol]
            cross = (lk[0] != lead_reg)
            score = score_plan('split_local', 2, True, False, cross, leftovers)
            possible_plans.append({
                "type": "split_local", "combo": (lk,), "take_vols": {lk: take_vol},
                "score": score, "desc": f"切分本地仓({lk[1]})"
            })
            
        # 方案D: 强切普通厂
        normal_keys = [k for k in candidate_keys if k[1] not in LOCAL_WHS and inv[k] >= gap_min]
        for nk in normal_keys:
            take_vol = min(inv[nk], gap_max)
            leftovers = [v for k, v in inv.items() if k != lead_key and k != nk] + [inv[nk] - take_vol]
            cross = (nk[0] != lead_reg)
            score = score_plan('split_normal', 2, False, True, cross, leftovers)
            possible_plans.append({
                "type": "split_normal", "combo": (nk,), "take_vols": {nk: take_vol},
                "score": score, "desc": f"强切普通厂({nk[1]})"
            })
            
        # 抉择最优
        if possible_plans:
            best_plan = min(possible_plans, key=lambda x: x['score'])
            alloc_items = {lead_key: lead_vol}
            alloc_items.update(best_plan["take_vols"])
            
            # 拼装备注1：精细调拨明细
            details = []
            total_cab_vol = lead_vol
            for k, v in best_plan["take_vols"].items():
                total_cab_vol += v
                transfer_type = "全部" if v >= inv[k] - 0.01 else "部分"
                details.append(f"【{k[1]}】{transfer_type}调往【{lead_wh}】{v:.2f}方")
            remark1_str = "；".join(details) + f"。(最终装柜:{total_cab_vol:.2f}方)"
            
            # 备注2：宏观策略
            remark2_str = best_plan["desc"]
            if any(k[0] != lead_reg for k in best_plan["combo"]):
                remark2_str = "跨区调拨：" + remark2_str
                
            allocations.append({
                "cab_id": f"整柜-{cab_counter:02d}",
                "addr": f"{lead_wh}装柜-{lead_reg}",
                "remark1": remark1_str,
                "remark2": remark2_str,
                "items": alloc_items
            })
            log(f"[AI决策] 主导:{lead_wh} 采用【{remark2_str}】。得分:{best_plan['score']}。")
            
            # 扣减库存
            inv[lead_key] = 0
            for k, v in best_plan["take_vols"].items(): inv[k] -= v
            inv = {k: v for k, v in inv.items() if v > 0.01}
            cab_counter += 1
        else:
            log(f"[沙盘死局] {lead_wh}({lead_reg}) 无法凑齐整柜，掉入散货池。")
            break

    # --- 阶段 3: 优先级5 散货清算 (严格 40方物理截断) ---
    inv = {k: v for k, v in inv.items() if v > 0.01}
    for region in ["华东", "华南"]:
        reg_inv = {k: v for k, v in inv.items() if k[0] == region}
        if not reg_inv: continue
        
        default_wh = "云仓" if region == "华东" else "深圳仓"
        final_addr_wh = default_wh
        is_reverse = False
        
        # 散货反转判定
        def_vol = sum(v for k, v in reg_inv.items() if k[1] == default_wh)
        for k, v in reg_inv.items():
            if k[1] not in LOCAL_WHS and v > (def_vol + 5):
                final_addr_wh = k[1]
                is_reverse = True
                break
                
        # 物理限制：每次最多塞 40 方
        cab_idx = 1
        while sum(reg_inv.values()) > 0.01:
            current_scatter_items = {}
            current_vol = 0
            
            for k, v in list(reg_inv.items()):
                if current_vol >= 40.0: break
                take_v = min(v, 40.0 - current_vol)
                current_scatter_items[k] = take_v
                current_vol += take_v
                reg_inv[k] -= take_v
                
            reg_inv = {k: v for k, v in reg_inv.items() if v > 0.01}
            
            # 生成散货调货明细
            details = []
            for k, v in current_scatter_items.items():
                if k[1] != final_addr_wh:
                    details.append(f"【{k[1]}】调往【{final_addr_wh}】{v:.2f}方")
                    
            remark1_str = "；".join(details) + f"。(最终装柜:{current_vol:.2f}方)" if details else f"全部在原地仓。(最终装柜:{current_vol:.2f}方)"
            remark2_str = f"{default_wh}反转调往-{final_addr_wh}" if is_reverse else "常规散货归集"
            
            addr_str = f"{final_addr_wh}{cab_idx}{cab_idx}-AMP散货-{region}" if is_endgame else f"AMP散货-{region}-{cab_idx:02d}"
                
            allocations.append({
                "cab_id": f"散货柜-{cab_counter:02d}",
                "addr": addr_str,
                "remark1": remark1_str,
                "remark2": remark2_str,
                "items": current_scatter_items 
            })
            log(f"[散货清算] {region} 截取 {current_vol:.1f}方 装入【{addr_str}】。")
            cab_idx += 1
            cab_counter += 1
            
    return allocations

# ==========================================
# 2. 上层面包：前置处理
# ==========================================
def extract_short(name):
    if pd.isna(name): return "未知"
    for k, v in SHORT_NAME_MAP.items():
        if k in str(name): return v
    return str(name)

# ==========================================
# 3. 下层面包：物理切分与双列备注回写
# ==========================================
def apply_allocations_to_df(df, allocations, pool_name):
    out_rows = []
    records = df.to_dict('records')
    
    for alloc in allocations:
        cab_id = f"{pool_name}-{alloc['cab_id']}"
        addr = alloc['addr']
        rem1 = alloc['remark1']
        rem2 = alloc['remark2']
        
        for (reg, wh), target_vol in alloc['items'].items():
            needed = round(target_vol, 2)
            
            for row in records:
                if needed <= 0.01: break
                if row.get('系统分配柜号') != "": continue 
                if row['当前区域'] == reg and row['最终库区简称'] == wh:
                    
                    row_vol = float(row.get('待发货体积(CBM)', 0))
                    if row_vol <= 0: continue
                    
                    if row_vol <= needed + 0.05: 
                        row['系统分配柜号'] = cab_id
                        row['装柜地址'] = addr
                        row['排柜备注1'] = rem1
                        row['排柜备注2'] = rem2
                        needed -= row_vol
                    else:
                        new_row = copy.deepcopy(row)
                        new_row['待发货体积(CBM)'] = needed
                        new_row['系统分配柜号'] = cab_id
                        new_row['装柜地址'] = addr
                        new_row['排柜备注1'] = "SKU切分：" + rem1
                        new_row['排柜备注2'] = rem2
                        out_rows.append(new_row)
                        
                        row['待发货体积(CBM)'] -= needed
                        needed = 0
                        
    assigned = [r for r in records if r.get('系统分配柜号') != ""]
    unassigned = [r for r in records if r.get('系统分配柜号') == ""]
    
    res_df = pd.DataFrame(assigned + out_rows + unassigned)
    cols = list(res_df.columns)
    for c in ['最终库区简称', '系统分配柜号', '装柜地址', '排柜备注1', '排柜备注2']:
        if c in cols: cols.remove(c)
        cols.append(c)
    return res_df[cols]

# ==========================================
# 4. 主控桥梁
# ==========================================
def process_full_pipeline(df, pool_name):
    global process_logs
    if df.empty: return df
    
    df = df.copy()
    for col in ['系统分配柜号', '装柜地址', '排柜备注1', '排柜备注2', '最终库区简称']:
        df[col] = ""
        
    df['待发货体积(CBM)'] = pd.to_numeric(df['待发货体积(CBM)'], errors='coerce').fillna(0)
    df['最终库区简称'] = df['当前库区'].apply(extract_short)
    df['是否商检'] = df.get('是否商检', '').fillna('').astype(str).str.strip()
    
    log(f"\n======== 开始处理 {pool_name} 数据池 ========")
    
    # 优先级0：商检强绑
    inspections = df[df['是否商检'] == '是']
    if not inspections.empty:
        log(f"[商检处理] 检出商检货。开始强制捆绑。")
        for region in inspections['当前区域'].unique():
            reg_insp = inspections[inspections['当前区域'] == region]
            main_wh = reg_insp.groupby('最终库区简称')['待发货体积(CBM)'].sum().idxmax()
            insp_vol = reg_insp['待发货体积(CBM)'].sum()
            cabs = max(1, math.ceil(insp_vol / 71.0))
            
            for i in range(cabs):
                for idx in reg_insp.index:
                    df.at[idx, '系统分配柜号'] = f"商检柜-{i+1:02d}"
                    df.at[idx, '装柜地址'] = f"{main_wh}装柜-{region}"
                    df.at[idx, '排柜备注1'] = f"商检捆绑装柜。(该批商检总积:{insp_vol:.2f}方)"
                    df.at[idx, '排柜备注2'] = "商检强制分配"
    
    normal_df = df[df['系统分配柜号'] == ""]
    inventory_dict = normal_df.groupby(['当前区域', '最终库区简称'])['待发货体积(CBM)'].sum().to_dict()
    
    log("\n--- 启动第一轮：同区沙盘计算 ---")
    final_allocations = run_sandbox_engine(inventory_dict, is_endgame=False)
    
    log("\n--- 启动第二轮：跨区尾盘大决战沙盘 ---")
    endgame_allocations = run_sandbox_engine(inventory_dict, is_endgame=True)
    
    final_allocations.extend(endgame_allocations)
    res_df = apply_allocations_to_df(normal_df, final_allocations, pool_name)
    
    return pd.concat([df[df['系统分配柜号'] != ""], res_df], ignore_index=True)

# ==========================================
# 5. UI 与 导出层
# ==========================================
st.title("📦 亚马逊智能排柜引擎 (终极 V-Final 版)")
st.markdown("集成 **运筹穷举沙盘** 与 **SKU行级物理截断**。严格执行 40方散货切分与双列明细。")

uploaded_file = st.file_uploader("请上传最新的《排柜草稿》", type=["xlsx", "csv"])

if uploaded_file is not None:
    try:
        raw_df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('.xlsx') else pd.read_csv(uploaded_file)
        
        if st.button("🚀 启动运筹沙盘演算", type="primary"):
            process_logs.clear() 
            
            with st.spinner('AI 正在全盘穷举并切分装柜数据...'):
                for c in ['尺寸类型', '运输方式', '入库配置方式']:
                    if c in raw_df.columns: raw_df[c] = raw_df[c].fillna('').astype(str).str.strip()
                
                mask_s1 = raw_df['尺寸类型'].str.contains('标准') & raw_df['运输方式'].str.contains('AGL') & raw_df['入库配置方式'].isin(['AOSS', 'AMP'])
                mask_s2 = raw_df['尺寸类型'].str.contains('标准') & raw_df['运输方式'].str.contains('AGL') & (raw_df['入库配置方式'] == 'MSS')
                mask_s3 = raw_df['尺寸类型'].str.contains('标准') & (raw_df['入库配置方式'] == 'SMP')
                
                res_s1 = process_full_pipeline(raw_df[mask_s1].copy(), "AOSS+AMP")
                res_s2 = process_full_pipeline(raw_df[mask_s2].copy(), "MSS")
                log_df = pd.DataFrame({"沙盘运筹推演日志 (Dispatch Logs)": process_logs})
                
            st.success("🎉 沙盘演算与 SKU 切分完美完成！")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                st.dataframe(res_s1[['最终库区简称', '系统分配柜号', '装柜地址', '排柜备注1', '排柜备注2', '待发货体积(CBM)']].head(20))
            with col2:
                st.dataframe(log_df, height=400)
                
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                res_s1.to_excel(writer, sheet_name="AOSS+AMP排柜", index=False)
                res_s2.to_excel(writer, sheet_name="MSS排柜", index=False)
                raw_df[mask_s3].to_excel(writer, sheet_name="SMP保留", index=False)
                raw_df[~(mask_s1 | mask_s2 | mask_s3)].to_excel(writer, sheet_name="其它隔离区", index=False)
                log_df.to_excel(writer, sheet_name="系统沙盘推演日志", index=False)
                
            st.download_button(
                label="⬇️ 完美版下载 (含精细调拨明细)",
                data=output.getvalue(),
                file_name="智能排柜_终极三明治完美版.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary"
            )
            
    except Exception as e:
        st.error(f"❌ 报错: {str(e)}")
