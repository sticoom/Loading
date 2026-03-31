"""
亚马逊智能排柜系统
特性：重量红线(19.5吨) / 商检软捆绑 / 尾盘跨区财务决战 / 40方散货截断 / 行级物理切割与格式保护
"""
import streamlit as st
import pandas as pd
import math
import itertools
import copy
import io

# ==========================================
# 0. 全局配置与基准参数 (👉 业务参数在此修改 👈)
# ==========================================
st.set_page_config(page_title="亚马逊智能排柜引擎", page_icon="📦", layout="wide")

# --- 物理红线 ---
VOL_MIN_CABINET = 60.0       # 整柜成柜底线
VOL_MAX_CABINET = 71.0       # 整柜体积上限
VOL_STANDARD_CROSS = 70.0    # 跨区测算标准凑柜基数
WEIGHT_LIMIT = 19500.0       # 整柜重量上限 (KG)
VOL_MAX_SCATTER = 40.0       # 散货单柜上限

# --- 财务报价基准 ---
EXCHANGE_RATE = 7.2          # 汇率
PRICE_USD_EAST = 135.26      # 江浙/华东散货头程单价 (USD/方)
PRICE_USD_SOUTH = 135.00     # 深圳/华南散货头程单价 (USD/方)

# 国内跨仓调拨附加费
TRANSFER_SURCHARGE_VOL = 15.0  # 低于此方量加收附加费
TRANSFER_SURCHARGE_FEE = 200.0 # 附加费金额 (RMB/趟)

def get_domestic_transfer_rate(vol):
    """国内跨仓调拨阶梯单价 (RMB/方)"""
    if vol <= 5.0: return 155.0
    elif vol <= 10.0: return 140.0
    else: return 125.0

# 默认供应商映射字典 (如果Excel里没有映射Sheet，则使用此默认字典)
DEFAULT_SHORT_NAME_MAP = {
    "成品一区": "深圳仓", "天源": "云仓"
}
LOCAL_WHS = ["云仓", "深圳仓"]

process_logs = []
def log(msg): process_logs.append(msg)

# ==========================================
# 1. 财务测算引擎 (跨区大决战核心)
# ==========================================
def calculate_financial_diff(transfer_vol, target_original_vol, transfer_from_region):
    """
    4步测算法：计算跨区拼柜与原地发散货的【综合价差】。
    返回负数代表【跨区运回拼柜更省钱】。
    """
    local_price_usd = PRICE_USD_EAST if transfer_from_region == "华东" else PRICE_USD_SOUTH
    target_price_usd = PRICE_USD_SOUTH if transfer_from_region == "华东" else PRICE_USD_EAST

    # 步骤1：就地直发成本
    cost_local_direct = local_price_usd * transfer_vol * EXCHANGE_RATE

    # 步骤2：国内运输费 (按总方量算1票)
    cost_domestic = get_domestic_transfer_rate(transfer_vol) * transfer_vol
    if transfer_vol < TRANSFER_SURCHARGE_VOL:
        cost_domestic += TRANSFER_SURCHARGE_FEE

    # 步骤3：拼成70方后，留在目标仓的剩余方量
    leftover_vol = max(0, transfer_vol - (VOL_STANDARD_CROSS - target_original_vol))

    # 步骤4：剩余散货在目标仓的头程费
    cost_target_leftover = target_price_usd * leftover_vol * EXCHANGE_RATE

    # 计算价差: (跨区运费 + 目标仓剩余发散货运费) - (原产地全发散货运费)
    diff = (cost_domestic + cost_target_leftover) - cost_local_direct
    return diff

# ==========================================
# 2. 中间的肉：核心沙盘打分与穷举引擎
# ==========================================
def score_plan(num_stops, split_local, split_normal, cross_region, leftover_vols):
    score = num_stops * 15
    if split_local: score += 25
    if split_normal: score += 1000 
    if cross_region: score += 10
    total_left = sum(leftover_vols)
    if VOL_MIN_CABINET <= total_left <= VOL_MAX_CABINET: score -= 50 
    tiny_frags = [v for v in leftover_vols if 0 < v < 15]
    if len(tiny_frags) >= 2: score += 30 * len(tiny_frags) 
    return score

def run_sandbox_engine(inventory_dict, insp_main_wh):
    """
    inventory_dict 结构: {(区域, 库区简称): {'vol': 体积, 'wt': 重量}}
    """
    allocations = []
    cab_counter = 1
    inv = {k: copy.deepcopy(v) for k, v in inventory_dict.items() if v['vol'] > 0.01}
    
    # --- 阶段 1: 同区寻找整柜 ---
    while True:
        # 主导厂资格：过滤掉 <50方的捷鹏，且商检虚拟块不作为主动发起者(除非只剩它了)
        valid_leads = {k: v for k, v in inv.items() if not (k[1] == "捷鹏" and v['vol'] < 50) and k[1] != "商检虚拟块"}
        if not valid_leads:
            valid_leads = {k: v for k, v in inv.items()} # 如果只剩捷鹏/商检，放开限制
        if not valid_leads: break
            
        lead_key = max(valid_leads.items(), key=lambda x: x[1]['vol'])[0]
        lead_reg, lead_wh = lead_key
        lead_vol = inv[lead_key]['vol']
        lead_wt = inv[lead_key]['wt']
        
        # 确定实际显示的主导地址
        display_addr_wh = insp_main_wh.get(lead_reg, "未知") if lead_wh == "商检虚拟块" else lead_wh
        
        # 优先级1：大户直通车
        if lead_vol >= VOL_MIN_CABINET:
            take_vol = min(lead_vol, VOL_MAX_CABINET)
            ratio = take_vol / lead_vol
            take_wt = lead_wt * ratio
            
            # 如果超重，按重量反推最大体积
            if take_wt > WEIGHT_LIMIT:
                take_wt = WEIGHT_LIMIT
                take_vol = (WEIGHT_LIMIT / lead_wt) * lead_vol
                
            if take_vol >= VOL_MIN_CABINET:
                allocations.append({
                    "cab_id": f"整柜-{cab_counter:02d}", "addr": f"{display_addr_wh}装柜-{lead_reg}",
                    "remark1": f"全部在原地仓。(最终装柜:{take_vol:.2f}方, {take_wt:.0f}KG)", 
                    "remark2": "大户直通成柜", "items": {lead_key: take_vol}
                })
                inv[lead_key]['vol'] -= take_vol
                inv[lead_key]['wt'] -= take_wt
                log(f"[大户直通] {display_addr_wh}({lead_reg}) 截取 {take_vol:.1f}方, {take_wt:.0f}KG。")
                cab_counter += 1
                continue
            
        # 优先级2-4：准备穷举拼凑 (需满足体积与重量双重红线)
        gap_min, gap_max = VOL_MIN_CABINET - lead_vol, VOL_MAX_CABINET - lead_vol
        candidate_keys = [k for k in inv.keys() if k != lead_key and k[0] == lead_reg]
        possible_plans = []
        
        # 方案A & B: 1家或2家无损拼图
        for combo_size in [1, 2]:
            for combo in itertools.combinations(candidate_keys, combo_size):
                combo_vol = sum(inv[k]['vol'] for k in combo)
                combo_wt = sum(inv[k]['wt'] for k in combo)
                
                if gap_min <= combo_vol <= gap_max and (lead_wt + combo_wt) <= WEIGHT_LIMIT:
                    leftovers = [v['vol'] for k, v in inv.items() if k != lead_key and k not in combo]
                    score = score_plan(len(combo)+1, False, False, False, leftovers)
                    possible_plans.append({
                        "type": "exact", "combo": combo, "take_vols": {k: inv[k]['vol'] for k in combo}, 
                        "take_wts": combo_wt, "score": score, "desc": "无损拼图"
                    })
                    
        # 方案C: 切分本地仓
        local_keys = [k for k in candidate_keys if k[1] in LOCAL_WHS]
        for lk in local_keys:
            max_vol_by_wt = ((WEIGHT_LIMIT - lead_wt) / inv[lk]['wt']) * inv[lk]['vol'] if inv[lk]['wt'] > 0 else gap_max
            take_vol = min(inv[lk]['vol'], gap_max, max_vol_by_wt)
            if take_vol >= gap_min:
                take_wt = (take_vol / inv[lk]['vol']) * inv[lk]['wt']
                leftovers = [v['vol'] for k, v in inv.items() if k != lead_key and k != lk] + [inv[lk]['vol'] - take_vol]
                score = score_plan(2, True, False, False, leftovers)
                possible_plans.append({
                    "type": "split_local", "combo": (lk,), "take_vols": {lk: take_vol}, 
                    "take_wts": take_wt, "score": score, "desc": f"切分({lk[1]})"
                })
            
        # 方案D: 强切普通厂
        normal_keys = [k for k in candidate_keys if k[1] not in LOCAL_WHS]
        for nk in normal_keys:
            max_vol_by_wt = ((WEIGHT_LIMIT - lead_wt) / inv[nk]['wt']) * inv[nk]['vol'] if inv[nk]['wt'] > 0 else gap_max
            take_vol = min(inv[nk]['vol'], gap_max, max_vol_by_wt)
            if take_vol >= gap_min:
                take_wt = (take_vol / inv[nk]['vol']) * inv[nk]['wt']
                leftovers = [v['vol'] for k, v in inv.items() if k != lead_key and k != nk] + [inv[nk]['vol'] - take_vol]
                score = score_plan(2, False, True, False, leftovers)
                possible_plans.append({
                    "type": "split_normal", "combo": (nk,), "take_vols": {nk: take_vol}, 
                    "take_wts": take_wt, "score": score, "desc": f"强切({nk[1]})"
                })
            
        # 抉择最优拼柜方案
        if possible_plans:
            best_plan = min(possible_plans, key=lambda x: x['score'])
            alloc_items = {lead_key: lead_vol}
            alloc_items.update(best_plan["take_vols"])
            
            details = []
            total_cab_vol = lead_vol
            total_cab_wt = lead_wt + best_plan["take_wts"]
            for k, v in best_plan["take_vols"].items():
                total_cab_vol += v
                transfer_type = "全部" if v >= inv[k]['vol'] - 0.01 else "部分"
                details.append(f"【{k[1]}】{transfer_type}调往【{display_addr_wh}】{v:.2f}方")
                
            remark1_str = "；".join(details) + f"。(装柜:{total_cab_vol:.2f}方, {total_cab_wt:.0f}KG)"
            
            allocations.append({
                "cab_id": f"整柜-{cab_counter:02d}", "addr": f"{display_addr_wh}装柜-{lead_reg}",
                "remark1": remark1_str, "remark2": best_plan["desc"], "items": alloc_items
            })
            log(f"[同区拼图] {display_addr_wh} 采用【{best_plan['desc']}】(总重 {total_cab_wt:.0f}KG)。")
            
            inv[lead_key]['vol'] = 0
            for k, v in best_plan["take_vols"].items(): 
                wt_ratio = v / (inv[k]['vol'] + 0.0001)
                inv[k]['wt'] -= inv[k]['wt'] * wt_ratio
                inv[k]['vol'] -= v
            inv = {k: v for k, v in inv.items() if v['vol'] > 0.01}
            cab_counter += 1
        else:
            break

    # --- 阶段 2: 尾盘跨区财务大决战 ---
    inv = {k: v for k, v in inv.items() if v['vol'] > 0.01}
    east_vol = sum(v['vol'] for k, v in inv.items() if k[0] == "华东")
    south_vol = sum(v['vol'] for k, v in inv.items() if k[0] == "华南")
    
    cross_decision = None # 记录跨区方向
    
    if east_vol + south_vol >= VOL_MIN_CABINET and east_vol > 0 and south_vol > 0:
        log(f"\n[财务决战] 华东剩 {east_vol:.1f}方，华南剩 {south_vol:.1f}方。总计 ≥60方，启动跨区财务算盘！")
        diff_east_to_south = calculate_financial_diff(east_vol, south_vol, "华东")
        diff_south_to_east = calculate_financial_diff(south_vol, east_vol, "华南")
        
        log(f"  > 测算A (华东运深圳): 综合价差为 {diff_east_to_south:.2f} RMB")
        log(f"  > 测算B (深圳运云仓): 综合价差为 {diff_south_to_east:.2f} RMB")
        
        best_diff = min(diff_east_to_south, diff_south_to_east)
        if best_diff < 0:
            cross_decision = "East_to_South" if diff_east_to_south <= diff_south_to_east else "South_to_East"
            log(f"🏆 决策：执行跨区！(省 {-best_diff:.0f} RMB)")
            
            target_reg = "华南" if cross_decision == "East_to_South" else "华东"
            source_reg = "华东" if cross_decision == "East_to_South" else "华南"
            
            # 找出目标区的最大主导厂
            target_leads = {k:v for k,v in inv.items() if k[0] == target_reg and k[1] != "商检虚拟块"}
            if not target_leads: target_leads = {k:v for k,v in inv.items() if k[0] == target_reg}
            target_lead_key = max(target_leads.items(), key=lambda x: x[1]['vol'])[0]
            display_addr_wh = insp_main_wh.get(target_reg, "未知") if target_lead_key[1] == "商检虚拟块" else target_lead_key[1]
            
            alloc_items = {}
            total_vol, total_wt = 0.0, 0.0
            
            # 目标仓所有货装入
            for k in list(inv.keys()):
                if k[0] == target_reg:
                    alloc_items[k] = inv[k]['vol']
                    total_vol += inv[k]['vol']
                    total_wt += inv[k]['wt']
                    inv[k]['vol'] = 0
            
            # 从原产地切体积补齐 70 方
            details = []
            for k in list(inv.keys()):
                if k[0] == source_reg and total_vol < VOL_STANDARD_CROSS:
                    max_vol_by_wt = ((WEIGHT_LIMIT - total_wt) / inv[k]['wt']) * inv[k]['vol'] if inv[k]['wt'] > 0 else 999
                    need = min(inv[k]['vol'], VOL_STANDARD_CROSS - total_vol, max_vol_by_wt)
                    if need > 0.01:
                        alloc_items[k] = need
                        take_wt = (need / inv[k]['vol']) * inv[k]['wt']
                        total_vol += need
                        total_wt += take_wt
                        inv[k]['vol'] -= need
                        inv[k]['wt'] -= take_wt
                        details.append(f"【{k[1]}】跨区调往【{display_addr_wh}】{need:.2f}方")
                        
            remark1_str = "；".join(details) + f"。(跨区装柜:{total_vol:.2f}方, {total_wt:.0f}KG)"
            allocations.append({
                "cab_id": f"跨区整柜-{cab_counter:02d}", "addr": f"{display_addr_wh}装柜-{target_reg}",
                "remark1": remark1_str, "remark2": f"财务判定跨区 (省{-best_diff:.0f}元)", "items": alloc_items
            })
            cab_counter += 1

    # --- 阶段 3: 绝望散货清算 (刚性40方截断与反转) ---
    inv = {k: v for k, v in inv.items() if v['vol'] > 0.01}
    
    # 如果发生跨区，把源产地剩下的货“身份转换”，丢给目标仓作为散货处理
    if cross_decision:
        target_reg = "华南" if cross_decision == "East_to_South" else "华东"
        target_default_wh = "深圳仓" if target_reg == "华南" else "云仓"
        source_reg = "华东" if cross_decision == "East_to_South" else "华南"
        
        for k in list(inv.keys()):
            if k[0] == source_reg and inv[k]['vol'] > 0.01:
                new_k = (target_reg, f"【原{source_reg}】{k[1]}") # 标记它来自于跨区
                inv[new_k] = inv.get(new_k, {'vol':0, 'wt':0})
                inv[new_k]['vol'] += inv[k]['vol']
                inv[new_k]['wt'] += inv[k]['wt']
                inv[k]['vol'] = 0
    
    inv = {k: v for k, v in inv.items() if v['vol'] > 0.01}
    
    for region in ["华东", "华南"]:
        reg_inv = {k: v for k, v in inv.items() if k[0] == region}
        if not reg_inv: continue
        
        default_wh = "云仓" if region == "华东" else "深圳仓"
        final_addr_wh = default_wh
        is_reverse = False
        
        # 反转判定
        def_vol = sum(v['vol'] for k, v in reg_inv.items() if default_wh in k[1])
        for k, v in reg_inv.items():
            # 过滤掉跨区残留物和商检块来判定反转
            if k[1] not in LOCAL_WHS and "【原" not in k[1] and k[1] != "商检虚拟块" and v['vol'] > (def_vol + 5):
                final_addr_wh = k[1]
                is_reverse = True
                break
                
        # 40方物理截断发散货
        cab_idx = 1
        while sum(v['vol'] for v in reg_inv.values()) > 0.01:
            current_scatter_items = {}
            current_vol, current_wt = 0.0, 0.0
            
            for k, v in list(reg_inv.items()):
                if current_vol >= VOL_MAX_SCATTER: break
                take_v = min(v['vol'], VOL_MAX_SCATTER - current_vol)
                if take_v > 0.01:
                    take_wt = (take_v / v['vol']) * v['wt']
                    # 为防止散货超重(虽然极少见)
                    if current_wt + take_wt > WEIGHT_LIMIT:
                        take_wt = WEIGHT_LIMIT - current_wt
                        take_v = (take_wt / v['wt']) * v['vol']
                        
                    current_scatter_items[k] = take_v
                    current_vol += take_v
                    current_wt += take_wt
                    reg_inv[k]['vol'] -= take_v
                    reg_inv[k]['wt'] -= take_wt
                
            reg_inv = {k: v for k, v in reg_inv.items() if v['vol'] > 0.01}
            
            details = []
            for k, v in current_scatter_items.items():
                real_wh = k[1].split('】')[-1] if '】' in k[1] else k[1]
                if real_wh != final_addr_wh:
                    details.append(f"【{real_wh}】调往【{final_addr_wh}】{v:.2f}方")
                    
            remark1_str = "；".join(details) + f"。(最终散货:{current_vol:.2f}方)" if details else f"全部原地发散货。(最终散货:{current_vol:.2f}方)"
            remark2_str = f"{default_wh}反转调往-{final_addr_wh}" if is_reverse else "常规散货归集"
            addr_str = f"{final_addr_wh}{cab_idx}{cab_idx}-AMP散货-{region}"
                
            allocations.append({
                "cab_id": f"散货柜-{cab_counter:02d}", "addr": addr_str,
                "remark1": remark1_str, "remark2": remark2_str, "items": current_scatter_items 
            })
            log(f"[散货截断] {region} 截取 {current_vol:.1f}方 装入【{addr_str}】。")
            cab_idx += 1
            cab_counter += 1
            
    return allocations

# ==========================================
# 3. 前后置映射与格式保护 (三明治上下层)
# ==========================================
def apply_allocations_to_df(df, allocations, pool_name):
    """底层物理行切割 (等比分割体积与重量)"""
    out_rows = []
    records = df.to_dict('records')
    
    for alloc in allocations:
        cab_id = f"{pool_name}-{alloc['cab_id']}"
        addr = alloc['addr']
        rem1 = alloc['remark1']
        rem2 = alloc['remark2']
        
        for (reg, wh_raw), target_vol in alloc['items'].items():
            needed = round(target_vol, 2)
            
            # 解析跨区标签
            real_wh = wh_raw.split('】')[-1] if '】' in wh_raw else wh_raw
            is_inspection_block = (wh_raw == "商检虚拟块")
            
            for row in records:
                if needed <= 0.01: break
                if row.get('系统柜号') != "": continue 
                
                # 行匹配逻辑
                match = False
                if is_inspection_block:
                    if row['当前区域'] == reg and row['是否商检'] == '是': match = True
                else:
                    if row['最终库区简称'] == real_wh and row['是否商检'] != '是':
                        if row['当前区域'] == reg or "跨区" in rem1 or "原" in wh_raw: match = True
                        
                if match:
                    row_vol = float(row.get('待发货体积(CBM)', 0))
                    row_wt = float(row.get('待发货重量(KG)', 0))
                    if row_vol <= 0: continue
                    
                    if row_vol <= needed + 0.05: 
                        row['系统柜号'] = cab_id
                        row['装柜地址'] = addr
                        row['排柜备注(操作明细)'] = rem1
                        row['系统决策说明'] = rem2
                        needed -= row_vol
                    else:
                        # 物理行分裂
                        ratio = needed / row_vol
                        new_row = copy.deepcopy(row)
                        new_row['待发货体积(CBM)'] = needed
                        new_row['待发货重量(KG)'] = row_wt * ratio
                        new_row['系统柜号'] = cab_id
                        new_row['装柜地址'] = addr
                        new_row['排柜备注(操作明细)'] = "SKU切分：" + rem1
                        new_row['系统决策说明'] = rem2
                        out_rows.append(new_row)
                        
                        row['待发货体积(CBM)'] -= needed
                        row['待发货重量(KG)'] -= (row_wt * ratio)
                        needed = 0
                        
    assigned = [r for r in records if r.get('系统柜号') != ""]
    unassigned = [r for r in records if r.get('系统柜号') == ""]
    res_df = pd.DataFrame(assigned + out_rows + unassigned)
    
    # 排列顺序保护
    cols = list(res_df.columns)
    for c in ['|---系统运算结果---|', '系统柜号', '装柜地址', '排柜备注(操作明细)', '系统决策说明']:
        if c in cols: cols.remove(c)
        cols.append(c)
    return res_df[cols]

def process_full_pipeline(df, pool_name, mapping_dict):
    global process_logs
    if df.empty: return df
    
    df = df.copy()
    # 建立隔离带和新列
    df['|---系统运算结果---|'] = ""
    for col in ['系统柜号', '装柜地址', '排柜备注(操作明细)', '系统决策说明', '最终库区简称']: df[col] = ""
        
    df['待发货体积(CBM)'] = pd.to_numeric(df['待发货体积(CBM)'], errors='coerce').fillna(0)
    df['待发货重量(KG)'] = pd.to_numeric(df['待发货重量(KG)'], errors='coerce').fillna(0)
    df['是否商检'] = df.get('是否商检', '').fillna('').astype(str).str.strip()
    
    def get_short_name(name):
        if pd.isna(name): return "未知"
        # 优先用上传的映射表，没有则用系统默认
        for k, v in mapping_dict.items():
            if k in str(name): return v
        for k, v in DEFAULT_SHORT_NAME_MAP.items():
            if k in str(name): return v
        return str(name)
        
    df['最终库区简称'] = df['当前库区'].apply(get_short_name)
    
    log(f"\n======== 开始处理 {pool_name} 数据池 ========")
    inventory_dict = {}
    insp_main_wh = {}
    
    # 将正常货和商检块分别聚合
    for region in df['当前区域'].unique():
        reg_df = df[df['当前区域'] == region]
        
        # 处理商检块
        insp_df = reg_df[reg_df['是否商检'] == '是']
        if not insp_df.empty:
            main_wh = insp_df.groupby('最终库区简称')['待发货体积(CBM)'].sum().idxmax()
            insp_main_wh[region] = main_wh
            v = insp_df['待发货体积(CBM)'].sum()
            w = insp_df['待发货重量(KG)'].sum()
            inventory_dict[(region, "商检虚拟块")] = {'vol': v, 'wt': w}
            log(f"[商检软捆绑] {region} 聚合成 {v:.1f}方, {w:.0f}KG的商检模块，最大厂为 {main_wh}。")
            
        # 处理普通货
        norm_df = reg_df[reg_df['是否商检'] != '是']
        for wh, group in norm_df.groupby('最终库区简称'):
            v = group['待发货体积(CBM)'].sum()
            w = group['待发货重量(KG)'].sum()
            inventory_dict[(region, wh)] = {'vol': v, 'wt': w}
    
    final_allocations = run_sandbox_engine(inventory_dict, insp_main_wh)
    res_df = apply_allocations_to_df(df, final_allocations, pool_name)
    return res_df

# ==========================================
# 4. Streamlit 网页 UI
# ==========================================
st.title("📦 亚马逊智能排柜系统)")
st.markdown("集成 **重量红线判定**、**商检填缝软捆绑**、**跨区财务价差对决** 与 **刚性 40方物理截断**。")

uploaded_file = st.file_uploader("请上传排柜草稿 (可包含 '供应商简称映射' Sheet)", type=["xlsx"])

if uploaded_file is not None:
    try:
        xls = pd.ExcelFile(uploaded_file)
        all_sheets = pd.read_excel(xls, sheet_name=None)
        
        # 识别主数据表
        main_sheet_name = None
        for sheet_name, sheet_df in all_sheets.items():
            if '待发货体积(CBM)' in sheet_df.columns:
                main_sheet_name = sheet_name
                break
                
        if not main_sheet_name:
            st.error("❌ 未在表格中找到核心列 '待发货体积(CBM)'，请检查文件。")
            st.stop()
            
        raw_df = all_sheets[main_sheet_name]
        
        # 识别供应商映射表
        mapping_dict = {}
        if '供应商简称映射' in all_sheets:
            map_df = all_sheets['供应商简称映射']
            if len(map_df.columns) >= 2:
                mapping_dict = dict(zip(map_df.iloc[:,0].astype(str), map_df.iloc[:,1].astype(str)))
                st.info(f"✅ 成功读取外部《供应商简称映射》配置 ({len(mapping_dict)}条记录)。")
        else:
            st.warning("⚠️ 未找到名为 '供应商简称映射' 的Sheet，系统将使用内置默认映射。")
            
        if st.button("🚀 启动全局运筹演算", type="primary"):
            process_logs.clear() 
            with st.spinner('AI 正在进行重量核验与跨区财务博弈，执行物理行切割...'):
                for c in ['尺寸类型', '运输方式', '入库配置方式']:
                    if c in raw_df.columns: raw_df[c] = raw_df[c].fillna('').astype(str).str.strip()
                
                mask_s1 = raw_df['尺寸类型'].str.contains('标准') & raw_df['运输方式'].str.contains('AGL') & raw_df['入库配置方式'].isin(['AOSS', 'AMP'])
                mask_s2 = raw_df['尺寸类型'].str.contains('标准') & raw_df['运输方式'].str.contains('AGL') & (raw_df['入库配置方式'] == 'MSS')
                mask_s3 = raw_df['尺寸类型'].str.contains('标准') & (raw_df['入库配置方式'] == 'SMP')
                
                res_s1 = process_full_pipeline(raw_df[mask_s1].copy(), "AOSS+AMP", mapping_dict)
                res_s2 = process_full_pipeline(raw_df[mask_s2].copy(), "MSS", mapping_dict)
                log_df = pd.DataFrame({"沙盘运筹推演日志 (Dispatch Logs)": process_logs})
                
            st.success("🎉 排柜计算、财务审核与行级切分完美收官！")
            
            col1, col2 = st.columns([3, 1])
            with col1: st.dataframe(res_s1[['|---系统运算结果---|', '系统柜号', '装柜地址', '排柜备注(操作明细)', '系统决策说明']].head(20))
            with col2: st.dataframe(log_df, height=400)
                
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                res_s1.to_excel(writer, sheet_name="AOSS+AMP排柜", index=False)
                res_s2.to_excel(writer, sheet_name="MSS排柜", index=False)
                raw_df[mask_s3].to_excel(writer, sheet_name="SMP保留", index=False)
                raw_df[~(mask_s1 | mask_s2 | mask_s3)].to_excel(writer, sheet_name="其它隔离区", index=False)
                log_df.to_excel(writer, sheet_name="系统决策日志", index=False)
                
            st.download_button("⬇️ 下载架构终极版 (带隔离保护)", data=output.getvalue(), file_name="智能排柜_工业最终版.xlsx", type="primary")
            
    except Exception as e:
        st.error(f"❌ 运行报错: {str(e)}")
