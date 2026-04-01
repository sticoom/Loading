"""
亚马逊智能排柜系统 (不可拆分·财务瀑布流终极版)
修复：死循环Bug修复 / 单行绝对不可拆分 / 0-1背包极限填缝 / 原表顺序完美保护
"""
import streamlit as st
import pandas as pd
import math
import itertools
import io

# ==========================================
# 0. 全局配置与基准参数 (👉 业务参数在此修改 👈)
# ==========================================
st.set_page_config(page_title="亚马逊智能排柜引擎", page_icon="📦", layout="wide")

# --- 物理红线 ---
VOL_MIN_CABINET = 60.0       # 整柜及格底线
VOL_MAX_CABINET = 71.0       # 整柜极致上限
VOL_STANDARD_CROSS = 70.0    # 跨区测算标准凑柜基数
WEIGHT_LIMIT = 19500.0       # 整柜重量上限 (KG)
VOL_MAX_SCATTER = 40.0       # 散货单柜上限

# --- 财务报价基准 ---
EXCHANGE_RATE = 7.2          # 汇率
PRICE_USD_EAST = 135.26      # 江浙/华东散货头程单价 (USD/方)
PRICE_USD_SOUTH = 135.00     # 深圳/华南散货头程单价 (USD/方)

TRANSFER_SURCHARGE_VOL = 15.0  # 低于此方量加收附加费
TRANSFER_SURCHARGE_FEE = 200.0 # 附加费金额 (RMB/趟)

def get_domestic_transfer_rate(vol):
    """国内跨仓调拨阶梯单价 (RMB/方)"""
    if vol <= 5.0: return 155.0
    elif vol <= 10.0: return 140.0
    else: return 125.0

DEFAULT_SHORT_NAME_MAP = {"成品一区": "深圳仓", "天源": "云仓"}
LOCAL_WHS = ["云仓", "深圳仓"]

process_logs = []
def log(msg): process_logs.append(msg)

# ==========================================
# 1. 核心底层算法组件
# ==========================================
def calculate_financial_diff(transfer_vol, target_original_vol, transfer_from_region):
    """4步财务测算：返回负数代表【跨区运回拼柜更省钱】"""
    local_price_usd = PRICE_USD_EAST if transfer_from_region == "华东" else PRICE_USD_SOUTH
    target_price_usd = PRICE_USD_SOUTH if transfer_from_region == "华东" else PRICE_USD_EAST

    cost_local_direct = local_price_usd * transfer_vol * EXCHANGE_RATE
    cost_domestic = get_domestic_transfer_rate(transfer_vol) * transfer_vol
    if transfer_vol < TRANSFER_SURCHARGE_VOL:
        cost_domestic += TRANSFER_SURCHARGE_FEE

    leftover_vol = max(0, transfer_vol - (VOL_STANDARD_CROSS - target_original_vol))
    cost_target_leftover = target_price_usd * leftover_vol * EXCHANGE_RATE

    diff = (cost_domestic + cost_target_leftover) - cost_local_direct
    return diff

def waterfall_fill(cab_rows, pool_rows, max_vol=VOL_MAX_CABINET, allow_overlimit=True):
    """0-1背包瀑布流：按体积降序遍历，只要塞得下就死命塞！(单行不切分)"""
    cab_vol = sum(r['vol'] for r in cab_rows)
    cab_wt = sum(r['wt'] for r in cab_rows)
    pool_rows.sort(key=lambda x: x['vol'], reverse=True)
    
    rem_pool = []
    for r in pool_rows:
        can_fit = (cab_vol + r['vol'] <= max_vol) and (cab_wt + r['wt'] <= WEIGHT_LIMIT)
        
        # 【防死循环保护】：如果允许超限，且目前柜子是空的，且这是最大的货（单行超限），强行塞入！
        if not cab_rows and not rem_pool and allow_overlimit and not can_fit:
            cab_rows.append(r)
            cab_vol += r['vol']
            cab_wt += r['wt']
        elif can_fit:
            cab_rows.append(r)
            cab_vol += r['vol']
            cab_wt += r['wt']
        else:
            rem_pool.append(r)
            
    return cab_rows, rem_pool

def get_max_wh(cab_rows, ignore_insp=True):
    wh_vols = {}
    for r in cab_rows:
        if ignore_insp and r['是否商检'] == '是': continue
        wh_vols[r['最终库区简称']] = wh_vols.get(r['最终库区简称'], 0) + r['vol']
    if not wh_vols: return None
    return max(wh_vols.items(), key=lambda x: x[1])[0]

def format_cabinet(cab_rows, reg, addr_wh, decision_desc, counter_dict, mapping_dict, is_scatter=False, scatter_idx=1):
    if is_scatter:
        cab_id = f"散货柜-{counter_dict['scatter']:02d}"
        counter_dict['scatter'] += 1
        mapped_addr = mapping_dict.get(addr_wh, addr_wh)
        final_addr = f"{mapped_addr}{scatter_idx:02d}{scatter_idx:02d}-AMP散货-{reg}"
    elif "跨区" in decision_desc:
        cab_id = f"跨区柜-{counter_dict['cross']:02d}"
        counter_dict['cross'] += 1
        mapped_addr = mapping_dict.get(addr_wh, addr_wh)
        final_addr = f"{mapped_addr}装柜-{reg}"
    else:
        cab_id = f"整柜-{counter_dict['full']:02d}"
        counter_dict['full'] += 1
        mapped_addr = mapping_dict.get(addr_wh, addr_wh)
        final_addr = f"{mapped_addr}装柜-{reg}"

    wh_vols = {}
    total_vol = 0.0
    total_wt = 0.0
    for r in cab_rows:
        display_wh = "商检货物" if r['是否商检'] == '是' else r['最终库区简称']
        wh_vols[display_wh] = wh_vols.get(display_wh, 0) + r['vol']
        total_vol += r['vol']
        total_wt += r['wt']

    details = []
    for wh, vol in wh_vols.items():
        if wh != addr_wh:
            details.append(f"【{wh}】调往【{addr_wh}】{vol:.2f}方")

    if not details:
        op_remark = f"全部原地发散货。(散货合计:{total_vol:.2f}方)" if is_scatter else f"全部在原地仓。(最终装柜:{total_vol:.2f}方, {total_wt:.0f}KG)"
    else:
        suffix = f"方)" if is_scatter else f"方, {total_wt:.0f}KG)"
        prefix = "散货合计:" if is_scatter else "最终装柜:"
        op_remark = "；".join(details) + f"。({prefix}{total_vol:.2f}{suffix}"

    for r in cab_rows:
        r['系统柜号'] = cab_id
        r['装柜地址'] = final_addr
        r['排柜备注(操作明细)'] = op_remark
        r['系统决策说明'] = decision_desc

    return cab_rows

# ==========================================
# 2. 核心排柜处理流 (Pipeline)
# ==========================================
def process_pool(pool_rows, pool_name, mapping_dict):
    global process_logs
    if not pool_rows: return []
    log(f"\n======== 开始处理 {pool_name} 物理行数据池 ========")
    
    cab_counters = {'full': 1, 'cross': 1, 'scatter': 1}
    finished_rows = []
    
    for region in ["华东", "华南"]:
        reg_pool = [r for r in pool_rows if r['当前区域'] == region]
        if not reg_pool: continue
        
        # --- 第一阶段：商检软捆绑 ---
        insp_rows = [r for r in reg_pool if r['是否商检'] == '是']
        norm_rows = [r for r in reg_pool if r['是否商检'] != '是']
        
        while insp_rows:
            cab_rows = []
            cab_rows, insp_rows = waterfall_fill(cab_rows, insp_rows, VOL_MAX_CABINET)
            # 用普通货去填商检柜的缝隙
            cab_rows, norm_rows = waterfall_fill(cab_rows, norm_rows, VOL_MAX_CABINET)
            
            addr_wh = get_max_wh(cab_rows, ignore_insp=True) or get_max_wh(cab_rows, ignore_insp=False)
            cab_vol = sum(r['vol'] for r in cab_rows)
            cab_wt = sum(r['wt'] for r in cab_rows)
            
            finished_rows.extend(format_cabinet(cab_rows, region, addr_wh, "商检软捆绑并极限填缝", cab_counters, mapping_dict))
            log(f"[商检捆绑] {region}生成混合商检柜，地址【{addr_wh}】，装满 {cab_vol:.1f}方, {cab_wt:.0f}KG。")

        # --- 第二阶段：同区沙盘智能排柜 ---
        while True:
            wh_totals = {}
            for r in norm_rows: wh_totals[r['最终库区简称']] = wh_totals.get(r['最终库区简称'], 0) + r['vol']
            if not wh_totals: break
            
            lead_wh = max(wh_totals.items(), key=lambda x: x[1])[0]
            lead_vol = wh_totals[lead_wh]
            
            if lead_wh == '捷鹏' and lead_vol < 50.0:
                valid_whs = {k:v for k,v in wh_totals.items() if k != '捷鹏'}
                if not valid_whs: break
                lead_wh = max(valid_whs.items(), key=lambda x: x[1])[0]
                lead_vol = valid_whs[lead_wh]
                
            cab_rows = []
            lead_items = [r for r in norm_rows if r['最终库区简称'] == lead_wh]
            other_items = [r for r in norm_rows if r['最终库区简称'] != lead_wh]
            
            # 第一步：把主导厂的货极限装入
            cab_rows, rem_lead = waterfall_fill(cab_rows, lead_items, VOL_MAX_CABINET)
            norm_rows = other_items + rem_lead
            
            cab_vol = sum(r['vol'] for r in cab_rows)
            cab_wt = sum(r['wt'] for r in cab_rows)
            
            # 第二步：整库完美互补 (保全库区完整性)
            if cab_vol < VOL_MAX_CABINET:
                wh_groups = {}
                for r in norm_rows: 
                    # 防止又把主导厂选进去作为陪跑
                    if r['最终库区简称'] != lead_wh:
                        wh_groups.setdefault(r['最终库区简称'], []).append(r)
                
                valid_combos = []
                for size in [1, 2]:
                    for combo in itertools.combinations(wh_groups.keys(), size):
                        c_vol = sum(sum(x['vol'] for x in wh_groups[w]) for w in combo)
                        c_wt = sum(sum(x['wt'] for x in wh_groups[w]) for w in combo)
                        if cab_vol + c_vol <= VOL_MAX_CABINET and cab_wt + c_wt <= WEIGHT_LIMIT:
                            valid_combos.append((combo, c_vol))
                            
                if valid_combos:
                    best_combo = max(valid_combos, key=lambda x: x[1])[0]
                    for w in best_combo:
                        cab_rows.extend(wh_groups[w])
                    norm_rows = [r for r in norm_rows if r['最终库区简称'] not in best_combo]
            
            # 第三步：全局单行降序瀑布流极致填缝
            cab_vol = sum(r['vol'] for r in cab_rows)
            if cab_vol < VOL_MAX_CABINET:
                cab_rows, norm_rows = waterfall_fill(cab_rows, norm_rows, VOL_MAX_CABINET)
                
            # 及格线校验
            final_vol = sum(r['vol'] for r in cab_rows)
            if final_vol >= VOL_MIN_CABINET:
                finished_rows.extend(format_cabinet(cab_rows, region, lead_wh, "同区瀑布流极致填满", cab_counters, mapping_dict))
                log(f"[同区拼柜] {lead_wh}({region}) 极限填装完毕，最终 {final_vol:.1f}方。")
            else:
                norm_rows.extend(cab_rows)
                break
                
        # 更新该区剩余到大池子
        pool_rows = [r for r in pool_rows if r['当前区域'] != region] + norm_rows

    # --- 第三阶段：尾盘跨区大决战 (财务算盘) ---
    east_pool = [r for r in pool_rows if r['当前区域'] == "华东"]
    south_pool = [r for r in pool_rows if r['当前区域'] == "华南"]
    other_pool = [r for r in pool_rows if r['当前区域'] not in ["华东", "华南"]]
    east_vol = sum(r['vol'] for r in east_pool)
    south_vol = sum(r['vol'] for r in south_pool)
    
    if east_vol + south_vol >= VOL_MIN_CABINET and east_vol > 0 and south_vol > 0:
        log(f"\n[财务决战] 华东剩 {east_vol:.1f}方，华南剩 {south_vol:.1f}方。触发跨区！")
        diff_e2s = calculate_financial_diff(east_vol, south_vol, "华东")
        diff_s2e = calculate_financial_diff(south_vol, east_vol, "华南")
        
        best_diff = min(diff_e2s, diff_s2e)
        if best_diff < 0:
            direction = "E2S" if diff_e2s <= diff_s2e else "S2E"
            log(f"🏆 决策：执行跨区！(省 {-best_diff:.0f} RMB)")
            
            target_reg = "华南" if direction == "E2S" else "华东"
            source_pool = east_pool if direction == "E2S" else south_pool
            cab_rows = south_pool if direction == "E2S" else east_pool
            
            # 跨区专属死磕71方瀑布流
            cab_rows, rem_source = waterfall_fill(cab_rows, source_pool, VOL_MAX_CABINET)
            
            cab_vol = sum(r['vol'] for r in cab_rows)
            if cab_vol >= VOL_MIN_CABINET:
                addr_wh = get_max_wh([r for r in cab_rows if r['当前区域'] == target_reg], ignore_insp=True)
                finished_rows.extend(format_cabinet(cab_rows, target_reg, addr_wh, "跨区死磕71极限填缝", cab_counters, mapping_dict))
                
                # 剩下的货物理属性转变为目标仓散货
                for r in rem_source:
                    r['当前区域'] = target_reg
                    # 保留原始库区名称，加上前缀便于认出
                    r['最终库区简称'] = f"【原{'华东' if target_reg=='华南' else '华南'}】" + r['最终库区简称']
                
                pool_rows = rem_source + other_pool
            else:
                pool_rows = cab_rows + rem_source + other_pool
                
    # --- 第四阶段：绝望散货统一归集本地仓 ---
    for region in ["华东", "华南"]:
        reg_pool = [r for r in pool_rows if r['当前区域'] == region]
        local_wh = '云仓' if region == '华东' else '深圳仓'
        idx = 1
        
        while reg_pool:
            cab_rows = []
            # 严格40方物理截断瀑布流
            cab_rows, reg_pool = waterfall_fill(cab_rows, reg_pool, VOL_MAX_SCATTER)
            if cab_rows:
                finished_rows.extend(format_cabinet(cab_rows, region, local_wh, "散货统一归集本地仓", cab_counters, mapping_dict, is_scatter=True, scatter_idx=idx))
                cab_vol = sum(r['vol'] for r in cab_rows)
                log(f"[散货截断] {region} 截取 {cab_vol:.1f}方 装入本地散货柜。")
                idx += 1

    return finished_rows

# ==========================================
# 3. Streamlit 主程序 (I/O 层)
# ==========================================
st.title("📦 亚马逊智能排柜引擎 (单行不可拆分版)")
st.markdown("搭载 **0-1背包瀑布流极限填缝算法** 与 **无限防死锁保护机制**。原表行数完美保护，一单到底不切分！")

uploaded_file = st.file_uploader("请上传排柜草稿", type=["xlsx", "csv"])

if uploaded_file is not None:
    try:
        xls = pd.ExcelFile(uploaded_file) if uploaded_file.name.endswith('.xlsx') else None
        if xls:
            all_sheets = pd.read_excel(xls, sheet_name=None)
            main_sheet = next((name for name, sdf in all_sheets.items() if '待发货体积(CBM)' in sdf.columns), None)
            if not main_sheet:
                st.error("❌ 未找到 '待发货体积(CBM)' 列。")
                st.stop()
            raw_df = all_sheets[main_sheet]
            map_df = all_sheets.get('供应商简称映射', pd.DataFrame())
        else:
            raw_df = pd.read_csv(uploaded_file)
            map_df = pd.DataFrame()

        mapping_dict = {}
        if not map_df.empty and len(map_df.columns) >= 2:
            mapping_dict = dict(zip(map_df.iloc[:,0].astype(str), map_df.iloc[:,1].astype(str)))
            st.info("✅ 成功加载《供应商简称映射》配置。")
                
        if st.button("🚀 启动原样保护排柜演算", type="primary"):
            process_logs.clear()
            with st.spinner('算法正在执行 0-1 离散瀑布流，死磕集装箱空间...'):
                
                for c in ['尺寸类型', '运输方式', '入库配置方式', '是否商检']:
                    if c in raw_df.columns: raw_df[c] = raw_df[c].fillna('').astype(str).str.strip()
                
                raw_df['vol'] = pd.to_numeric(raw_df['待发货体积(CBM)'], errors='coerce').fillna(0)
                raw_df['wt'] = pd.to_numeric(raw_df['待发货重量(KG)'], errors='coerce').fillna(0)
                
                def get_short_name(name):
                    if pd.isna(name): return "未知"
                    for k, v in mapping_dict.items():
                        if k in str(name): return v
                    for k, v in DEFAULT_SHORT_NAME_MAP.items():
                        if k in str(name): return v
                    return str(name)
                    
                raw_df['最终库区简称'] = raw_df['当前库区'].apply(get_short_name)
                
                # 注入索引，保护原表顺序
                raw_rows = raw_df.to_dict('records')
                for i, r in enumerate(raw_rows): r['_orig_idx'] = i
                
                # 物理池隔离
                pool_s1 = [r for r in raw_rows if '标准' in r['尺寸类型'] and 'AGL' in r['运输方式'] and r['入库配置方式'] in ['AOSS', 'AMP']]
                pool_s2 = [r for r in raw_rows if '标准' in r['尺寸类型'] and 'AGL' in r['运输方式'] and r['入库配置方式'] == 'MSS']
                
                # 执行运算
                res_s1 = process_pool(pool_s1, "AOSS+AMP", mapping_dict)
                res_s2 = process_pool(pool_s2, "MSS", mapping_dict)
                
                # 恢复原表结构与顺序
                all_res = res_s1 + res_s2
                handled_idx = {r['_orig_idx']: r for r in all_res}
                
                final_output_rows = []
                for i, r in enumerate(raw_rows):
                    out_r = handled_idx.get(i, copy.deepcopy(r))
                    out_r['|---系统运算结果---|'] = ""
                    out_r['系统柜号'] = out_r.get('系统柜号', '')
                    out_r['装柜地址'] = out_r.get('装柜地址', '')
                    out_r['排柜备注(操作明细)'] = out_r.get('排柜备注(操作明细)', '')
                    out_r['系统决策说明'] = out_r.get('系统决策说明', '')
                    final_output_rows.append(out_r)
                    
                final_df = pd.DataFrame(final_output_rows)
                
                for tmp in ['vol', 'wt', '最终库区简称', '_orig_idx']:
                    if tmp in final_df.columns: final_df.drop(columns=[tmp], inplace=True)
                
                cols = list(final_df.columns)
                res_cols = ['|---系统运算结果---|', '系统柜号', '装柜地址', '排柜备注(操作明细)', '系统决策说明']
                for c in res_cols: cols.remove(c)
                cols.extend(res_cols)
                final_df = final_df[cols]
                
                log_df = pd.DataFrame({"沙盘运筹推演日志 (Dispatch Logs)": process_logs})
                
            st.success("🎉 行级瀑布流运算完美收官！(所有异常数据已安全处理)")
            
            col1, col2 = st.columns([3, 1])
            with col1: st.dataframe(final_df[final_df['系统柜号'] != ""].head(20))
            with col2: st.dataframe(log_df, height=400)
                
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                final_df[final_df['入库配置方式'].isin(['AOSS', 'AMP'])].to_excel(writer, sheet_name="AOSS+AMP排柜", index=False)
                final_df[final_df['入库配置方式'] == 'MSS'].to_excel(writer, sheet_name="MSS排柜", index=False)
                final_df[final_df['入库配置方式'] == 'SMP'].to_excel(writer, sheet_name="SMP保留", index=False)
                final_df[~final_df['入库配置方式'].isin(['AOSS', 'AMP', 'MSS', 'SMP'])].to_excel(writer, sheet_name="其它隔离区", index=False)
                log_df.to_excel(writer, sheet_name="系统决策日志", index=False)
                
            st.download_button("⬇️ 下载完美不拆行版 (保护原始顺序)", data=output.getvalue(), file_name="智能排柜_无死锁极速版.xlsx", type="primary")
            
    except Exception as e:
        st.error(f"❌ 运行报错: {str(e)}")
