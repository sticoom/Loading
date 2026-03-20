# ================= 网页 UI 界面 =================
st.set_page_config(page_title="亚马逊智能排柜系统", page_icon="📦", layout="wide")

st.title("📦 智能排柜系统 (Beta)")
st.write("根据 SOP 自动计算 AOSS/AMP 拼柜逻辑，生成系统柜号与装柜地址。")

# 【修复关键 1】初始化一个“记忆库”，用来保存处理后的数据
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

uploaded_file = st.file_uploader("请上传《排柜草稿》Excel 文件", type=["xlsx", "csv"])

if uploaded_file is not None:
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
            
        st.success(f"✅ 文件读取成功！共 {len(df)} 行数据。")
        
        # 【修复关键 2】按钮点击后，把结果存入记忆库，而不是“阅后即焚”
        if st.button("🚀 开始智能排柜", type="primary"):
            with st.spinner('正在执行 3D 贪心装箱算法与跨区调拨测算...'):
                
                # 强健性优化：确保体积列是纯数字，防止 Excel 里混入文本导致计算静默崩溃
                if '待发货体积(CBM)' in df.columns:
                    df['待发货体积(CBM)'] = pd.to_numeric(df['待发货体积(CBM)'], errors='coerce').fillna(0)
                    
                # 运行核心算法
                result_df = process_shipping_data(df)
                st.session_state.processed_data = result_df  # 存入记忆库
        
        # 【修复关键 3】只要记忆库里有数据，就一直显示预览和下载按钮，无论怎么刷新都不会丢
        if st.session_state.processed_data is not None:
            st.success("🎉 排柜计算完成！请预览数据并下载。")
            
            # 数据预览 (只显示关键列)
            show_cols = ['单据编号', 'SKU', '待发货体积(CBM)', '系统分配柜号', '当前库区', '最终库区简称', '装柜地址', '排柜备注']
            # 过滤出实际存在的列，防止用户的草稿有列名差异导致报错
            exist_cols = [c for c in show_cols if c in st.session_state.processed_data.columns]
            st.dataframe(st.session_state.processed_data[exist_cols].head(20))
            
            # 转换为 CSV 供下载
            csv = st.session_state.processed_data.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="⬇️ 下载排柜结果",
                data=csv,
                file_name="排柜结果_已完成.csv",
                mime="text/csv",
            )
            
    except Exception as e:
        st.error(f"❌ 读取或处理文件时出错: {e}")
        st.info("请确保上传的文件格式与标准模板一致，且包含关键列名。")
