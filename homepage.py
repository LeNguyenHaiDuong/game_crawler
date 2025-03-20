import streamlit as st
import pandas as pd

st.set_page_config(
    page_title = 'Giới thiệu',
    layout = 'wide',

)


st.sidebar.markdown("Giới thiệu")

### Title

st.markdown(
    """
    <style>
    .title {
        text-align: center;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown(f'<h1 class="title">PROJECT LUYỆN TẬP LÀM DASHBOARD</h1>', unsafe_allow_html=True)

st.markdown("## Thông tin về bộ dữ liệu")
st.markdown("- Bộ dữ liệu này được thu thập từ VGChartz, một trang web chuyên cung cấp thông tin chi tiết về doanh số bán hàng của trò chơi điện tử trên nhiều nền tảng khác nhau. VGChartz cung cấp dữ liệu tổng hợp về số lượng bán ra, xếp hạng game, thông tin phát hành, đánh giá từ người dùng và nhiều yếu tố khác liên quan đến ngành công nghiệp game.")
st.markdown("- Nguồn: https://www.vgchartz.com")
st.markdown("## Ý nghĩa của từng cột")

data = pd.read_csv(r"data/vgsales_final.csv")
st.dataframe(data)
