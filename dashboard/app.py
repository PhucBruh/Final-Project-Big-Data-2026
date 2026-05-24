import streamlit as st
import streamlit_antd_components as sac
from team_dashboard import render_team_dashboard
from player_dashboard import render_player_dashboard
import queries

st.set_page_config(
    page_title="NBA Big Data Analytics",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for premium design
st.markdown("""
<style>
    .main {
        background-color: #0E1117;
        color: #FAFAFA;
        font-family: 'Inter', sans-serif;
    }
    h1, h2, h3 {
        color: #FFFFFF;
        font-weight: 700;
    }
    .stSelectbox label, .stSlider label {
        color: #A0AEC0;
    }
    .kpi-card {
        background-color: #1A202C;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #00BFFF;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }
    .kpi-title {
        color: #A0AEC0;
        font-size: 14px;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .kpi-value {
        color: #FFFFFF;
        font-size: 28px;
        font-weight: 800;
    }
</style>
""", unsafe_allow_html=True)

def main():
    st.sidebar.title("🏀 NBA Analytics Platform")
    
    # Global Sidebar Controls
    st.sidebar.markdown("### Controls")
    
    # Era Quick Filters
    era = st.sidebar.selectbox("Era Quick Filters", [
        "All Time", "Late 90s (1997-1999)", "2000s (2000-2009)", 
        "Pace & Space (2010-2015)", "Modern NBA (2016-2026)"
    ])
    
    if era == "Late 90s (1997-1999)":
        default_range = (1997, 1999)
    elif era == "2000s (2000-2009)":
        default_range = (2000, 2009)
    elif era == "Pace & Space (2010-2015)":
        default_range = (2010, 2015)
    elif era == "Modern NBA (2016-2026)":
        default_range = (2016, 2026)
    else:
        default_range = (1997, 2026)
    
    season_range = st.sidebar.slider("Season Range", 1997, 2026, default_range)
    
    clutch_mode = st.sidebar.toggle("Clutch Mode", value=False)
    if clutch_mode:
        st.sidebar.markdown("#### Advanced Clutch Filters")
        st.sidebar.markdown("<small style='color:gray;'>*Applies to Shot Charts & Profiles</small>", unsafe_allow_html=True)
        clutch_time = st.sidebar.slider("Time Remaining (mins)", 1, 12, 5)
        clutch_margin = st.sidebar.slider("Score Margin (<=)", 1, 15, 5)
    else:
        clutch_time = 5
        clutch_margin = 5
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### System Performance")
    st.sidebar.markdown("""
    <div style='font-size: 0.85em; color: #A0AEC0; background-color: #1A202C; padding: 10px; border-radius: 5px; border-left: 3px solid #00E676;'>
    <b>Data Scale:</b> 1997-2026 (~15M+ Events)<br>
    <b>ETL Engine:</b> Apache Spark (Distributed)<br>
    <b>ETL Time:</b> ~4 mins<br>
    <b>OLAP Engine:</b> DuckDB (In-Memory)<br>
    <b>Query Latency:</b> < 150ms
    </div>
    """, unsafe_allow_html=True)
    
    # Modern Top Navigation using Streamlit Antd Components
    page = sac.segmented(
        items=[
            sac.SegmentedItem(label='Team Analytics', icon='shield-fill'),
            sac.SegmentedItem(label='Player Analytics', icon='person-fill'),
        ],
        align='center', 
        size='md', 
        color='blue',
        use_container_width=True
    )
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    with st.spinner('Fetching Big Data...'):
        if page == "Team Analytics":
            render_team_dashboard(season_range, clutch_mode, clutch_time, clutch_margin)
        else:
            render_player_dashboard(season_range, clutch_mode, clutch_time, clutch_margin)

if __name__ == "__main__":
    main()
