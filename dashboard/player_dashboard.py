import streamlit as st
import queries
import charts

def render_player_dashboard(season_range, clutch_mode, clutch_time=5, clutch_margin=5):
    st.title("⭐ Player Evolution Analytics")
    
    start_season, end_season = season_range
    all_players = queries.get_all_players()
    selected_player = st.selectbox("Select Player", all_players if all_players else ["J. Embiid", "J. Tatum", "L. James"])
    
    if not all_players:
        st.warning("No data found. Please run the ETL pipeline first.")
        return
        
    df_player = queries.get_player_stats(selected_player, start_season, end_season, clutch_mode)
    df_league_player = queries.get_league_player_stats(start_season, end_season, clutch_mode)
    
    st.markdown("### 📊 Efficiency & Shot Profile")
    if not df_player.empty:
        latest_stats = df_player.iloc[-1]
        latest_league_stats = df_league_player.iloc[-1] if not df_league_player.empty else None
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.plotly_chart(charts.plot_efficiency_bars(latest_stats, latest_league_stats), use_container_width=True)
            
        with col2:
            df_shot_profile = queries.get_player_shot_profile(selected_player, start_season, end_season, clutch_mode, clutch_time, clutch_margin)
            st.plotly_chart(charts.plot_shot_profile_stacked_bar(df_shot_profile), use_container_width=True)
            
    else:
        st.info("No data available for this player in the selected time range.")
        
    st.markdown("---")
    
    st.markdown("### Career Evolution")
    col5, col6 = st.columns(2)
    
    with col5:
        st.plotly_chart(charts.plot_career_efficiency(df_player, df_league_player), use_container_width=True)
        
    with col6:
        df_shots = queries.get_player_shot_heatmap(selected_player, start_season, end_season, clutch_mode, clutch_time, clutch_margin)
        st.markdown("#### Player Shot Chart")
        
        shot_filter_player = st.radio("Filter Shots:", ["All", "Made", "Missed"], horizontal=True, key="player_shots")
        if shot_filter_player == "Made":
            df_shots = df_shots[df_shots['made_flag'] == 1]
        elif shot_filter_player == "Missed":
            df_shots = df_shots[df_shots['made_flag'] == 0]
            
        st.plotly_chart(charts.plot_shot_heatmap(df_shots, f"{selected_player} Shot Chart"), use_container_width=True)
