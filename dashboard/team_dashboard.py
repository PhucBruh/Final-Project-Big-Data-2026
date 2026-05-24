import streamlit as st
import queries
import charts

def render_team_dashboard(season_range, clutch_mode, clutch_time=5, clutch_margin=5):
    st.title("🛡️ Team Systems & Evolution")
    
    all_teams = queries.get_all_teams()
    compare_teams = st.multiselect(
        "Select Teams to Analyze (Leave empty to view League Average across all teams)", 
        all_teams if all_teams else ["BOS", "LAL", "GSW"], 
        default=[]
    )
    
    st.markdown("### 📊 Evolution & Identity")
    col1, col2 = st.columns(2)
    start_season, end_season = season_range
    
    df_league = queries.get_league_evolution(start_season, end_season, clutch_mode)
    
    if compare_teams:
        df_team_compare = queries.get_team_evolution(start_season, end_season, clutch_mode, teams=compare_teams)
        df_shot_dist = queries.get_league_evolution(start_season, end_season, clutch_mode, teams=compare_teams)
    else:
        df_team_compare = None
        df_shot_dist = df_league
        
    with col1:
        st.plotly_chart(charts.plot_shot_distribution_evolution(df_shot_dist), use_container_width=True)
    
    with col2:
        st.plotly_chart(charts.plot_pace_evolution(df_league, df_team_compare), use_container_width=True)
        
    st.markdown("---")
    
    col3, col4 = st.columns(2)
    
    if all_teams:
        df_shots = queries.get_team_shot_heatmap(compare_teams, start_season, end_season, clutch_mode, clutch_time, clutch_margin)
        
        with col3:
            title_suffix = ", ".join(compare_teams) if compare_teams else "League"
            st.markdown(f"#### {title_suffix} Shot Chart")
            shot_filter = st.radio("Filter Shots:", ["All", "Made", "Missed"], horizontal=True, key="team_shots")
            df_shots_filtered = df_shots
            if shot_filter == "Made":
                df_shots_filtered = df_shots[df_shots['made_flag'] == 1]
            elif shot_filter == "Missed":
                df_shots_filtered = df_shots[df_shots['made_flag'] == 0]
                
            st.plotly_chart(charts.plot_shot_heatmap(df_shots_filtered, f"{title_suffix} Shot Chart"), use_container_width=True)
            
        with col4:
            st.markdown("#### Offensive Profile Radar")
            if compare_teams:
                st.plotly_chart(charts.plot_offensive_radar(df_team_compare), use_container_width=True)
            else:
                df_league_radar = df_league.copy()
                df_league_radar['team'] = 'League Average'
                st.plotly_chart(charts.plot_offensive_radar(df_league_radar), use_container_width=True)
            
    st.markdown("---")
    
    st.markdown("### 📈 Efficiency Comparison")
    col5, col6 = st.columns(2)
    
    df_all_teams = queries.get_team_evolution(start_season, end_season, clutch_mode, teams=compare_teams if compare_teams else None)
    
    df_avg_teams = df_all_teams.groupby('team').mean(numeric_only=True).reset_index()
    
    with col5:
        st.plotly_chart(charts.plot_ortg_drtg_scatter(df_avg_teams), use_container_width=True)
        
    with col6:
        st.markdown("#### Efficiency Metrics Table (Average)")
        st.dataframe(
            df_avg_teams, 
            use_container_width=True,
            hide_index=True,
            column_config={
                "team": st.column_config.TextColumn("Team"),
                "season": None, # Hide season if it exists
                "pace": st.column_config.NumberColumn("Pace", format="%.1f"),
                "ORtg": st.column_config.NumberColumn("ORtg", format="%.1f"),
                "DRtg": st.column_config.NumberColumn("DRtg", format="%.1f"),
                "three_point_rate": st.column_config.ProgressColumn("3PT%", format="%.2f", min_value=0, max_value=0.6),
                "rim_rate": st.column_config.ProgressColumn("Rim%", format="%.2f", min_value=0, max_value=0.6),
                "midrange_rate": st.column_config.ProgressColumn("Mid%", format="%.2f", min_value=0, max_value=0.6),
            }
        )
