import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
def plot_shot_distribution_evolution(df):
    # df should have season, rim_rate, midrange_rate, three_point_rate (we can split 3pt into corner and above break if available, or just use these 3)
    # The requirement says: rim attempts, midrange attempts, corner 3, above break 3. We didn't split 3PT rate into corner/above break in fact_team_metrics.
    # For now, let's just plot what we have.
    df_melted = df.melt(id_vars=["season"], value_vars=["rim_rate", "midrange_rate", "three_point_rate"], var_name="Shot Zone", value_name="Rate")
    
    fig = px.area(df_melted, x="season", y="Rate", color="Shot Zone", 
                  title="Shot Distribution Evolution",
                  labels={"Rate": "Frequency", "season": "Season"},
                  template="plotly_dark",
                  color_discrete_sequence=["#FF4B4B", "#FFA500", "#00BFFF"])
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), hovermode="x unified")
    return fig

def plot_pace_evolution(df_league, df_team=None):
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(x=df_league['season'], y=df_league['pace'], mode='lines+markers', name='League Average', line=dict(color='gray', dash='dash')))
    
    if df_team is not None and not df_team.empty:
        if 'team' in df_team.columns and len(df_team['team'].unique()) > 1:
            for team in df_team['team'].unique():
                team_data = df_team[df_team['team'] == team]
                fig.add_trace(go.Scatter(x=team_data['season'], y=team_data['pace'], mode='lines+markers', name=team))
        else:
            fig.add_trace(go.Scatter(x=df_team['season'], y=df_team['pace'], mode='lines+markers', name='Team Pace', line=dict(color='#00BFFF', width=3)))
            
    fig.update_layout(title="Pace Evolution", xaxis_title="Season", yaxis_title="Possessions per 48 min", template="plotly_dark", hovermode="x unified")
    return fig

def plot_shot_heatmap(df_shots, title="Shot Chart"):
    if df_shots.empty:
        return px.scatter(title="No data available")
        
    # Dùng Scatter Plot kinh điển, nhưng áp dụng opacity blending để tự động hiển thị density
    # Nếu có quá nhiều điểm ném, ta lấy mẫu để tránh bị một cục màu kịt lại
    if len(df_shots) > 2500:
        df_shots = df_shots.sample(n=2500, random_state=42)
        title += " (2500 điểm gần nhất)"
        
    df_shots['Result'] = df_shots['made_flag'].apply(lambda x: 'Made' if x == 1 else 'Missed')
    
    # Đánh weight: Tính mật độ cục bộ (local density) để chỉnh độ lớn chấm
    # Chia mặt sân thành lưới 25x25 để đo mật độ
    x_coords = df_shots['x'].values
    y_coords = df_shots['y'].values
    heatmap, xedges, yedges = np.histogram2d(x_coords, y_coords, bins=25)
    
    # Tìm ô lưới tương ứng cho từng điểm ném
    x_idx = np.clip(np.digitize(x_coords, xedges) - 1, 0, len(xedges)-2)
    y_idx = np.clip(np.digitize(y_coords, yedges) - 1, 0, len(yedges)-2)
    
    df_shots['Density'] = heatmap[x_idx, y_idx]
    
    fig = px.scatter(df_shots, x="x", y="y", color="Result", title=title, 
                     size="Density", size_max=12,
                     template="plotly_dark", opacity=0.7,
                     color_discrete_map={"Made": "#00FF7F", "Missed": "#FF4B4B"})
                     
    fig.update_traces(marker=dict(line=dict(width=0)))
    
    # NBA Court Shapes
    court_color = 'rgba(255, 255, 255, 0.4)'
    court_shapes = [
        dict(type="rect", x0=-250, y0=-47.5, x1=250, y1=422.5, line=dict(color=court_color, width=2), layer='below'),
        dict(type="circle", x0=-7.5, y0=-7.5, x1=7.5, y1=7.5, line=dict(color='orange', width=2), layer='below'),
        dict(type="line", x0=-30, y0=-7.5, x1=30, y1=-7.5, line=dict(color=court_color, width=2), layer='below'),
        dict(type="rect", x0=-80, y0=-47.5, x1=80, y1=143.5, line=dict(color=court_color, width=2), layer='below'),
        dict(type="rect", x0=-60, y0=-47.5, x1=60, y1=143.5, line=dict(color=court_color, width=2), layer='below'),
        dict(type="path", path="M -60 143.5 A 60 60 0 0 1 60 143.5", line=dict(color=court_color, width=2), layer='below'),
        dict(type="path", path="M -60 143.5 A 60 60 0 0 0 60 143.5", line=dict(color=court_color, width=2, dash='dash'), layer='below'),
        dict(type="path", path="M -40 -7.5 A 40 40 0 0 1 40 -7.5", line=dict(color=court_color, width=2), layer='below'),
        dict(type="line", x0=-220, y0=-47.5, x1=-220, y1=140, line=dict(color=court_color, width=2), layer='below'),
        dict(type="line", x0=220, y0=-47.5, x1=220, y1=140, line=dict(color=court_color, width=2), layer='below'),
        dict(type="path", path="M -220 140 A 237.5 237.5 0 0 1 220 140", line=dict(color=court_color, width=2), layer='below')
    ]
    
    fig.update_layout(
        shapes=court_shapes,
        xaxis=dict(range=[-250, 250], showgrid=False, zeroline=False, visible=False),
        yaxis=dict(range=[-50, 422.5], showgrid=False, zeroline=False, visible=False),
        plot_bgcolor="#0E1117", paper_bgcolor="#0E1117", margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig

def plot_offensive_radar(df):
    if df.empty:
        return go.Figure()
    
    metrics = ['pace', 'ORtg', 'three_point_rate', 'rim_rate', 'midrange_rate']
    max_values = [110, 120, 0.6, 0.6, 0.6]
    
    fig = go.Figure()
    
    if 'team' in df.columns:
        df_avg = df.groupby('team')[metrics].mean(numeric_only=True).reset_index()
    else:
        df_avg = pd.DataFrame([df[metrics].mean(numeric_only=True)])
        df_avg['team'] = 'Average'
        
    for _, row in df_avg.iterrows():
        values = row[metrics].tolist()
        normalized_values = [min(1.0, max(0.0, v/m)) for v, m in zip(values, max_values)]
        normalized_values.append(normalized_values[0])
        theta = ['Pace', 'ORtg', '3PT Rate', 'Rim Rate', 'Midrange Rate', 'Pace']
        
        fig.add_trace(go.Scatterpolar(
            r=normalized_values,
            theta=theta,
            fill='toself',
            name=row['team'],
            opacity=0.5
        ))
        
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=False, range=[0, 1])),
        showlegend=True,
        template="plotly_dark",
        title="Offensive Profile Radar"
    )
    return fig

def plot_ortg_drtg_scatter(df):
    if df.empty:
        return go.Figure()
    
    fig = px.scatter(df, x="ORtg", y="DRtg", color="team", hover_name="team",
                     title="ORtg vs DRtg Scatter", template="plotly_dark",
                     size="pace", # Using pace as size proxy
                     color_discrete_sequence=px.colors.qualitative.Plotly)
    
    # Invert Y axis because lower DRtg is better
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    return fig

def plot_career_efficiency(df, df_league=None):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['season'], y=df['TS_pct'], mode='lines+markers', name='TS%', line=dict(color='#00E676')))
    fig.add_trace(go.Scatter(x=df['season'], y=df['eFG_pct'], mode='lines+markers', name='eFG%', line=dict(color='#00BFFF')))
    
    if df_league is not None and not df_league.empty:
        fig.add_trace(go.Scatter(x=df_league['season'], y=df_league['TS_pct'], mode='lines', name='League TS%', line=dict(color='gray', dash='dash')))
        fig.add_trace(go.Scatter(x=df_league['season'], y=df_league['eFG_pct'], mode='lines', name='League eFG%', line=dict(color='darkgray', dash='dot')))
        
    fig.update_layout(title="Career Efficiency Curves", template="plotly_dark", hovermode="x unified")
    return fig

def plot_shot_profile_stacked_bar(df):
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df['season'], y=df['rim_rate'], name='Rim', marker_color='#00E676'))
    fig.add_trace(go.Bar(x=df['season'], y=df['midrange_rate'], name='Midrange', marker_color='#FFA500'))
    fig.add_trace(go.Bar(x=df['season'], y=df['three_point_rate'], name='3PT', marker_color='#FF4B4B'))
    
    fig.update_layout(barmode='stack', title="Shot Profile Evolution",
                      template="plotly_dark", hovermode="x unified",
                      yaxis=dict(tickformat=".0%"), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    return fig

def plot_efficiency_bars(latest_stats, latest_league_stats=None):
    fig = go.Figure()
    
    # Player bars
    fig.add_trace(go.Bar(
        name='Player',
        x=['TS%', 'eFG%', '3PT%'], 
        y=[latest_stats.get('TS_pct', 0), latest_stats.get('eFG_pct', 0), latest_stats.get('three_point_efficiency', 0)],
        marker_color=['#00E676', '#00BFFF', '#FF4B4B'],
        text=[f"{latest_stats.get('TS_pct', 0):.1%}", f"{latest_stats.get('eFG_pct', 0):.1%}", f"{latest_stats.get('three_point_efficiency', 0):.1%}"],
        textposition='auto'
    ))
    
    # League bars
    if latest_league_stats is not None and not latest_league_stats.empty:
        fig.add_trace(go.Bar(
            name='League Avg',
            x=['TS%', 'eFG%', '3PT%'],
            y=[latest_league_stats.get('TS_pct', 0), latest_league_stats.get('eFG_pct', 0), latest_league_stats.get('three_point_efficiency', 0)],
            marker_color='gray',
            text=[f"{latest_league_stats.get('TS_pct', 0):.1%}", f"{latest_league_stats.get('eFG_pct', 0):.1%}", f"{latest_league_stats.get('three_point_efficiency', 0):.1%}"],
            textposition='auto'
        ))
        
    fig.update_layout(
        title="Current Efficiency Metrics", 
        template="plotly_dark", 
        yaxis=dict(tickformat=".0%", range=[0, 1]),
        barmode='group'
    )
    return fig
