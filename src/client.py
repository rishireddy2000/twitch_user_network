import streamlit as st
import requests
import socks
import socket
import networkx as nx
import plotly.graph_objects as go
from datetime import timedelta

# Configure SOCKS proxy globally
socks.set_default_proxy(socks.SOCKS5, "localhost", 9875)
socket.socket = socks.socksocket

def create_network_graph(connections, user_id):
    G = nx.Graph()
    G.add_node(user_id)
    for conn in connections:
        G.add_edge(user_id, conn['connected_to'])
    pos = nx.spring_layout(G, k=1, iterations=50)
    
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines')

    node_x = []
    node_y = []
    node_text = []
    node_colors = []
    
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_text.append(f'User {node}')
        node_colors.append('#FF0000' if node == user_id else '#6959CD')

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        hoverinfo='text',
        text=node_text,
        textposition="bottom center",
        marker=dict(
            color=node_colors,
            size=20,
            line_width=2))

    fig = go.Figure(data=[edge_trace, node_trace],
                   layout=go.Layout(
                       showlegend=False,
                       hovermode='closest',
                       margin=dict(b=20,l=5,r=5,t=40),
                       xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                       yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                       plot_bgcolor='rgba(0,0,0,0)',
                       paper_bgcolor='rgba(0,0,0,0)',
                       title=dict(text="Network Visualization", x=0.5)
                   ))
    return fig

@st.cache_data(ttl=timedelta(hours=1))
def get_connections(user_id):
    """Fetch connections from remote API with caching"""
    try:
        full_url = f"http://10.0.0.38:5555/api/connections/{user_id}"
        response = requests.get(full_url, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except requests.RequestException:
        return None

def display_grid(connections, cols=5):
    """Display users in a grid with navigation links"""
    for i in range(0, len(connections), cols):
        row = st.columns(cols)
        for col_idx, conn in enumerate(connections[i:i+cols]):
            with row[col_idx]:
                user_id = conn['connected_to']
                # Use markdown to create a link that updates query params
                st.markdown(f'''
                    <a href="?user_id={user_id}" target="_self" style="
                        text-decoration: none;
                        color: white;
                        display: block;
                        padding: 8px 16px;
                        background-color: rgba(255, 255, 255, 0.1);
                        border-radius: 4px;
                        text-align: center;
                        margin: 4px 0;">
                        🔍 User {user_id}
                    </a>
                    ''', 
                    unsafe_allow_html=True
                )

def main():
    st.set_page_config(layout="wide")
    
    st.title("Twitch Network Explorer")
    st.write("Explore user connections in the Twitch network")

    # Get user_id from query params or default
    query_params = st.query_params
    current_user = int(query_params.get("user_id", "7929"))

    # Input for user ID
    user_id = st.number_input("Enter User ID", min_value=0, value=current_user)
    
    if st.button("Find Connections"):
        # Update query params when search is clicked
        st.query_params["user_id"] = str(user_id)
        current_user = user_id

    # Display network for current user
    if current_user > 0:
        # Using cached function
        response = get_connections(current_user)
        
        if response and response['status'] == 'success':
            connections = response['connections']
            total_connections = response['total_connections']
            
            st.metric("Total Connections", total_connections)
            
            if total_connections <= 30:
                fig = create_network_graph(connections, current_user)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Network is too large to display (> 30 connections). Showing connected users only.")
            
            st.subheader("Connected Users")
            st.write("Click on any user to explore their connections")
            
            # Display grid with navigation links
            display_grid(connections)
        else:
            st.error("Error fetching data. Please try again.")

if __name__ == '__main__':
    main()