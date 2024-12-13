import streamlit as st
import requests
import socks
import socket

# Configure SOCKS proxy globally
socks.set_default_proxy(socks.SOCKS5, "localhost", 9875)
socket.socket = socks.socksocket

def get_connections(user_id):
    """Fetch connections from remote API"""
    try:
        full_url = f"http://10.0.0.38:5555/api/connections/{user_id}"
        response = requests.get(full_url, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API returned status code {response.status_code}")
            st.write(f"Response content: {response.text}")
            return None
            
    except requests.RequestException as e:
        st.error(f"Error connecting to API: {str(e)}")
        return None

def main():
    st.title("Twitch Network Explorer")
    st.write("Explore user connections in the Twitch network")

    user_id = st.number_input("Enter User ID", min_value=0, value=25949)
    
    if st.button("Find Connections"):
        if user_id > 0:
            with st.spinner("Fetching connections..."):
                response = get_connections(user_id)
                
                if response and response['status'] == 'success':
                    st.metric("Total Connections", response['total_connections'])
                    
                    st.subheader("Connected Users")
                    st.write("User IDs that are connected to user " + str(user_id) + ":")
                    st.write(response['connections'])
                else:
                    st.error("Error fetching data from API")
        else:
            st.warning("Please enter a valid user ID")

if __name__ == '__main__':
    main()