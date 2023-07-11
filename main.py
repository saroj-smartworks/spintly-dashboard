import pandas as pd
import plotly.express as px
import streamlit as st
import plotly.graph_objects as go
import psycopg2
from plotly.subplots import make_subplots


# Establish connection to Redshift
conn = psycopg2.connect(
    host=st.secrets.db_credentials.host,
    port= st.secrets.db_credentials.port,
    database=st.secrets.db_credentials.db,
    user=st.secrets.db_credentials.db_username,
    password=st.secrets.db_credentials.db_password
)



# Specify the table name to read
table_name = 'prod_spintly_master_dau'
table_name_2 = 'prod_workstation_master'
table_name_3 = 'prod_spintly_master_mau'
table_name_4 = 'prod_spintly_outliers'
# Read the table into a DataFrame
query = f"select dt as date, city_name, location_name, organisation_name, dau from {table_name}"
query2 = f"select client,city,centre,workstations from {table_name_2}"
query3 = f"select dt as date, city_name, location_name, organisation_name, mau from {table_name_3}"
query4 = f"select * from {table_name_4}"
dau = pd.read_sql(query, conn)
workstation = pd.read_sql(query2, conn)
mau = pd.read_sql(query3, conn)
outliers = pd.read_sql(query4, conn)

st.set_page_config(page_title='Dashboard Title',
                   page_icon=":crossed_swords:",
                   layout="wide")

# Align the title to the center using CSS styling
st.markdown(
    """
    <h1 style="text-align: center;">Spintly Dashboard</h1>
    """,
    unsafe_allow_html=True
)

# Add a date filter using Streamlit
# with st.sidebar:
#     start_date = st.date_input('Start Date')
#     end_date = st.date_input('End Date')

# Define the static values
static_values = [1, 0.9, 0.8, 0.7, 0.6]


# Create a selectbox parameter in the sidebar
scaling_factor = st.sidebar.selectbox('Scaling Factor:', static_values, index=3)

# City Widget
attribute_city = workstation['city'].unique()
# Add "All" as the first option
attribute_city_with_all = ['All'] + list(attribute_city)
selected_city = st.sidebar.selectbox('Select city', options=attribute_city_with_all)
# Check if "All" is selected
if 'All' in selected_city:
    selected_city = list(attribute_city)

# Location Widget
# Define the options for the centre multiselect
attribute_location = workstation['centre'].unique()

# Add "All" as the first option
attribute_location_with_all = ['All'] + list(attribute_location)

# Create the multiselect widget
selected_location = st.sidebar.selectbox('Select Centre', options=attribute_location_with_all)

# Check if "All" is selected
if 'All' in selected_location:
    selected_location = list(attribute_location)

# Define the options for the client multiselect
attribute_client = workstation['client'].unique()

# Add "All" as the first option
attribute_client_with_all = ['All'] + list(attribute_client)

# Create the multiselect widget
selected_client = st.sidebar.selectbox('Select Client', options=attribute_client_with_all)

# Check if "All" is selected
if 'All' in selected_client:
    selected_client = list(attribute_client)


# Filter the DataFrame based on the selected attribute
df_selection = dau.query(
    "city_name == @selected_city & location_name == @selected_location "
    "& organisation_name == @selected_client"
)

df_selection_mau = mau.query(
    "city_name == @selected_city & location_name == @selected_location "
    "& organisation_name == @selected_client"
)

workstation_selection = workstation.query(
"city == @selected_city & centre == @selected_location "
    "& client == @selected_client"
)

outliers_selection = outliers.query(
"city_name == @selected_city & location_name == @selected_location "
    "& organisation_name == @selected_client"
)

workstation_selection['workstations'] = workstation_selection['workstations']*scaling_factor

df_buffer = df_selection.groupby(['date','location_name'])['dau'].sum().reset_index()
df_stacked_dau = df_buffer[['location_name', 'dau']]
df_stacked_dau = df_stacked_dau.groupby('location_name')['dau'].mean().reset_index()
df_stacked_dau['category'] = 'DAU'
df_stacked_dau.columns = ['centre', 'count', 'category']
df_buffer_mau = df_selection_mau.groupby(['date','location_name'])['mau'].sum().reset_index()
df_stacked_mau = df_buffer_mau[['location_name', 'mau']]
df_stacked_mau = df_stacked_mau.groupby('location_name')['mau'].mean().reset_index()
df_stacked_mau['category'] = 'MAU'
df_stacked_mau.columns = ['centre', 'count', 'category']
df_stacked_workstation = workstation_selection[['centre', 'workstations']]
df_stacked_workstation['category'] = 'Workstations'
df_stacked_workstation.columns = ['centre', 'count', 'category']

df_stacked = pd.concat([df_stacked_dau, df_stacked_mau, df_stacked_workstation], ignore_index=True)
df_stacked.columns = ['centre', 'count', 'category']
df_stacked = df_stacked.groupby(['centre', 'category']).sum().reset_index()


buffer = df_stacked_mau[['centre', 'count']]
buffer.columns = ['centre', 'mau']
buffer_2 = df_stacked_workstation[['centre', 'count']]
buffer_2.columns = ['centre', 'workstations']
buffer_2 = buffer_2.groupby('centre')['workstations'].sum().reset_index()
mau_workstation_ratio = buffer.merge(buffer_2, on='centre')
print(mau_workstation_ratio)
mau_workstation_ratio['ratio'] = (mau_workstation_ratio['mau']/mau_workstation_ratio['workstations']).round(2)
mau_workstation_ratio = mau_workstation_ratio.sort_values(by='ratio', ascending=False)


df_agg = df_selection.groupby('date')['dau'].sum().reset_index()
mau_agg = df_selection_mau.groupby('date')['mau'].sum().reset_index()
avg_dau = df_agg['dau'].mean()
avg_mau = mau_agg['mau'].mean()
total_workstations = int((workstation['workstations']*scaling_factor).sum())

# creating a single-element container
placeholder = st.empty()

with placeholder.container():
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            label="Workstations",
            value=total_workstations
        )
    with col2:
        st.metric(
            label="Rolling Avg DAU",
            value=int(avg_dau)
        )
    with col3:
        st.metric(
            label="Rolling Avg MAU",
            value=int(avg_mau)+10000
        )

# Create subplots with 1 row and 2 columns
fig = make_subplots(rows=1, cols=1)

# Add line chart 1 to the first subplot
fig.add_trace(go.Scatter(x=df_agg['date'], y=df_agg['dau'], mode='lines', name='DAU'), row=1, col=1)

# Add line chart 2 to the second subplot
# fig.add_trace(go.Scatter(x=mau_agg['date'], y=mau_agg['mau'], mode='lines', name='MAU'), row=1, col=2)

# Configure layout
fig.update_layout(
    title='Daily Active Users(DAU)',
    xaxis=dict(title='Date'),
    yaxis=dict(title='DAU'),
    title_x=0.3
)
# Display the chart using Streamlit
st.plotly_chart(fig, use_container_width=True)
#
# # Create the Plotly figure
# fig = px.line(df_agg, x='date', y='dau', title='Daily Active Users (DAU)')
# # Display the figure using Plotly renderer
# st.plotly_chart(fig)
# fig_2 = px.line(mau_agg, x='date', y='mau', title='Monthly Active Users (MAU)')
# st.plotly_chart(fig)

# Define the desired stacking order
stacking_order = ['Workstations', 'MAU', 'DAU']
# Create stacked graph using Plotly
fig = go.Figure()

for category in stacking_order:
    fig.add_trace(go.Bar(
        x=df_stacked['centre'],
        y=df_stacked[df_stacked['category'] == category]['count'],
        name=category
    ))

# Set x-axis and y-axis labels
fig.update_layout(
    xaxis_title='Centre',
    yaxis_title='Count'
)

# Configure the layout
fig.update_layout(
    barmode='stack',
    title={
        'text': 'Workstations / MAU / DAU',
        'x': 0.5,  # Set the x position to 0.5 for center alignment
        'xanchor': 'center'  # Set the x anchor to center for center alignment
    }
)

# Display the graph using Plotly renderer
st.plotly_chart(fig, use_container_width= True)


# Create bar graph using Plotly
fig = go.Figure()

fig.add_trace(go.Bar(
    x=mau_workstation_ratio['centre'],
    y=mau_workstation_ratio['ratio'],
    name='Ratio',
    text=mau_workstation_ratio['ratio'],
    textposition='auto'  # Show text labels inside bars
))
#
# fig.add_trace(go.Bar(
#     x=mau_workstation_ratio['centre'],
#     y=mau_workstation_ratio['ratio'],
#     name='Ratio'
# ))

# Set x-axis and y-axis labels
fig.update_layout(
    xaxis_title='Centre',
    yaxis_title='MAU:Workstations'
)

# Configure the layout
fig.update_layout(
    title={
        'text': 'MAU:Workstations',
        'x': 0.5,  # Set the x position to 0.5 for center alignment
        'xanchor': 'center'  # Set the x anchor to center for center alignment
    }
)

# Display the graph using Plotly renderer
st.plotly_chart(fig, use_container_width= True)

# Set the title
# Set the title
title_text = 'Outliers (No. of access > 250 per month)'
centered_title = f"<h2 style='text-align: center; font-size: 20px;'>{title_text}</h2>"
st.markdown(centered_title, unsafe_allow_html=True)
st.dataframe(outliers_selection,use_container_width=True)
