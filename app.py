################
# Landing Page #
################

import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import plotly.graph_objects as go
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder

st.set_page_config(layout="wide", initial_sidebar_state="expanded")

if "data_loaded" not in st.session_state:
    try:
        db_url = st.secrets["DB_URL"]
        engine = create_engine(db_url, pool_pre_ping=True)
        st.session_state["engine"] = engine

        inspector = inspect(engine)
        data_folder = "data"

        if not os.path.exists(data_folder):
            st.error(f"Data directory not found: {data_folder}")
        else:
            files_loaded = []
            for filename in os.listdir(data_folder):
                if filename.endswith(".csv"):
                    filepath = os.path.join(data_folder, filename)
                    table_name = filename.replace(".csv", "").lower()

                    if table_name in inspector.get_table_names():
                        st.info(f"⏩ Table `{table_name}` already exists. Skipping load.")
                        continue
                    
                    try:
                        df = pd.read_csv(filepath, encoding="latin1")

                        with engine.begin() as conn:
                            df.to_sql(table_name, conn, if_exists="replace", index=False)

                        st.success(f"✅ {table_name} loaded with {len(df)} rows.")
                        files_loaded.append(table_name)

                    except Exception as e:
                        st.error(f"Failed to load {filename}: {e}")

            st.session_state["data_loaded"] = True
            st.session_state["files_loaded"] = files_loaded

    except SQLAlchemyError as db_err:
        st.error(f"Database connection error: {db_err}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")

else:
    engine = st.session_state["engine"]

st.sidebar.title("Navigation")
section = st.sidebar.radio(
    "Go to",
    ["Home", "a", "b", "c", "d", "e", "Insights"],
    index=st.session_state.get("section_index", 0),
    key="section_radio"
)
st.session_state["section_index"] = ["Home", "a", "b", "c", "d", "e", "Insights"].index(section)

# data exploration
def show_home():
    st.title("Marketplace Decision Analytics")
    st.markdown("""
        This is a simple Streamlit app to load CSV files into a hosted PostgreSQL database in order to perform data analytics on an online marketplace.
        The Homepage contains a basic EDA (Exploratory Data Analysis) of the data, an essential step in any analytics project.
        The Insights page contains business recommendations.
        The remaining pages contain the suppoprting data analysis.
    """)

    st.subheader("EDA (Exploratory Data Analysis)")
    st.markdown("""
        Assumptions about the data model:
        - Customers include both buyers and sellers, as this is a marketplace. However, the focus of this analysis is on sellers.
        - The `listings` table contains data about sellers' product listings.
        - Given that the `clicks` tables contains rich data on behaviour on the listing form, where one row is one click made by seller, we can think of this table as a detailed log of what sellers did as they filled out or edited a listing.
        - `brands` is a dimension table that contains information about the brands associated with the listings.
        - `category` is a dimension table that contains information about the categories of the listings
                
        Conceptual flow of the data model:
        1.  clicks records behavior while listing is being created:
            - This can be thought of as "in-progress" behavior.
            - Multiple rows per seller per session.
            - Captures form interactions, e.g. selecting a brand, typing a price, navigating pages.
        2. Once a seller completes the listing:
            - It becomes a row in listings.
            - Has timestamps for DATE_CREATED, DATE_COMPLETED, and maybe DATE_SOLD.
    """)

    with st.expander("📂 Data Exploration", expanded=False):
        with st.container():
            for filename in os.listdir("data"):
                if filename.endswith(".csv"):
                    st.write(f"**{os.path.splitext(filename)[0]} table**")
                    df = pd.read_csv(os.path.join("data", filename), encoding='latin1')
                    
                    st.write("Column names:", df.columns.tolist())
                    date_cols = [col for col in df.columns if col.upper().startswith("DATE")]

                    for col in date_cols:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None).dt.floor('s')

                    if filename.lower() == "clicks.csv":
                        st.write("Distinct values for events-related columns in `clicks` table")
                        event_cols = [col for col in df.columns if col.upper().startswith("EVENT_")]

                        if event_cols:
                            for col in event_cols:
                                st.write(f"**{col}** - Distinct values:")
                                distinct_vals = pd.DataFrame(df[col].dropna().unique(), columns=[col])
                                st.dataframe(distinct_vals, hide_index=True)
                    
                    df_describe = df.describe(include='all')        
                    st.dataframe(df_describe, use_container_width=True)
                    st.write("Sample Data:")
                    st.dataframe(df.head(), use_container_width=True)


def show_section_a():
    st.write("#### Per customer, show number of total listings started, listings completed and listings sold; order by listings started descending")

    query_a = """
        SELECT
            "ID_CUSTOMER" AS customer
            ,COUNT(DISTINCT "ID") AS listings_started
            ,COUNT(DISTINCT "ID") FILTER (WHERE "DATE_COMPLETED" IS NOT NULL) AS listings_completed
            ,COUNT(DISTINCT "ID") FILTER (WHERE "DATE_SOLD" IS NOT NULL) AS listings_sold
            ,ROUND(
                100.0 * COUNT(DISTINCT "ID") FILTER (WHERE "DATE_COMPLETED" IS NOT NULL) 
                / COUNT(DISTINCT "ID"), 2
            ) AS listings_completed_pct
            ,ROUND(
                100.0 * COUNT(DISTINCT "ID") FILTER (WHERE "DATE_SOLD" IS NOT NULL) 
                / COUNT(DISTINCT "ID"), 2
            ) AS listings_sold_pct
        FROM listings
        GROUP BY "ID_CUSTOMER"
        ORDER BY 2 DESC
        ;
    """

    df_a = pd.read_sql(query_a, con=engine)
    st.dataframe(df_a)
    with st.expander("Click to view SQL query"):
        st.code(query_a, language='sql')

    # dataviz
    st.write("##### _Overall Listings Conversions Funnel_")
    st.write("Total listings started, completed, and sold across all customers:")
    st.dataframe(df_a[["listings_started", "listings_completed", "listings_sold"]].sum().to_frame().T, hide_index=True)

    totals = df_a[["listings_started", "listings_completed", "listings_sold"]].sum()

    funnel = go.Funnel(
        y=["Started", "Completed", "Sold"],
        x=totals.values,
        textinfo="value+percent previous"
    )

    fig = go.Figure(funnel)
    fig.update_layout() 

    st.plotly_chart(fig)

    st.write("##### _Listings Conversions Funnel for Selected Seller_")
    selected_customer = st.text_input("Enter customer ID:")

    if selected_customer:
        query = text("""
            SELECT
                "ID_CUSTOMER" AS customer
                ,COUNT(DISTINCT "ID") AS listings_started
                ,COUNT(DISTINCT "ID") FILTER (WHERE "DATE_COMPLETED" IS NOT NULL) AS listings_completed
                ,COUNT(DISTINCT "ID") FILTER (WHERE "DATE_SOLD" IS NOT NULL) AS listings_sold
                ,ROUND(
                    100.0 * COUNT(DISTINCT "ID") FILTER (WHERE "DATE_COMPLETED" IS NOT NULL) 
                    / COUNT(DISTINCT "ID"), 2
                ) AS listings_completed_pct
                ,ROUND(
                    100.0 * COUNT(DISTINCT "ID") FILTER (WHERE "DATE_SOLD" IS NOT NULL) 
                    / COUNT(DISTINCT "ID"), 2
                ) AS listings_sold_pct
            FROM listings
            WHERE "ID_CUSTOMER" = :customer_id
            GROUP BY "ID_CUSTOMER"
            ORDER BY 2 DESC
            ;
        """)

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"customer_id": selected_customer})

        st.dataframe(df)

        if df.empty:
            st.warning("No data available for the selected customer.")
        else:   
            seller = df.iloc[0]
            stages = ["Started", "Completed", "Sold"]
            values = [
                seller["listings_started"],
                seller["listings_completed"],
                seller["listings_sold"]
            ]

            funnel_trace = go.Funnel(
                y=stages,
                x=values,
                textinfo="value+percent previous"
            )

            fig = go.Figure(funnel_trace)
            fig.update_layout(title_text=f"Listings Conversions Funnel for Seller: {seller['customer']}")

            st.plotly_chart(fig)
    else:
        st.info("Please enter a customer ID to view the seller's listing(s) conversion funnel.")


def show_section_b():
    st.write("#### Per customer, if True or False they listed items in Clothing, Bags, Shoes and Accessories categories")

    query_b = """
        SELECT
            l."ID_CUSTOMER" AS customer
            ,CASE
                WHEN COUNT(DISTINCT l."ID_CATEGORY") FILTER (WHERE c."CATEGORY" = 'Clothing') > 0 THEN TRUE
                ELSE FALSE
            END AS listed_in_clothing
            ,CASE
                WHEN COUNT(DISTINCT l."ID_CATEGORY") FILTER (WHERE c."CATEGORY" = 'Bags') > 0 THEN TRUE
                ELSE FALSE
            END AS listed_in_bags
            ,CASE
                WHEN COUNT(DISTINCT l."ID_CATEGORY") FILTER (WHERE c."CATEGORY" = 'Shoes') > 0 THEN TRUE
                ELSE FALSE
            END AS listed_in_shoes
            ,CASE
                WHEN COUNT(DISTINCT l."ID_CATEGORY") FILTER (WHERE c."CATEGORY" = 'Accessories') > 0 THEN TRUE
                ELSE FALSE
            END AS listed_in_accessories
            ,CASE 
                WHEN COUNT(DISTINCT l."ID_CATEGORY") FILTER (WHERE c."CATEGORY" = 'Clothing') > 0 
                AND COUNT(DISTINCT l."ID_CATEGORY") FILTER (WHERE c."CATEGORY" = 'Bags') > 0
                AND COUNT(DISTINCT l."ID_CATEGORY") FILTER (WHERE c."CATEGORY" = 'Shoes') > 0
                AND COUNT(DISTINCT l."ID_CATEGORY") FILTER (WHERE c."CATEGORY" = 'Accessories') > 0
                THEN TRUE
                ELSE FALSE
            END AS listed_in_clothing_bags_shoes_accessories
        FROM listings l
        LEFT JOIN category c ON l."ID_CATEGORY" = c."ID_CATEGORY"
        WHERE c."CATEGORY" IN ('Clothing', 'Bags', 'Shoes', 'Accessories')
        GROUP BY l."ID_CUSTOMER"
        ;
    """

    df_b = pd.read_sql(query_b, con=engine)
    st.dataframe(df_b)
    with st.expander("Click to view SQL query"):
        st.code(query_b, language='sql')

    # dataviz
    category_counts = {
        'Clothing': df_b['listed_in_clothing'].sum(),
        'Bags': df_b['listed_in_bags'].sum(),
        'Shoes': df_b['listed_in_shoes'].sum(),
        'Accessories': df_b['listed_in_accessories'].sum(),
        'All Categories': df_b['listed_in_clothing_bags_shoes_accessories'].sum(),
    }

    categories = list(category_counts.keys())
    counts = list(category_counts.values())

    fig = go.Figure(data=[
        go.Bar(x=categories, y=counts, marker_color='indigo')
    ])

    fig.update_layout(
        title='Number of Customers Listed in Each Category',
        xaxis_title='Category',
        yaxis_title='Number of Customers',
        template='plotly_white'
    )

    st.plotly_chart(fig)


def show_section_c():
    st.write("#### Per seller: date of their first listing, date of their last listing, date of last sale, and name of last action (completed list / sale)")

    query_c = """
        WITH last_action_by_seller AS (
            SELECT
                "ID_USER"
                ,"DATETIME_SESSION" AS last_action_date
                ,"EVENT_ACTION" AS last_action_type
            FROM (
                SELECT
                    *
                    ,ROW_NUMBER() OVER (PARTITION BY "ID_USER" ORDER BY "DATETIME_SESSION" DESC) AS rn
                FROM clicks
            ) sub
            WHERE rn = 1
        )
        ,listing_level AS (
            SELECT
                l."ID" AS listing_id
                ,l."ID_CUSTOMER" AS seller
                ,l."DATE_CREATED"
                ,l."DATE_COMPLETED"
                ,l."DATE_SOLD"
                ,EXTRACT(EPOCH FROM (l."DATE_SOLD"::timestamp - l."DATE_COMPLETED"::timestamp)) / 60 AS minutes_to_sale
                ,CASE
                    WHEN l."DATE_COMPLETED" IS NOT NULL THEN 'LIsting Completed'   
                    ELSE 'Listing Not Completed'
                END AS listing_status
                ,CASE 
                    WHEN l."DATE_SOLD" IS NOT NULL AND l."DATE_COMPLETED" IS NOT NULL THEN 'Sale'
                    ELSE 'No Sale'
                END AS sale_status
            FROM listings l
        )
        ,seller_level AS (
            SELECT
                seller
                ,MIN("DATE_CREATED")::timestamp AS first_listing_ts
                ,MAX("DATE_CREATED")::timestamp AS last_listing_ts
                ,MAX("DATE_SOLD")::timestamp AS last_sale_ts
                ,AVG(minutes_to_sale) AS avg_minutes_to_sale
                ,COUNT(*) FILTER (WHERE listing_status = 'LIsting Completed') AS listings_completed
                ,COUNT(*) FILTER (WHERE sale_status = 'Sale') AS listings_sold
            FROM listing_level
            GROUP BY seller
        )
        SELECT
            s.*
            ,COALESCE(la.last_action_type, 'no action recorded') AS last_action
        FROM seller_level s
        LEFT JOIN last_action_by_seller la ON s.seller = la."ID_USER"
        ;
    """

    df_c = pd.read_sql(query_c, con=engine)
    st.dataframe(df_c)
    with st.expander("Click to view SQL query"):
        st.code(query_c, language='sql')


    listing_id_not_in_clicks = """"
    SELECT DISTINCT l."ID_CUSTOMER"
    FROM listings l
    LEFT JOIN clicks c ON l."ID_CUSTOMER" = c."ID_USER"
    WHERE c."ID_USER" IS NULL
    ;
    """

    clicks_id_not_in_listings = """
    SELECT DISTINCT c."ID_USER"
    FROM clicks c
    LEFT JOIN listings l ON c."ID_USER" = l."ID_CUSTOMER"
    WHERE l."ID_CUSTOMER" IS NULL
    ;
    """

    st.markdown("""
                Peculiarities in the data:
                - There are 71 sellers in the `listings` table who are not present in the `clicks` table, meaning their interactions with the listing form were not accurately/successfully tracked. Possible explanations could include: automated listing creation, &/or a bug in the tracking.
                - There are 942 users in the `clicks` table who are not in the `listings` table, indicating they have interacted with the platform but have not finished creating any listings.
                - There are some users whose last action occured before their first listing, which may indicate incomplete/abandoned listing sessions &/or a tracking bug.
                """)

    #dataiuz
    df_c['first_listing_ts'] = pd.to_datetime(df_c['first_listing_ts'])
    df_c['last_listing_ts'] = pd.to_datetime(df_c['last_listing_ts'])
    df_c['last_sale_ts'] = pd.to_datetime(df_c['last_sale_ts'])
    df_c['active_minutes'] = (df_c['last_listing_ts'] - df_c['first_listing_ts']).dt.total_seconds()/60

    df_c['has_sold'] = df_c['last_sale_ts'].notna()
    df_c['avg_days_to_sale'] = round(pd.to_numeric(df_c['avg_minutes_to_sale']).astype(float)/60/24,1)

    df_c.rename(columns={
        'first_listing_ts': 'first_listing_date',
        'last_listing_ts': 'last_listing_date',
        'last_sale_ts': 'last_sale_date'
    }, inplace=True)
    
    # sales activity lifecycle
    fig1 = px.histogram(
        df_c,
        x='active_minutes',
        nbins=50,
        title='Distribution of Seller Active Minutes (Time Between First and Last Listing)',
        labels={'active_minutes': 'Active Minutes'},
        color='has_sold',
        color_discrete_map={True: 'blue', False: 'red'},
        category_orders={"has_sold": [True, False]}
    )
    fig1.update_layout(bargap=0.1)
    st.plotly_chart(fig1)

    sales_counts = df_c['has_sold'].value_counts(normalize=True).rename({True: 'Sold At Least Once', False: 'No Sales'}) * 100

    # percentage of sellers by sale status
    fig_sales = go.Figure(data=[
        go.Bar(
            x=sales_counts.index,
            y=sales_counts.values,
            marker_color=['red', 'blue'],
            text=[f'{v:.1f}%' for v in sales_counts.values],
            textposition='auto'
        )
    ])

    fig_sales.update_layout(
        title='Percentage of Sellers by Sale Status',
        xaxis_title='Sale Status',
        yaxis_title='Percentage of Sellers',
        yaxis_ticksuffix='%',
        yaxis_range=[0, 100],
    )

    st.plotly_chart(fig_sales)

    # avg days to sale 
    fig3 = px.histogram(
        df_c,
        x='avg_days_to_sale',
        nbins=40,
        title='Distribution of Average Days to Sale (Per Seller)',
        labels={'avg_days_to_sale': 'Average Days to Sale'},
        color='has_sold',
        color_discrete_map={True: 'blue', False: 'red'},
    )
    fig3.update_layout(bargap=0.1)
    st.plotly_chart(fig3)

    # last action type 
    sale_filter = st.radio(
        "Filter by Sale Status:",
        options=["All", "Sold At Least Once", "No Sales"]
    )

    if sale_filter == "Sold At Least Once":
        df_filtered = df_c[df_c['has_sold'] == True]
    elif sale_filter == "No Sales":
        df_filtered = df_c[df_c['has_sold'] == False]
    else:
        df_filtered = df_c

    action_counts = df_filtered['last_action'].value_counts()

    fig4 = px.bar(
        x=action_counts.index,
        y=action_counts.values,
        title=f'Count of Sellers by Last Action Type ({sale_filter})',
        labels={'x': 'Last Action Type', 'y': 'Number of Sellers'},
        color=action_counts.values,
        color_continuous_scale='Viridis'
    )

    st.plotly_chart(fig4)


def show_section_d():
    st.write("#### Number of Clicks made on listing form for submitted products")

    query_d = """
        WITH completed_listings AS (
            SELECT "ID" AS listing_id
            FROM listings
            WHERE "DATE_COMPLETED" IS NOT NULL
        ),
        clicks_per_listing AS (
            SELECT
                c."ID_PRODUCT" AS product_id
                ,COUNT(*) AS num_clicks
            FROM clicks c
            JOIN completed_listings cl ON c."ID_PRODUCT" = cl.listing_id
            GROUP BY c."ID_PRODUCT"
        )
        SELECT
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY num_clicks) AS median_clicks
            ,AVG(num_clicks) AS avg_clicks
        FROM clicks_per_listing
        ;
    """

    df_d = pd.read_sql(query_d, con=engine)
    st.metric(label="Median # of Clicks", value=df_d['median_clicks'].astype(int), delta=None, help="Median number of clicks made on listing form for submitted products")
    st.metric(label="Mean-average # of Clicks", value=df_d['avg_clicks'].astype(int), delta=None, help="Mean-average number of clicks made on listing form for submitted products")
    with st.expander("Click to view SQL query"):
        st.code(query_d, language='sql')
    

def show_section_e():
    st.write("#### Per product that was not submitted : last seller's action on listing form")

    query_e = """
        WITH unsubmitted_products AS (
            SELECT DISTINCT c."ID_PRODUCT"
            FROM clicks c
            LEFT JOIN listings l ON c."ID_PRODUCT" = l."ID"
            WHERE l."DATE_COMPLETED" IS NULL OR l."ID" IS NULL
        ),
        last_actions AS (
            SELECT
                c."ID_PRODUCT"
                ,c."ID_USER"
                ,c."DATETIME_SESSION"
                ,c."EVENT_ACTION"
                ,ROW_NUMBER() OVER (PARTITION BY c."ID_PRODUCT" ORDER BY c."DATETIME_SESSION" DESC) AS rn
            FROM clicks c
            JOIN unsubmitted_products u ON c."ID_PRODUCT" = u."ID_PRODUCT"
        )
        SELECT
            "ID_PRODUCT"
            ,"ID_USER"
            ,"EVENT_ACTION" AS last_action
        FROM last_actions
        WHERE rn = 1
        ;
    """

    df_e = pd.read_sql(query_e, con=engine)
    st.dataframe(df_e, hide_index=True)
    with st.expander("Click to view SQL query"):
        st.code(query_e, language='sql')

    # dataviz
    action_counts = df_e['last_action'].value_counts().sort_values(ascending=False)
    action_percent = (action_counts / action_counts.sum() * 100).round(1)

    abandon_df = pd.DataFrame({
        'Last Action': action_counts.index,
        'Abandoned Listings': action_counts.values,
        'Percentage of Total': action_percent.values
    })

    abandon_df['Values'] = abandon_df.apply(
        lambda row: f"{row['Abandoned Listings']} ({row['Percentage of Total']}% of total)", axis=1
    )

    fig = px.bar(
        abandon_df,
        x='Last Action',
        y='Abandoned Listings',
        text='Values',
        title="Where Users Abandon the Listing Flow",
        labels='Number of Abandoned Listings',
        color='Abandoned Listings',
        color_continuous_scale='OrRd'
    )

    fig.update_traces(textposition='outside')
    fig.update_layout(xaxis_tickangle=45, uniformtext_minsize=8, uniformtext_mode='hide')

    st.plotly_chart(fig)


def show_insights():
    st.header("Key Insights & Recommendations")
    # st.markdown("""
    #             1. **Listing Completion Rate**: 
    #                 - The percentage of listings that are completed after being started is 85%. This 15% drop-off during the listing process suggests that sellers may be facing challenges or losing interest when creating a listing. 
    #                 - The median number of clicks on the listing form for completed listings is 61, indicating that sellers are engaging with the form but may be encountering obstacles that prevent them from completing their listings.
    #                 - To improve this, consider simplifying the listing form, providing clearer instructions, or offering support to sellers who abandon their listings. 
    #                 - Additionally, the marketplace could implement a follow-up mechanism.
    #                 - Of the completed listings, 17% result in a sale. This point in the seller funnel could be optimized to increase conversion rates.
    #             2. **Product Categories**: 
    #                 - The most popular category of listings is Clothing. Bags and Shoes are equally popular, while Accessories have the lowest number of listings. This suggests that the marketplace could benefit from targeted marketing efforts in the Accessories category to boost listings.
    #             3. **Sales Activity**: 
    #                 - ~30% of sellers have made at least one sale on their listings. This indicates that while many sellers are active, a significant portion is not converting their listings into sales.
    #             4. **Seller Engagement**:
    #                 - Half of the users who abandoned the listing flow did so on the "tap back button" action, indicating that sellers are confused or hesitant when creating the listing.
    #                 - The second most common (~25% of total abandonned listings) abandonment point was on the "delete draft" action, suggesting that sellers could have been frustrated with the listing process or changed their mind and chose to delete their drafts rather than complete them.
    #             """)
    data = {
        "KPI": [
            "Listing Completion Rate (85%)",
            "Category Popularity (# of listings)",
            "Sales Conversion (17%)",
        ],
        "Issue": [
            "15% of listings are abandoned during listing creation",
            "Accessories listings significantly lag behind Clothing, Bags, and Shoes",
            "Low conversion from completed listings to sales",
        ],
        "Supporting Evidence": [
            "• It takes over 60 clicks to complete a listing. \n • 50% abandon via back button \n • 25% delete draft listing",
            "• 547 customers created a Clothing listing \n • Bags and Shoes are tied in second place at about 300 listings each \n • Accessories have only 168 listings",
            "• About 1 in 3 sellers make a sale after completing a listing \n • Sellers' last action is pressing the back button regardless of whether a sale was made or not",
        ],
        "Business Impact": [
            "Lost potential listings and sales",
            "Missed growth opportunity in Accessories category",
            "Reduced marketplace revenue",
        ],
        "Further Analysis Next Steps": [
            "Run A/B tests on listing form changes to measure improvement on completion rates",
            "Analyze listing completion rates by category to see if Accessories listings are more likely to be abandoned",
            "• Understand why sellers press 'back' so often. \n • Analze sales by category to understand which products are more likely to sell",
        ],
        "Recommendation Based on Further Analysis Next Steps": [
            "Simplify listing form, add clearer instructions, follow-up support for abandoners",
            "Target marketing and incentives to grow Accessories listings",
            "Optimize listing form for sales conversion, analyze seller behavior to identify friction points",
        ]
    }

    df = pd.DataFrame(data)
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        wrapText=True,
        autoHeight=True,
        resizable=True
    )
    gb.configure_column(
        "KPI",
        cellClass="bold-cell"
    )
    gb.configure_grid_options(domLayout='autoHeight')
    gridOptions = gb.build()

    custom_css = {
        ".ag-header-cell-label": {
            "white-space": "normal",
            "height": "auto",
            "line-height": "1.2",
            "font-size": "11px"
        },
        ".ag-header-row": {
            "height": "72px"
        },
        ".ag-cell": {
            "white-space": "pre-wrap",
            "line-height": "1.4",
            "padding-top": "8px",
            "padding-bottom": "8px",
            "font-size": "14px"
        },
        ".bold-cell": {
            "font-weight": "700"
        }
    }

    AgGrid(
        df,
        gridOptions=gridOptions,
        height=350,
        fit_columns_on_grid_load=True,
        use_container_width=True,
        custom_css=custom_css
    )


page_map = {
    "Home": show_home,
    "a": show_section_a,
    "b": show_section_b,
    "c": show_section_c,
    "d": show_section_d,
    "e": show_section_e,
    "Insights": show_insights
}

page_map[section]() 